use std::{any::Any, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use mq_bridge::{
    errors::{ConsumerError as BridgeConsumerError, InvalidConfig},
    support::stream_batch,
    traits::{BatchCommitFunc, BoxFuture, MessageConsumer, MessageDisposition},
    CanonicalMessage, ReceivedBatch,
};
use pulsar::{
    consumer::{Consumer, ConsumerOptions, InitialPosition as PulsarInitialPosition},
    error::Error as PulsarError,
    SubType, TokioExecutor,
};
use tokio::sync::Mutex;

use crate::{
    config::{self, InitialPosition},
    connect,
};

type SharedConsumer = Arc<Mutex<Consumer<Vec<u8>, TokioExecutor>>>;

struct PulsarConsumer {
    inner: SharedConsumer,
    exit_on_empty: bool,
    /// A stream error that ended the last batch early, reported on the next receive.
    pending_error: Option<PulsarError>,
}

fn from_pulsar_message(
    payload: Vec<u8>,
    properties: impl IntoIterator<Item = (String, String)>,
) -> CanonicalMessage {
    let mut message = CanonicalMessage::from(payload);
    message.metadata.extend(properties);
    message
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let (config, topic, subscription) =
        config::resolve(route_name, value).map_err(InvalidConfig)?;
    let client = connect(&config.url).await?;
    let consumer = client
        .consumer()
        .with_topic(topic)
        .with_consumer_name(format!("mq-bridge-{route_name}"))
        .with_subscription_type(SubType::Shared)
        .with_subscription(subscription)
        .with_options(
            ConsumerOptions::default()
                .with_initial_position(initial_position(config.initial_position)),
        )
        .build::<Vec<u8>>()
        .await
        .context("failed to create Pulsar consumer")?;
    Ok(Box::new(PulsarConsumer {
        inner: Arc::new(Mutex::new(consumer)),
        exit_on_empty: false,
        pending_error: None,
    }))
}

#[async_trait]
impl MessageConsumer for PulsarConsumer {
    fn commit_requires_order(&self) -> bool {
        false
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    /// Closes the broker-side consumer. The trait's `close()` awaits this hook,
    /// so both route shutdown and an explicit `close()` release the subscription.
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            self.inner
                .lock()
                .await
                .close()
                .await
                .context("failed to close Pulsar consumer")
        }))
    }

    async fn receive_batch(
        &mut self,
        max_messages: usize,
    ) -> Result<ReceivedBatch, BridgeConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }
        if let Some(error) = self.pending_error.take() {
            return Err(consumer_error(error));
        }

        let mut consumer = self.inner.lock().await;
        let received = match stream_batch::next_batch(
            &mut *consumer,
            max_messages,
            self.exit_on_empty,
        )
        .await
        {
            Ok(Some(received)) => received,
            Ok(None) => return Err(BridgeConsumerError::EndOfStream),
            Err(partial) if partial.items.is_empty() => return Err(consumer_error(partial.error)),
            Err(partial) => {
                self.pending_error = Some(partial.error);
                partial.items
            }
        };
        drop(consumer);
        if received.is_empty() {
            return Ok(ReceivedBatch::empty());
        }

        let mut messages = Vec::with_capacity(received.len());
        let mut acknowledgements = Vec::with_capacity(received.len());
        for message in received {
            acknowledgements.push((message.topic.clone(), message.message_id().clone()));
            let properties = message
                .metadata()
                .properties
                .iter()
                .map(|property| (property.key.clone(), property.value.clone()))
                .collect::<Vec<_>>();
            messages.push(from_pulsar_message(message.payload.data, properties));
        }

        let shared = Arc::clone(&self.inner);
        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                let mut consumer = shared.lock().await;
                for ((topic, id), disposition) in acknowledgements.into_iter().zip(dispositions) {
                    let result = match disposition {
                        MessageDisposition::Nack => consumer.nack_with_id(&topic, id).await,
                        _ => consumer.ack_with_id(&topic, id).await,
                    };
                    result.context("failed to commit Pulsar message disposition")?;
                }
                Ok(())
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Only honoured when Pulsar creates the subscription. An existing
/// subscription resumes from its own cursor whatever this says.
fn initial_position(position: InitialPosition) -> PulsarInitialPosition {
    match position {
        InitialPosition::Latest => PulsarInitialPosition::Latest,
        InitialPosition::Earliest => PulsarInitialPosition::Earliest,
    }
}

fn consumer_error(error: PulsarError) -> BridgeConsumerError {
    BridgeConsumerError::Connection(anyhow::Error::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earliest_maps_to_pulsars_backlog_reading_position() {
        assert_eq!(
            initial_position(InitialPosition::Earliest),
            PulsarInitialPosition::Earliest
        );
        assert_eq!(
            initial_position(InitialPosition::Latest),
            PulsarInitialPosition::Latest
        );
    }

    #[test]
    fn pulsar_properties_become_canonical_metadata() {
        let message = from_pulsar_message(
            b"payload".to_vec(),
            [("source".to_owned(), "test".to_owned())],
        );

        assert_eq!(message.get_payload_str(), "payload");
        assert_eq!(
            message.metadata.get("source").map(String::as_str),
            Some("test")
        );
    }
}
