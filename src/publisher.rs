use std::any::Any;

use async_trait::async_trait;
use mq_bridge::{errors::PublisherError, traits::MessagePublisher, CanonicalMessage, SentBatch};
use rclrs::{
    DynamicMessageMetadata, DynamicPublisher, MessageTypeName, PublisherOptions, RclrsError,
};

use crate::{
    config,
    message,
    runtime::{self, Ros2Runtime},
};

struct Ros2Publisher {
    publisher: DynamicPublisher,
    /// When enabled, a publisher waits until discovery has matched a compatible
    /// reader before allowing the route to commit the send.
    require_reader: bool,
    /// Also the factory for the messages being sent: a dynamic message can only
    /// be built from the metadata of its own type.
    metadata: DynamicMessageMetadata,
    payload_field: String,
    /// Declared after the publisher so the executor stops before the publisher
    /// it belongs to is torn down.
    runtime: Ros2Runtime,
}

pub(crate) async fn create(
    route_name: &str,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let endpoint = config::resolve_for_publisher(route_name, value)?;

    let message_type = MessageTypeName::try_from(endpoint.config.message_type.as_str())
        .map_err(|error| non_retryable(anyhow::Error::new(error)))?;
    let metadata = DynamicMessageMetadata::new(message_type.clone())
        .map_err(|error| non_retryable(anyhow::Error::new(error)))?;
    message::carrier_for(metadata.structure(), &endpoint.config.payload_field)
        .map_err(non_retryable)?;

    let qos = endpoint.config.qos.profile();
    let require_reader = endpoint.config.qos.require_subscribers;
    let (runtime, publisher) = Ros2Runtime::start(&endpoint, |node| {
        let mut options = PublisherOptions::new(&endpoint.topic);
        options.qos = qos;
        node.create_dynamic_publisher(message_type.clone(), options)
    })
    .map_err(|error| {
        setup_error(error).context(format!(
            "failed to publish on ROS 2 topic {}",
            endpoint.topic
        ))
    })?;

    Ok(Box::new(Ros2Publisher {
        publisher,
        require_reader,
        metadata,
        payload_field: endpoint.config.payload_field.clone(),
        runtime,
    }))
}

impl Ros2Publisher {
    /// Publishing is synchronous and produces no receipt, so this is the whole
    /// of sending one message. A successful `publish` still does not confirm
    /// that a reader processed the message; when `require_subscribers` is set,
    /// the count check prevents acceptance until a compatible reader has
    /// matched. Note that no `.await` may appear here: a
    /// `DynamicMessage` owns raw type-support memory and is not `Send`.
    fn publish(&self, message: &CanonicalMessage) -> Result<(), PublisherError> {
        let mut outgoing = self
            .metadata
            .create()
            .map_err(|error| PublisherError::NonRetryable(anyhow::Error::new(error)))?;
        // A payload that is not valid UTF-8 for a string field, or does not fit
        // a bounded one, will not become valid by being sent again.
        message::set_payload(&mut outgoing, &self.payload_field, &message.payload)
            .map_err(PublisherError::NonRetryable)?;

        if self.require_reader {
            let readers = self
                .publisher
                .get_subscription_count()
                .map_err(publisher_error)?;
            if readers == 0 {
                return Err(PublisherError::Retryable(anyhow::anyhow!(
                    "no compatible ROS 2 reader has matched the publisher yet"
                )));
            }
        }

        self.publisher.publish(outgoing).map_err(publisher_error)
    }
}

#[async_trait]
impl MessagePublisher for Ros2Publisher {
    /// Metadata does not survive this direction. A ROS 2 message carries only
    /// the fields its type declares, with no property map to put a canonical
    /// message's metadata in, so it is dropped rather than smuggled somewhere a
    /// consumer would not think to look.
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        // Every message is attempted: one the middleware rejects must not hide
        // the fate of the rest, and the failures are handed back attached to the
        // messages the route has to retry.
        let failed: Vec<(CanonicalMessage, PublisherError)> = messages
            .into_iter()
            .filter_map(|message| match self.publish(&message) {
                Ok(()) => None,
                Err(error) => Some((message, error)),
            })
            .collect();

        if failed.is_empty() {
            Ok(SentBatch::Ack)
        } else {
            Ok(SentBatch::Partial {
                responses: None,
                failed,
            })
        }
    }

    /// Nothing to do: `publish` hands the sample straight to the middleware, and
    /// there is no producer-side batch to force out. Getting the sample to a
    /// reader is the `reliability` policy's job, not a flush's. The route may
    /// commit after this publisher returns, but that commit only records that
    /// the route accepted the send attempt; ROS output remains fire-and-forget.
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Drop for Ros2Publisher {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}

fn non_retryable(error: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(PublisherError::NonRetryable(error))
}

/// Leaving a transient failure unclassified is deliberate: the route then treats
/// it as a connection failure and retries on its reconnect interval.
fn setup_error(error: RclrsError) -> anyhow::Error {
    if runtime::is_permanent(&error) {
        non_retryable(anyhow::Error::new(error))
    } else {
        anyhow::Error::new(error)
    }
}

fn publisher_error(error: RclrsError) -> PublisherError {
    if runtime::is_permanent(&error) {
        PublisherError::NonRetryable(anyhow::Error::new(error))
    } else {
        PublisherError::Retryable(anyhow::Error::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rclrs::{DynamicMessageError, RclReturnCode};

    #[test]
    fn publisher_errors_are_classified_for_retry() {
        // A publisher that timed out may well succeed next time.
        assert!(matches!(
            publisher_error(RclrsError::RclError {
                code: RclReturnCode::Timeout,
                msg: None
            }),
            PublisherError::Retryable(_)
        ));
        // A message type that is not installed will never install itself.
        assert!(matches!(
            publisher_error(RclrsError::DynamicMessageError {
                err: DynamicMessageError::InvalidMessageType
            }),
            PublisherError::NonRetryable(_)
        ));
        // Neither will a topic name `rcl` refuses.
        assert!(matches!(
            publisher_error(RclrsError::RclError {
                code: RclReturnCode::TopicNameInvalid,
                msg: None
            }),
            PublisherError::NonRetryable(_)
        ));
    }

}
