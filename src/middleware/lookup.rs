//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Enriches each message with the responses of other, request-capable endpoints.

use crate::endpoints::create_publisher_from_route;
use crate::models::{Endpoint, LookupMiddleware};
use crate::support::interpolation::CompiledTemplate;
use crate::traits::{
    BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

const FOUND: &str = "lookup.found";
const HTTP_STATUS_CODE: &str = "http_status_code";

struct Entry {
    from: Arc<dyn MessagePublisher>,
    metadata: Vec<(String, CompiledTemplate)>,
    payload: Option<CompiledTemplate>,
    into: Vec<String>,
    found_key: String,
}

impl Entry {
    async fn new(
        from: &Endpoint,
        metadata: &HashMap<String, String>,
        payload: Option<&str>,
        into: &str,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        let into: Vec<String> = into
            .trim()
            .trim_start_matches('$')
            .trim_start_matches('.')
            .split('.')
            .map(str::to_string)
            .collect();
        if into.iter().any(String::is_empty) {
            anyhow::bail!("lookup: invalid `into` path '{}'", into.join("."));
        }
        let metadata = metadata
            .iter()
            .map(|(k, t)| Ok((k.clone(), CompiledTemplate::compile(t, None)?)))
            .collect::<anyhow::Result<_>>()?;
        let payload = payload
            .map(|t| CompiledTemplate::compile(t, None))
            .transpose()?;
        // Box::pin breaks the recursive async type, as in the dlq middleware.
        let from = Box::pin(create_publisher_from_route(route_name, from)).await?;
        let found_key = format!("lookup.{}.found", into.join("."));
        Ok(Self {
            from,
            metadata,
            payload,
            into,
            found_key,
        })
    }

    /// Asks `from` for this message; `None` when it found nothing.
    async fn fetch(&self, msg: &CanonicalMessage) -> Result<Option<Value>, PublisherError> {
        let mut request = match &self.payload {
            Some(t) => CanonicalMessage::new(t.render(Some(msg)), None),
            None => msg.clone(),
        };
        for (key, template) in &self.metadata {
            let value = String::from_utf8_lossy(&template.render(Some(msg))).into_owned();
            request.metadata.insert(key.clone(), value);
        }
        let response = match self.from.send(request).await? {
            Sent::Response(response) => response,
            Sent::Ack => {
                return Err(PublisherError::NonRetryable(anyhow::anyhow!(
                    "lookup: the `from` endpoint returned no response; it must be request-capable"
                )))
            }
        };
        if let Some(status) = response
            .metadata
            .get(HTTP_STATUS_CODE)
            .and_then(|s| s.parse::<u16>().ok())
        {
            match status {
                404 => return Ok(None),
                408 | 429 | 500..=504 => {
                    return Err(PublisherError::Retryable(anyhow::anyhow!(
                        "lookup: HTTP status {status}"
                    )))
                }
                400.. => {
                    return Err(PublisherError::NonRetryable(anyhow::anyhow!(
                        "lookup: HTTP status {status}"
                    )))
                }
                _ => {}
            }
        }
        if response.payload.is_empty() {
            return Ok(None);
        }
        let value = serde_json::from_slice(&response.payload).unwrap_or_else(|_| {
            Value::String(String::from_utf8_lossy(&response.payload).into_owned())
        });
        Ok((!value.is_null()).then_some(value))
    }
}

/// The entries of one `lookup` middleware, shared by its publisher and consumer side.
struct Lookup {
    entries: Vec<Entry>,
    concurrency: usize,
}

impl Lookup {
    async fn new(config: &LookupMiddleware, route_name: &str) -> anyhow::Result<Self> {
        let mut entries = Vec::with_capacity(config.entries.len() + 1);
        match (&config.from, &config.into) {
            (Some(from), Some(into)) => {
                let payload = config.payload.as_deref();
                entries.push(Entry::new(from, &config.metadata, payload, into, route_name).await?);
            }
            (None, None) if config.metadata.is_empty() && config.payload.is_none() => {}
            _ => anyhow::bail!("lookup: `from` and `into` must be set together"),
        }
        for e in &config.entries {
            let payload = e.payload.as_deref();
            entries.push(Entry::new(&e.from, &e.metadata, payload, &e.into, route_name).await?);
        }
        if entries.is_empty() {
            anyhow::bail!("lookup: set `from` and `into`, or list `entries`");
        }
        Ok(Self {
            entries,
            concurrency: config.concurrency.max(1),
        })
    }

    /// Runs all entries for this message in parallel and writes their results.
    async fn enrich(
        &self,
        mut msg: CanonicalMessage,
    ) -> Result<CanonicalMessage, (CanonicalMessage, PublisherError)> {
        let mut doc: Value = match serde_json::from_slice(&msg.payload) {
            Ok(doc) => doc,
            Err(e) => {
                let e = PublisherError::NonRetryable(anyhow::anyhow!(
                    "lookup: payload is not JSON: {e}"
                ));
                return Err((msg, e));
            }
        };
        let fetches = self.entries.iter().map(|entry| entry.fetch(&msg));
        let results = match futures::future::try_join_all(fetches).await {
            Ok(results) => results,
            Err(e) => return Err((msg, e)),
        };
        let mut all_found = true;
        for (entry, found) in self.entries.iter().zip(results) {
            all_found &= found.is_some();
            msg.metadata
                .insert(entry.found_key.clone(), found.is_some().to_string());
            if let Err(e) = insert_at(&mut doc, &entry.into, found.unwrap_or(Value::Null)) {
                return Err((msg, PublisherError::NonRetryable(e)));
            }
        }
        msg.metadata
            .insert(FOUND.to_string(), all_found.to_string());
        match serde_json::to_vec(&doc) {
            Ok(bytes) => {
                msg.payload = bytes.into();
                Ok(msg)
            }
            Err(e) => Err((msg, PublisherError::NonRetryable(e.into()))),
        }
    }

    async fn enrich_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Vec<Result<CanonicalMessage, (CanonicalMessage, PublisherError)>> {
        futures::stream::iter(messages)
            .map(|m| self.enrich(m))
            .buffered(self.concurrency)
            .collect()
            .await
    }
}

/// Writes `value` at the dotted `path`, creating objects along the way.
fn insert_at(root: &mut Value, path: &[String], value: Value) -> anyhow::Result<()> {
    let (last, parents) = path.split_last().expect("path is non-empty");
    let mut cur = root;
    for key in parents {
        let Value::Object(map) = cur else {
            anyhow::bail!("lookup: cannot nest under '{key}': it is not an object");
        };
        cur = map
            .entry(key.as_str())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    let Value::Object(map) = cur else {
        anyhow::bail!("lookup: cannot set '{last}': its parent is not an object");
    };
    map.insert(last.clone(), value);
    Ok(())
}

pub struct LookupPublisher {
    inner: Box<dyn MessagePublisher>,
    lookup: Lookup,
}

impl LookupPublisher {
    pub async fn new(
        inner: Box<dyn MessagePublisher>,
        config: &LookupMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            lookup: Lookup::new(config, route_name).await?,
        })
    }
}

#[async_trait]
impl MessagePublisher for LookupPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let message = self.lookup.enrich(message).await.map_err(|(_, e)| e)?;
        self.inner.send(message).await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let results = self.lookup.enrich_batch(messages).await;
        let mut enriched = Vec::with_capacity(results.len());
        let mut failed = Vec::new();
        for result in results {
            match result {
                Ok(m) => enriched.push(m),
                Err(f) => failed.push(f),
            }
        }
        if failed.is_empty() {
            return self.inner.send_batch(enriched).await;
        }
        if enriched.is_empty() {
            return Ok(SentBatch::Partial {
                responses: None,
                failed,
            });
        }
        match self.inner.send_batch(enriched).await? {
            SentBatch::Ack => Ok(SentBatch::Partial {
                responses: None,
                failed,
            }),
            SentBatch::Partial {
                responses,
                failed: mut inner_failed,
            } => {
                inner_failed.extend(failed);
                Ok(SentBatch::Partial {
                    responses,
                    failed: inner_failed,
                })
            }
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush().await
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    fn requires_ordered_publish(&self) -> bool {
        self.inner.requires_ordered_publish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Enriches each received batch before the handler sees it. A failed lookup nacks the whole
/// batch: retryable errors reconnect the route, permanent ones stop it.
pub struct LookupConsumer {
    inner: Box<dyn MessageConsumer>,
    lookup: Lookup,
}

impl LookupConsumer {
    pub async fn new(
        inner: Box<dyn MessageConsumer>,
        config: &LookupMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            lookup: Lookup::new(config, route_name).await?,
        })
    }
}

#[async_trait]
impl MessageConsumer for LookupConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.inner.set_exit_on_empty(exit_on_empty);
    }

    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }

    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        let ReceivedBatch { messages, commit } = self.inner.receive_batch(max_messages).await?;
        if messages.is_empty() {
            return Ok(ReceivedBatch { messages, commit });
        }
        let len = messages.len();
        let mut enriched = Vec::with_capacity(len);
        for result in self.lookup.enrich_batch(messages).await {
            match result {
                Ok(m) => enriched.push(m),
                Err((_, e)) => {
                    if let Err(nack) = commit(vec![MessageDisposition::Nack; len]).await {
                        tracing::warn!("lookup: failed to nack the batch: {nack}");
                    }
                    return Err(match e {
                        PublisherError::NonRetryable(e) => ConsumerError::Permanent(e),
                        PublisherError::Retryable(e) | PublisherError::Connection(e) => {
                            ConsumerError::Connection(e)
                        }
                    });
                }
            }
        }
        Ok(ReceivedBatch {
            messages: enriched,
            commit,
        })
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::MemoryPublisher;
    use serde_json::json;

    async fn lookup(
        sink_name: &str,
        config_yaml: &str,
    ) -> (LookupPublisher, crate::endpoints::memory::MemoryChannel) {
        let sink = MemoryPublisher::new_local(sink_name, 16);
        let channel = sink.channel();
        let config: LookupMiddleware = serde_yaml_ng::from_str(config_yaml).unwrap();
        let publisher = LookupPublisher::new(Box::new(sink), &config, "lookup_test")
            .await
            .unwrap();
        (publisher, channel)
    }

    fn msg(value: Value) -> CanonicalMessage {
        CanonicalMessage::from_json(value).unwrap()
    }

    fn payload(m: &CanonicalMessage) -> Value {
        serde_json::from_slice(&m.payload).unwrap()
    }

    #[tokio::test]
    async fn writes_the_response_at_the_into_path() {
        let (publisher, channel) = lookup(
            "lookup_into",
            r#"
from: { static: { body: '{"name":"user-${payload:id}"}', raw: true } }
into: customer.profile
"#,
        )
        .await;
        let result = publisher
            .send_batch(vec![msg(json!({"id": 1})), msg(json!({"id": 2}))])
            .await
            .unwrap();
        assert!(matches!(result, SentBatch::Ack));
        let sent = channel.drain_messages();
        assert_eq!(
            payload(&sent[0]),
            json!({"id": 1, "customer": {"profile": {"name": "user-1"}}})
        );
        assert_eq!(payload(&sent[1])["customer"]["profile"]["name"], "user-2");
        assert_eq!(
            sent[1].metadata.get(FOUND).map(String::as_str),
            Some("true")
        );
    }

    #[tokio::test]
    async fn an_empty_or_404_response_writes_null() {
        for from in [
            r#"{ static: { body: "", raw: true } }"#,
            r#"{ static: { body: '{"a":1}', raw: true, metadata: { http_status_code: "404" } } }"#,
        ] {
            let (publisher, channel) =
                lookup("lookup_null", &format!("from: {from}\ninto: user")).await;
            publisher.send(msg(json!({"id": 1}))).await.unwrap();
            let sent = channel.drain_messages();
            assert_eq!(payload(&sent[0]), json!({"id": 1, "user": null}), "{from}");
            assert_eq!(
                sent[0].metadata.get(FOUND).map(String::as_str),
                Some("false")
            );
        }
    }

    #[tokio::test]
    async fn an_endpoint_without_a_response_fails_the_message() {
        let (publisher, channel) =
            lookup("lookup_no_response", "from: { null: null }\ninto: user").await;
        let result = publisher
            .send_batch(vec![msg(json!({"id": 1}))])
            .await
            .unwrap();
        let SentBatch::Partial { failed, .. } = result else {
            panic!("expected a partial failure");
        };
        assert_eq!(failed.len(), 1);
        assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));
        assert_eq!(payload(&failed[0].0), json!({"id": 1}));
        assert!(channel.drain_messages().is_empty());
    }

    #[tokio::test]
    async fn entries_run_in_parallel_with_a_found_flag_each() {
        let (publisher, channel) = lookup(
            "lookup_entries",
            r#"
from: { static: { body: '{"n":1}', raw: true }, middlewares: [ { delay: { delay_ms: 300 } } ] }
into: user
entries:
  - from: { static: { body: "", raw: true }, middlewares: [ { delay: { delay_ms: 300 } } ] }
    into: features.device
"#,
        )
        .await;
        let started = std::time::Instant::now();
        publisher.send(msg(json!({"id": 1}))).await.unwrap();
        assert!(started.elapsed() < std::time::Duration::from_millis(550));
        let sent = channel.drain_messages();
        assert_eq!(
            payload(&sent[0]),
            json!({"id": 1, "user": {"n": 1}, "features": {"device": null}})
        );
        let meta = |k: &str| sent[0].metadata.get(k).map(String::as_str);
        assert_eq!(meta("lookup.user.found"), Some("true"));
        assert_eq!(meta("lookup.features.device.found"), Some("false"));
        assert_eq!(meta(FOUND), Some("false"));
    }

    #[tokio::test]
    async fn from_and_into_must_be_set_together() {
        let config: LookupMiddleware = serde_yaml_ng::from_str("into: user").unwrap();
        let sink = MemoryPublisher::new_local("lookup_half", 16);
        let err = LookupPublisher::new(Box::new(sink), &config, "lookup_test")
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("together"), "{err}");
    }

    fn memory_source(topic: &str) -> crate::endpoints::memory::MemoryConsumer {
        crate::endpoints::memory::MemoryConsumer::new(&crate::models::MemoryConfig {
            topic: topic.to_string(),
            capacity: Some(16),
            enable_nack: true,
            ..Default::default()
        })
        .unwrap()
    }

    #[tokio::test]
    async fn the_consumer_enriches_before_the_batch_is_returned() {
        let source = memory_source("lookup_consumer_ok");
        source
            .channel()
            .fill_messages(vec![msg(json!({"id": 1})), msg(json!({"id": 2}))])
            .await
            .unwrap();
        let config: LookupMiddleware = serde_yaml_ng::from_str(
            r#"{ from: { static: { body: '{"v":"${payload:id}"}', raw: true } }, into: prev }"#,
        )
        .unwrap();
        let mut consumer = LookupConsumer::new(Box::new(source), &config, "lookup_test")
            .await
            .unwrap();
        let batch = consumer.receive_batch(10).await.unwrap();
        assert_eq!(
            payload(&batch.messages[0]),
            json!({"id": 1, "prev": {"v": "1"}})
        );
        assert_eq!(payload(&batch.messages[1])["prev"]["v"], "2");
        (batch.commit)(vec![MessageDisposition::Ack; 2])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_failed_consumer_lookup_nacks_the_batch_for_redelivery() {
        let source = memory_source("lookup_consumer_fail");
        let channel = source.channel();
        channel
            .fill_messages(vec![msg(json!({"id": 1}))])
            .await
            .unwrap();
        let config: LookupMiddleware =
            serde_yaml_ng::from_str("{ from: { null: null }, into: prev }").unwrap();
        let mut consumer = LookupConsumer::new(Box::new(source), &config, "lookup_test")
            .await
            .unwrap();
        let err = consumer.receive_batch(10).await.err().unwrap();
        assert!(matches!(err, ConsumerError::Permanent(_)), "{err}");

        let mut again = memory_source("lookup_consumer_fail");
        let batch = again.receive_batch(10).await.unwrap();
        assert_eq!(payload(&batch.messages[0]), json!({"id": 1}));
    }
}
