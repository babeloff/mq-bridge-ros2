//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Enriches each outgoing message with the response of another, request-capable endpoint.

use crate::endpoints::create_publisher_from_route;
use crate::models::LookupMiddleware;
use crate::support::interpolation::CompiledTemplate;
use crate::traits::{BoxFuture, EndpointStatus, MessagePublisher, PublisherError, Sent, SentBatch};
use crate::CanonicalMessage;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{Map, Value};
use std::any::Any;
use std::sync::Arc;

const FOUND: &str = "lookup.found";
const HTTP_STATUS_CODE: &str = "http_status_code";

pub struct LookupPublisher {
    inner: Box<dyn MessagePublisher>,
    from: Arc<dyn MessagePublisher>,
    metadata: Vec<(String, CompiledTemplate)>,
    payload: Option<CompiledTemplate>,
    into: Vec<String>,
    concurrency: usize,
}

impl LookupPublisher {
    pub async fn new(
        inner: Box<dyn MessagePublisher>,
        config: &LookupMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        let into: Vec<String> = config
            .into
            .trim()
            .trim_start_matches('$')
            .trim_start_matches('.')
            .split('.')
            .map(str::to_string)
            .collect();
        if into.iter().any(String::is_empty) {
            anyhow::bail!("lookup: invalid `into` path '{}'", config.into);
        }
        let metadata = config
            .metadata
            .iter()
            .map(|(k, t)| Ok((k.clone(), CompiledTemplate::compile(t, None)?)))
            .collect::<anyhow::Result<_>>()?;
        let payload = config
            .payload
            .as_deref()
            .map(|t| CompiledTemplate::compile(t, None))
            .transpose()?;
        // Box::pin breaks the recursive async type, as in the dlq middleware.
        let from = Box::pin(create_publisher_from_route(route_name, &config.from)).await?;
        Ok(Self {
            inner,
            from,
            metadata,
            payload,
            into,
            concurrency: config.concurrency.max(1),
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

    async fn enrich(
        &self,
        mut msg: CanonicalMessage,
    ) -> Result<CanonicalMessage, (CanonicalMessage, PublisherError)> {
        let found = match self.fetch(&msg).await {
            Ok(found) => found,
            Err(e) => return Err((msg, e)),
        };
        let mut doc: Value = match serde_json::from_slice(&msg.payload) {
            Ok(doc) => doc,
            Err(e) => {
                let e = PublisherError::NonRetryable(anyhow::anyhow!(
                    "lookup: payload is not JSON: {e}"
                ));
                return Err((msg, e));
            }
        };
        msg.metadata
            .insert(FOUND.to_string(), found.is_some().to_string());
        if let Err(e) = insert_at(&mut doc, &self.into, found.unwrap_or(Value::Null)) {
            return Err((msg, PublisherError::NonRetryable(e)));
        }
        match serde_json::to_vec(&doc) {
            Ok(bytes) => {
                msg.payload = bytes.into();
                Ok(msg)
            }
            Err(e) => Err((msg, PublisherError::NonRetryable(e.into()))),
        }
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

#[async_trait]
impl MessagePublisher for LookupPublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let message = self.enrich(message).await.map_err(|(_, e)| e)?;
        self.inner.send(message).await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let results: Vec<_> = futures::stream::iter(messages)
            .map(|m| self.enrich(m))
            .buffered(self.concurrency)
            .collect()
            .await;
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
        let (publisher, channel) = lookup("lookup_no_response", "from: null\ninto: user").await;
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
}
