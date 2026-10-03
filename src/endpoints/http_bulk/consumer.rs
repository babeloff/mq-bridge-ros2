//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Input for HTTP APIs that list JSON documents page by page.
//!
//! One request reads one page. Where the next page starts is configuration: a
//! value in the response, a field of the last document, or the documents read.

use super::{quoted, Connection, JSON};
use crate::checkpoint::{self, CheckpointBackend, CheckpointStore, VersionedCheckpoint};
use crate::endpoints::poll::PollBackoff;
use crate::models::{HttpBulkConfig, HttpBulkRead};
use crate::support::http_status;
use crate::traits::{BoxFuture, ConsumerError, MessageConsumer, MessageDisposition, ReceivedBatch};
use crate::CanonicalMessage;
use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, trace, warn};

const CURSOR: &str = "{cursor}";
const LIMIT: &str = "{limit}";

/// Where the next page starts. `End` is stored too, so a finished scan stays finished.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Position {
    At(Value),
    End,
}

/// Where the position after a page comes from.
enum CursorSource {
    /// The number of documents read so far.
    Count,
    /// A JSON pointer into the response.
    Response(String),
    /// A JSON pointer into the last document of the page.
    Item(String),
}

pub struct HttpBulkConsumer {
    connection: Connection,
    method: reqwest::Method,
    path: String,
    body: Option<String>,
    items: String,
    source: CursorSource,
    backoff: PollBackoff,
    checkpoint: Option<Arc<dyn CheckpointStore>>,
    progress: Arc<Mutex<Progress>>,
    /// A draining route ends on the first empty page, so it does not wait for it.
    exit_on_empty: bool,
}

/// The read position, and how often a nack has moved it back.
struct Progress {
    position: Position,
    rollbacks: u64,
    /// Pages handed out and not yet committed.
    outstanding: usize,
}

/// Names the listing in a checkpoint store: host and path, without the query.
fn source_name(config: &HttpBulkConfig, read: &HttpBulkRead) -> String {
    let host = url::Url::parse(&config.url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default();
    let path = read.path.split('?').next().unwrap_or_default();
    format!("{host}{path}")
}

/// The checkpoint of an input, or `None` without `read` and its `cursor_id` and `checkpoint_store`.
pub(crate) async fn cursor_checkpoint(
    config: &HttpBulkConfig,
) -> anyhow::Result<Option<Arc<VersionedCheckpoint>>> {
    let Some(read) = &config.read else {
        return Ok(None);
    };
    let (Some(cursor_id), Some(spec)) = (&read.cursor_id, &read.checkpoint_store) else {
        return Ok(None);
    };
    match checkpoint::parse_checkpoint_store(spec)? {
        CheckpointBackend::Source { .. } => Err(anyhow!(
            "http_bulk needs an external read.checkpoint_store (file://, postgres://, mongodb:// or s3://)"
        )),
        external => {
            let source = source_name(config, read);
            let store = checkpoint::build_external_store(external, &source, cursor_id).await?;
            Ok(Some(Arc::new(VersionedCheckpoint::new(
                store,
                format!("http_bulk:{source}"),
            ))))
        }
    }
}

impl HttpBulkConsumer {
    pub async fn new(config: &HttpBulkConfig, no_resume: bool) -> anyhow::Result<Self> {
        let connection = Connection::new(config)?;
        let Some(read) = &config.read else {
            bail!("http_bulk used as an input needs 'read'");
        };
        if !read.path.starts_with('/') {
            bail!("http_bulk read.path must start with '/'");
        }
        let in_body = read.body.as_deref().is_some_and(|b| b.contains(CURSOR));
        if !read.path.contains(CURSOR) && !in_body {
            bail!(
                "http_bulk read needs '{CURSOR}' in its path or body, or every page is the first"
            );
        }
        let default_method = if read.body.is_some() { "POST" } else { "GET" };
        let method = read
            .method
            .as_deref()
            .unwrap_or(default_method)
            .to_ascii_uppercase();
        let (source, start) = match (&read.cursor.response, &read.cursor.item) {
            (Some(_), Some(_)) => bail!("http_bulk read.cursor sets both 'response' and 'item'"),
            (Some(pointer), None) => (CursorSource::Response(pointer.clone()), Value::Null),
            (None, Some(pointer)) => (CursorSource::Item(pointer.clone()), Value::Null),
            (None, None) => (CursorSource::Count, Value::from(0u64)),
        };
        let start = read.cursor.start.clone().unwrap_or(start);
        if matches!(source, CursorSource::Count) && !start.is_u64() {
            bail!("http_bulk read.cursor.start must be a whole number when the cursor is a count");
        }

        let checkpoint: Option<Arc<dyn CheckpointStore>> = if no_resume {
            None
        } else {
            match cursor_checkpoint(config).await? {
                Some(checkpoint) => Some(checkpoint),
                None => {
                    warn!(
                        path = %read.path,
                        "http_bulk input has no read.cursor_id and read.checkpoint_store; every start reads from the beginning"
                    );
                    None
                }
            }
        };
        let saved = match &checkpoint {
            Some(checkpoint) => checkpoint.load().await?.and_then(|text| {
                let position = serde_json::from_str::<Position>(&text).ok();
                if position.is_none() {
                    warn!(value = %text, "Ignoring an unreadable http_bulk cursor; reading from the beginning");
                }
                position
            }),
            None => None,
        };
        info!(path = %read.path, resumed = saved.is_some(), "http_bulk input ready");

        Ok(Self {
            connection,
            method: reqwest::Method::from_bytes(method.as_bytes())
                .with_context(|| format!("http_bulk read.method '{method}' is not valid"))?,
            path: read.path.clone(),
            body: read.body.clone(),
            items: read.items.clone(),
            source,
            backoff: PollBackoff::new(
                Duration::from_millis(read.polling_interval_ms.unwrap_or(1000)),
                read.max_polling_interval_ms.map(Duration::from_millis),
            ),
            checkpoint,
            progress: Arc::new(Mutex::new(Progress {
                position: saved.unwrap_or(Position::At(start)),
                rollbacks: 0,
                outstanding: 0,
            })),
            exit_on_empty: false,
        })
    }

    /// Requests the page at `cursor` and returns the parsed response.
    async fn page(&self, cursor: &Value, limit: usize) -> Result<Value, ConsumerError> {
        let label = format!("{} {}", self.method, self.path);
        let limit = limit.to_string();
        let in_url = match cursor {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let url = format!(
            "{}{}",
            self.connection.base,
            self.path
                .replace(
                    CURSOR,
                    &utf8_percent_encode(&in_url, NON_ALPHANUMERIC).to_string()
                )
                .replace(LIMIT, &limit)
        );
        let mut request = self.connection.request(self.method.clone(), &url);
        if let Some(body) = &self.body {
            request = request.header(CONTENT_TYPE, JSON).body(
                body.replace(CURSOR, &cursor.to_string())
                    .replace(LIMIT, &limit),
            );
        }
        let response = self.connection.send(request).await.map_err(|e| {
            let error = anyhow!("{label} failed: {e}");
            if e.retryable {
                ConsumerError::Connection(error)
            } else {
                ConsumerError::Permanent(error)
            }
        })?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| ConsumerError::Connection(anyhow!("{label} response was cut off: {e}")))?;
        if !status.is_success() {
            let error = anyhow!("{label} answered {status}: {}", quoted(&text));
            return Err(if http_status::is_retryable(status.as_u16()) {
                ConsumerError::Connection(error)
            } else {
                ConsumerError::Permanent(error)
            });
        }
        serde_json::from_str(&text).map_err(|e| {
            ConsumerError::Permanent(anyhow!(
                "{label} did not answer with JSON ({e}): {}",
                quoted(&text)
            ))
        })
    }

    /// The position after each document of a page; `None` where a page cannot be split.
    fn positions(
        &self,
        response: &Value,
        items: &[Value],
        cursor: &Value,
    ) -> Result<Vec<Option<Position>>, ConsumerError> {
        match &self.source {
            CursorSource::Count => {
                let read = cursor.as_u64().unwrap_or_default();
                Ok((1..=items.len() as u64)
                    .map(|n| Some(Position::At(Value::from(read + n))))
                    .collect())
            }
            CursorSource::Item(pointer) => items
                .iter()
                .map(|item| match item.pointer(pointer) {
                    Some(value) if !value.is_null() => Ok(Some(Position::At(value.clone()))),
                    _ => Err(ConsumerError::Permanent(anyhow!(
                        "http_bulk read.cursor.item '{pointer}' is missing in a document"
                    ))),
                })
                .collect(),
            CursorSource::Response(pointer) => {
                let next = match response.pointer(pointer) {
                    Some(Value::Null) => Position::End,
                    Some(value) => Position::At(value.clone()),
                    None => {
                        return Err(ConsumerError::Permanent(anyhow!(
                            "http_bulk read.cursor.response '{pointer}' is missing in the response"
                        )))
                    }
                };
                let mut positions = vec![None; items.len()];
                match positions.last_mut() {
                    Some(last) => *last = Some(next),
                    // An empty page may still move on, or end the read.
                    None => positions.push(Some(next)),
                }
                Ok(positions)
            }
        }
    }

    async fn idle(&mut self) -> ReceivedBatch {
        if !self.exit_on_empty {
            tokio::time::sleep(self.backoff.idle_delay()).await;
        }
        ReceivedBatch::empty()
    }
}

async fn save(checkpoint: &Option<Arc<dyn CheckpointStore>>, position: &Position) {
    let (Some(checkpoint), Ok(text)) = (checkpoint, serde_json::to_string(position)) else {
        return;
    };
    if let Err(error) = checkpoint.save(&text).await {
        warn!(%error, "Failed to save the http_bulk cursor; documents may be read again after a restart");
    }
}

#[async_trait]
impl MessageConsumer for HttpBulkConsumer {
    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }
        loop {
            let (before, rollbacks) = {
                let progress = self.progress.lock().unwrap();
                (progress.position.clone(), progress.rollbacks)
            };
            let Position::At(cursor) = &before else {
                return Ok(self.idle().await);
            };
            let response = self.page(cursor, max_messages).await?;
            let items = match response.pointer(&self.items) {
                Some(Value::Array(items)) => items,
                _ => {
                    return Err(ConsumerError::Permanent(anyhow!(
                        "http_bulk read.items '{}' is not an array in the response",
                        self.items
                    )))
                }
            };
            let mut positions = self.positions(&response, items, cursor)?;
            if items.is_empty() {
                if let Some(next) = positions.pop().flatten().filter(|next| *next != before) {
                    let settled = {
                        let mut progress = self.progress.lock().unwrap();
                        let current = progress.rollbacks == rollbacks;
                        if current {
                            progress.position = next.clone();
                        }
                        current && progress.outstanding == 0
                    };
                    // Saved past a page that is still out, a restart would skip it.
                    if settled {
                        save(&self.checkpoint, &next).await;
                    }
                }
                return Ok(self.idle().await);
            }
            // Advance optimistically; the commit rolls back to the last acked document.
            {
                let mut progress = self.progress.lock().unwrap();
                if progress.rollbacks != rollbacks {
                    // A nack moved the cursor back while this page was read.
                    continue;
                }
                if let Some(Some(last)) = positions.last() {
                    progress.position = last.clone();
                }
                progress.outstanding += 1;
            }
            self.backoff.reset();

            let messages: Vec<CanonicalMessage> = items
                .iter()
                .map(|item| {
                    CanonicalMessage::new(serde_json::to_vec(item).unwrap_or_default(), None)
                })
                .collect();
            trace!(count = messages.len(), path = %self.path, "Read documents");

            let checkpoint = self.checkpoint.clone();
            let progress = self.progress.clone();
            let commit = Box::new(move |dispositions: Vec<MessageDisposition>| {
                Box::pin(async move {
                    let acked = dispositions
                        .iter()
                        .take(positions.len())
                        .take_while(|d| {
                            matches!(d, MessageDisposition::Ack | MessageDisposition::Reply(_))
                        })
                        .count();
                    let boundary = positions[..acked].last().cloned().flatten();
                    {
                        let mut progress = progress.lock().unwrap();
                        progress.outstanding = progress.outstanding.saturating_sub(1);
                        // An earlier nack rewound past this page: it is read again.
                        if progress.rollbacks != rollbacks {
                            return Ok(());
                        }
                        if acked < positions.len() {
                            progress.position = boundary.clone().unwrap_or(before);
                            progress.rollbacks += 1;
                        }
                    }
                    if let Some(boundary) = boundary {
                        save(&checkpoint, &boundary).await;
                    }
                    Ok(())
                }) as BoxFuture<'static, anyhow::Result<()>>
            });
            return Ok(ReceivedBatch { messages, commit });
        }
    }

    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(all(test, feature = "plugin", feature = "test-utils"))]
mod tests {
    use super::*;
    use crate::plugin::test_support::{payload_texts, StubHttpServer, StubRequest};
    use serde_json::json;

    async fn server(
        respond: impl Fn(&StubRequest) -> (u16, String) + Send + Sync + 'static,
    ) -> StubHttpServer {
        StubHttpServer::start(respond).await.expect("stub server")
    }

    async fn consumer(server: &StubHttpServer, mut config: Value) -> HttpBulkConsumer {
        config["url"] = json!(server.url());
        config["read"]["polling_interval_ms"] = json!(1);
        let config: HttpBulkConfig = serde_json::from_value(config).expect("config");
        HttpBulkConsumer::new(&config, false)
            .await
            .expect("consumer")
    }

    /// Reads one page and answers every document with `disposition`.
    async fn read(consumer: &mut HttpBulkConsumer, acked: usize) -> Vec<String> {
        let batch = consumer.receive_batch(2).await.expect("page");
        let texts = payload_texts(&batch.messages);
        let dispositions = (0..texts.len())
            .map(|i| {
                if i < acked {
                    MessageDisposition::Ack
                } else {
                    MessageDisposition::Nack
                }
            })
            .collect();
        (batch.commit)(dispositions).await.expect("commit");
        texts
    }

    fn targets(server: &StubHttpServer) -> Vec<String> {
        server.requests().into_iter().map(|r| r.target).collect()
    }

    fn store() -> (std::path::PathBuf, String) {
        let path = std::env::temp_dir().join(format!(
            "mqb-http-bulk-read-{}.json",
            fast_uuid_v7::gen_id_string()
        ));
        let url = format!("file://{}", path.display());
        (path, url)
    }

    #[tokio::test]
    async fn without_a_cursor_source_the_documents_read_are_the_offset() {
        let server = server(|request| {
            let page = match request.target.as_str() {
                "/docs?offset=0&limit=2" => r#"{"results":[{"id":1},{"id":2}]}"#,
                "/docs?offset=2&limit=2" => r#"{"results":[{"id":3}]}"#,
                _ => r#"{"results":[]}"#,
            };
            (200, page.to_string())
        })
        .await;
        let mut consumer = consumer(
            &server,
            json!({"read": {"path": "/docs?offset={cursor}&limit={limit}", "items": "/results"}}),
        )
        .await;

        assert_eq!(read(&mut consumer, 2).await, [r#"{"id":1}"#, r#"{"id":2}"#]);
        assert_eq!(read(&mut consumer, 1).await, [r#"{"id":3}"#]);
        assert!(read(&mut consumer, 0).await.is_empty());
        assert_eq!(targets(&server).last().unwrap(), "/docs?offset=3&limit=2");
    }

    #[tokio::test]
    async fn a_nacked_document_is_read_again_from_the_last_acked_one() {
        let server = server(|request| {
            let page = match request.target.as_str() {
                "/rows?id=gt.0" => r#"[{"id":"a b"},{"id":"c"}]"#,
                "/rows?id=gt.a%20b" => r#"[{"id":"c"}]"#,
                _ => "[]",
            };
            (200, page.to_string())
        })
        .await;
        let (path, url) = store();
        let config = json!({
            "read": {
                "path": "/rows?id=gt.{cursor}",
                "cursor": {"item": "/id", "start": 0},
                "cursor_id": "copy",
                "checkpoint_store": url,
            },
        });
        let mut first = consumer(&server, config.clone()).await;
        assert_eq!(read(&mut first, 1).await.len(), 2);
        assert_eq!(read(&mut first, 1).await, [r#"{"id":"c"}"#]);

        // A restart resumes after the last acked document.
        let mut second = consumer(&server, config).await;
        assert!(read(&mut second, 0).await.is_empty());
        assert_eq!(targets(&server).last().unwrap(), "/rows?id=gt.c");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_page_read_past_a_nacked_one_does_not_move_the_checkpoint() {
        let server = server(|request| {
            let page = match request.target.as_str() {
                "/docs?offset=0&limit=2" => r#"{"results":[{"id":1},{"id":2}]}"#,
                "/docs?offset=2&limit=2" => r#"{"results":[{"id":3},{"id":4}]}"#,
                _ => r#"{"results":[]}"#,
            };
            (200, page.to_string())
        })
        .await;
        let (path, url) = store();
        let config = json!({"read": {
            "path": "/docs?offset={cursor}&limit={limit}", "items": "/results",
            "cursor_id": "copy", "checkpoint_store": url,
        }});
        let mut first = consumer(&server, config.clone()).await;
        let earlier = first.receive_batch(2).await.expect("page");
        let later = first.receive_batch(2).await.expect("page");
        (earlier.commit)(vec![MessageDisposition::Nack; 2])
            .await
            .expect("commit");
        (later.commit)(vec![MessageDisposition::Ack; 2])
            .await
            .expect("commit");

        assert_eq!(read(&mut first, 0).await, [r#"{"id":1}"#, r#"{"id":2}"#]);
        // The acked later page saved nothing: a restart starts over as well.
        let mut second = consumer(&server, config).await;
        assert_eq!(read(&mut second, 0).await.len(), 2);
        assert_eq!(targets(&server).last().unwrap(), "/docs?offset=0&limit=2");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_response_cursor_goes_into_the_body_and_null_ends_the_read_for_good() {
        let server = server(|request| {
            let body = String::from_utf8_lossy(&request.body).into_owned();
            let page = match body.as_str() {
                r#"{"limit":2,"offset":null}"# => r#"{"points":[{"id":1},{"id":2}],"next":"p 2"}"#,
                r#"{"limit":2,"offset":"p 2"}"# => r#"{"points":[{"id":3}],"next":null}"#,
                other => panic!("unexpected body {other}"),
            };
            (200, page.to_string())
        })
        .await;
        let (path, url) = store();
        let config = json!({
            "read": {
                "path": "/scroll",
                "body": r#"{"limit":{limit},"offset":{cursor}}"#,
                "items": "/points",
                "cursor": {"response": "/next"},
                "cursor_id": "scan",
                "checkpoint_store": url,
            },
        });
        let mut first = consumer(&server, config.clone()).await;
        // A page whose cursor covers all of it is read again as a whole after a nack.
        assert_eq!(read(&mut first, 1).await.len(), 2);
        assert_eq!(read(&mut first, 2).await.len(), 2);
        assert_eq!(read(&mut first, 1).await, [r#"{"id":3}"#]);
        assert!(read(&mut first, 0).await.is_empty());

        let mut second = consumer(&server, config).await;
        assert!(read(&mut second, 0).await.is_empty());
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].header("content-type"), Some(JSON));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn an_empty_page_can_still_move_the_cursor() {
        let server = server(|request| {
            let page = match request.target.as_str() {
                "/changes?since=0" => r#"{"results":[],"last_seq":"7-x"}"#,
                _ => r#"{"results":[],"last_seq":"7-x"}"#,
            };
            (200, page.to_string())
        })
        .await;
        let mut consumer = consumer(
            &server,
            json!({"read": {
                "path": "/changes?since={cursor}",
                "items": "/results",
                "cursor": {"response": "/last_seq", "start": 0},
            }}),
        )
        .await;
        assert!(read(&mut consumer, 0).await.is_empty());
        assert!(read(&mut consumer, 0).await.is_empty());
        assert_eq!(
            targets(&server),
            ["/changes?since=0", "/changes?since=7%2Dx"]
        );
    }

    #[tokio::test]
    async fn an_empty_page_does_not_save_past_a_page_that_is_still_out() {
        let server = server(|request| {
            let page = match request.target.as_str() {
                "/changes?since=0" => r#"{"results":[{"id":1}],"last_seq":5}"#,
                _ => r#"{"results":[],"last_seq":9}"#,
            };
            (200, page.to_string())
        })
        .await;
        let (path, url) = store();
        let config = json!({"read": {
            "path": "/changes?since={cursor}", "items": "/results",
            "cursor": {"response": "/last_seq", "start": 0},
            "cursor_id": "feed", "checkpoint_store": url,
        }});
        let mut first = consumer(&server, config.clone()).await;
        let out = first.receive_batch(2).await.expect("page");
        assert!(read(&mut first, 0).await.is_empty());

        let mut second = consumer(&server, config.clone()).await;
        assert_eq!(
            second.receive_batch(2).await.expect("page").messages.len(),
            1
        );
        assert_eq!(targets(&server).last().unwrap(), "/changes?since=0");

        // Once the page is acked, an empty page moves the checkpoint again.
        (out.commit)(vec![MessageDisposition::Ack])
            .await
            .expect("commit");
        for since in ["/changes?since=5", "/changes?since=9"] {
            let mut next = consumer(&server, config.clone()).await;
            assert!(read(&mut next, 0).await.is_empty());
            assert_eq!(targets(&server).last().unwrap(), since);
        }
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_busy_source_is_retried_and_a_refusal_or_wrong_shape_is_not() {
        let server = server(|request| match request.target.as_str() {
            "/busy?o=0" => (503, "later".to_string()),
            "/refused?o=0" => (404, "no such index".to_string()),
            "/html?o=0" => (200, "<html>".to_string()),
            _ => (200, r#"{"rows":{}}"#.to_string()),
        })
        .await;
        for (path, permanent, text) in [
            ("/busy?o={cursor}", false, "answered 503"),
            ("/refused?o={cursor}", true, "no such index"),
            ("/html?o={cursor}", true, "did not answer with JSON"),
            ("/shape?o={cursor}", true, "is not an array"),
        ] {
            let mut consumer =
                consumer(&server, json!({"read": {"path": path, "items": "/rows"}})).await;
            let Err(error) = consumer.receive_batch(2).await else {
                panic!("{path} was read");
            };
            assert_eq!(matches!(error, ConsumerError::Permanent(_)), permanent);
            assert!(error.to_string().contains(text), "{error}");
        }
    }

    #[tokio::test]
    async fn a_config_that_cannot_read_is_refused_at_startup() {
        for (config, text) in [
            (json!({"upsert": {"path": "/d"}}), "needs 'read'"),
            (
                json!({"read": {"path": "docs?o={cursor}"}}),
                "start with '/'",
            ),
            (json!({"read": {"path": "/docs"}}), "'{cursor}'"),
            (
                json!({"read": {"path": "/d?o={cursor}", "cursor": {"response": "/a", "item": "/b"}}}),
                "both",
            ),
            (
                json!({"read": {"path": "/d?o={cursor}", "cursor": {"start": "a"}}}),
                "whole number",
            ),
            (
                json!({"read": {"path": "/d?o={cursor}", "cursor_id": "c", "checkpoint_store": "cursors"}}),
                "external read.checkpoint_store",
            ),
        ] {
            let mut config = config;
            config["url"] = json!("http://localhost:1");
            let config: HttpBulkConfig = serde_json::from_value(config).expect("config");
            let error = HttpBulkConsumer::new(&config, false)
                .await
                .err()
                .expect("refused");
            assert!(error.to_string().contains(text), "{error}");
        }
    }
}
