//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! HTTP APIs that take many JSON documents in one request; the input is in `read`.
//!
//! Search engines and similar stores differ in three things only: the body an
//! upsert carries, how documents are deleted, and how the outcome is reported.
//! All three are configuration here, so a new target is a recipe, not code.
//! Messages are sent in order: a batch is cut into runs of upserts and deletes,
//! and after a request that may be retried nothing later in the batch is sent.

mod presets;
mod read;
pub use presets::{preset_names, register_preset};
pub(crate) use read::cursor_checkpoint;
pub use read::HttpBulkConsumer;

use crate::models::{
    Compression, HttpBulkConfig, HttpBulkDelete, HttpBulkFormat, HttpBulkItems, HttpBulkJob,
    HttpBulkLines, HttpBulkResult,
};
use crate::support::change_op::ChangeOp;
use crate::support::compression_pool::{gzip_default, lz4_pooled, zstd_pooled};
use crate::support::interpolation::CompiledTemplate;
use crate::support::poll_job::{poll_until, PollSchedule};
use crate::support::{http_status, ndjson};
use crate::traits::{MessagePublisher, PublisherError, SentBatch};
use crate::CanonicalMessage;
use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE};
use serde_json::Value;
use std::ops::Range;
use std::time::Duration;
use tracing::{trace, warn};

const IDS: &str = "{ids}";
const JOB_ID: &str = "{id}";
const DOCUMENTS: &str = "{documents}";
const LINE_ID: &str = "{id}";
const JSON: &str = "application/json";
const NDJSON: &str = "application/x-ndjson";
/// Most characters of a response body quoted in an error.
const QUOTED_RESPONSE_CHARS: usize = 500;

/// A failed request, kept as text so every message it covered gets its own error.
struct Failure {
    retryable: bool,
    text: String,
}

impl Failure {
    fn permanent(text: String) -> Self {
        Self {
            retryable: false,
            text,
        }
    }

    fn retryable(text: String) -> Self {
        Self {
            retryable: true,
            text,
        }
    }

    fn error(&self) -> PublisherError {
        let error = anyhow!("{}", self.text);
        if self.retryable {
            PublisherError::Retryable(error)
        } else {
            PublisherError::NonRetryable(error)
        }
    }
}

enum ResultMode {
    Status,
    Lines(HttpBulkLines),
    Items(HttpBulkItems),
    Job(HttpBulkJob),
}

impl ResultMode {
    fn from_config(result: &HttpBulkResult, request: &str) -> anyhow::Result<Self> {
        match (&result.lines, &result.items, &result.job) {
            (Some(lines), None, None) => Ok(Self::Lines(lines.clone())),
            (None, Some(items), None) => Ok(Self::Items(items.clone())),
            (None, None, Some(job)) => {
                if !job.poll.contains(JOB_ID) {
                    bail!("http_bulk {request}.result.job.poll must contain '{JOB_ID}'");
                }
                Ok(Self::Job(job.clone()))
            }
            (None, None, None) => Ok(Self::Status),
            _ => bail!("http_bulk {request}.result sets more than one of 'lines', 'items', 'job'"),
        }
    }
}

struct Request {
    method: reqwest::Method,
    path: String,
    result: ResultMode,
}

impl Request {
    fn new(
        method: Option<&str>,
        path: &str,
        result: &HttpBulkResult,
        name: &str,
    ) -> anyhow::Result<Self> {
        if !path.starts_with('/') {
            bail!("http_bulk {name}.path must start with '/'");
        }
        let method = method.unwrap_or("POST").to_ascii_uppercase();
        Ok(Self {
            method: reqwest::Method::from_bytes(method.as_bytes())
                .with_context(|| format!("http_bulk {name}.method '{method}' is not valid"))?,
            path: path.to_string(),
            result: ResultMode::from_config(result, name)?,
        })
    }
}

/// The text before and after a placeholder, which must occur exactly once.
fn around(template: &str, placeholder: &str, name: &str) -> anyhow::Result<(String, String)> {
    match template.split_once(placeholder) {
        Some((before, after)) if !after.contains(placeholder) => {
            Ok((before.to_string(), after.to_string()))
        }
        _ => bail!("http_bulk {name} must contain '{placeholder}' exactly once"),
    }
}

/// How the ids of a delete request are sent.
enum DeleteBody {
    Url,
    /// A JSON array between the two halves of the envelope.
    Array(String, String),
    /// One line per id, the id between the two halves.
    Lines(String, String),
}

/// Documents that go out in one request, with the batch positions they came from.
#[derive(Default)]
struct Chunk {
    body: Vec<u8>,
    indices: Vec<usize>,
}

/// The client, base URL and headers both directions of the endpoint share.
struct Connection {
    http: reqwest::Client,
    base: String,
    headers: HeaderMap,
}

impl Connection {
    fn new(config: &HttpBulkConfig) -> anyhow::Result<Self> {
        let url = url::Url::parse(&config.url).context("Invalid http_bulk URL")?;
        if !url.has_host() || !matches!(url.scheme(), "http" | "https") {
            bail!("http_bulk URL must be an absolute http(s) URL, e.g. 'http://localhost:7700'");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("http_bulk URL must not carry a query or fragment; put it into the request path");
        }
        let mut headers = HeaderMap::new();
        for (name, value) in &config.headers {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes())
                    .with_context(|| format!("http_bulk header name '{name}' is not valid"))?,
                HeaderValue::from_str(value)
                    .with_context(|| format!("http_bulk header '{name}' has an invalid value"))?,
            );
        }
        // No redirect following: reqwest would resend custom credential headers to another host.
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(
                config.connect_timeout_ms.unwrap_or(10_000),
            ));
        if let Some(ms) = config.request_timeout_ms {
            builder = builder.timeout(Duration::from_millis(ms));
        }
        if config.tls.accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca) = &config.tls.ca_file {
            let pem = std::fs::read(ca)
                .with_context(|| format!("Failed to read http_bulk CA file '{ca}'"))?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&pem)
                    .with_context(|| format!("Invalid http_bulk CA certificate '{ca}'"))?,
            );
        }
        Ok(Self {
            http: builder
                .build()
                .context("Failed to build http_bulk client")?,
            base: url.as_str().trim_end_matches('/').to_string(),
            headers,
        })
    }
}

pub struct HttpBulkPublisher {
    http: reqwest::Client,
    base: String,
    headers: HeaderMap,
    upsert: Request,
    format: HttpBulkFormat,
    content_type: HeaderValue,
    action: Option<CompiledTemplate>,
    envelope: (String, String),
    compression: Compression,
    delete: Option<(Request, HttpBulkDelete, DeleteBody)>,
    operation: Option<CompiledTemplate>,
    delete_values: Vec<String>,
    max_request_bytes: usize,
}

impl HttpBulkPublisher {
    pub fn new(config: &HttpBulkConfig) -> anyhow::Result<Self> {
        let Connection {
            http,
            base,
            headers,
        } = Connection::new(config)?;
        let Some(upsert) = &config.upsert else {
            bail!("http_bulk used as an output needs 'upsert'");
        };
        let content_type = upsert.content_type.as_deref().unwrap_or({
            match upsert.format {
                HttpBulkFormat::Ndjson => NDJSON,
                HttpBulkFormat::JsonArray => JSON,
            }
        });
        let ndjson = upsert.format == HttpBulkFormat::Ndjson;
        if upsert.action.is_some() && !ndjson {
            bail!("http_bulk upsert.action needs format 'ndjson'");
        }
        let envelope = match &upsert.envelope {
            Some(_) if ndjson => bail!("http_bulk upsert.envelope needs format 'json_array'"),
            Some(envelope) => around(envelope, DOCUMENTS, "upsert.envelope")?,
            None => Default::default(),
        };
        let delete = match &config.delete {
            Some(delete) => {
                if delete.max_ids == 0 {
                    bail!("http_bulk delete.max_ids must be at least 1");
                }
                let request = Request::new(
                    delete.method.as_deref(),
                    &delete.path,
                    &delete.result,
                    "delete",
                )?;
                let in_url = delete.path.contains(IDS);
                let body = match (&delete.envelope, &delete.line) {
                    (None, None) if in_url => DeleteBody::Url,
                    (None, None) => DeleteBody::Array(String::new(), String::new()),
                    _ if in_url => bail!(
                        "http_bulk delete sends the ids in the path, so it takes no 'envelope' or 'line'"
                    ),
                    (Some(envelope), None) => {
                        let (before, after) = around(envelope, IDS, "delete.envelope")?;
                        DeleteBody::Array(before, after)
                    }
                    (None, Some(line)) => {
                        let (before, after) = around(line, LINE_ID, "delete.line")?;
                        DeleteBody::Lines(before, after)
                    }
                    (Some(_), Some(_)) => bail!("http_bulk delete sets both 'envelope' and 'line'"),
                };
                Some((request, delete.clone(), body))
            }
            None => None,
        };
        Ok(Self {
            http,
            base,
            headers,
            upsert: Request::new(
                upsert.method.as_deref(),
                &upsert.path,
                &upsert.result,
                "upsert",
            )?,
            format: upsert.format,
            content_type: HeaderValue::from_str(content_type)
                .context("http_bulk upsert.content_type is not a valid header value")?,
            action: upsert
                .action
                .as_deref()
                .map(|template| CompiledTemplate::compile(template, Some(JSON)))
                .transpose()
                .context("Invalid http_bulk upsert.action template")?,
            envelope,
            compression: config.compression,
            delete,
            operation: config
                .operation
                .as_deref()
                .map(|template| CompiledTemplate::compile(template, None))
                .transpose()
                .context("Invalid http_bulk operation template")?,
            delete_values: config.delete_values.clone(),
            max_request_bytes: config.max_request_bytes,
        })
    }

    fn operation(&self, message: &CanonicalMessage) -> ChangeOp {
        let Some(template) = &self.operation else {
            return ChangeOp::Upsert;
        };
        let rendered = template.render(Some(message));
        let operation = std::str::from_utf8(&rendered).ok().map(str::trim);
        ChangeOp::classify(operation.filter(|op| !op.is_empty()), &self.delete_values)
    }

    fn append_document(
        &self,
        body: &mut Vec<u8>,
        message: &CanonicalMessage,
    ) -> anyhow::Result<()> {
        let payload = message.payload.trim_ascii();
        if payload.first() != Some(&b'{') {
            bail!("the payload is not a JSON object");
        }
        match self.format {
            HttpBulkFormat::Ndjson => {
                if let Some(action) = &self.action {
                    body.extend_from_slice(&action.render(Some(message)));
                    body.push(b'\n');
                }
                ndjson::append_line(body, payload)
            }
            HttpBulkFormat::JsonArray => {
                if body.is_empty() {
                    body.extend_from_slice(self.envelope.0.as_bytes());
                    body.push(b'[');
                } else {
                    body.push(b',');
                }
                body.extend_from_slice(payload);
                Ok(())
            }
        }
    }

    fn close_body(&self, mut body: Vec<u8>) -> Vec<u8> {
        if self.format == HttpBulkFormat::JsonArray {
            body.push(b']');
            body.extend_from_slice(self.envelope.1.as_bytes());
        }
        body
    }

    /// Frames the run's payloads into request bodies no larger than the limit. A
    /// payload that is not a JSON object fails alone and is left out.
    fn upsert_chunks(
        &self,
        messages: &[CanonicalMessage],
        run: Range<usize>,
        outcomes: &mut [Option<PublisherError>],
    ) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut chunk = Chunk::default();
        for index in run {
            let message = &messages[index];
            let mark = chunk.body.len();
            if let Err(error) = self.append_document(&mut chunk.body, message) {
                chunk.body.truncate(mark);
                outcomes[index] = Some(PublisherError::NonRetryable(error));
                continue;
            }
            if mark > 0 && chunk.body.len() > self.max_request_bytes {
                chunk.body.truncate(mark);
                chunks.push(std::mem::take(&mut chunk));
                // The same payload was accepted a moment ago.
                let _ = self.append_document(&mut chunk.body, message);
            }
            chunk.indices.push(index);
        }
        if !chunk.indices.is_empty() {
            chunks.push(chunk);
        }
        chunks
    }

    async fn upsert(
        &self,
        messages: &[CanonicalMessage],
        run: Range<usize>,
        outcomes: &mut [Option<PublisherError>],
    ) -> Result<(), (usize, Failure)> {
        for chunk in self.upsert_chunks(messages, run, outcomes) {
            let url = format!("{}{}", self.base, self.upsert.path);
            let body = Some((self.content_type.clone(), self.close_body(chunk.body)));
            let sent = self
                .send(&self.upsert, &url, body, chunk.indices.len())
                .await;
            trace!(count = chunk.indices.len(), path = %self.upsert.path, "Upserted documents");
            record(sent, &chunk.indices, outcomes)?;
        }
        Ok(())
    }

    async fn delete(
        &self,
        messages: &[CanonicalMessage],
        run: Range<usize>,
        outcomes: &mut [Option<PublisherError>],
    ) -> Result<(), (usize, Failure)> {
        let Some((request, delete, shape)) = &self.delete else {
            let text = "http_bulk has no 'delete' request configured for a delete message";
            for index in run {
                outcomes[index] = Some(PublisherError::NonRetryable(anyhow!(text)));
            }
            return Ok(());
        };
        let mut ids: Vec<(usize, Value)> = Vec::new();
        for index in run {
            let id = serde_json::from_slice::<Value>(&messages[index].payload)
                .ok()
                .and_then(|mut document| document.get_mut(&delete.id_field).map(Value::take))
                .filter(|id| id.is_string() || id.is_number());
            match id {
                Some(id) => ids.push((index, id)),
                None => {
                    outcomes[index] = Some(PublisherError::NonRetryable(anyhow!(
                        "the delete message has no string or number in '{}'",
                        delete.id_field
                    )));
                }
            }
        }
        for chunk in ids.chunks(delete.max_ids) {
            let indices: Vec<usize> = chunk.iter().map(|(index, _)| *index).collect();
            let mut url = format!("{}{}", self.base, request.path);
            let body = match shape {
                DeleteBody::Url => {
                    let list: Vec<String> = chunk.iter().map(|(_, id)| id_in_url(id)).collect();
                    url = format!(
                        "{}{}",
                        self.base,
                        request.path.replace(IDS, &list.join(","))
                    );
                    None
                }
                DeleteBody::Array(before, after) => {
                    let list: Vec<&Value> = chunk.iter().map(|(_, id)| id).collect();
                    let body = format!(
                        "{before}{}{after}",
                        Value::from_iter(list.into_iter().cloned())
                    );
                    Some((HeaderValue::from_static(JSON), body.into_bytes()))
                }
                DeleteBody::Lines(before, after) => {
                    let body: String = chunk
                        .iter()
                        .map(|(_, id)| format!("{before}{id}{after}\n"))
                        .collect();
                    Some((HeaderValue::from_static(NDJSON), body.into_bytes()))
                }
            };
            let sent = self.send(request, &url, body, chunk.len()).await;
            trace!(count = chunk.len(), path = %request.path, "Deleted documents");
            record(sent, &indices, outcomes)?;
        }
        Ok(())
    }

    /// Sends one request and returns the documents the target rejected, as
    /// positions within the request and the reason.
    async fn send(
        &self,
        request: &Request,
        url: &str,
        body: Option<(HeaderValue, Vec<u8>)>,
        documents: usize,
    ) -> Result<Vec<(usize, String)>, Failure> {
        let label = format!("{} {}", request.method, request.path);
        let mut builder = self
            .http
            .request(request.method.clone(), url)
            .headers(self.headers.clone());
        if let Some((content_type, body)) = body {
            builder = builder.header(CONTENT_TYPE, content_type);
            builder = match self.compress(body).await {
                Ok((body, Some(encoding))) => builder.header(CONTENT_ENCODING, encoding).body(body),
                Ok((body, None)) => builder.body(body),
                Err(error) => {
                    return Err(Failure::permanent(format!(
                        "{label} body could not be compressed: {error}"
                    )))
                }
            };
        }
        let response = builder
            .send()
            .await
            .map_err(|e| Failure::retryable(format!("{label} failed: {e}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| Failure::retryable(format!("{label} response was cut off: {e}")))?;
        if !status.is_success() {
            return Err(Failure {
                retryable: http_status::is_retryable(status.as_u16()),
                text: format!("{label} answered {status}: {}", quoted(&text)),
            });
        }
        match &request.result {
            ResultMode::Status => Ok(Vec::new()),
            ResultMode::Lines(lines) => rejected_lines(lines, &text, documents, &label),
            ResultMode::Items(items) => rejected_items(items, &text, documents, &label),
            ResultMode::Job(job) => self
                .wait_for_job(job, &text, &label)
                .await
                .map(|()| Vec::new()),
        }
    }

    /// Compresses off the async workers: a body can be tens of MB.
    async fn compress(&self, body: Vec<u8>) -> std::io::Result<(Vec<u8>, Option<&'static str>)> {
        let compression = self.compression;
        if compression == Compression::None {
            return Ok((body, None));
        }
        tokio::task::spawn_blocking(move || {
            Ok(match compression {
                Compression::None => (body, None),
                Compression::Gzip => (gzip_default(&body)?, Some("gzip")),
                Compression::Zstd => (
                    zstd_pooled(&body, zstd::DEFAULT_COMPRESSION_LEVEL)?,
                    Some("zstd"),
                ),
                Compression::Lz4 => (lz4_pooled(&body)?, Some("lz4")),
            })
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn wait_for_job(
        &self,
        job: &HttpBulkJob,
        answer: &str,
        label: &str,
    ) -> Result<(), Failure> {
        let id = serde_json::from_str::<Value>(answer)
            .ok()
            .and_then(|answer| answer.pointer(&job.id).and_then(scalar_text))
            .ok_or_else(|| {
                Failure::permanent(format!("{label} answered without a job id at '{}'", job.id))
            })?;
        let url = format!("{}{}", self.base, job.poll.replace(JOB_ID, &id));
        let (this, url, id) = (self, url.as_str(), id.as_str());
        poll_until(
            PollSchedule::default(),
            |waited| warn!(job = id, waited = ?waited, "http_bulk job is still running"),
            || async move {
                let response = this
                    .http
                    .get(url)
                    .headers(this.headers.clone())
                    .send()
                    .await
                    .ok()?;
                let status = response.status();
                let text = response.text().await.ok()?;
                if !status.is_success() {
                    return (!http_status::is_retryable(status.as_u16())).then(|| {
                        Err(Failure::permanent(format!(
                            "polling job {id} answered {status}: {}",
                            quoted(&text)
                        )))
                    });
                }
                let state: Option<Value> = serde_json::from_str(&text).ok();
                let name = state.as_ref().and_then(|state| state.pointer(&job.status));
                // A wrong pointer would otherwise be polled forever.
                let (Some(state), Some(name)) = (&state, name.and_then(Value::as_str)) else {
                    return Some(Err(Failure::permanent(format!(
                        "polling job {id} gave no state at '{}': {}",
                        job.status,
                        quoted(&text)
                    ))));
                };
                if job.succeeded.iter().any(|value| value == name) {
                    Some(Ok(()))
                } else if job.failed.iter().any(|value| value == name) {
                    let reason = job
                        .error
                        .as_deref()
                        .and_then(|pointer| state.pointer(pointer))
                        .and_then(scalar_text)
                        .unwrap_or_else(|| name.to_string());
                    Some(Err(Failure::permanent(format!(
                        "job {id} {name}: {reason}"
                    ))))
                } else {
                    None
                }
            },
        )
        .await
    }
}

/// Writes the outcome of one request into `outcomes`. A request that may be
/// retried stops the batch, since later messages may depend on it; one that
/// failed for good costs only its own documents.
fn record(
    sent: Result<Vec<(usize, String)>, Failure>,
    indices: &[usize],
    outcomes: &mut [Option<PublisherError>],
) -> Result<(), (usize, Failure)> {
    match sent {
        Ok(rejected) => {
            for (position, reason) in rejected {
                outcomes[indices[position]] =
                    Some(PublisherError::NonRetryable(anyhow!("{reason}")));
            }
            Ok(())
        }
        Err(failure) if failure.retryable => Err((indices[0], failure)),
        Err(failure) => {
            for index in indices {
                outcomes[*index] = Some(failure.error());
            }
            Ok(())
        }
    }
}

fn quoted(text: &str) -> String {
    match text.char_indices().nth(QUOTED_RESPONSE_CHARS) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn id_in_url(id: &Value) -> String {
    let text = scalar_text(id).unwrap_or_default();
    utf8_percent_encode(&text, NON_ALPHANUMERIC).to_string()
}

fn rejected_lines(
    lines: &HttpBulkLines,
    text: &str,
    documents: usize,
    label: &str,
) -> Result<Vec<(usize, String)>, Failure> {
    let answers: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    if answers.len() != documents {
        return Err(Failure::permanent(format!(
            "{label} answered {} result lines for {documents} documents",
            answers.len()
        )));
    }
    let mut rejected = Vec::new();
    for (position, answer) in answers.into_iter().enumerate() {
        let parsed: Option<Value> = serde_json::from_str(answer).ok();
        let written = parsed
            .as_ref()
            .and_then(|value| value.pointer(&lines.success))
            .and_then(Value::as_bool);
        if written != Some(true) {
            let reason = lines
                .error
                .as_deref()
                .and_then(|pointer| parsed.as_ref()?.pointer(pointer))
                .and_then(scalar_text)
                .unwrap_or_else(|| answer.to_string());
            rejected.push((position, reason));
        }
    }
    Ok(rejected)
}

fn rejected_items(
    items: &HttpBulkItems,
    text: &str,
    documents: usize,
    label: &str,
) -> Result<Vec<(usize, String)>, Failure> {
    let answer: Option<Value> = serde_json::from_str(text).ok();
    let entries = answer
        .as_ref()
        .and_then(|answer| answer.pointer(&items.path))
        .and_then(Value::as_array);
    let Some(entries) = entries.filter(|entries| entries.len() == documents) else {
        return Err(Failure::permanent(format!(
            "{label} answered no array of {documents} entries at '{}': {}",
            items.path,
            quoted(text)
        )));
    };
    let failed = |entry: &Value| match entry.pointer(&items.error) {
        None | Some(Value::Null) => None,
        Some(reason) => Some(scalar_text(reason).unwrap_or_else(|| reason.to_string())),
    };
    Ok(entries
        .iter()
        .enumerate()
        .filter_map(|(position, entry)| Some((position, failed(entry)?)))
        .collect())
}

#[async_trait]
impl MessagePublisher for HttpBulkPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        let operations: Vec<ChangeOp> = messages.iter().map(|m| self.operation(m)).collect();
        let mut outcomes: Vec<Option<PublisherError>> = messages.iter().map(|_| None).collect();
        let mut next = 0;
        while next < messages.len() {
            let operation = operations[next];
            let length = operations[next..]
                .iter()
                .take_while(|other| **other == operation)
                .count();
            let run = next..next + length;
            next = run.end;
            let sent = match operation {
                ChangeOp::Upsert => self.upsert(&messages, run, &mut outcomes).await,
                ChangeOp::Delete => self.delete(&messages, run, &mut outcomes).await,
                ChangeOp::Truncate => {
                    for index in run {
                        outcomes[index] = Some(PublisherError::NonRetryable(anyhow!(
                            "http_bulk cannot apply a truncate"
                        )));
                    }
                    Ok(())
                }
            };
            if let Err((from, failure)) = sent {
                for outcome in &mut outcomes[from..] {
                    outcome.get_or_insert_with(|| failure.error());
                }
                break;
            }
        }
        Ok(SentBatch::from_outcomes(messages, outcomes))
    }

    /// With `operation` set the messages are changes, and a delete must not
    /// overtake the upsert of the batch before it.
    fn requires_ordered_publish(&self) -> bool {
        self.operation.is_some()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(all(test, feature = "plugin", feature = "test-utils"))]
mod tests {
    use super::*;
    use crate::plugin::test_support::{StubHttpServer, StubRequest};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn server(
        respond: impl Fn(&StubRequest) -> (u16, String) + Send + Sync + 'static,
    ) -> StubHttpServer {
        StubHttpServer::start(respond).await.expect("stub server")
    }

    fn publisher(server: &StubHttpServer, mut config: Value) -> HttpBulkPublisher {
        config["url"] = json!(server.url());
        let config: HttpBulkConfig = serde_json::from_value(config).expect("config");
        HttpBulkPublisher::new(&config).expect("publisher")
    }

    fn documents(payloads: &[&str]) -> Vec<CanonicalMessage> {
        payloads
            .iter()
            .map(|p| CanonicalMessage::from(*p))
            .collect()
    }

    /// The failed payloads with whether each may be retried, and the error text.
    fn failures(sent: SentBatch) -> Vec<(String, bool, String)> {
        match sent {
            SentBatch::Ack => Vec::new(),
            SentBatch::Partial { failed, .. } => failed
                .into_iter()
                .map(|(message, error)| {
                    let payload = String::from_utf8_lossy(&message.payload).into_owned();
                    let retryable = matches!(error, PublisherError::Retryable(_));
                    (payload, retryable, error.to_string())
                })
                .collect(),
        }
    }

    fn calls(server: &StubHttpServer) -> Vec<(String, String, String)> {
        server
            .requests()
            .into_iter()
            .map(|r| (r.method, r.target, String::from_utf8(r.body).unwrap()))
            .collect()
    }

    #[tokio::test]
    async fn a_batch_is_one_ndjson_request_and_a_bad_payload_fails_alone() {
        let server = server(|_| (200, String::new())).await;
        let publisher = publisher(
            &server,
            json!({"headers": {"x-key": "secret"}, "upsert": {"path": "/docs?mode=upsert"}}),
        );
        let sent = publisher
            .send_batch(documents(&[r#"{"id":1}"#, "not json", r#"{"id":2}"#]))
            .await
            .unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert_eq!((failed[0].0.as_str(), failed[0].1), ("not json", false));
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].target, "/docs?mode=upsert");
        assert_eq!(
            requests[0].header("content-type"),
            Some("application/x-ndjson")
        );
        assert_eq!(requests[0].header("x-key"), Some("secret"));
        assert_eq!(requests[0].body, b"{\"id\":1}\n{\"id\":2}\n");
    }

    #[tokio::test]
    async fn a_batch_over_the_size_limit_is_split_and_json_array_stays_valid() {
        let server = server(|_| (200, String::new())).await;
        let publisher = publisher(
            &server,
            json!({"max_request_bytes": 20, "upsert": {"path": "/docs", "method": "put", "format": "json_array"}}),
        );
        let sent = publisher
            .send_batch(documents(&[r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":3}"#]))
            .await
            .unwrap();

        assert!(matches!(sent, SentBatch::Ack));
        let bodies: Vec<String> = calls(&server).into_iter().map(|call| call.2).collect();
        assert_eq!(bodies, [r#"[{"id":1},{"id":2}]"#, r#"[{"id":3}]"#]);
        assert_eq!(server.requests()[0].method, "PUT");
        assert_eq!(
            server.requests()[0].header("content-type"),
            Some("application/json")
        );
    }

    #[tokio::test]
    async fn a_result_line_per_document_fails_only_the_rejected_one() {
        let server = server(|_| {
            let body = "{\"success\":true}\n{\"success\":false,\"error\":\"Bad field\"}\n";
            (200, body.to_string())
        })
        .await;
        let publisher = publisher(
            &server,
            json!({"upsert": {"path": "/import", "content_type": "text/plain",
                "result": {"lines": {"success": "/success", "error": "/error"}}}}),
        );
        let sent = publisher
            .send_batch(documents(&[r#"{"id":1}"#, r#"{"id":2}"#]))
            .await
            .unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert_eq!((failed[0].0.as_str(), failed[0].1), (r#"{"id":2}"#, false));
        assert!(failed[0].2.contains("Bad field"), "{}", failed[0].2);
        assert_eq!(
            server.requests()[0].header("content-type"),
            Some("text/plain")
        );
    }

    fn job_config() -> Value {
        json!({"upsert": {"path": "/docs", "result": {"job": {
            "id": "/taskUid", "poll": "/tasks/{id}", "status": "/status",
            "succeeded": ["succeeded"], "failed": ["failed", "canceled"],
            "error": "/error/message"}}}})
    }

    #[tokio::test]
    async fn a_job_is_polled_until_it_succeeds() {
        let polls = Arc::new(AtomicUsize::new(0));
        let seen = polls.clone();
        let server = server(move |request| match request.method.as_str() {
            "POST" => (202, r#"{"taskUid":7}"#.to_string()),
            _ if seen.fetch_add(1, Ordering::SeqCst) < 2 => {
                (200, r#"{"status":"processing"}"#.to_string())
            }
            _ => (200, r#"{"status":"succeeded"}"#.to_string()),
        })
        .await;
        let sent = publisher(&server, job_config())
            .send_batch(documents(&[r#"{"id":1}"#]))
            .await
            .unwrap();

        assert!(matches!(sent, SentBatch::Ack));
        assert_eq!(polls.load(Ordering::SeqCst), 3);
        assert_eq!(server.requests()[1].target, "/tasks/7");
    }

    #[tokio::test]
    async fn a_failed_job_fails_its_documents_for_good_with_the_reason() {
        let server = server(|request| match request.method.as_str() {
            "POST" => (202, r#"{"taskUid":"a1"}"#.to_string()),
            _ => (
                200,
                r#"{"status":"failed","error":{"message":"no primary key"}}"#.to_string(),
            ),
        })
        .await;
        let sent = publisher(&server, job_config())
            .send_batch(documents(&[r#"{"id":1}"#, r#"{"id":2}"#]))
            .await
            .unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 2);
        assert!(failed
            .iter()
            .all(|f| !f.1 && f.2.contains("no primary key")));
    }

    #[tokio::test]
    async fn deletes_keep_their_place_between_upserts() {
        let server = server(|_| (200, String::new())).await;
        let publisher = publisher(
            &server,
            json!({"operation": "${metadata:op}", "upsert": {"path": "/docs"},
                "delete": {"path": "/docs/delete-batch", "id_field": "key"}}),
        );
        let mut batch = documents(&[
            r#"{"key":1}"#,
            r#"{"key":1}"#,
            r#"{"key":"b"}"#,
            r#"{"other":3}"#,
            r#"{"key":4}"#,
        ]);
        for message in &mut batch[1..=3] {
            *message = message.clone().with_metadata_kv("op", "DELETE");
        }
        let sent = publisher.send_batch(batch).await.unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert_eq!(
            (failed[0].0.as_str(), failed[0].1),
            (r#"{"other":3}"#, false)
        );
        let expected = [
            ("POST", "/docs", "{\"key\":1}\n"),
            ("POST", "/docs/delete-batch", r#"[1,"b"]"#),
            ("POST", "/docs", "{\"key\":4}\n"),
        ];
        let calls = calls(&server);
        let got: Vec<_> = calls
            .iter()
            .map(|c| (c.0.as_str(), c.1.as_str(), c.2.as_str()))
            .collect();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn ids_go_into_the_url_when_the_path_asks_for_them() {
        let server = server(|_| (200, r#"{"num_deleted":2}"#.to_string())).await;
        let publisher = publisher(
            &server,
            json!({"operation": "${metadata:op}", "upsert": {"path": "/docs"},
                "delete": {"method": "DELETE", "path": "/docs?filter_by=id:[{ids}]", "max_ids": 2}}),
        );
        let batch = documents(&[r#"{"id":"a b"}"#, r#"{"id":2}"#, r#"{"id":3}"#])
            .into_iter()
            .map(|message| message.with_metadata_kv("op", "d"))
            .collect();
        let sent = publisher.send_batch(batch).await.unwrap();

        assert!(matches!(sent, SentBatch::Ack));
        let calls = calls(&server);
        assert_eq!(calls[0].0, "DELETE");
        assert_eq!(calls[0].1, "/docs?filter_by=id:[a%20b,2]");
        assert_eq!(calls[1].1, "/docs?filter_by=id:[3]");
        assert!(calls.iter().all(|call| call.2.is_empty()));
    }

    #[tokio::test]
    async fn a_busy_target_fails_the_rest_of_the_batch_as_retryable() {
        let server = server(|request| match request.target.as_str() {
            "/docs" => (200, String::new()),
            _ => (503, "busy".to_string()),
        })
        .await;
        let publisher = publisher(
            &server,
            json!({"operation": "${metadata:op}", "upsert": {"path": "/docs"},
                "delete": {"path": "/delete"}}),
        );
        let mut batch = documents(&[r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":3}"#]);
        batch[1] = batch[1].clone().with_metadata_kv("op", "delete");
        let sent = publisher.send_batch(batch).await.unwrap();

        let failed = failures(sent);
        let payloads: Vec<&str> = failed.iter().map(|f| f.0.as_str()).collect();
        assert_eq!(payloads, [r#"{"id":2}"#, r#"{"id":3}"#]);
        assert!(failed.iter().all(|f| f.1));
        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn a_request_refused_for_good_costs_only_its_own_documents() {
        let server = server(|request| match request.body.starts_with(b"{\"id\":1}") {
            true => (400, "x".repeat(2000)),
            false => (200, String::new()),
        })
        .await;
        let publisher = publisher(
            &server,
            json!({"max_request_bytes": 10, "upsert": {"path": "/docs"}}),
        );
        let sent = publisher
            .send_batch(documents(&[r#"{"id":1}"#, r#"{"id":2}"#]))
            .await
            .unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert_eq!((failed[0].0.as_str(), failed[0].1), (r#"{"id":1}"#, false));
        assert!(failed[0].2.len() < 700, "{}", failed[0].2.len());
        assert_eq!(server.requests().len(), 2);
    }

    #[tokio::test]
    async fn a_poll_answer_without_the_state_fails_instead_of_polling_forever() {
        let server = server(|request| match request.method.as_str() {
            "POST" => (202, r#"{"taskUid":7}"#.to_string()),
            _ => (200, r#"{"state":"done"}"#.to_string()),
        })
        .await;
        let sent = publisher(&server, job_config())
            .send_batch(documents(&[r#"{"id":1}"#]))
            .await
            .unwrap();

        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert!(failed[0].2.contains("/status"), "{}", failed[0].2);
    }

    #[tokio::test]
    async fn only_a_change_stream_asks_for_ordered_batches() {
        let server = server(|_| (200, String::new())).await;
        let plain = publisher(&server, json!({"upsert": {"path": "/docs"}}));
        let changes = publisher(
            &server,
            json!({"operation": "${metadata:op}", "upsert": {"path": "/docs"}}),
        );
        assert!(!plain.requires_ordered_publish());
        assert!(changes.requires_ordered_publish());
    }

    #[tokio::test]
    async fn an_action_line_precedes_each_document_and_array_entries_report_failures() {
        let server = server(|_| {
            let items = json!({"errors": true, "items": [
                {"index": {"status": 201}},
                {"index": {"status": 400, "error": {"reason": "bad year"}}},
            ]});
            (200, items.to_string())
        })
        .await;
        let publisher = publisher(
            &server,
            json!({"upsert": {
                "path": "/_bulk",
                "action": r#"{"index":{"_id":"${payload:id}"}}"#,
                "result": {"items": {"path": "/items", "error": "/index/error/reason"}},
            }}),
        );
        let sent = publisher
            .send_batch(documents(&[r#"{"id":"a\"b"}"#, r#"{"id":2}"#]))
            .await
            .unwrap();

        assert_eq!(
            calls(&server)[0].2,
            "{\"index\":{\"_id\":\"a\\\"b\"}}\n{\"id\":\"a\\\"b\"}\n{\"index\":{\"_id\":\"2\"}}\n{\"id\":2}\n"
        );
        let failed = failures(sent);
        assert_eq!(failed.len(), 1);
        assert_eq!((failed[0].0.as_str(), failed[0].1), (r#"{"id":2}"#, false));
        assert!(failed[0].2.contains("bad year"), "{}", failed[0].2);
    }

    #[tokio::test]
    async fn an_envelope_wraps_the_documents_and_the_ids() {
        let server = server(|_| (200, "[]".to_string())).await;
        let publisher = publisher(
            &server,
            json!({
                "operation": "${metadata:op}",
                "upsert": {"path": "/points", "method": "PUT", "format": "json_array",
                    "envelope": r#"{"points": {documents}, "wait": true}"#},
                "delete": {"path": "/points/delete", "envelope": r#"{"points": {ids}}"#},
            }),
        );
        let mut batch = documents(&[r#"{"id":1}"#, r#"{"id":2}"#, r#"{"id":"x"}"#]);
        batch[2] = batch[2].clone().with_metadata_kv("op", "delete");
        assert!(failures(publisher.send_batch(batch).await.unwrap()).is_empty());

        let calls = calls(&server);
        assert_eq!(
            calls[0].2,
            r#"{"points": [{"id":1},{"id":2}], "wait": true}"#
        );
        assert_eq!(calls[1].2, r#"{"points": ["x"]}"#);
    }

    #[tokio::test]
    async fn a_delete_line_per_id_and_a_compressed_body() {
        let server = server(|_| (200, String::new())).await;
        let publisher = publisher(
            &server,
            json!({
                "operation": "${metadata:op}",
                "compression": "gzip",
                "upsert": {"path": "/_bulk"},
                "delete": {"path": "/_bulk", "line": r#"{"delete":{"_id":{id}}}"#},
            }),
        );
        let batch = documents(&[r#"{"id":1}"#, r#"{"id":"x"}"#])
            .into_iter()
            .map(|message| message.with_metadata_kv("op", "delete"))
            .collect();
        assert!(failures(publisher.send_batch(batch).await.unwrap()).is_empty());

        let request = &server.requests()[0];
        assert_eq!(request.header("content-encoding"), Some("gzip"));
        assert_eq!(request.header("content-type"), Some(NDJSON));
        let mut body = String::new();
        std::io::Read::read_to_string(
            &mut flate2::read::GzDecoder::new(request.body.as_slice()),
            &mut body,
        )
        .unwrap();
        assert_eq!(
            body,
            "{\"delete\":{\"_id\":1}}\n{\"delete\":{\"_id\":\"x\"}}\n"
        );
    }

    #[test]
    fn the_recipes_in_the_book_are_valid_configurations() {
        let book = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/apps/mq-bridge-app/dev/docs/connectors/"
        );
        for (name, recipes) in [
            ("http-bulk.md", 7),
            ("typesense.md", 2),
            ("elasticsearch.md", 2),
            ("postgrest.md", 2),
        ] {
            // The book is not part of the published crate.
            let Ok(page) = std::fs::read_to_string(format!("{book}{name}")) else {
                return;
            };
            let blocks = page.split("```yaml\n").skip(1);
            let mut found = 0;
            for recipe in blocks.filter_map(|rest| rest.split("```").next()) {
                let route: Value = serde_yaml_ng::from_str(recipe).expect("yaml");
                // Either an `input:` or `output:` fragment or a whole named route.
                let route = match route.get("output").or(route.get("input")) {
                    Some(_) => &route,
                    None => route.as_object().unwrap().values().next().unwrap(),
                };
                found += 1;
                // An `input:` fragment is a read recipe; its consumer is built without a server.
                if let Some(input) = route.get("input").and_then(|i| i.get("http_bulk")) {
                    let mut config: HttpBulkConfig = serde_json::from_value(input.clone())
                        .unwrap_or_else(|error| panic!("{name}: {error}"));
                    if let Some(read) = &mut config.read {
                        read.checkpoint_store = None;
                    }
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap()
                        .block_on(HttpBulkConsumer::new(&config, true))
                        .unwrap_or_else(|error| panic!("{name}: {error}"));
                    continue;
                }
                let output = &route["output"];
                let config = match output.get("custom") {
                    Some(custom) => {
                        presets::resolve(custom["name"].as_str().unwrap(), &custom["config"])
                    }
                    None => serde_json::from_value(output["http_bulk"].clone()).map_err(Into::into),
                };
                let config: HttpBulkConfig =
                    config.unwrap_or_else(|error| panic!("{name}: {error:#}"));
                HttpBulkPublisher::new(&config).unwrap_or_else(|error| panic!("{name}: {error}"));
            }
            assert_eq!(found, recipes, "{name}");
        }
    }

    #[test]
    fn a_config_that_cannot_work_is_refused_at_startup() {
        let refused = |config: Value| {
            let config: HttpBulkConfig = serde_json::from_value(config).expect("config");
            HttpBulkPublisher::new(&config)
                .err()
                .expect("refused")
                .to_string()
        };
        let lines = json!({"success": "/ok"});
        let job =
            json!({"id": "/id", "poll": "/jobs", "status": "/s", "succeeded": [], "failed": []});
        assert!(refused(json!({"url": "localhost", "upsert": {"path": "/d"}})).contains("URL"));
        assert!(
            refused(json!({"url": "http://h?a=1", "upsert": {"path": "/d"}})).contains("query")
        );
        assert!(refused(json!({"url": "http://h", "upsert": {"path": "d"}})).contains("path"));
        assert!(refused(json!({"url": "http://h", "upsert": {"path": "/d",
            "result": {"job": job.clone()}}}))
        .contains("{id}"));
        assert!(refused(json!({"url": "http://h", "upsert": {"path": "/d",
            "result": {"lines": lines, "job": job}}}))
        .contains("more than one"));
        for (upsert, expected) in [
            (
                json!({"path": "/d", "action": "{}", "format": "json_array"}),
                "ndjson",
            ),
            (
                json!({"path": "/d", "envelope": "{documents}"}),
                "json_array",
            ),
            (
                json!({"path": "/d", "format": "json_array", "envelope": "{}"}),
                "exactly once",
            ),
        ] {
            let error = refused(json!({"url": "http://h", "upsert": upsert}));
            assert!(error.contains(expected), "{error}");
        }
        for (delete, expected) in [
            (
                json!({"path": "/d", "envelope": "{ids}", "line": "{id}"}),
                "both",
            ),
            (json!({"path": "/d/{ids}", "line": "{id}"}), "in the path"),
            (json!({"path": "/d", "line": "x"}), "exactly once"),
        ] {
            let config = json!({"url": "http://h", "upsert": {"path": "/d"}, "delete": delete});
            let error = refused(config);
            assert!(error.contains(expected), "{error}");
        }
    }
}
