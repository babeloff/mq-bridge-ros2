//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Named endpoints that are an `http_bulk` configuration: a few fields and a
//! URI of their own, and the generic publisher underneath.

use super::{HttpBulkConsumer, HttpBulkPublisher};
use crate::errors::InvalidConfig;
use crate::models::{Compression, HttpBulkConfig};
use crate::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};
use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

/// One named endpoint: the schema it shows and how its config becomes `http_bulk`.
#[derive(Debug)]
struct Preset {
    name: &'static str,
    schema: fn() -> Value,
    /// Takes the route name and the endpoint's config.
    resolve: fn(&str, &Value) -> anyhow::Result<Value>,
}

const PRESETS: [Preset; 3] = [
    Preset {
        name: "elasticsearch",
        schema: elasticsearch_schema,
        resolve: elasticsearch,
    },
    Preset {
        name: "meilisearch",
        schema: meilisearch_schema,
        resolve: meilisearch,
    },
    Preset {
        name: "typesense",
        schema: typesense_schema,
        resolve: typesense,
    },
];

/// The endpoint names [`register_preset`] accepts.
pub fn preset_names() -> impl Iterator<Item = &'static str> {
    PRESETS.iter().map(|preset| preset.name)
}

/// Registers the named endpoint, e.g. `typesense`, as a custom endpoint factory.
pub fn register_preset(name: &str) -> anyhow::Result<()> {
    let preset = PRESETS
        .into_iter()
        .find(|preset| preset.name == name)
        .ok_or_else(|| anyhow!("no http_bulk preset is named `{name}`"))?;
    crate::extensions::register_endpoint_factory(name, Arc::new(preset))
}

/// The `http_bulk` configuration a named endpoint stands for.
#[cfg(test)]
pub(super) fn resolve(name: &str, config: &Value) -> anyhow::Result<HttpBulkConfig> {
    let preset = PRESETS.into_iter().find(|preset| preset.name == name);
    preset
        .ok_or_else(|| anyhow!("no http_bulk preset is named `{name}`"))?
        .config("route", config)
}

impl Preset {
    fn config(&self, route_name: &str, value: &Value) -> anyhow::Result<HttpBulkConfig> {
        let resolved = (self.resolve)(route_name, value).map_err(InvalidConfig)?;
        serde_json::from_value(resolved)
            .with_context(|| format!("{} as http_bulk", self.name))
            .map_err(|error| InvalidConfig(error).into())
    }
}

#[async_trait]
impl CustomEndpointFactory for Preset {
    async fn create_consumer(
        &self,
        route_name: &str,
        config: &Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        let config = self.config(route_name, config)?;
        if config.read.is_none() {
            return Err(InvalidConfig(anyhow!(
                "{} is an output only; read it through http_bulk with a 'read' request",
                self.name
            ))
            .into());
        }
        Ok(Box::new(HttpBulkConsumer::new(&config, false).await?))
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        config: &Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        let config = self.config(route_name, config)?;
        Ok(Box::new(HttpBulkPublisher::new(&config)?))
    }

    fn config_schema(&self) -> Option<Value> {
        Some((self.schema)())
    }
}

/// `name://host` is plain HTTP, `name+https://` and `names://` are HTTPS; `http(s)://` passes.
fn base_url(name: &str, url: &str) -> anyhow::Result<String> {
    let url = url.trim_end_matches('/');
    let rest = url.strip_prefix(name).unwrap_or(url);
    let rest = rest.strip_prefix('+').unwrap_or(rest);
    match rest.split_once("://") {
        Some(("", host)) if rest.len() < url.len() => Ok(format!("http://{host}")),
        Some(("s", host)) if rest.len() < url.len() => Ok(format!("https://{host}")),
        Some(("http" | "https", _)) => Ok(rest.to_string()),
        _ => bail!("'url' must start with {name}://, {name}+https://, http:// or https://"),
    }
}

/// A collection or index name as one path segment.
fn segment<'a>(field: &str, name: &'a str) -> anyhow::Result<&'a str> {
    if name.is_empty() || name.contains(['/', '?', '#', ' ']) {
        bail!("'{field}' must be one name without '/', '?', '#' or spaces, got '{name}'");
    }
    Ok(name)
}

fn default_id_field() -> String {
    "id".to_string()
}

/// The fields every preset shares, as schema properties.
fn common_properties(example: &str) -> serde_json::Map<String, Value> {
    let properties = json!({
        "url": {
            "type": "string",
            "description": format!("Address of the server, e.g. `{example}`."),
            "x-mqb-uri": "origin"
        },
        "api_key": {
            "type": "string",
            "format": "password",
            "description": "API key sent with every request."
        },
        "operation": {
            "type": "string",
            "description": "Template for a message's operation, e.g. `${metadata:postgres.operation}`."
        },
        "compression": {
            "type": "string",
            "enum": ["none", "gzip", "zstd", "lz4"],
            "default": "none",
            "description": "Compression of request bodies."
        },
        "request_timeout_ms": {
            "type": "integer",
            "description": "Request timeout in milliseconds. Unset = no timeout."
        }
    });
    match properties {
        Value::Object(map) => map,
        _ => unreachable!("a JSON object literal"),
    }
}

fn schema(title: &str, example: &str, own: Value, required: &[&str]) -> Value {
    let mut properties = common_properties(example);
    if let Value::Object(own) = own {
        properties.extend(own);
    }
    json!({
        "title": title,
        "type": "object",
        "additionalProperties": false,
        "properties": properties,
        "required": required,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Typesense {
    url: String,
    collection: String,
    api_key: Option<String>,
    operation: Option<String>,
    #[serde(default)]
    compression: Compression,
    request_timeout_ms: Option<u64>,
}

fn typesense_schema() -> Value {
    schema(
        "Typesense",
        "typesense://localhost:8108",
        json!({
            "collection": {
                "type": "string",
                "description": "Collection the documents are written to. It must exist.",
                "x-mqb-uri": "path"
            }
        }),
        &["url", "collection"],
    )
}

fn typesense(_route: &str, value: &Value) -> anyhow::Result<Value> {
    let config: Typesense = serde_json::from_value(value.clone())?;
    let collection = segment("collection", &config.collection)?;
    let mut headers = serde_json::Map::new();
    if let Some(key) = config.api_key {
        headers.insert("X-TYPESENSE-API-KEY".into(), json!(key));
    }
    Ok(json!({
        "url": base_url("typesense", &config.url)?,
        "headers": headers,
        "operation": config.operation,
        "compression": config.compression,
        "request_timeout_ms": config.request_timeout_ms,
        "upsert": {
            "path": format!("/collections/{collection}/documents/import?action=upsert"),
            "content_type": "text/plain",
            "result": {"lines": {"success": "/success", "error": "/error"}}
        },
        "delete": {
            "method": "DELETE",
            "path": format!("/collections/{collection}/documents?filter_by=id:[{{ids}}]"),
            "max_ids": 100
        }
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Elasticsearch {
    url: String,
    index: String,
    api_key: Option<String>,
    #[serde(default = "default_id_field")]
    id_field: String,
    operation: Option<String>,
    #[serde(default)]
    compression: Compression,
    request_timeout_ms: Option<u64>,
}

fn elasticsearch_schema() -> Value {
    schema(
        "Elasticsearch",
        "elasticsearch://localhost:9200",
        json!({
            "index": {
                "type": "string",
                "description": "Index the documents are written to.",
                "x-mqb-uri": "path"
            },
            "id_field": {
                "type": "string",
                "default": "id",
                "description": "Top-level payload field that becomes the document `_id`."
            }
        }),
        &["url", "index"],
    )
}

fn elasticsearch(_route: &str, value: &Value) -> anyhow::Result<Value> {
    let config: Elasticsearch = serde_json::from_value(value.clone())?;
    let index = segment("index", &config.index)?;
    let id_field = &config.id_field;
    if id_field.is_empty() || id_field.contains(['}', '"', '\\']) {
        bail!("'id_field' must be a plain field name, got '{id_field}'");
    }
    let mut headers = serde_json::Map::new();
    if let Some(key) = config.api_key {
        headers.insert("Authorization".into(), json!(format!("ApiKey {key}")));
    }
    let path = format!("/{index}/_bulk");
    Ok(json!({
        "url": base_url("elasticsearch", &config.url)?,
        "headers": headers,
        "operation": config.operation,
        "compression": config.compression,
        "request_timeout_ms": config.request_timeout_ms,
        "upsert": {
            "path": path,
            "action": format!(r#"{{"index":{{"_id":"${{payload:{id_field}}}"}}}}"#),
            "result": {"items": {"path": "/items", "error": "/index/error/reason"}}
        },
        "delete": {
            "path": path,
            "id_field": id_field,
            "line": r#"{"delete":{"_id":{id}}}"#,
            "result": {"items": {"path": "/items", "error": "/delete/error/reason"}}
        }
    }))
}

/// Options of the `mq-bridge-meilisearch` plugin that this endpoint leaves out.
const MEILISEARCH_PLUGIN_ONLY: [&str; 8] = [
    "settings",
    "method",
    "update_where",
    "update_into",
    "swap_into",
    "create_index",
    "wait_for_task",
    "task_timeout_ms",
];

fn plugin_only(what: &str) -> anyhow::Error {
    anyhow!(
        "{what} needs the mq-bridge-meilisearch plugin: install it and set \
         MQB_PLUGIN_OVERRIDE=meilisearch"
    )
}

fn default_meilisearch_request_bytes() -> u64 {
    90_000_000
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Meilisearch {
    url: String,
    api_key: Option<String>,
    index: Option<String>,
    primary_key: Option<String>,
    operation: Option<String>,
    delete_values: Option<Vec<String>>,
    #[serde(default = "default_meilisearch_request_bytes")]
    max_request_bytes: u64,
    #[serde(default)]
    compression: Compression,
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    fields: Option<String>,
    cursor_id: Option<String>,
    checkpoint_store: Option<String>,
    polling_interval_ms: Option<u64>,
    max_polling_interval_ms: Option<u64>,
}

fn meilisearch_schema() -> Value {
    let mut schema = schema(
        "Meilisearch",
        "meilisearch://localhost:7700",
        json!({
            "url": {
                "type": "string",
                "description": "Address of the server, e.g. `meilisearch://localhost:7700`.",
                "x-mqb-uri": "url"
            },
            "index": {
                "type": "string",
                "description": "Index UID. Defaults to the route name."
            },
            "primary_key": {
                "type": "string",
                "description": "The document field Meilisearch keys documents by."
            },
            "delete_values": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Operation values that mean delete. Defaults to `delete` and `d`."
            },
            "max_request_bytes": {
                "type": "integer",
                "default": default_meilisearch_request_bytes(),
                "description": "Largest request body in bytes; a bigger batch is split."
            },
            "connect_timeout_ms": {
                "type": "integer",
                "description": "Connection timeout in milliseconds. Defaults to 10000ms."
            },
            "fields": {
                "type": "string",
                "description": "(Consumer only) Comma-separated document fields to read. Default: all."
            },
            "cursor_id": {
                "type": "string",
                "description": "(Consumer only) Cursor id that keys the saved read position."
            },
            "checkpoint_store": {
                "type": "string",
                "format": "password",
                "description": "(Consumer only) Where the read position is saved, e.g. `file:///var/lib/mqb/cursors.json`."
            },
            "polling_interval_ms": {
                "type": "integer",
                "description": "(Consumer only) Wait in milliseconds after an empty page. Defaults to 1000ms."
            },
            "max_polling_interval_ms": {
                "type": "integer",
                "description": "(Consumer only) If set, the wait doubles after each empty page up to this value."
            }
        }),
        &["url"],
    );
    // Meilisearch indexes one large request far faster than many small ones.
    schema["x-mqb-default-batch-size"] = json!(50_000);
    schema
}

fn meilisearch(route: &str, value: &Value) -> anyhow::Result<Value> {
    if let Some(field) = MEILISEARCH_PLUGIN_ONLY
        .iter()
        .find(|field| value.get(**field).is_some())
    {
        return Err(plugin_only(&format!("'{field}'")));
    }
    let config: Meilisearch = serde_json::from_value(value.clone())?;
    let index = config.index.as_deref().unwrap_or(route);
    if index.contains("${") {
        return Err(plugin_only("a template in 'index'"));
    }
    let index = segment("index", index)?;
    let mut upsert = format!("/indexes/{index}/documents");
    if let Some(key) = &config.primary_key {
        upsert = format!("{upsert}?primaryKey={}", segment("primary_key", key)?);
    }
    let mut read = format!("/indexes/{index}/documents?offset={{cursor}}&limit={{limit}}");
    if let Some(fields) = &config.fields {
        if fields.contains(['&', '#', ' ']) {
            bail!("'fields' must be field names separated by commas, got '{fields}'");
        }
        read = format!("{read}&fields={fields}");
    }
    let mut headers = serde_json::Map::new();
    if let Some(key) = config.api_key {
        headers.insert("Authorization".into(), json!(format!("Bearer {key}")));
    }
    let task = json!({"job": {
        "id": "/taskUid",
        "poll": "/tasks/{id}",
        "status": "/status",
        "succeeded": ["succeeded"],
        "failed": ["failed", "canceled"],
        "error": "/error/message"
    }});
    let mut resolved = json!({
        "url": base_url("meilisearch", &config.url)?,
        "headers": headers,
        "operation": config.operation,
        "max_request_bytes": config.max_request_bytes,
        "compression": config.compression,
        "connect_timeout_ms": config.connect_timeout_ms,
        "request_timeout_ms": config.request_timeout_ms,
        "upsert": {"path": upsert, "result": task},
        "delete": {
            "path": format!("/indexes/{index}/documents/delete-batch"),
            "id_field": config.primary_key.unwrap_or_else(default_id_field),
            "result": task
        },
        "read": {
            "path": read,
            "items": "/results",
            "cursor_id": config.cursor_id,
            "checkpoint_store": config.checkpoint_store,
            "polling_interval_ms": config.polling_interval_ms,
            "max_polling_interval_ms": config.max_polling_interval_ms
        }
    });
    if let Some(values) = config.delete_values {
        resolved["delete_values"] = json!(values);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::test_support::StubHttpServer;
    use crate::CanonicalMessage;

    fn preset(name: &str) -> Preset {
        PRESETS
            .into_iter()
            .find(|preset| preset.name == name)
            .expect("preset")
    }

    fn calls(server: &StubHttpServer) -> Vec<(String, String, String)> {
        server
            .requests()
            .into_iter()
            .map(|r| (r.method, r.target, String::from_utf8(r.body).unwrap()))
            .collect()
    }

    fn delete(payload: &str) -> CanonicalMessage {
        CanonicalMessage::from(payload).with_metadata_kv("op", "delete")
    }

    #[tokio::test]
    async fn typesense_imports_by_line_and_deletes_by_filter() {
        let server = StubHttpServer::start(|_| (200, "{\"success\":true}\n".to_string()))
            .await
            .expect("stub server");
        let config = json!({
            "url": server.url(), "collection": "books", "api_key": "k",
            "operation": "${metadata:op}"
        });
        let publisher = preset("typesense")
            .create_publisher("route", &config)
            .await
            .unwrap();
        publisher
            .send_batch(vec![r#"{"id":"1"}"#.into(), delete(r#"{"id":"2"}"#)])
            .await
            .unwrap();

        assert_eq!(
            server.requests()[0].header("x-typesense-api-key"),
            Some("k")
        );
        assert_eq!(
            calls(&server),
            vec![
                (
                    "POST".to_string(),
                    "/collections/books/documents/import?action=upsert".to_string(),
                    "{\"id\":\"1\"}\n".to_string()
                ),
                (
                    "DELETE".to_string(),
                    "/collections/books/documents?filter_by=id:[2]".to_string(),
                    String::new()
                ),
            ]
        );
    }

    #[tokio::test]
    async fn elasticsearch_names_the_id_in_bulk_actions() {
        let server = StubHttpServer::start(|_| (200, r#"{"items":[{"index":{}}]}"#.to_string()))
            .await
            .expect("stub server");
        let config = json!({
            "url": server.url(), "index": "books", "api_key": "k", "id_field": "isbn",
            "operation": "${metadata:op}"
        });
        let publisher = preset("elasticsearch")
            .create_publisher("route", &config)
            .await
            .unwrap();
        publisher
            .send_batch(vec![r#"{"isbn":"a"}"#.into(), delete(r#"{"isbn":"b"}"#)])
            .await
            .unwrap();

        assert_eq!(
            server.requests()[0].header("authorization"),
            Some("ApiKey k")
        );
        assert_eq!(
            calls(&server),
            vec![
                (
                    "POST".to_string(),
                    "/books/_bulk".to_string(),
                    "{\"index\":{\"_id\":\"a\"}}\n{\"isbn\":\"a\"}\n".to_string()
                ),
                (
                    "POST".to_string(),
                    "/books/_bulk".to_string(),
                    "{\"delete\":{\"_id\":\"b\"}}\n".to_string()
                ),
            ]
        );
    }

    #[test]
    fn the_scheme_picks_http_or_https_and_a_bad_config_is_named() {
        let url = |value: Value| typesense("route", &value).map(|config| config["url"].clone());
        let config = |url: &str| json!({"url": url, "collection": "books"});
        assert_eq!(url(config("typesense://h:8108/")).unwrap(), "http://h:8108");
        assert_eq!(url(config("typesense+https://h")).unwrap(), "https://h");
        assert!(url(config("://h")).is_err());
        assert_eq!(url(config("https://h")).unwrap(), "https://h");
        assert!(url(config("ftp://h")).is_err());

        for (name, bad) in [
            ("typesense", json!({"url": "http://h", "collection": "a/b"})),
            (
                "typesense",
                json!({"url": "http://h", "collection": "a", "index": "b"}),
            ),
            (
                "elasticsearch",
                json!({"url": "http://h", "index": "a", "id_field": "x}"}),
            ),
            ("elasticsearch", json!({"url": "http://h"})),
        ] {
            let error = preset(name).config("route", &bad).expect_err("refused");
            assert!(error.is::<InvalidConfig>(), "{name}: {error:#}");
        }
    }

    #[tokio::test]
    async fn every_preset_declares_a_uri_and_only_meilisearch_is_an_input() {
        for name in preset_names() {
            let preset = preset(name);
            let schema = preset.config_schema().expect("schema");
            assert!(
                schema["properties"]["url"]["x-mqb-uri"].is_string(),
                "{name}"
            );
            let config = match name {
                "typesense" => json!({"url": "http://localhost:1", "collection": "a"}),
                _ => json!({"url": "http://localhost:1", "index": "a"}),
            };
            let consumer = preset.create_consumer("route", &config).await;
            match name {
                "meilisearch" => assert!(consumer.is_ok()),
                _ => assert!(consumer.err().expect("output only").is::<InvalidConfig>()),
            }
        }
    }

    #[tokio::test]
    async fn meilisearch_waits_for_the_task_and_reads_by_offset() {
        let server = StubHttpServer::start(|request| {
            let body = match request.target.as_str() {
                "/tasks/7" => r#"{"status":"succeeded"}"#,
                target if target.contains("offset=") => r#"{"results":[{"id":1}]}"#,
                _ => r#"{"taskUid":7}"#,
            };
            (200, body.to_string())
        })
        .await
        .expect("stub server");
        let config = json!({
            "url": server.url(), "api_key": "k", "primary_key": "isbn",
            "operation": "${metadata:op}", "fields": "isbn,title"
        });
        let preset = preset("meilisearch");
        let publisher = preset.create_publisher("books", &config).await.unwrap();
        publisher
            .send_batch(vec![r#"{"isbn":"a"}"#.into(), delete(r#"{"isbn":"b"}"#)])
            .await
            .unwrap();
        let mut consumer = preset.create_consumer("books", &config).await.unwrap();
        let read = consumer.receive_batch(10).await.unwrap();
        assert_eq!(read.messages.len(), 1);

        assert_eq!(
            server.requests()[0].header("authorization"),
            Some("Bearer k")
        );
        let calls = calls(&server);
        let targets: Vec<&str> = calls.iter().map(|call| call.1.as_str()).collect();
        assert_eq!(
            targets,
            [
                "/indexes/books/documents?primaryKey=isbn",
                "/tasks/7",
                "/indexes/books/documents/delete-batch",
                "/tasks/7",
                "/indexes/books/documents?offset=0&limit=10&fields=isbn,title",
            ]
        );
        assert_eq!(calls[2].2, r#"["b"]"#);
    }

    #[test]
    fn meilisearch_names_what_only_the_plugin_does() {
        for bad in [
            json!({"url": "http://h", "settings": {}}),
            json!({"url": "http://h", "swap_into": "live"}),
            json!({"url": "http://h", "index": "${metadata:postgres.table}"}),
        ] {
            let error = preset("meilisearch")
                .config("route", &bad)
                .expect_err("refused");
            assert!(
                format!("{error:#}").contains("MQB_PLUGIN_OVERRIDE"),
                "{error:#}"
            );
        }
    }
}
