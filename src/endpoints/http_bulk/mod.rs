//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! HTTP APIs that take many JSON documents in one request; the input is in `consumer`.
//!
//! Search engines and similar stores differ in three things only: the body an
//! upsert carries, how documents are deleted, and how the outcome is reported.
//! All three are configuration here, so a new target is a recipe, not code.
//! Messages are sent in order: a batch is cut into runs of upserts and deletes,
//! and after a request that may be retried nothing later in the batch is sent.

mod consumer;
mod presets;
mod publisher;
pub(crate) use consumer::cursor_checkpoint;
pub use consumer::HttpBulkConsumer;
pub use presets::{preset_names, register_preset};
pub use publisher::HttpBulkPublisher;

use crate::models::HttpBulkConfig;
use anyhow::{bail, Context};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::time::Duration;

const JSON: &str = "application/json";
/// Most characters of a response body quoted in an error.
const QUOTED_RESPONSE_CHARS: usize = 500;

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
        if config.tls.required && url.scheme() != "https" {
            bail!("http_bulk tls.required needs an https URL");
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
        if config.tls.cert_password.is_some() {
            bail!("http_bulk tls.cert_password is not supported; use an unencrypted key");
        }
        match (&config.tls.cert_file, &config.tls.key_file) {
            (Some(cert), key) => {
                let mut pem = std::fs::read(cert)
                    .with_context(|| format!("Failed to read http_bulk cert file '{cert}'"))?;
                if let Some(key) = key {
                    pem.push(b'\n');
                    pem.extend(
                        std::fs::read(key).with_context(|| {
                            format!("Failed to read http_bulk key file '{key}'")
                        })?,
                    );
                }
                builder =
                    builder.identity(reqwest::Identity::from_pem(&pem).with_context(|| {
                        format!("Invalid http_bulk client certificate '{cert}'")
                    })?);
            }
            (None, Some(_)) => bail!("http_bulk tls.key_file needs tls.cert_file"),
            (None, None) => {}
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

fn quoted(text: &str) -> String {
    match text.char_indices().nth(QUOTED_RESPONSE_CHARS) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

#[cfg(all(test, feature = "plugin", feature = "test-utils"))]
mod tests {
    use super::*;
    use serde_json::Value;

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
            // A Windows checkout may hold the page with CRLF line ends.
            let page = page.replace("\r\n", "\n");
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
}
