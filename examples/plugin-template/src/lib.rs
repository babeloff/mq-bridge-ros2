//! Myendpoint output endpoint extension for `mq-bridge`.
//!
//! The output posts each batch as NDJSON to one URL. Replace `MyendpointPublisher`
//! with the calls your system needs; the factory, the plugin export and the
//! packaging around this crate stay as they are.
//!
//! The same implementation is used three ways:
//!
//! * linked directly by a Rust program, which calls [`register`];
//! * loaded from the compiled `cdylib` by any mq-bridge host through
//!   `mq_bridge::plugin::load_endpoint_plugin`;
//! * from Python or Node.js, whose `mq-bridge-myendpoint` packages ship that
//!   same library and call the host's generic loader.

use std::any::Any;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use mq_bridge::errors::{InvalidConfig, PublisherError};
use mq_bridge::support::{http_status, ndjson};
use mq_bridge::traits::{CustomEndpointFactory, MessagePublisher};
use mq_bridge::{CanonicalMessage, SentBatch};
use schemars::JsonSchema;
use serde::Deserialize;

/// Batch size `mqb copy` uses for this endpoint unless `--batch-size` is given.
const DEFAULT_BATCH_SIZE: u64 = 5000;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MyendpointConfig {
    /// URL every batch is posted to.
    pub url: String,
    /// Largest request body in bytes; a bigger batch is split.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: usize,
}

fn default_max_request_bytes() -> usize {
    10 * 1024 * 1024
}

#[derive(Debug, Default)]
pub struct MyendpointFactory;

// Exports the same factory as a loadable plugin. `register()` below covers the
// directly linked case; this covers every host that loads the compiled library.
#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "myendpoint",
    factory: MyendpointFactory,
}

/// Registers this crate's factory under `myendpoint`. Only needed when linking
/// this crate directly; a host that loads the plugin registers it while loading.
pub fn register() -> anyhow::Result<()> {
    mq_bridge::extensions::register_endpoint_factory("myendpoint", Arc::new(MyendpointFactory))
}

#[async_trait]
impl CustomEndpointFactory for MyendpointFactory {
    fn config_schema(&self) -> Option<serde_json::Value> {
        let mut schema = serde_json::to_value(schemars::schema_for!(MyendpointConfig)).ok()?;
        schema["x-mqb-default-batch-size"] = DEFAULT_BATCH_SIZE.into();
        Some(schema)
    }

    async fn create_publisher(
        &self,
        _route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        let config: MyendpointConfig =
            serde_json::from_value(value.clone()).map_err(|e| InvalidConfig(e.into()))?;
        Ok(Box::new(MyendpointPublisher {
            client: reqwest::Client::new(),
            config,
        }))
    }
}

struct MyendpointPublisher {
    client: reqwest::Client,
    config: MyendpointConfig,
}

#[async_trait]
impl MessagePublisher for MyendpointPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let payloads = messages.iter().map(|message| message.payload.as_ref());
        let bodies = ndjson::chunk(payloads, self.config.max_request_bytes)
            .map_err(PublisherError::NonRetryable)?;
        for body in bodies {
            let response = self
                .client
                .post(&self.config.url)
                .header("content-type", "application/x-ndjson")
                .body(body)
                .send()
                .await
                .map_err(|e| PublisherError::Retryable(e.into()))?;
            let status = response.status();
            if !status.is_success() {
                let text = response.text().await.unwrap_or_default();
                return Err(http_status::publisher_error(
                    status.as_u16(),
                    anyhow!("{} answered {status}: {text}", self.config.url),
                ));
            }
        }
        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
