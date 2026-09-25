//! OTLP trace export for the `otel` middleware, configured by the standard `OTEL_*` env vars.

use mq_bridge::opentelemetry::global;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Flushes and shuts the tracer provider down when dropped.
pub struct TracerGuard(SdkTracerProvider);

impl Drop for TracerGuard {
    fn drop(&mut self) {
        if let Err(e) = self.0.shutdown() {
            tracing::warn!("Failed to flush OpenTelemetry spans: {e}");
        }
    }
}

/// Installs the global tracer provider when `OTEL_EXPORTER_OTLP_ENDPOINT` or
/// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. Call before any route starts.
pub fn init_from_env() -> anyhow::Result<Option<TracerGuard>> {
    let configured = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .iter()
    .any(|key| std::env::var_os(key).is_some_and(|v| !v.is_empty()));
    if !configured {
        return Ok(None);
    }
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()?;
    let mut resource = Resource::builder();
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name("mq-bridge-app");
    }
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.build())
        .build();
    global::set_tracer_provider(provider.clone());
    tracing::info!("OpenTelemetry OTLP trace export enabled");
    Ok(Some(TracerGuard(provider)))
}
