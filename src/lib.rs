//! ROS 2 input/output endpoint extension for `mq-bridge`.
//!
//! Based on mq-bridge's
//! [plugin template](https://github.com/marcomq/mq-bridge/tree/main/examples/plugin-template),
//! with a ROS 2 implementation using `rclrs`.
//!
//! The same implementation is used three ways:
//!
//! * linked directly by a Rust program, which calls [`register`];
//! * loaded from the compiled `cdylib` by any mq-bridge host through
//!   `mq_bridge::plugin::load_endpoint_plugin`;
//! * from Python or Node.js, whose `mq-bridge-ros2` packages ship that same
//!   library and call the host's generic loader.
//!
//! Messages are carried with `rclrs`' dynamic messages, so the ROS message type
//! is named in the route's configuration rather than compiled in, and any type
//! installed on the machine can be used without generating bindings for it.
//!
//! # What ROS 2 does not provide
//!
//! Two habits from broker-backed endpoints do not survive the move, and routes
//! have to be written accordingly:
//!
//! * **No acknowledgement, so no redelivery.** DDS sends nothing back from a
//!   reader to a writer. A `Nack` from a route's batch commit is accepted and
//!   the message is gone.
//! * **No metadata.** A ROS 2 message has only the fields its type declares. A
//!   published message's metadata is dropped, and a received one carries just
//!   `ros2_topic` and `ros2_message_type`.

mod config;
mod consumer;
mod message;
mod publisher;
mod runtime;

use std::sync::Arc;

use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

// tag::public-api[]
pub use config::{Durability, History, QosConfig, Reliability, Ros2Config};
// end::public-api[]

// tag::factory[]
#[derive(Debug, Default)]
pub struct Ros2Factory;
// end::factory[]

// Exports the same factory as a loadable plugin. `register()` below covers the
// directly linked case; this covers every host that loads the compiled library,
// including the Python and Node.js packages.
#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "ros2",
    factory: Ros2Factory,
}

/// Registers this crate's factory under `ros2`. Call once, before starting
/// routes that use it. Only needed when linking this crate directly; a host that
/// loads the compiled plugin registers the endpoint as part of loading it.
// tag::register[]
pub fn register() -> anyhow::Result<()> {
    mq_bridge::extensions::register_endpoint_factory("ros2", Arc::new(Ros2Factory))
}
// end::register[]

#[async_trait]
impl CustomEndpointFactory for Ros2Factory {
    // tag::factory-create-consumer[]
    async fn create_consumer(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        consumer::create(route_name, value).await
    }
    // end::factory-create-consumer[]

    // tag::factory-create-publisher[]
    async fn create_publisher(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        publisher::create(route_name, value).await
    }
    // end::factory-create-publisher[]
}
