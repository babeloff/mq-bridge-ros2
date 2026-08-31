//! The same ROS 2 implementation, reached two ways.
//!
//! One conformance suite runs against the factory linked directly into this
//! test binary, and again against the factory the host builds from the compiled
//! plugin library. Identical results mean the ABI round trip preserved the
//! endpoint's behaviour — and that the Python and npm packages, which load that
//! very library, get the same endpoint.
//!
//! Needs a sourced ROS 2 installation, like every test in this crate:
//!
//!     cargo test --test plugin -- --ignored --nocapture

use std::time::Duration;

use mq_bridge::plugin::conformance::{self, ConformanceOptions};
use mq_bridge::plugin::{load_endpoint_plugin, test_support::build_plugin_cdylib};
use mq_bridge_ros2::Ros2Factory;

/// DDS has no consumer acknowledgement, so a negatively acknowledged message
/// cannot be redelivered — there is nothing to redeliver it from. The suite is
/// told not to expect it rather than being left to time out waiting.
///
/// `transient_local` with `keep_all` is what makes the suite deterministic: the
/// publisher retains its samples, so nothing depends on whether discovery had
/// matched the subscription before the first message was sent.
fn options(topic: &str) -> ConformanceOptions {
    let mut options = ConformanceOptions::new(
        topic,
        serde_json::json!({
            "topic": topic,
            "domain_id": 88,
            "qos": {
                "durability": "transient_local",
                "history": "keep_all",
            }
        }),
    );
    options.messages = 4;
    options.receive_timeout = Duration::from_secs(30);
    options.expect_redelivery = false;
    // A ROS 2 message carries only the fields its type declares, so there is
    // nowhere for a canonical message's metadata to ride along. This is the one
    // capability the endpoint genuinely does not have, rather than a gap in the
    // implementation, so the suite is told so explicitly.
    options.expect_metadata = false;
    options
}

/// ROS names allow no hyphens, so the run id cannot simply be a uuid.
fn topic(stage: &str, run: &uuid::Uuid) -> String {
    format!("mq_bridge_conformance_{stage}_{}", run.simple())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a sourced ROS 2 installation"]
async fn the_endpoint_behaves_the_same_linked_directly_and_loaded_as_a_plugin() {
    let run = uuid::Uuid::new_v4();

    let direct = conformance::run(&Ros2Factory, options(&topic("direct", &run)))
        .await
        .expect("the directly linked endpoint should pass conformance");

    let library = build_plugin_cdylib(".", "mq-bridge-ros2").expect("build the plugin");
    let info = load_endpoint_plugin(&library).expect("load the plugin");
    assert_eq!(info.name, "ros2");
    assert!(info.supports_consumer && info.supports_publisher);
    let factory = mq_bridge::extensions::get_endpoint_factory(&info.name)
        .expect("loading a plugin registers its endpoint");

    let loaded = conformance::run(factory.as_ref(), options(&topic("plugin", &run)))
        .await
        .expect("the plugin-loaded endpoint should pass the same suite");

    assert_eq!(direct, loaded);
}
