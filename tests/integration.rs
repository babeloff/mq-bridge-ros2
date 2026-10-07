//! End-to-end tests against a real ROS 2 middleware.
//!
//! There is no broker to start: ROS 2 is peer to peer, so a publisher and a
//! consumer built from the same factory in this one process discover each other
//! over the loopback interface and that is the whole round trip.
//!
//! These need a sourced ROS 2 installation — without one the test binary cannot
//! even load, since `rclrs` links the ROS libraries. Run them with:
//!
//!     cargo test --test integration -- --ignored --nocapture
//!
//! Both tests ask for `transient_local` durability. That is not incidental: a
//! `volatile` publisher's samples go nowhere until discovery has matched a
//! subscription, so a test that published straight away would be racing the
//! middleware. `transient_local` makes the publisher retain its samples and
//! deliver them when the match happens, which is also the setting a route that
//! must not drop its first messages wants in production.

use std::time::Duration;

use mq_bridge::{
    traits::{MessageDisposition, MessagePublisher},
    CanonicalMessage, SentBatch,
};

/// Both tests share one process, and a second `register()` is an error.
fn register_once() {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| mq_bridge_ros2::register().expect("register ROS 2 endpoint"));
}

/// A domain of its own, so nothing else on the machine can join these tests and
/// nothing they publish escapes into a real graph.
const TEST_DOMAIN_ID: u32 = 87;

/// A fresh topic per test. ROS names allow no hyphens, which is why this is not
/// simply a uuid.
fn test_topic(prefix: &str) -> String {
    format!("mq_bridge_test_{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn config(topic: &str, node: &str) -> serde_json::Value {
    serde_json::json!({
        "node": node,
        "topic": topic,
        "domain_id": TEST_DOMAIN_ID,
        "qos": {
            "durability": "transient_local",
            "history": "keep_all",
        }
    })
}

async fn factory() -> std::sync::Arc<dyn mq_bridge::traits::CustomEndpointFactory> {
    register_once();
    mq_bridge::extensions::get_endpoint_factory("ros2")
        .expect("ros2 endpoint factory should be registered")
}

/// Drains `expected` payloads, committing every batch, and fails rather than
/// hangs if the middleware never delivers.
async fn drain(
    consumer: &mut Box<dyn mq_bridge::traits::MessageConsumer>,
    expected: usize,
) -> Vec<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut payloads = Vec::with_capacity(expected);
        while payloads.len() < expected {
            let batch = consumer
                .receive_batch(expected - payloads.len())
                .await
                .expect("receive a ROS 2 batch");
            if batch.messages.is_empty() {
                continue;
            }

            let count = batch.messages.len();
            payloads.extend(
                batch
                    .messages
                    .iter()
                    .map(|message| message.payload.to_vec()),
            );
            (batch.commit)(vec![MessageDisposition::Ack; count])
                .await
                .expect("commit a ROS 2 batch");
        }
        payloads
    })
    .await
    .expect("timed out waiting for ROS 2 messages")
}

#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn ros2_publisher_consumer_round_trip_preserves_payload_order() {
    let factory = factory().await;
    let topic = test_topic("round_trip");
    let route_name = "round-trip";

    // Distinct node names: two endpoints of one route would otherwise both be
    // called `mq_bridge_round_trip` in the same graph.
    let mut consumer = factory
        .create_consumer(route_name, &config(&topic, "round_trip_in"))
        .await
        .expect("create ROS 2 consumer");
    let publisher = factory
        .create_publisher(route_name, &config(&topic, "round_trip_out"))
        .await
        .expect("create ROS 2 publisher");

    let expected = vec![
        CanonicalMessage::from(b"one".to_vec()),
        CanonicalMessage::from(b"two".to_vec()),
        CanonicalMessage::from(b"three".to_vec()),
    ];
    publisher
        .send_batch(expected.clone())
        .await
        .expect("publish a ROS 2 batch");

    assert_eq!(
        drain(&mut consumer, expected.len()).await,
        expected
            .iter()
            .map(|message| message.payload.to_vec())
            .collect::<Vec<_>>()
    );

    consumer.close().await.expect("close ROS 2 consumer");
}

/// The optional subscriber requirement must report an unmatched publish as
/// retryable, which gives route middleware a chance to hold the source message
/// until ROS discovery completes.
#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn require_subscribers_returns_a_retryable_failure_without_a_reader() {
    let factory = factory().await;
    let topic = test_topic("require_subscribers");
    let route_name = "require-subscribers";
    let mut value = config(&topic, "require_subscribers_out");
    value["qos"]["require_subscribers"] = serde_json::json!(true);

    let publisher = factory
        .create_publisher(route_name, &value)
        .await
        .expect("create ROS 2 publisher");
    let outcome = publisher
        .send_batch(vec![CanonicalMessage::from(b"waiting".to_vec())])
        .await
        .expect("unmatched publishes are reported per message");

    let SentBatch::Partial { failed, .. } = outcome else {
        panic!("an unmatched required subscriber must fail the message");
    };
    assert_eq!(failed.len(), 1);
    assert!(matches!(
        &failed[0].1,
        mq_bridge::errors::PublisherError::Retryable(_)
    ));
}

/// The ROS 2 counterpart of reading a backlog: the publish deliberately happens
/// before any subscription exists, so only a retained sample can satisfy it.
#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn transient_local_reaches_a_subscription_created_after_the_publish() {
    let factory = factory().await;
    let topic = test_topic("backlog");
    let route_name = "backlog";

    let expected = vec![
        CanonicalMessage::from(b"backlog-one".to_vec()),
        CanonicalMessage::from(b"backlog-two".to_vec()),
    ];
    // The publisher is kept alive on purpose: a transient-local sample is
    // retained by its *writer*, so dropping it here would discard the backlog.
    let publisher = factory
        .create_publisher(route_name, &config(&topic, "backlog_out"))
        .await
        .expect("create ROS 2 publisher");
    publisher
        .send_batch(expected.clone())
        .await
        .expect("publish a ROS 2 backlog");

    let mut consumer = factory
        .create_consumer(route_name, &config(&topic, "backlog_in"))
        .await
        .expect("create ROS 2 consumer");

    assert_eq!(
        drain(&mut consumer, expected.len()).await,
        expected
            .iter()
            .map(|message| message.payload.to_vec())
            .collect::<Vec<_>>()
    );

    consumer.close().await.expect("close ROS 2 consumer");
}

/// A payload can travel as bytes rather than text, which is what a route
/// carrying anything non-textual through ROS 2 needs.
#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn a_byte_sequence_message_type_carries_a_payload_that_is_not_utf8() {
    let factory = factory().await;
    let topic = test_topic("bytes");
    let route_name = "bytes";

    let byte_config = |node: &str| {
        let mut value = config(&topic, node);
        value["message_type"] = serde_json::json!("std_msgs/msg/UInt8MultiArray");
        value
    };

    let mut consumer = factory
        .create_consumer(route_name, &byte_config("bytes_in"))
        .await
        .expect("create ROS 2 consumer");
    let publisher = factory
        .create_publisher(route_name, &byte_config("bytes_out"))
        .await
        .expect("create ROS 2 publisher");

    // Deliberately not valid UTF-8, so a string field could not have carried it.
    let payload = vec![0xff, 0x00, 0xfe, 0x01];
    publisher
        .send_batch(vec![CanonicalMessage::from(payload.clone())])
        .await
        .expect("publish a ROS 2 byte payload");

    assert_eq!(drain(&mut consumer, 1).await, vec![payload]);

    consumer.close().await.expect("close ROS 2 consumer");
}

/// A message type that is not installed cannot start working later, so the
/// route has to be told to stop rather than reconnect forever.
#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn an_unknown_message_type_fails_permanently() {
    let factory = factory().await;
    let result = factory
        .create_consumer(
            "unknown-type",
            &serde_json::json!({
                "topic": "mq_bridge_test_unknown",
                "message_type": "std_msgs/msg/NoSuchMessage",
                "domain_id": TEST_DOMAIN_ID,
            }),
        )
        .await;
    let Err(error) = result else {
        panic!("an unknown message type must not produce a working consumer");
    };

    assert!(
        matches!(
            error.downcast_ref::<mq_bridge::errors::ConsumerError>(),
            Some(mq_bridge::errors::ConsumerError::Permanent(_))
        ),
        "expected a permanent error, got: {error:#}"
    );
}

/// The payload field is checked against the real message definition, so a route
/// pointed at a field that cannot hold bytes fails at startup.
#[tokio::test]
#[ignore = "requires a sourced ROS 2 installation"]
async fn a_payload_field_that_cannot_carry_bytes_fails_permanently() {
    let factory = factory().await;
    let result = factory
        .create_publisher(
            "bad-field",
            &serde_json::json!({
                "topic": "mq_bridge_test_bad_field",
                // `layout` is a nested message, not somewhere bytes can go.
                "message_type": "std_msgs/msg/UInt8MultiArray",
                "payload_field": "layout",
                "domain_id": TEST_DOMAIN_ID,
            }),
        )
        .await;
    let Err(error) = result else {
        panic!("an unusable payload field must not produce a working publisher");
    };

    assert!(
        matches!(
            error.downcast_ref::<mq_bridge::errors::PublisherError>(),
            Some(mq_bridge::errors::PublisherError::NonRetryable(_))
        ),
        "expected a non-retryable error, got: {error:#}"
    );
    // The message has to say what to fix.
    let report = format!("{error:#}");
    assert!(report.contains("layout"), "{report}");
}
