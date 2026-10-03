//! The same implementation, reached two ways: through the factory linked into
//! this test binary, and through the factory the host builds from the compiled
//! plugin library. Identical results mean the ABI round trip kept the
//! endpoint's behaviour, and that the Python and npm packages get the same one.

use mq_bridge::errors::PublisherError;
use mq_bridge::plugin::load_endpoint_plugin;
use mq_bridge::plugin::test_support::{build_plugin_cdylib, StubHttpServer};
use mq_bridge::traits::CustomEndpointFactory;
use mq_bridge::{CanonicalMessage, SentBatch};
use mq_bridge_myendpoint::MyendpointFactory;
use serde_json::json;

/// Sends two documents and returns the body the server received.
async fn posted_body(factory: &dyn CustomEndpointFactory) -> String {
    let server = StubHttpServer::start(|_| (200, String::new()))
        .await
        .expect("start the stub server");
    let publisher = factory
        .create_publisher("route", &json!({"url": server.url()}))
        .await
        .expect("create the publisher");
    let batch = vec![
        CanonicalMessage::from(json!({"id": 1}).to_string()),
        CanonicalMessage::from(json!({"id": 2}).to_string()),
    ];
    let sent = publisher.send_batch(batch).await.expect("send the batch");
    assert!(matches!(sent, SentBatch::Ack));
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    String::from_utf8(requests[0].body.clone()).expect("an utf-8 body")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_endpoint_behaves_the_same_linked_directly_and_loaded_as_a_plugin() {
    let direct = posted_body(&MyendpointFactory).await;
    assert_eq!(direct, "{\"id\":1}\n{\"id\":2}\n");

    let library = build_plugin_cdylib(".", "mq-bridge-myendpoint").expect("build the plugin");
    let info = load_endpoint_plugin(&library).expect("load the plugin");
    assert_eq!(info.name, "myendpoint");
    let factory = mq_bridge::extensions::get_endpoint_factory(&info.name)
        .expect("loading a plugin registers its endpoint");

    assert_eq!(posted_body(factory.as_ref()).await, direct);
}

#[tokio::test]
async fn a_busy_server_is_retried_and_a_rejected_request_is_not() {
    for (status, retryable) in [(503, true), (400, false)] {
        let server = StubHttpServer::start(move |_| (status, "no".to_owned()))
            .await
            .expect("start the stub server");
        let publisher = MyendpointFactory
            .create_publisher("route", &json!({"url": server.url()}))
            .await
            .expect("create the publisher");
        let error = publisher
            .send_batch(vec![CanonicalMessage::from("{}")])
            .await
            .expect_err("the server refused the batch");
        assert_eq!(matches!(error, PublisherError::Retryable(_)), retryable);
    }
}

#[test]
fn the_schema_states_the_default_batch_size() {
    let schema = MyendpointFactory.config_schema().expect("a schema");
    assert_eq!(schema["x-mqb-default-batch-size"], json!(5000));
}
