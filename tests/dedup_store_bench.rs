// TEMPORARY dedup store throughput bench. Run with:
// DEDUP_BENCH_STORE=sqlite://... DEDUP_BENCH_N=5000 cargo test --release --features dedup,sqlx \
//   --test dedup_store_bench -- --ignored --nocapture
#![cfg(feature = "dedup")]

use mq_bridge::models::{
    DeduplicationMiddleware, Endpoint, EndpointType, MemoryConfig, Middleware,
};
use mq_bridge::{CanonicalMessage, Route};
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual bench"]
async fn dedup_store_throughput() {
    let n: usize = std::env::var("DEDUP_BENCH_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let dir = tempfile::tempdir().unwrap();
    let store = std::env::var("DEDUP_BENCH_STORE")
        .unwrap_or_else(|_| format!("sled://{}", dir.path().join("dedup").display()));
    let run = fast_id();
    let (in_topic, out_topic) = (format!("bench_in_{run}"), format!("bench_out_{run}"));
    let cap = 2 * n + 10;

    let input = Endpoint::new(EndpointType::Memory(MemoryConfig::new(
        &in_topic,
        Some(cap),
    )));
    let input = if store == "none" {
        input
    } else {
        input.add_middleware(Middleware::Deduplication(DeduplicationMiddleware {
            store: Some(store.clone()),
            sled_path: None,
            ttl_seconds: 3600,
            key: Some("${payload:id}".to_string()),
            replay_response: false,
        }))
    };
    let in_channel = Endpoint::new_memory(&in_topic, cap).channel().unwrap();
    let mut messages = Vec::with_capacity(2 * n + 1);
    for pass in 0..2u128 {
        for i in 0..n {
            let body = format!(r#"{{"id":"{run}-{i}"}}"#);
            messages.push(CanonicalMessage::new(
                body.into_bytes(),
                Some(pass << 64 | i as u128),
            ));
        }
    }
    messages.push(CanonicalMessage::new(
        format!(r#"{{"id":"{run}-end"}}"#).into_bytes(),
        Some(u128::MAX),
    ));
    in_channel.fill_messages(messages).await.unwrap();

    let out_channel = Endpoint::new_memory(&out_topic, cap).channel().unwrap();
    let started = Instant::now();
    Route::new(input, Endpoint::new_memory(&out_topic, cap))
        .with_batch_size(128)
        .deploy(&format!("bench_{run}"))
        .await
        .unwrap();
    let mut seen = 0;
    loop {
        let drained = out_channel.drain_messages();
        seen += drained.len();
        if drained.iter().any(|m| m.message_id == u128::MAX) {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(600), "timed out");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let secs = started.elapsed().as_secs_f64();
    Route::stop(&format!("bench_{run}")).await;
    let expected = if store == "none" { 2 * n + 1 } else { n + 1 };
    assert_eq!(seen, expected, "every unique key once");
    println!(
        "BENCH store={} n={n} inputs={} secs={secs:.3} msgs/s={:.0}",
        store.split("://").next().unwrap(),
        2 * n + 1,
        (2 * n + 1) as f64 / secs
    );
}

fn fast_id() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
