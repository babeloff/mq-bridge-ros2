# Test a message handler without a broker

A route with a handler, tested end to end in-process. The test swaps the production transport for
mq-bridge's `memory` endpoint, so it needs no Kafka, no Docker and no mocks. The handler and the
route code are the ones that run in production.

Explained in the book: [Test Kafka consumers without a broker](https://marcomq.github.io/mq-bridge/use-cases/test-without-a-broker.html).

The Rust and Python folders are the same example in two languages: an `orders_route` whose
`check_order` handler drops orders without a positive amount and marks the rest `checked: true`.
Pick the one you use; neither depends on the other. They differ only in what is idiomatic for each
API: the Rust route takes two `Endpoint` values and its handler sees every message, while the Python
route takes a config dict and registers the handler for messages with `kind: order.created`.

## Rust

```bash
cargo test
```

[`src/lib.rs`](src/lib.rs) defines `orders_route(input, output)` and a test that passes two
`Endpoint::new_memory` endpoints, sends two orders and asserts on what comes out.

## Python

```bash
cd python
pip install -r requirements.txt
pytest
```

[`python/orders.py`](python/orders.py) builds the route from a config dict;
[`python/test_orders.py`](python/test_orders.py) passes `memory` endpoints, publishes three orders
and polls the results.

## What this does not test

Broker behaviour: consumer-group rebalancing, partition ordering, offset commits, TLS and
authentication. Keep a small number of integration tests against a real broker for those.
