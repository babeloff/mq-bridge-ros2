# Quick start: library

The engine is a library first. The same route config runs embedded in your **Rust**,
**Python**, or **Node.js** service and in `mqb` — design it once, load it anywhere.

## Install

| Language | Package | Install |
| :--- | :--- | :--- |
| Rust | [`mq-bridge`](https://crates.io/crates/mq-bridge) | `cargo add mq-bridge --features kafka,nats,yaml` |
| Python | [`mq-bridge`](https://pypi.org/project/mq-bridge/) | `pip install mq-bridge` |
| Node.js | [`mq-bridge`](https://www.npmjs.com/package/mq-bridge) | `npm install mq-bridge` |

The Rust crate enables connectors through Cargo features (`kafka`, `nats`, `mongodb`, …, or
`full` for all of them); `yaml` adds YAML config files (JSON works without it). The Python
and Node.js packages ship with all of them built in.

## Describe a route

A route moves messages from one `input` endpoint to one `output` endpoint. This one reads a
Kafka topic and publishes to NATS, retrying failed sends:

```yaml
# routes.yaml
kafka_to_nats:
  input:
    kafka: { url: "localhost:9092", topic: "orders", group_id: "bridge" }
  output:
    middlewares:
      - retry: { max_attempts: 5 }
    nats: { url: "nats://localhost:4222", subject: "orders.processed" }
```

## Run it

**Python**

```python
from mq_bridge import Route

route = Route.from_file("routes.yaml", "kafka_to_nats")
route.run()  # blocks; use route.start() to keep running your own code
```

**Node.js**

```js
import { Route } from "mq-bridge";

Route.fromFile("routes.yaml", "kafka_to_nats").start();
```

**Rust**

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    mq_bridge::deploy_file("routes.yaml").await?; // every route in the file, in the background
    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

## Add business logic

Attach a handler to transform, filter, or answer messages in between. Returning a message
publishes it to the output; returning nothing acknowledges and drops it.

```python
def enrich(message):
    order = message.json()
    order["processed"] = True
    return message.with_json(order)

Route.from_file("routes.yaml", "kafka_to_nats").with_handler(enrich).run()
```

```rust
use mq_bridge::{CanonicalMessage, Handled, Route};

Route::from_file("routes.yaml", "kafka_to_nats")?
    .with_handler(|message: CanonicalMessage| async move {
        let mut order: serde_json::Value = message.parse().map_err(anyhow::Error::from)?;
        order["processed"] = true.into();
        Ok(Handled::Publish(CanonicalMessage::from(order)))
    })
    .deploy("kafka_to_nats")
    .await?;
```

Handlers, typed dispatch on the `kind` field, and request/reply are covered in
[Embed the library](../tutorials/embedding.md) and [Core concepts](concepts.md).

## Same settings everywhere

The YAML fields above are the same fields the CLI takes as URL query parameters, so
everything in the [connector pages](../connectors/README.md) and the
[URL parameter reference](../reference/README.md) applies to library configs too:

```bash
mqb copy 'kafka://localhost:9092?topic=orders&group_id=bridge' \
         'nats://localhost:4222?subject=orders.processed'
```

The full config grammar — routes, middleware lists, structural endpoints — is in
[Configuration grammar](../engine/configuration.md) and
[Middleware & structural endpoints](../engine/reference.md).
