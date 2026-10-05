# Fan-out

The [`fanout`](../engine/reference.md#fanout) structural endpoint publishes **each message to
every listed endpoint**. Output only. The value is a plain list — each branch may have its own
middleware and may itself be structural.

```yaml
output:
  fanout:
    - kafka: { topic: "audit", url: "localhost:9092" }
    - file: { path: "audit.jsonl" }
    - nats: { subject: "audit", url: "nats://localhost:4222" }
```

All branches receive the same message. Use it to mirror a stream to an archive while it also
flows to its primary sink, to tee traffic to an audit log, or to emulate a static subscriber set
(one branch per subscriber).

## Fan-out vs. switch

- [`fanout`](../engine/reference.md#fanout) → **every** destination gets the message.
- [`switch`](switch.md) → **one** destination, chosen by a metadata key.

## Give one branch its own reliability

Because each branch is a full endpoint, you can wrap just the fragile one with retry/dlq while
the others stay plain:

```yaml
output:
  fanout:
    - kafka: { topic: "orders", url: "localhost:9092" }
    - middlewares:
        - retry: { max_attempts: 5 }
        - dlq: { endpoint: { file: { path: "audit-failed.jsonl" } } }
      http: { url: "https://audit.internal/ingest", method: "POST" }
```

## From the CLI

`copy` can build a fan-out too — `fanout:?to=<uri>&mirror=<uri>`, where a `mirror` branch's
response and failures are discarded. See
[Structural endpoints in the URI](../reference/cli.md#structural-endpoints-in-the-uri).
