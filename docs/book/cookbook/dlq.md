# Dead-letter queues

By default, a message that fails **permanently** is logged at `error` level and **dropped** — the
route tolerates it and keeps going. A [`dlq`](../engine/reference.md#dlq) middleware is the only
mechanism that **retains** those failures for inspection or replay.

## Basic DLQ

Add `dlq` to the **output** middleware list. The DLQ endpoint is a full endpoint, so failed
messages can land anywhere — a file, another queue, a database:

```yaml
output:
  middlewares:
    - dlq:
        endpoint:
          file: { path: "dead-letters.jsonl" }
  kafka: { topic: "orders", url: "localhost:9092" }
```

From the `copy` CLI, `dlq`'s `endpoint` is a URL-encoded endpoint URI:

```bash
--to 'kafka://broker:9092?topic=orders|dlq?endpoint=file%3A%2F%2F%2Ftmp%2Ffailed.jsonl'
```

## Pair it with `retry` — and mind the order

`retry` alone does **not** retain a failed message; once its attempts are exhausted it hands the
still-failing message on to be dropped (or to a following `dlq`). Put `retry` before `dlq` so the
`dlq` is the **outermost** layer and captures what retry gives up on:

```yaml
output:
  middlewares:
    - retry: { max_attempts: 3 }
    - dlq: { endpoint: { file: { path: "rejected.jsonl" } } }   # last = outermost
```

On an **output**, publisher middlewares wrap in list order, so the *last* entry is outermost. See
the [ordering rule](../engine/reference.md#ordering--read-this-before-combining-middleware).

## What gets dead-lettered

- ✅ `NonRetryable` failures (data/type errors the sink rejects, poison payloads).
- ✅ `Retryable` failures whose retries are exhausted.
- ❌ **Connection errors are not dead-lettered** — they propagate so the route can reconnect.
  (If the DLQ send itself fails with a connection error, that error propagates rather than
  silently dropping the message.)

## Catching transform rejections

A [`transform`](transform.md) failure on an output is non-retryable and flows to a following
`dlq`, so invalid records are captured with the reason:

```yaml
output:
  middlewares:
    - transform: { schema_file: "schemas/user.json" }
    - dlq: { endpoint: { file: { path: "rejected.jsonl" } } }
  kafka: { topic: "users", url: "localhost:9092" }
```

## No DLQ? Watch status and logs

Without a `dlq`, a *systematic* failure (e.g. every row hitting a column-type mismatch) drains
the input while delivering nothing to the sink and still ends `completed`. Route status retains the drop
count and last rejection cause, and the logs contain a burst of
`Dropping message … due to non-retryable error`. Do not use the outcome alone as a delivery
check. See
[Troubleshooting](../operations/troubleshooting.md#messages-disappear-and-the-route-still-ends-completed).
