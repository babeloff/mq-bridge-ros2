# Retries & backoff

The [`retry`](../engine/reference.md#retry) middleware retries failed sends with exponential
backoff. **Output only.**

```yaml
output:
  middlewares:
    - retry: { max_attempts: 5, initial_interval_ms: 200, max_interval_ms: 10000, multiplier: 2.0 }
  kafka: { topic: "orders", url: "localhost:9092" }
```

From the `copy` CLI:

Attach the chain to `--to`, since `retry` is output-only:

```bash
--to 'kafka://localhost:9092?topic=orders|retry?max_attempts=5&initial_interval_ms=200'
```

| Field | Default |
|---|---|
| `max_attempts` | `3` |
| `initial_interval_ms` | `100` |
| `max_interval_ms` | `5000` |
| `multiplier` | `2.0` |

## What gets retried

Only `Retryable` and connection errors are retried; `NonRetryable` failures pass straight
through. Once attempts are exhausted, the error is marked so a following [`dlq`](dlq.md) treats
it as permanent.

## Always pair retry with dlq

`retry` does **not** retain a message it gives up on — it hands the still-failing message on to
be dropped, or to a following `dlq`. Put `dlq` **after** `retry` so it's the outermost layer and
captures the exhausted failures:

```yaml
output:
  middlewares:
    - retry: { max_attempts: 3 }
    - dlq: { endpoint: { file: { path: "rejected.jsonl" } } }
```

## Don't over-tune

Several endpoints already retry connection/timeout errors **internally**, and the `retry`
middleware adds backoff *on top* — so you rarely need a large `max_attempts`. Cap
`max_interval_ms` to match your latency budget so retries never stall a route indefinitely. See
[Performance tuning → Retry & backoff](../operations/tuning.md#retry--backoff).
