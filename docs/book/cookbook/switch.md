# Content-based routing (switch)

The [`switch`](../engine/reference.md#switch) structural endpoint picks **one** destination per
message. Output only. It has two modes and uses exactly one of them: value lookup on a
**metadata key**, or `when` **predicates** over the payload. Naming both is a startup error.

```yaml
output:
  switch:
    metadata_key: "country_code"
    cases:
      US: { kafka: { topic: "us_orders", url: "kafka-us:9092" } }
      EU: { nats:  { subject: "eu_orders", url: "nats-eu:4222" } }
    default: { file: { path: "/var/data/unroutable_orders.log" } }
```

A message whose key is missing or unmatched goes to `default`; **without a `default` it is
dropped.**

From the CLI, the same endpoint is a URI:
`switch:?metadata_key=country_code&case.US=<uri>&default=<uri>` — see
[Structural endpoints in the URI](../reference/cli.md#structural-endpoints-in-the-uri).

## Route on the payload (`when`)

`when` takes an ordered list of predicates and sends the message to the first one that matches.
The expression language is the one [`--filter`](../reference/cli.md#filtering) uses: payload
fields by bare name including nested paths, metadata under `meta.`, and `and` / `or`.

```yaml
output:
  switch:
    when:
      - if: "amount > 10000"
        to: { kafka: { topic: "large_orders", url: "kafka:9092" } }
      - if: "order.status == 'refunded'"
        to: { nats: { subject: "refunds", url: "nats://localhost:4222" } }
    default: { file: { path: "/var/data/orders.jsonl" } }
```

A predicate parses the payload, while value lookup is a HashMap get on metadata — which is why
the modes cannot be mixed in one endpoint, and why a metadata key you already have is the
cheaper branch. A message matching no predicate goes to `default`, and **without a `default` it
is dropped**, exactly as in value-lookup mode.

The CLI spells this as `when=<expression>` / `to=<uri>` pairs:
`switch:?when=amount > 10000&to=<uri>&default=<uri>`.

## Promote a payload value into metadata

Value-lookup mode matches on **metadata**, not payload fields. When you want that mode — an
exact-match table rather than predicates — promote the value into metadata first. Two common
ways:

- An endpoint that already emits a status key — e.g. `http_status_code` from an HTTP
  [`request`](../engine/reference.md#request), or `mongodb.outcome` from a Mongo upsert (see
  [Upserts](upserts.md)).
- [`transform`](transform.md) with `on_error: pass_through`, which sets `mqb.transform_error` on
  failed records so you can shunt them aside:

```yaml
output:
  middlewares:
    - transform: { schema_file: "schemas/order.json", on_error: pass_through }
  switch:
    metadata_key: "mqb.transform_error"
    cases: {}                                  # (no exact-match cases)
    default: { kafka: { topic: "orders", url: "localhost:9092" } }
```

## Split HTTP responses by status

Pair `switch` with [`request`](../engine/reference.md#request), which forwards a response (or,
on error/timeout, the original message) tagged with a status key:

```yaml
output:
  request:
    to: { http: { url: "https://api.internal/score" } }
    forward_to:
      switch:
        metadata_key: "http_status_code"
        cases:
          "200": { nats: { subject: "ok", url: "nats://localhost:4222" } }
          "404": { file: { path: "not-found.jsonl" } }
        default: { file: { path: "other.jsonl" } }
```

See also [Fan-out](fanout.md) to send to *all* destinations instead of picking one.
