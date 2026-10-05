# Transform & schema mapping

The [`transform`](../engine/reference.md#transform) middleware reshapes JSON payloads
declaratively, so renaming fields and fixing types doesn't need a custom handler. It runs two
optional stages over a single parse: **`mapping`** (rename, move, nest) then **`schema`**
(coerce, apply defaults, validate). Input and output.

## Rename + type-fix a CSV feed

```yaml
csv_to_kafka:
  input:
    file: { path: "users.csv", format: csv }
  output:
    middlewares:
      - transform:
          mapping:
            firstName: "$.first_name"
            lastName: "$.last_name"
            id: "$.user_id"
            "address.city": { path: "$.city", default: "unknown" }
          schema_file: "schemas/user.json"
      - dlq:
          endpoint: { file: { path: "rejected.jsonl" } }
    kafka: { topic: "users", url: "localhost:9092" }
```

With the CSV producing `{"first_name":"John","last_name":"Smith","user_id":"42"}`, the mapping
yields `{"firstName":"John","lastName":"Smith","id":"42","address":{"city":"unknown"}}` (`$.city`
is absent, so `address.city` falls back to its default) and the schema then coerces `id` to the
integer `42`.

- Paths accept `$.field`, `$.a.b`, `$.items[0]` (the `$.` prefix is optional). Dots in the
  *output* key nest the result. An absent optional source field is omitted, not emitted as null.
- Paths have no quoting: a CSV column whose header contains `.` or `[` (e.g. `price.usd`)
  cannot be addressed by a mapping.
- Coercions are the lossless ones only: `string → integer`, `string → number`,
  `string → boolean`, `number → string`. `string → number` keeps integers exact within
  the i64/u64 range (`"42"` → `42`); larger integers and fractional text go through f64
  (~17 significant digits) and may lose precision.

## Decode an embedded JSON string

A field carrying a JSON document *as a string* is decoded via `contentMediaType` (JSON Schema
2020-12) — opt-in per field, never done by plain coercion:

```yaml
- transform:
    schema:
      type: object
      properties:
        payload:
          type: string
          contentMediaType: application/json
          contentSchema:
            type: object
            properties:
              qty: { type: integer }
```

The string is replaced by the parsed document, and `contentSchema` (if given) is applied with the
same coercion/defaults/validation — so an inner `qty: "7"` arrives as `7`.

## Handling failures

Failures are non-retryable and name the field, e.g.
`transform failed at $.items[1].qty [coercion]: cannot coerce string "oops" to integer`.

- On an **output**, the message is failed so a following [`dlq`](dlq.md) captures it.
- On an **input**, it's dropped from the batch and acknowledged, keeping invalid data out.
- `on_error: pass_through` instead forwards the original payload with the reason in the
  `mqb.transform_error` metadata key — which a [`switch`](switch.md) can route on.

Full option list, the supported schema subset, and the exact coercion rules are in the
[middleware reference → `transform`](../engine/reference.md#transform).
