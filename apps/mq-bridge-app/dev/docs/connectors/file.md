# File (CSV / JSON / JSONL)

Reads from or writes to a local file. Useful as a one-shot source/sink for
migrating data in or out of the other connectors.

## URL format

```text
file:///absolute/path/to/file?format=<normal|json|text|raw|csv>
```

The path comes from the URI path itself (`file:///...`), not a query param.
`format` defaults to `normal` (the full message serialized as JSON).

## Examples

**Load a CSV file into MongoDB, one-shot (first row = header):**

```bash
mqb copy --drain \
  --from file:///data/customers.csv?format=csv \
  --to 'mongodb://localhost?database=app&collection=customers'
```

**Export a table to JSONL, one-shot:**

```bash
mqb copy --drain \
  --from postgres://user:pass@localhost/app?table=orders \
  --to file:///data/orders.jsonl?format=json
```

**Tail a file as it grows (broadcast/subscribe mode), continuous:**

```bash
mqb copy \
  --from file:///var/log/app/events.log?mode=subscribe \
  --to kafka://kafka.local:9092?topic=app-events
```

## Key options

| Option | Purpose |
|---|---|
| `format` | `normal`, `json`, `text`, `raw`, or `csv`. |
| `delimiter` | Message delimiter. Defaults to newline. |
| `mode` | Consumer only: `consume` (from start), `subscribe` (tail from end), or persistent offset-tracked modes. |
| `compression` | Compress/decompress each batch: `none` (default), `gzip`, `lz4`, `zstd` (needs the `compression` build feature). A source must declare the same codec the file was written with. See [Compression](../cookbook/compression.md). |

Full field list: [reference/file.md](../reference/file.md).

## CSV

- **Reading:** the first record is the header, and each later record becomes a JSON object
  keyed by it. Every value is read as a string; type them with a
  [`transform`](../cookbook/transform.md) schema. Quoted fields may hold commas, doubled
  quotes (`""`) and line breaks. CRLF files, a leading UTF-8 byte-order mark (Excel's
  "CSV UTF-8") and blank lines are handled: the BOM is stripped and blank lines are skipped.
  A repeated header name gets a suffix (`a,a` → keys `a`, `a_2`) so no column is lost.
- **Writing:** the payload must be a JSON object. A new file takes its columns from the
  first message's keys, sorted; appending to a non-empty file keeps that file's header, so
  rows stay aligned with it. Fields containing `,`, `"`, a line break or the `delimiter` are
  quoted. Nested objects and arrays are written as their JSON text, and `null` as `null`. A
  payload that is not an object, or a string with no UTF-8 spelling (a lone `\ud800`
  escape), fails that message instead of writing a broken row.
- `delimiter` separates records (rows), not fields; the field separator is always `,`. For
  CSV it must not contain `,` or `"`.

## JSON lines (`normal`, `json`, `text`)

With `format: json`, a pretty-printed payload is written on one line (its line breaks are
insignificant JSON whitespace), so each message stays one line of the file. With a custom
`delimiter`, any occurrence of it inside a JSON string is written as a `\uXXXX` escape, which
decodes to the same value. A delimiter that would appear in JSON syntax itself, such as `,`,
fails the message; `0x1e` (record separator) never occurs in JSON and is a safe choice.
`raw` writes the payload untouched, so its delimiter must not occur in the payloads.
