# MongoDB

Reads from or writes to a MongoDB collection. `database` and `collection`
are separate query params — MongoDB URIs don't encode the database in the
path the way this connector reads them (unlike PostgreSQL).

## URL format

```text
mongodb://[user:pass@]host[:port]?database=<db>&collection=<name>
```

## Config (YAML / library)

The same settings as a route endpoint in a config file, or in `Route.from_config` /
`fromConfig`. Every URL query parameter is a field of the same name under `mongodb:`.

```yaml
output:
  mongodb: { url: "mongodb://localhost:27017", database: "app", collection: "orders" }
```

## Examples

**Load a CSV file into a collection, one-shot:**

```bash
mqb copy --drain \
  --from file:///data/customers.csv?format=csv \
  --to 'mongodb://localhost?database=app&collection=customers'
```

**Non-destructive read of an existing collection (default source behavior — replica set required):**

```bash
mqb copy --drain \
  --from 'mongodb://localhost?database=app&collection=customers' \
  --to 'postgres://user:pass@localhost/app?table=customers&auto_create_table=true'
```

By default (no `consume` given), a MongoDB *source* reads existing documents
then watches for changes (`capture_all`) — pointing at a collection never
claims or deletes its documents. This mode reads the oplog, so it **requires a
replica set** (a single-node one is enough) and refuses to start without one.

**Read a standalone `mongod` (no replica set), one-shot:**

```bash
mqb copy --drain \
  --from 'mongodb://localhost?database=app&collection=customers&consume=snapshot' \
  --to file:///data/customers.jsonl
```

`snapshot` pages the collection by `_id` and ends the route on drain. It is
non-destructive and needs no replica set, but it is not a tail and not
resumable — it delivers what exists when the run starts, and rejects
`cursor_id`.

**Watch for new documents only (change stream), continuous:**

```bash
mqb copy \
  --from 'mongodb://localhost?database=app&collection=orders&consume=capture_new' \
  --to kafka://kafka.local:9092?topic=orders
```

## Key options

| Option | Purpose |
|---|---|
| `database` | **Required.** |
| `collection` | Collection name. |
| `consume` | `capture_all` (default — snapshot then change stream, replica set required), `capture_new` (changes only, replica set required), `snapshot` (one-shot read, no replica set), or `consumer` (durable work queue — destructive). |
| `checkpoint_store` | Where to persist the resume cursor for `capture_new`/`capture_all`. |
| `username` / `password` | Take precedence over credentials embedded in `url`. |

Full field list: [reference/mongodb.md](../reference/mongodb.md).
