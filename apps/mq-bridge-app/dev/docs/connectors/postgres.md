# PostgreSQL / MySQL / MariaDB / SQLite

The `sqlx` connector reads and writes rows in a relational database table.
The same connector backs all four schemes — the URL scheme just selects the
driver. Postgres additionally has a dedicated [CDC connector](#postgresql-cdc)
for streaming change data instead of table reads.

## URL format

```text
postgres://[user:pass@]host[:port]/database?table=<name>
```

The scheme (`postgres`/`postgresql`, `mysql`, `mariadb`, `sqlite`) and
everything up to the query string is passed straight through as the driver
connection string; `table` (and any other recognised field below) is pulled
out of the query string into config. For SQLite, `host`/`database` are
replaced by a file path, e.g. `sqlite:///var/data/app.db?table=orders`.

## Config (YAML / library)

The same settings as a route endpoint in a config file, or in `Route.from_config` /
`fromConfig`. Every URL query parameter is a field of the same name under `sqlx:`.

```yaml
input:
  sqlx: { url: "postgres://user:pass@localhost/app", table: "orders", cursor_column: "id" }
```

The config key is `sqlx` for PostgreSQL, MySQL/MariaDB, and SQLite; the connection string is the `url`.

## Examples

**Full-table read (source), one-shot:**

```bash
mqb copy --drain \
  --from postgres://user:pass@localhost/app?table=orders \
  --to null:
```

**Write with auto-created table (destination):**

```bash
mqb copy --drain \
  --from file:///data/orders.csv?format=csv \
  --to 'postgres://user:pass@localhost/app?table=orders&auto_create_table=true'
```

**Resumable incremental read, keyed by a monotonic column, continuous:**

```bash
mqb copy \
  --from 'postgres://user:pass@localhost/app?table=orders&cursor_column=id&cursor_id=orders_export' \
  --to kafka://kafka.local:9092?topic=orders
```

Each restart resumes from the last `id` seen (persisted via `cursor_id`)
instead of re-copying from the start.

**Custom multi-column insert, MySQL:**

```bash
mqb copy --drain \
  --from postgres://user:pass@src/app?table=orders \
  --to 'mysql://user:pass@dst/app?table=orders&insert_query=INSERT+INTO+orders+%28id%2C+sku%2C+qty%29+VALUES+%28%24%7Bpayload%3Aid%7D%2C+%24%7Bpayload%3Asku%7D%2C+%24%7Bpayload%3Aqty%7D%29'
```

(`insert_query` is shown URL-encoded above — the SQL is
`INSERT INTO orders (id, sku, qty) VALUES (${payload:id}, ${payload:sku}, ${payload:qty})`.)

## Key options

| Option | Purpose |
|---|---|
| `table` | **Required.** Table to read from / write to. |
| `cursor_column` + `cursor_id` | Non-destructive, resumable incremental reads instead of a one-shot full-table copy. |
| `checkpoint_store` | (Consumer, `cursor_column` mode) Where to persist the resume cursor. Absent → a `mqb_cursors_<table>` table in the **source** database; a bare name reuses the source datastore with that table; a URL (`file://`, `postgres://`, `mysql://`, `mongodb://`, `s3://`/`gs://`/`az://`/`abfs://`) selects an external backend. Treated as a secret since it may embed credentials. |
| `auto_create_table` | Publisher creates the destination table if missing. |
| `insert_query` | Custom INSERT with `${payload:field}` / `${metadata:key}` tokens for multi-column writes. |
| `bulk_copy` | PostgreSQL only — use `COPY FROM STDIN` for high-throughput bulk loads. |
| `delete_after_read` | Consumer deletes rows after they're processed (mutually exclusive with `cursor_column`). |

Any other query parameter (e.g. `sslmode=disable`) is left on the connection
URL untouched and passed to the driver as-is.

Full field list, types, and defaults: [reference/postgres.md](../reference/postgres.md).

## PostgreSQL CDC

A separate connector for streaming logical-replication changes (insert/
update/delete) instead of reading a table snapshot. Uses `postgres-cdc://`
(alias `pgcdc://`) to select the endpoint kind; the connection URL underneath
it is a plain Postgres URL.

```text
postgres-cdc://[user:pass@]host[:port]/database?publication=<name>&slot_name=<name>
```

In config, the key is `postgres_cdc`:

```yaml
input:
  postgres_cdc: { url: "postgres://user:pass@localhost:5432/app", publication: "orders_pub", slot_name: "mqb_orders" }
```

**Stream changes from a publication into Kafka, continuous:**

```bash
mqb copy \
  --from 'postgres-cdc://user:pass@localhost/app?publication=mqb_pub&slot_name=mqb_slot' \
  --to kafka://kafka.local:9092?topic=app-changes
```

**Replicate a table into another PostgreSQL instance, continuous:**

```bash
mqb copy \
  --from 'postgres-cdc://user:pass@localhost/app?publication=mqb_pub&slot_name=mqb_slot' \
  --to 'postgres://user:pass@otherhost/replica?table=orders&auto_create_table=true'
```

`publication` must already exist on the source (`CREATE PUBLICATION mqb_pub
FOR TABLE orders;`); `slot_name` is created automatically if missing.

Full field list: [reference/postgres-cdc.md](../reference/postgres-cdc.md).
