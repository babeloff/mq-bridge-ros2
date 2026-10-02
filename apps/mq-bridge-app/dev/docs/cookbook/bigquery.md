# Load into BigQuery

There is no native BigQuery connector. The [`object_store`](../connectors/object-store.md) sink writes
Parquet to Google Cloud Storage, and BigQuery either loads the files or queries them in place
through an external table.

`format: parquet` needs the `parquet` feature, which the app's default build includes.

To insert rows into a table directly instead, the [Connect plugin](../connectors/connect.md) has
a [`gcp_bigquery` output](../connectors/connect-outputs.md#gcp_bigquery).

## Write Parquet to GCS

```yaml
orders_to_gcs:
  batch_size: 10000
  input:
    postgres_cdc:
      url: "postgres://user:pass@localhost:5432/app"
      publication: "mqb_pub"
      slot_name: "mqb_bigquery"
  output:
    middlewares:
      - transform:
          schema:
            type: object
            required: [id]
            properties:
              id: { type: integer }
              country: { type: string, default: "" }
              amount: { type: number, default: 0 }
              note: { type: string, default: "" }
    object_store:
      url: "gs://my-bucket/orders"
      format: parquet
      compression: zstd
```

- Each payload must be a JSON object; its top-level fields become the columns.
- The sink writes one Parquet file per flushed batch, so `batch_size` sets the file size.
- The [`transform`](transform.md) schema coerces types (`"42"` becomes `42`) and rejects rows that
  do not fit, so a column does not flip between string and number across files. A `number`
  column is still written as a 64-bit integer when a batch holds only whole numbers, so declare
  it as a float column in the warehouse.
- A rejected row is dropped and logged. Add a [`dlq`](dlq.md) after the `transform` to keep it.
- `null` takes the field's `default` if it has one and is rejected otherwise. Mark a column the
  source can leave empty as `nullable: true` to keep the `null`.
- `postgres_cdc` emits one message per change, so the files are a change log, not the table's
  current state. A delete carries the key with every other column `null` (default replica
  identity); it is rejected by the schema above unless those columns are `nullable`. The
  operation is in the `postgres.operation` metadata, not in the payload.
- GCS credentials are read from the environment (`GOOGLE_SERVICE_ACCOUNT`, …).

## Schema drift between files

The Parquet schema is inferred **per batch**. An optional field that no row of a batch carries is
**absent from that file**. Give such a field a `default` in the schema, as the optional columns have above, or
create the BigQuery table with every column up front so a file that lacks one loads `NULL`.
BigQuery matches Parquet columns by name.

## Load it

> Verify this SQL against the current BigQuery documentation.

A load job copies the data into a native table:

```sql
LOAD DATA INTO mydataset.orders
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://my-bucket/orders/*.parquet']
);
```

A wildcard load requires every matched file to have the same schema. Give every optional column a
`default` so no file lacks it, or load files with differing schemas in separate jobs.

A load job appends whatever the URI matches. Running it twice over the same files loads them
twice, so load each file once (for example by date prefix) or use an external table instead.

An external table leaves the data in GCS and reads the current set of files on every query:

```sql
CREATE EXTERNAL TABLE mydataset.orders_ext
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-bucket/orders/*.parquet']
);
```

## Replays and layout

With a replayable input such as Postgres CDC, the default `name_by: auto` names each file after
the source range it holds, and a restart skips ranges already written. For date folders that a
load job can address by prefix, see [Choose the file layout](duckdb.md#choose-the-file-layout).
