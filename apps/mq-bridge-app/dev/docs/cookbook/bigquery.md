# Load into BigQuery

There is no BigQuery connector. The [`object_store`](../connectors/object-store.md) sink writes
Parquet to Google Cloud Storage, and BigQuery either loads the files or queries them in place
through an external table.

`format: parquet` needs the `parquet` feature, which the app's default build includes.

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
              country: { type: string }
              amount: { type: number }
              note: { type: string, default: "" }
    object_store:
      url: "gs://my-bucket/orders"
      format: parquet
      compression: zstd
```

- Each payload must be a JSON object; its top-level fields become the columns.
- The sink writes one Parquet file per flushed batch, so `batch_size` sets the file size.
- The [`transform`](transform.md) schema coerces types and rejects rows that do not fit, so a
  column keeps one type across files.
- GCS credentials are read from the environment (`GOOGLE_SERVICE_ACCOUNT`, …).

## Schema drift between files

The Parquet schema is inferred **per batch**. An optional field that no row of a batch carries is
**absent from that file**. Give such a field a `default` in the schema, as `note` has above, or
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
