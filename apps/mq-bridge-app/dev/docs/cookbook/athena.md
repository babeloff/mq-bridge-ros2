# Query from Athena / Trino

There is no Athena or Trino connector, and none is needed: both query Parquet files where they
lie. The [`object_store`](../connectors/object-store.md) sink writes the files, and an external
table makes them queryable.

`format: parquet` needs the `parquet` feature, which the app's default build includes.

## Write Parquet to S3

```yaml
orders_to_s3:
  batch_size: 10000
  input:
    kafka: { topic: "orders", url: "localhost:9092", group_id: "athena-export" }
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
      url: "s3://my-bucket/orders"
      format: parquet
      compression: zstd
      name_by: write_time
      date_partition_style: hive   # year=YYYY/month=MM/day=DD/<uuidv7>.parquet
```

- Each payload must be a JSON object; its top-level fields become the columns.
- The sink writes one Parquet file per flushed batch, so `batch_size` sets the file size. Athena
  bills by data scanned and slows down on many small files, so keep batches large.
- The [`transform`](transform.md) schema coerces types and rejects rows that do not fit, so a
  column keeps one type across files.
- The date folders are the write time (UTC), not a field of the record. Write-time names do not
  recognise a replay; drop the last two lines to get replay-safe names in a flat layout instead.
  See [Choose the file layout](duckdb.md#choose-the-file-layout).

## Schema drift between files

The Parquet schema is inferred **per batch**. An optional field that no row of a batch carries is
**absent from that file**. Declare every column in the table definition and let the engine
resolve Parquet columns **by name**: a file that lacks a column then reads `NULL` for it. Giving
the field a `default` in the schema, as `note` has above, keeps the column in every file.

## Query it

> Verify this SQL against the current Athena and Trino documentation.

```sql
CREATE EXTERNAL TABLE orders (
  id      bigint,
  country string,
  amount  double,
  note    string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://my-bucket/orders/';

MSCK REPAIR TABLE orders;   -- registers new year=/month=/day= folders

SELECT country, sum(amount)
FROM orders
WHERE year = '2026' AND month = '09'
GROUP BY country;
```

`MSCK REPAIR TABLE` has to run again when a new day's folder appears; partition projection
avoids that.

In Trino, the same files are a Hive-connector table:

```sql
CREATE TABLE hive.lake.orders (
  id bigint, country varchar, amount double, note varchar,
  year varchar, month varchar, day varchar
)
WITH (
  external_location = 's3://my-bucket/orders/',
  format = 'PARQUET',
  partitioned_by = ARRAY['year', 'month', 'day']
);
```

Keep the Hive connector reading Parquet columns by name (`hive.parquet.use-column-names`).
