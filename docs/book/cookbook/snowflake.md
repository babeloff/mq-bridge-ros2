# Load into Snowflake

There is no Snowflake connector. The [`object_store`](../connectors/object-store.md) sink writes
Parquet to S3, GCS or Azure, and Snowflake loads it from an external stage with `COPY INTO` or
Snowpipe.

`format: parquet` needs the `parquet` feature, which the app's default build includes.

## Write Parquet to the stage location

```yaml
orders_to_snowflake_stage:
  batch_size: 10000
  input:
    kafka: { topic: "orders", url: "localhost:9092", group_id: "snowflake-export" }
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
- S3 credentials are read from the environment (`AWS_ACCESS_KEY_ID`, …).

## Schema drift between files

The Parquet schema is inferred **per batch**. An optional field that no row of a batch carries is
**absent from that file**, not written as a null column. Give such a field a `default` in the
schema, as `note` has above, or match columns by name on the Snowflake side. Never load by column
position.

## Load it

> Verify this SQL against the current Snowflake documentation.

```sql
CREATE STAGE orders_stage
  URL = 's3://my-bucket/orders/'
  STORAGE_INTEGRATION = my_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

COPY INTO orders
  FROM @orders_stage
  FILE_FORMAT = (TYPE = PARQUET)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

`MATCH_BY_COLUMN_NAME` loads each file by column name, so a file that lacks a column loads `NULL`
for it.

For continuous loading, wrap the same statement in a pipe and point the bucket's event
notifications at it:

```sql
CREATE PIPE orders_pipe AUTO_INGEST = TRUE AS
  COPY INTO orders
    FROM @orders_stage
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

## Replays

With a replayable input such as Kafka, the default `name_by: auto` names each file after the
source range it holds, and a restart skips ranges already written. See
[Choose the file layout](duckdb.md#choose-the-file-layout) for the naming options and date
partitions.
