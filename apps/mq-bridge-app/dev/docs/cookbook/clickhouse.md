# ClickHouse: endpoint or `s3()`

ClickHouse is the one analytics database here with a native endpoint. You can insert rows
directly, or write Parquet to object storage and let ClickHouse read it with the `s3()` table
function.

| | [`clickhouse`](../connectors/clickhouse.md) endpoint | Parquet + `s3()` |
|---|---|---|
| Latency | Rows are queryable after each batch insert | Rows appear when you run the load |
| Schema | The target table defines it; a bad row fails the insert | Inferred per file |
| Keeps a copy outside ClickHouse | No | Yes, the Parquet files |
| Best fit | Streaming into one ClickHouse table | A lake that other engines read too |

## Insert directly

```yaml
orders_to_clickhouse:
  batch_size: 10000
  input:
    kafka: { topic: "orders", url: "localhost:9092", group_id: "clickhouse-export" }
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
    clickhouse: { url: "http://localhost:8123", database: "analytics", table: "orders" }
```

The [`transform`](transform.md) schema rejects rows that would not fit the table before they
reach ClickHouse. Put a [`dlq`](dlq.md) after it to keep them.

## Write Parquet, load with `s3()`

```yaml
orders_to_lake:
  batch_size: 10000
  input:
    kafka: { topic: "orders", url: "localhost:9092", group_id: "lake-export" }
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

`format: parquet` needs the `parquet` feature, which the app's default build includes. The sink
writes one Parquet file per flushed batch.

The Parquet schema is inferred **per batch**. An optional field that no row of a batch carries is
**absent from that file**. Give such a field a `default` in the schema, as `note` has above, or
let ClickHouse fill missing columns and merge the file schemas by name.

> Verify this SQL against the current ClickHouse documentation.

```sql
INSERT INTO analytics.orders
SELECT id, country, amount, note
FROM s3('https://my-bucket.s3.amazonaws.com/orders/*.parquet', 'Parquet')
SETTINGS
  input_format_parquet_allow_missing_columns = 1,  -- a column absent from a file reads as default
  schema_inference_mode = 'union';                 -- merge the schemas of all files by name
```

Running the `INSERT` again loads the same files again. Select by path (`_path`) or load each
prefix once.
