# Transactional outbox: Postgres → Kafka

The application writes an event row into an `outbox` table in the same transaction as its business
change. mq-bridge reads the table's inserts from the write-ahead log and publishes each one to
Kafka, keyed by the aggregate id. A rolled-back transaction publishes nothing.

Explained in the book: [Transactional outbox with Postgres and Kafka](https://marcomq.github.io/mq-bridge/use-cases/transactional-outbox-postgres-kafka.html).

```mermaid
flowchart LR
  A[Application] -- "one transaction:<br/>orders + outbox" --> PG[(Postgres)]
  PG -- logical replication --> B[mq-bridge]
  B -- "key = aggregate_id" --> K[[Kafka topic<br/>order-events]]
```

## Run it

Needs Docker.

```bash
docker compose up -d --wait
docker compose exec -T postgres psql -U app -d app <<'SQL'
BEGIN;
INSERT INTO orders (sku, qty) VALUES ('book-42', 2);
INSERT INTO outbox (aggregate_id, event_type, payload)
VALUES ('order-1', 'order.placed', '{"order_id": 1, "sku": "book-42", "qty": 2}');
COMMIT;
SQL
./consume.sh
```

`consume.sh` prints each record as `<key> <value>`, the value being the outbox row as JSON:

```text
order-1 {"id":1,"aggregate_id":"order-1","event_type":"order.placed","payload":{"qty":2,"sku":"book-42","order_id":1},"created_at":"2026-10-05 18:34:49.707225+00"}
```

The relay only sees rows written after its replication slot exists, which takes a few seconds
after the first start. `./test.sh` waits for the slot, then checks a commit, a rollback, a cleanup
`DELETE` and a restart; CI executes it. `docker compose down -v` removes everything.

## Files

| File | Purpose |
| :--- | :--- |
| [`routes.yaml`](routes.yaml) | The route: `postgres_cdc` input, `kafka` output keyed by `aggregate_id` |
| [`docker-compose.yml`](docker-compose.yml) | Postgres 16, Apache Kafka 3.9.0 (KRaft), mq-bridge-app 0.4.18 |
| [`init.sql`](init.sql) | The `orders` and `outbox` tables and an insert-only publication |
| [`consume.sh`](consume.sh) | Reads the topic from the beginning with the Kafka console consumer |
