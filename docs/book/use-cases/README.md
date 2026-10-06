<!-- description: Problem-first guides for mq-bridge: keep a vector index in sync with Postgres, implement the transactional outbox pattern, test message handlers without a broker. Each has a runnable example. -->

# Use cases

Each page starts from a problem, shows one working solution with mq-bridge, and says where it
stops working. Every page has a runnable example in the repository's
[`examples/`](https://github.com/marcomq/mq-bridge/tree/main/examples) directory that CI executes.

| Problem | Solution | Example |
| :--- | :--- | :--- |
| A RAG or semantic-search index drifts from the Postgres rows it was built from | [Keep a vector index in sync with Postgres](sync-postgres-to-qdrant.md): CDC → embeddings API → Qdrant, with backfill and deletes | [`postgres-cdc-to-qdrant`](https://github.com/marcomq/mq-bridge/tree/main/examples/postgres-cdc-to-qdrant) |
| A service must update its database and publish an event, and the two writes can diverge | [Transactional outbox with Postgres and Kafka](transactional-outbox-postgres-kafka.md): relay an outbox table from the WAL | [`transactional-outbox`](https://github.com/marcomq/mq-bridge/tree/main/examples/transactional-outbox) |
| Tests for Kafka, NATS or RabbitMQ consumers need a running broker or mock the client | [Test message handlers without a broker](test-without-a-broker.md): swap the transport for an in-memory endpoint | [`test-without-a-broker`](https://github.com/marcomq/mq-bridge/tree/main/examples/test-without-a-broker) |

Every page has the same sections: the problem, the solution with a diagram and configuration, a
runnable example, failure semantics (restarts, duplicates, deletes, an unavailable sink), when not
to use mq-bridge, and a comparison with alternatives.

Related recipes elsewhere in this book: [query a stream in DuckDB](../cookbook/duckdb.md),
[Postgres CDC to a file](../tutorials/postgres-cdc.md),
[cross-process IPC](../tutorials/ipc-bridge.md).
