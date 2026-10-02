# Meilisearch

Meilisearch is an output (a document sink for a search index) and an input (a scan of an index's
documents). The endpoint lives in its own repository,
[mq-bridge-meilisearch](https://github.com/marcomq/mq-bridge-meilisearch), and talks to the REST
API directly. `mqb` has it built in; the library and the bindings load it as a plugin.

## Keep an index in sync with a Postgres table

This is the pipeline a CDC-to-search tool such as Sequin runs: read the rows that already exist,
then follow every insert, update and delete. Here it is one route.

```yaml
movies_to_search:
  batch_size: 1000
  input:
    postgres_cdc:
      url: "postgres://user:pass@localhost/app"
      publication: "movies_pub"
      slot_name: "mqb_meili"
      consume: capture_all                 # backfill first, then stream changes
      cursor_id: "movies_backfill"
      checkpoint_store: "file:///var/lib/mq-bridge/movies-phase.json"
  output:
    middlewares:
      - retry: { max_attempts: 5 }
    custom:
      name: meilisearch
      config:
        url: "http://localhost:7700"
        api_key: "${MEILI_MASTER_KEY}"
        index: "movies"
        primary_key: "id"
        operation: "${metadata:postgres.operation}"
        settings:
          searchableAttributes: ["title", "overview"]
          filterableAttributes: ["genre", "year"]
```

| A Sequin Meilisearch sink has | Here |
|---|---|
| Endpoint URL, API key | `url`, `api_key` |
| Index name | `index`; may be a template such as `${metadata:postgres.table}` |
| Primary key (default `id`) | `primary_key`; no default, so set it |
| Index action (create or update a document) | Any change that is not a delete; `method` picks `replace`, `update` or `update_existing` |
| Delete action | `operation` mapped to `postgres.operation`; `delete_values` lists what counts as a delete |
| Function action (Meilisearch function-based edits) | Not supported. Compute the value in a [`transform`](../cookbook/transform.md) before the sink |
| Transform function | [`transform`](../cookbook/transform.md) middleware; the primary key must stay a top-level field |
| Routing function | A templated `index`, or a [`switch`](../cookbook/switch.md) output |
| Backfill | `consume: capture_all` on [`postgres_cdc`](../tutorials/postgres-cdc.md) |

> The left column is taken from Sequin's sink reference. Verify it against the current Sequin
> documentation before relying on a one-to-one migration.

What to know before running it:

- **Use a literal `index` with a backfill.** Backfilled rows carry neither `postgres.operation`
  nor `postgres.table`. A missing operation is an upsert, which is right, but a templated index
  cannot resolve and the row is dead-lettered. Give each table its own publication and route.
- **Delivery is at-least-once.** A row changed during the backfill is read twice. Both writes
  carry the same `primary_key`, so the later one wins.
- **`cursor_id` and `checkpoint_store` make the backfill resumable.** Write the store as
  `file:///…`; a plain path is read as a table name in the source database.
- **`capture_all` needs a single-column primary key.** For a composite key, build one id field
  with a `transform` expression and backfill through a
  [`sequence`](../engine/reference.md#sequence) input instead.
- **Writes are confirmed.** Meilisearch answers `202 Accepted` and indexes later, so the sink
  waits for the task (`wait_for_task`, default on) before the replication slot advances.
- **Order is kept at any `concurrency`.** Two writes to the same document reach the index in
  source order.
- **A `truncate` is dead-lettered**, since it carries no row.

## Options

| Field | Applies to | Default | Meaning |
|---|---|---|---|
| `url` | both | required | Base URL. `meilisearch://` and `meilisearchs://` become `http(s)://`. |
| `api_key` | both | none | Sent as `Authorization: Bearer`. |
| `index` | both | route name | Index UID. On an output it may contain templates. |
| `primary_key` | both | none | The field Meilisearch keys documents by. |
| `method` | output | `replace` | `replace`, `update` (merge top-level fields) or `update_existing` (merge only into an existing document). |
| `operation` | output | none | Each message's change operation. |
| `delete_values` | output | `["delete"]` | Operation values that remove the document. |
| `create_index` | output | `true` | Create the index before the first write. |
| `settings` | output | none | Passed to `PATCH /indexes/{uid}/settings` before the first document. |
| `wait_for_task` | output | `true` | Wait for the indexing task before acknowledging. |
| `task_timeout_ms` | output | `60000` | How long to wait for one task. |
| `max_request_bytes` | output | `90000000` | Split a batch rather than exceed this body size. |
| `fields` | input | all | Comma-separated document fields to read. |
| `cursor_id`, `checkpoint_store` | input | none | Persist the read position (a `file://` store). |

Left unset, `primary_key` is inferred by Meilisearch from the first batch, possibly wrongly and
permanently.

## One-off copy or rebuild

```bash
mqb copy --drain \
  'postgres://user:pass@localhost/app?table=movies&cursor_column=id' \
  'meilisearch://localhost:7700?index=movies&primary_key=id&api_key=KEY'
```

Without `operation` every message is an upsert, which is all a copy needs. The
[plugin README](https://github.com/marcomq/mq-bridge-meilisearch#readme) covers a rebuild
without downtime (`/swap-indexes`), merging several tables into one document, joining a foreign
key with [`lookup`](../cookbook/lookup.md), Supabase, and reading an index back out.

## Outside `mqb`

```bash
brew install marcomq/tap/mq-bridge-meilisearch
conda install -c marcomq mq-bridge-meilisearch
pip install mq-bridge mq-bridge-meilisearch    # then: mq_bridge_meilisearch.register()
npm install mq-bridge mq-bridge-meilisearch    # then: import { register } from "mq-bridge-meilisearch"
```

From Rust, add the `mq-bridge-meilisearch` crate and call `mq_bridge_meilisearch::register()?`
before any route starts. The plugin needs mq-bridge 0.4.13 or newer.

## Limitations

- `update` merges top-level fields only; a list replaces the whole field.
- A field joined with `lookup` goes stale when the referenced row changes later.
- There is no replay log: recovering from a bad transform means reading the source again.
