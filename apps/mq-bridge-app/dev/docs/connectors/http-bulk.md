# HTTP bulk (search engines)

Writes JSON documents in bulk to an HTTP API, and reads them back page by page:
one request per batch instead of one per message. It is meant for search engines
and similar document stores. The endpoint knows no product by name. As an output
you describe three things, and a new target is a few lines of configuration:

- **upsert**: the request that writes documents, and the body it carries;
- **delete**: the request that removes documents by id;
- **result**: how the target reports what happened.

As an input you describe one: **read**, the request that lists a page of
documents and where the next page starts. See [Reading](#reading).

It needs the `http-bulk` feature, which `full` includes.

## On the command line

The configuration is too nested for query parameters, so the `http-bulk:` scheme
takes it whole: from a file with `config_file`, or inline as YAML or JSON with
`config`. The value holds the fields listed below, with or without the
`http_bulk:` key around them. `url` replaces the target, so one file serves
several hosts.

```sh
mqb copy 'postgres://app@localhost/shop?table=books' \
  'http-bulk:?config_file=meilisearch.yaml&url=http://search:7700'

mqb copy 'file://books.jsonl' \
  'http-bulk:?config={url: "http://localhost:8108", upsert: {path: /docs}}'

mqb copy 'http-bulk:?config_file=couchdb-changes.yaml' 'file://changes.jsonl' --drain
```

Percent-encode a `&`, `#` or `?` inside an inline `config`; a file needs no
escaping.

## Tested targets

Each recipe was run against the version named. Anything else with a bulk HTTP
API is a few lines of configuration away, but has not been tried.

| Target | Version | Upsert | Delete | Failures reported | Read |
| --- | --- | --- | --- | --- | --- |
| [Meilisearch](#meilisearch) | 1.53 | yes | yes | per request (task) | [by offset](#by-offset-meilisearch) |
| [Typesense](./typesense.md) | 29 | yes | yes | per document | not tried |
| [Elasticsearch](./elasticsearch.md) | 8.19 | yes | yes | per document | not tried |
| [PostgREST / Supabase](./postgrest.md) | PostgREST 16.4 | yes | yes | per request | [by key](#by-key-postgrest) |
| [Qdrant](#qdrant) | 1.19 | yes | yes | per request | [full scan](#full-scan-qdrant) |
| [CouchDB](#couchdb) | 3.5 | insert only | no | per document | [change feed](#change-feed-couchdb) |

## Recipes

In the recipes `operation` reads the operation a Postgres change stream sets,
which tells an upsert from a delete.

### Meilisearch

Meilisearch answers at once with a task and indexes later, so the outcome is a
polled job. A failed task fails every document of its request.

```yaml
output:
  http_bulk:
    url: http://localhost:7700
    headers:
      Authorization: Bearer <api key>
    operation: "${metadata:postgres.operation}"
    upsert:
      path: /indexes/books/documents?primaryKey=id
      result: &task
        job:
          id: /taskUid
          poll: /tasks/{id}
          status: /status
          succeeded: [succeeded]
          failed: [failed, canceled]
          error: /error/message
    delete:
      path: /indexes/books/documents/delete-batch
      result: *task
```

The built-in [`meilisearch`](./meilisearch.md) endpoint is this recipe under a
name of its own. Index settings and partial updates are in the separate plugin. Loading 200,000 small
documents took the same time with either (about 4 to 5 s at a batch size of
50,000 on a laptop).

### Qdrant

A point is `{id, vector, payload}`, and the id is an unsigned integer or a UUID,
so shape the message with a [`transform`](../cookbook/transform.md) first.
`wait=true` makes Qdrant answer after the points are stored.

```yaml
output:
  http_bulk:
    url: http://localhost:6333
    operation: "${metadata:postgres.operation}"
    upsert:
      method: PUT
      path: /collections/books/points?wait=true
      format: json_array
      envelope: '{"points": {documents}}'
    delete:
      path: /collections/books/points/delete?wait=true
      envelope: '{"points": {ids}}'
```

### CouchDB

Inserts only. CouchDB updates and deletes a document by its current `_rev`,
which a change message does not carry, so a document whose `_id` exists is
reported as a conflict and fails alone.

```yaml
output:
  http_bulk:
    url: http://localhost:5984
    headers:
      Authorization: Basic <base64 of user:password>
    upsert:
      path: /books/_bulk_docs
      format: json_array
      envelope: '{"docs": {documents}}'
      result:
        items:
          error: /reason
```

## Reading

As an `input` the endpoint asks for one page per batch. `{limit}` in the request
is the route's `batch_size`, and `{cursor}` is the read position. Each document
of the page becomes one message with the document as its JSON payload. Where the
position after a page comes from is the only thing that differs between APIs:

| `read.cursor` | Next position | Fits |
| --- | --- | --- |
| unset | the number of documents read so far | `offset=` paging |
| `item: <pointer>` | a field of the last document | `id > last` paging |
| `response: <pointer>` | a value in the response | change feeds, scroll tokens |

With `cursor_id` and `checkpoint_store` in `read` the position is saved after every
acknowledged batch and a restart continues there. Without them every start
reads from the beginning.

### Change feed: CouchDB

A real change stream: updates and deletes arrive too, and the feed never ends.
A deleted document has `"deleted": true` next to its `doc`.

```yaml
input:
  http_bulk:
    url: http://localhost:5984
    headers:
      Authorization: Basic <base64 of user:password>
    read:
      path: /books/_changes?include_docs=true&limit={limit}&since={cursor}
      items: /results
      cursor:
        response: /last_seq
        start: 0
      cursor_id: books
      checkpoint_store: file:///var/lib/mqb/cursors.json
```

### Full scan: Qdrant

Scroll reads every point once. Qdrant answers the last page with a `null`
offset, which ends the read; the end is saved, so a restart reads nothing. It is
a copy, not a change feed: points written behind the position are missed.

```yaml
input:
  http_bulk:
    url: http://localhost:6333
    read:
      path: /collections/books/points/scroll
      body: '{"limit": {limit}, "offset": {cursor}, "with_payload": true, "with_vector": true}'
      items: /result/points
      cursor:
        response: /result/next_page_offset
```

### By offset: Meilisearch

```yaml
input:
  http_bulk:
    url: http://localhost:7700
    headers:
      Authorization: Bearer <api key>
    read:
      path: /indexes/books/documents?offset={cursor}&limit={limit}
      items: /results
```

An offset is only stable while nothing is deleted or reordered during the read.

### By key: PostgREST

The response is the array itself, so `items` stays empty. New rows with a larger
key are picked up by later polls; updates of rows already read are not.

```yaml
input:
  http_bulk:
    url: http://localhost:3000
    read:
      path: /books?id=gt.{cursor}&order=id&limit={limit}
      cursor:
        item: /id
        start: 0
```

With direct access to the database, the [Postgres connector](./postgres.md) is
the better source: its change stream sees updates and deletes.

## Fields

| Field | Default | Meaning |
| --- | --- | --- |
| `url` | required | Base URL of the target |
| `headers` | none | Sent with every request; stored as secrets |
| `upsert` | required for an output | The request that writes documents |
| `read` | required for an input | The request that reads a page of documents |
| `delete` | none | The request that removes documents |
| `operation` | none | Template for a message's operation; without it every message is an upsert |
| `delete_values` | `delete`, `d` | Operation values that mean delete, ignoring case |
| `max_request_bytes` | 10 MiB | A larger batch is split into several requests |
| `compression` | `none` | `gzip`, `zstd` or `lz4` for request bodies, named in `Content-Encoding` |
| `request_timeout_ms` | none | Timeout of one request |
| `connect_timeout_ms` | 10000 | Connection timeout |
| `tls` | none | `ca_file` and `accept_invalid_certs` for `https://` |

`upsert`:

| Field | Default | Meaning |
| --- | --- | --- |
| `path` | required | Path and query appended to `url` |
| `method` | `POST` | HTTP method |
| `format` | `ndjson` | `ndjson` (one document per line) or `json_array` |
| `content_type` | by format | `application/x-ndjson` or `application/json` |
| `action` | none | `ndjson` only: a line sent before each document, e.g. `{"index":{"_id":"${payload:id}"}}`. Values taken from the message are JSON-escaped. |
| `envelope` | none | `json_array` only: the body around the array; `{documents}` marks where the array goes |
| `result` | HTTP status | See below |

`read`:

| Field | Default | Meaning |
| --- | --- | --- |
| `path` | required | Path and query. `{cursor}` is replaced by the position, percent-encoded; `{limit}` by the batch size |
| `method` | `GET`, or `POST` with a `body` | HTTP method |
| `body` | none | JSON request body; `{cursor}` is replaced by the position as JSON (`7`, `"a"`, `null`) and `{limit}` by the batch size |
| `items` | the response | JSON pointer to the array of documents |
| `cursor.response` | none | JSON pointer to the next position in the response; `null` there ends the read |
| `cursor.item` | none | JSON pointer to the position in the last document of a page |
| `cursor.start` | `0` for a count, else `null` | Position before the first read |
| `cursor_id` | none | Names the saved read position |
| `checkpoint_store` | none | Where the position is saved: `file://`, `postgres://`, `mongodb://` or `s3://` |
| `polling_interval_ms` | 1000 | Wait after an empty page |
| `max_polling_interval_ms` | none | The wait doubles up to this while pages stay empty |

`{cursor}` must occur in the path or the body.

`delete`:

| Field | Default | Meaning |
| --- | --- | --- |
| `path` | required | With `{ids}` the ids go there, comma-separated and percent-encoded; without it they are sent as a JSON array body. An id containing a comma cannot be sent in the URL form. |
| `method` | `POST` | HTTP method |
| `id_field` | `id` | Top-level payload field holding the id (a string or a number) |
| `max_ids` | 1000 | Most ids in one request |
| `envelope` | none | The body around the id array; `{ids}` marks where the array goes |
| `line` | none | One body line per id instead of an array; `{id}` is replaced by the id as JSON (`7` or `"a"`) |
| `result` | HTTP status | See below |

`result` takes at most one of:

| Field | Meaning |
| --- | --- |
| `lines` | The response has one JSON line per document, in order. `success` is a JSON pointer to a boolean; `error` points to the reason. |
| `items` | The response is one JSON document with an array entry per document, in order. `path` is a JSON pointer to the array (empty when the response is the array); `error` points to an entry's error text, and an entry that has it failed. |
| `job` | The response names a job. `id` points to it, `poll` is the path asked with GET (`{id}` is replaced), `status` points to its state. `succeeded` and `failed` list the final states; any other state is polled again. `error` points to the reason. |

Without `result`, a 2xx status means every document was written.

## Behaviour

- **Order is kept.** A batch is cut into runs of upserts and deletes and sent in
  that order. After a request that failed but may be retried, nothing later in
  the batch is sent. With `operation` set, batches are also sent one at a time,
  whatever the route's `concurrency`, so a delete cannot overtake an upsert.
- **Failures are per document where the target says so.** A payload that is not
  a JSON object, a rejected result line and a delete without an id fail that
  message for good. A failed job or a refused request fails every document of
  that request for good, and the rest of the batch is still sent.
- **Payloads are not parsed.** A payload that starts like a JSON object but is
  not valid JSON goes out as it is, and the target refuses the whole request.
- **Compression needs a target that reads it.** The body is compressed whole
  and named in `Content-Encoding`. `gzip` was run against Elasticsearch; check
  your target before choosing `zstd` or `lz4`.
- **Redirects are not followed**, so credentials in `headers` stay with the
  configured host.
- **Retries follow the status.** 408, 429 and 5xx (except 501 and 505) and
  connection errors are retryable; other statuses are not. Add the `retry`
  middleware to retry them and `dlq` to keep what failed for good.
- **A job has no deadline.** The endpoint polls until the job ends and logs a
  warning every minute.
- **Truncate is not applied.** A truncate message fails for good.

As an input:

- **At least once.** The position moves when a batch is acknowledged. After a
  failed message the read continues behind the last acknowledged document; a
  page whose position comes from the response is read again as a whole.
- **Errors follow the status** as above. A response that is not JSON, or has no
  array at `items` or no position where `cursor` points, stops the route.
- **Documents are not reshaped.** The payload is the array entry as it came. Use
  a [`transform`](../cookbook/transform.md) to take CouchDB's `doc` out of a
  change, for example.
- **A draining route does not wait.** With `--drain` the first empty page ends
  the copy.

Set the route's `batch_size` to a few thousand or more (`--batch-size` on the
command line, where the default is 1024): each batch is one request, and search
engines index large requests far faster than small ones. Against Meilisearch,
200,000 documents took 16 s at 1024 and 4 s at 50,000.

A target that is not listed needs three answers from its API documentation:
what the bulk write request looks like, how documents are removed by id, and
whether the response says anything per document. To read from it, it needs one
more: how a request names the page after the last one. If no combination of the
fields above fits, the endpoint cannot be used with it. An export that streams
everything in one response (Typesense) is not a paged listing and cannot be read.
