# HttpArena entries for mq-bridge

Entries for [HttpArena](https://github.com/MDA2AV/HttpArena) (the spiritual
successor to TechEmpower), shaped to drop into a fork of that repo under
`frameworks/`. Each protocol variant is its own framework directory, because
HttpArena models a "framework" as one binary opting into a set of test profiles
via `meta.json`.

## Frameworks in this directory

| Directory                | Language | Port  | Profiles |
|--------------------------|----------|-------|----------|
| `frameworks/mq-bridge/`            | Rust   | 8080-8082 + 8443 | baseline, async, latency-1m, latency-10k, latency-500k-8cpu, pipelined, limited-conn, json-comp, json-tls, 8gbit, static-tls, async-db, fortunes, baseline-h2, static-h2, baseline-h2c, json-h2c, unary-grpc, unary-grpc-tls |
| `frameworks/mq-bridge-h2c/`        | Rust   | 8082 | baseline-h2c, json-h2c |
| `frameworks/mq-bridge-websocket/`  | Rust   | 8080 | echo-ws |
| `frameworks/mq-bridge-py/`         | Python | 8080-8082 + 8443 | baseline, latency-1m, latency-10k, latency-500k-8cpu, pipelined, limited-conn, json-comp, 8gbit, async-db, fortunes, json-tls, baseline-h2, static-tls, static-h2, baseline-h2c, json-h2c, async |

## Endpoint contract (implemented)

A single catch-all `http -> response` route dispatches on `http_method` /
`http_path` / `http_query` metadata:

| Route                          | Behaviour                                            | Profiles            | Entries |
|--------------------------------|------------------------------------------------------|---------------------|---------|
| `GET  /pipeline`               | `ok`                                                 | baseline/pipelined/limited-conn | Rust, Python |
| `GET  /baseline11?a=&b=`       | `a+b`                                                | baseline            | Rust, Python |
| `POST /baseline11?a=&b=`+body  | `a+b+body`                                           | baseline            | Rust, Python |
| `GET  /baseline2?a=&b=`        | `a+b`                                                | baseline            | Rust, Python |
| `GET  /json/{count}?m=`        | processed dataset JSON, `total = price*quantity*m`   | json / json-comp    | Rust, Python |
| `POST /echo`+body              | the request body, unchanged                          | 8gbit / in-out      | Rust, Python |
| `GET  /delay/{ms}`             | `ms`, after waiting `ms` ms                          | async               | Rust, Python |
| `GET  /async-db?min=&max=&limit=` | Postgres `items` rows as JSON                     | async-db            | Rust, Python |
| `GET  /fortunes`               | rendered HTML table                                  | fortunes            | Rust, Python |
| `GET/POST/PUT /crud/items[/{id}]` | paginated list, cached item, upsert, update       | crud                | Rust, Python |
| `GET  /static/{file}`          | file from `/data/static` (path-traversal-safe)       | static              | Rust, Python |
| `POST /benchmark.BenchmarkService/GetSum` | gRPC `SumReply{result=a+b}`, `grpc-status` trailer | unary-grpc, unary-grpc-tls | Rust only |

Harness inputs: dataset from `/data/dataset.json` (`DATASET_PATH`), static assets
from `/data/static` (`STATIC_DIR`), Postgres from `DATABASE_URL`. A missing DB is
non-fatal — `/async-db` returns `{"items":[],"count":0}` so the cleartext
profiles still run.

## How it works

- **Auto for core, explicit H2 for h2c.** mq-bridge's default HTTP server uses
  hyper-util's `AutoBuilder`, which negotiates HTTP/1.1 **and** HTTP/2
  prior-knowledge (h2c) on the same plaintext port. The core `mq-bridge` entry
  binds **8080** (HTTP/1.1 + h2c) and **8443** (HTTP/2 over TLS) in one process,
  sharing the same handler; `mq-bridge-h2c` uses the library's
  `server_protocol = http2_only` setting on **8082** so HttpArena's explicit
  h2c profiles cannot fall back to HTTP/1.1.
- **HTTP/2 over TLS (`baseline-h2`, `static-h2`).** The library advertises ALPN
  `h2` on the TLS listener, so conformant clients negotiate HTTP/2; the TLS route
  reads its cert/key from `/certs/server.crt` + `/certs/server.key` (overridable
  via `TLS_CERT` / `TLS_KEY`) and is skipped when the certs are absent, so a local
  plaintext-only run still works. Uses the `ring` crypto provider.
- **json-comp via response compression.** Setting `compression_enabled: true` (with a
  256-byte threshold) makes the server gzip bodies when the client advertises
  `Accept-Encoding: gzip`, identity otherwise — so the one `/json` handler serves
  both `json` and `json-comp`.
- **echo-ws via `websocket -> response`.** mq-bridge's WebSocket consumer turns
  each inbound frame into a message tagged with `ws_message_type` (text/binary);
  the handler returns it unchanged and the Response output replies on the same
  socket, preserving frame type.
- **async-db without the sqlx *endpoint*.** mq-bridge's `sqlx` endpoint models a
  table as a message queue (publisher `INSERT`, consumer polling `SELECT`), which
  has no per-request parameter or request↔reply correlation. So the handler owns
  a pool directly: `sqlx::PgPool` (Rust) / `psycopg_pool` (Python), running
  `SELECT ... FROM items WHERE price BETWEEN $1 AND $2 LIMIT $3`.
- **unary-grpc on the HTTP listener (Rust entry).** gRPC is HTTP/2, so the same route serves
  `POST /benchmark.BenchmarkService/GetSum` on 8080 (h2c) and 8443 (h2 over TLS).
  The handler decodes the length-prefixed `SumRequest` frame and replies with
  `content-type: application/grpc`; reply metadata `http_trailer.grpc-status`
  is sent as an HTTP/2 trailer (needs mq-bridge >= 0.4.17).
- **Off-GIL (Python).** All HTTP framing and the inline response stay in Rust;
  the Python handler runs only the per-request dispatch and JSON assembly.

## Caveats — what is *not* reachable on the stock library

These profiles are intentionally **not** included, because the library cannot
serve them faithfully without further work:

- **Streaming gRPC (`stream-grpc` etc.).** Not implemented in these entries;
  only the unary `GetSum` is served.
- **HTTP/3 / QUIC.** No `quinn`/`h3` transport in the library; HTTP/3 would need a
  new UDP/QUIC listener, not a config change.

> **HTTP/2 over TLS is now supported.** Earlier revisions of these entries
> excluded it because `create_rustls_server_config` did not set
> `alpn_protocols`. The library now advertises ALPN `h2` on the TLS listener, so
> the core entry covers `baseline-h2` / `static-h2` on 8443 directly — no separate
> `mq-bridge-tls` framework needed.

## Layout

```text
scripts/httparena/frameworks/
  mq-bridge/           Cargo.toml  Dockerfile  meta.json  src/main.rs   # core: 8080 (H/1.1 + h2c) + 8443 (h2 over TLS)
  mq-bridge-h2c/       Cargo.toml  Dockerfile  meta.json  src/main.rs   # explicit h2c on 8082
  mq-bridge-websocket/ Cargo.toml  Dockerfile  meta.json  src/main.rs   # echo-ws on 8080
  mq-bridge-py/        Dockerfile  meta.json   server.py               # Python core on 8080
```

## Submitting upstream

1. Pin the version: each Rust `Cargo.toml` and the Python `Dockerfile` reference
   this repo (Rust `v0.4.17` for the trailer support, Python `0.4.16`) — bump to
   the release you want to benchmark.
2. Fork `MDA2AV/HttpArena` and copy each `scripts/httparena/frameworks/<name>/`
   into the fork's `frameworks/<name>/`.
3. On the PR, validate and benchmark per framework:
   `/validate -f mq-bridge`, `/benchmark -f mq-bridge` (repeat for `-h2c`,
   `-websocket`, `-py`).
