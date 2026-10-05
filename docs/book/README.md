# mq-bridge Docs

This directory is the source of the **mq-bridge documentation book** (built with
[mdbook](https://rust-lang.github.io/mdBook/)) *and* the standalone Markdown pages it
is assembled from. `SUMMARY.md` is the book's table of contents.

## Building the book

```bash
# from the mq-bridge repository root
docs/book/sync-engine-docs.sh
mdbook build docs/book
mdbook serve docs/book --open
```

`engine/` and `book/` are build artifacts (git-ignored). The engine
reference (`REFERENCE.md`, `CONFIGURATION.md`, `ARCHITECTURE.md`, `DELIVERY.md`,
`EXTENDING.md`, `PLUGINS.md`) is vendored from the root `docs/` directory by
`sync-engine-docs.sh` — **never edit those copies here; edit the root
`docs/` directory and re-run the script.**

## Key entry points

- **[Quick Start](./quick-start.md)** — five working `copy` commands
  (Postgres → ClickHouse, Postgres CDC → Postgres, MQTT → Kafka, RabbitMQ →
  HTTP, File → MongoDB). Start here.
- **[Connectors](./connectors/)** — per-connector pages: purpose, URL format,
  practical examples, and a link to the full option list.
- **[URL Parameter Reference](./reference/)** — every connector's recognised
  query parameters (name, type, default, required, description),
  auto-generated from the JSON Schemas `mqb copy` uses to parse
  `--from`/`--to`. Regenerate with:

  ```bash
  cargo run -p mq-bridge-app --example gen_url_docs
  ```
- **Connect plugin components** — `connectors/connect-inputs.md`, `connect-outputs.md`
  and `connect-processors.md` are generated from the plugin's component specs (do not
  edit by hand). Regenerate after a plugin release, with a checkout of
  `mq-bridge-connect` and a Go toolchain:

  ```bash
  python3 apps/mq-bridge-app/dev/scripts/gen-connect-docs.py ../mq-bridge-connect
  ```
- **[MCP Server](./MCP.md)** — `mqb mcp`: running the bridge as an
  MCP server, registering it with a client via `mcp install`, its five tools,
  endpoint/message shapes, and examples.

## Folder structure

```text
docs/book/
├── SUMMARY.md              book table of contents
├── book.toml               mdbook config (src = ".")
├── sync-engine-docs.sh     vendors engine docs into engine/ (build step)
├── README.md               this file
├── introduction.md         book landing page
├── INSTALL.md BUILD.md quick-start.md MCP.md IBM_MQ_SETUP.md   top-level pages
├── getting-started/        run-forms, core concepts
├── tutorials/              end-to-end, copy-pasteable walkthroughs
├── cookbook/               short task-focused recipes
├── operations/             deploying, observability, tuning, troubleshooting
├── extending/              custom endpoints/middleware, contributing
├── connectors/             hand-written, one page per connector
│   ├── README.md
│   └── postgres.md clickhouse.md mqtt.md kafka.md rabbitmq.md http.md mongodb.md file.md
├── reference/              cli/mcp/bindings/endpoints (hand-written) +
│   │                       per-connector URL params (generated — do not edit by hand)
│   └── README.md postgres.md clickhouse.md ...
├── engine/                 vendored from mq-bridge/docs/ (git-ignored artifact)
└── book/                   mdbook output (git-ignored)
```

Connector pages are hand-written and stay that way: purpose, URL format, and
examples need human judgment. The reference pages are generated from the
JSON Schemas so parameter docs never drift from the actual config structs —
see `crates/cli/examples/gen_url_docs.rs`.
