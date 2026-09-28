# The three ways to run it

mq-bridge is one engine with one config format, exposed three ways: the library and the two forms of `mq-bridge-app`. Build and test a
route in the UI, export the JSON/YAML, then run that config in a config-mode service or from
library code. The `copy` CLI takes the same settings, but expressed as endpoint URIs and flags
rather than a config file.

| Form | What it is | Quick install |
| --- | --- | --- |
| [Desktop app (UI)](#desktop-app-ui) | The visual workbench — build/test routes, run request/response traffic, inspect message history | `brew install --cask marcomq/tap/mq-bridge` |
| [CLI / server](#cli--server) | Headless binary: a one-line `copy`, a drain-then-exit batch job, or a long-lived bridge (also serves the same UI in a browser) | `brew install marcomq/tap/mq-bridge-app` |
| [Library](#library) | The engine embedded in your own code — native Rust, Python, or Node.js bindings | `cargo add` / `pip` / `npm` |

See [Installation](../INSTALL.md) for every install method and platform.

## Desktop app (UI)

The desktop app is a Tauri bundle of the full messaging workbench: manage
publishers/consumers/routes, run request/response traffic (like Postman for REST), inspect
message history, and import Postman/OpenAPI/AsyncAPI definitions. It is the **same UI** the CLI
serves in a browser — only the packaging differs.

The UI is generated **dynamically from the Rust configuration structures**: the backend uses
`schemars` to produce a JSON Schema for the `AppConfig` struct (exposed at `/schema.json`, also
`mqb --schema <path>`), and the frontend renders a complete config form from that schema. So
when a new middleware or option is added to the engine, the schema updates automatically and
the UI reflects it with no frontend change.

## CLI / server

The CLI (`mqb`) is a headless binary that runs in three modes. They share the same
engine and config format but differ in how you drive them — and only **config mode** serves the
browser UI:

| Mode | Invocation | Serves web UI? | Best for |
| --- | --- | --- | --- |
| **Config mode** (default) | `mqb [--config x.yml]` | **Only when asked** — via `ui_addr` in the config, `--ui`, or a `y` at the prompt | Long-lived bridge from a config file; Container/Kubernetes deployments |
| **`copy`** | `mqb copy --from … --to …` | No (headless) | Ad-hoc one-route job from two endpoint URIs, no config file; add `--drain` to exit once the source is empty |
| **`mcp`** | `mqb mcp` | No (headless) | Expose the bridge as MCP tools so an LLM agent can publish and route from natural language |

```bash
# Config mode: run a long-lived bridge from a file
mqb --config config.yml

# Seed config.yml from a template on first run only
mqb --config config.yml --init-config dev/config/file-to-http.yml

# Start empty, then open the UI to build your config
mqb --ui
```

In config mode the CLI can also **serve the browser UI** (the same UI as the desktop app) on the
configured port. It is never opened implicitly: set `ui_addr` in the config, pass `--ui`, or
answer the prompt — an unattended run without `--ui` stays headless. See the
[CLI reference](../reference/cli.md#starting-the-web-ui) for the full rule and every flag, and
[Configuration grammar](../engine/configuration.md) for the config format.

### What a config file has to contain

Everything a config file defines is **started at boot**, in both headless and UI runs. A route
can be held back with `enabled: false`; the UI starts it on demand.

The full form is a `routes:` map alongside the application settings, which is what the UI writes
out. Routes may also be written at the top level, so a file that is nothing but a route needs
neither the wrapper nor a name — these three are the same bridge:

```yaml
# 1. the full form
routes:
  proxy:
    input:  { http: { url: "0.0.0.0:8443" } }
    output: { http: { url: "https://upstream.internal/" } }

# 2. named at the top level, as in the Configuration grammar
proxy:
  input:  { http: { url: "0.0.0.0:8443" } }
  output: { http: { url: "https://upstream.internal/" } }

# 3. a single unnamed route — it runs as `route`
input:  { http: { url: "0.0.0.0:8443" } }
output: { http: { url: "https://upstream.internal/" } }
```

Application settings (`ui_addr`, `metrics_addr`, `log_level`, `plugins`, ...) stay at the top
level in every form. In form 3 every other top-level key belongs to the route, so route options
such as `batch_size` and `exit_on_empty` go there — and a misspelled one is reported by name
rather than ignored.

### The configuration-first workflow

The point of one shared config format is that you can **test connections and dial in a route in
the UI, export the JSON/YAML, then run that exact config unchanged** — as a config-mode service
or loaded from library code. A known-good route shape from the UI drops straight into
production.

`copy` is the exception: it takes no config file, so a route you built in the UI has to be
mapped by hand onto `--from` / `--to` endpoint URIs and route flags. The settings are the same,
only the way you pass them differs.

## Library

Beyond running standalone, the core engine is available as a library so you can produce or
consume messages with a unified API — no broker-specific SDK, one config format across all
three bindings:

- **Rust** — [`mq-bridge`](https://crates.io/crates/mq-bridge) (`cargo add mq-bridge`)
- **Python** — [`pip install mq-bridge`](https://pypi.org/project/mq-bridge/)
- **Node.js** — [`npm install mq-bridge`](https://www.npmjs.com/package/mq-bridge)

The core of the library are the `MessageConsumer` and `MessagePublisher` traits in
`mq_bridge::traits`. See the [Language bindings API](../reference/bindings.md) and the
[Embed the library](../tutorials/embedding.md) tutorial.

## How the UI differs from API clients

The UI overlaps with API clients like Postman, Bruno, and Insomnia, but its centre of gravity
is different: it is designed around **message bridging, runtime operation, and long-lived route
management** rather than just request composition. It adds broker pub/sub workflows, long-lived
consumers/routes, bridging traffic between protocols, hex-level payload debugging, replay,
local-first git-friendly config, and encrypted config — while leaving scripting and complex
request workflows to the dedicated API clients. Use an API client when your main job is
crafting and sharing API requests; use `mq-bridge-app` when you need to connect systems, move
messages between protocols, inspect live traffic, and manage bridge-style runtime
configuration.
