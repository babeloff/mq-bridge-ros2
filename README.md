# mq-bridge-ros2

An external [mq-bridge](https://github.com/marcomq/mq-bridge) endpoint for
ROS 2, implemented with [`rclrs`](https://github.com/ros2-rust/ros2_rust). It
supports both ROS 2 inputs and outputs without adding ROS dependencies to
mq-bridge.

Based on mq-bridge's [plugin template](https://github.com/marcomq/mq-bridge-plugin-template),
with a ROS 2 endpoint implementation using `rclrs`.

Message types are resolved at run time through `rclrs`' dynamic messages, so the
type a route carries is named in its configuration and any message type
installed on the machine works without generating bindings for it.

## License

MIT — see [LICENSE](LICENSE).

## Documentation

Published at **<https://phreed.github.io/mq-bridge-ros2/>**, rendered from
`docs/` by [`.github/workflows/pages.yml`](.github/workflows/pages.yml) on every
push to `main` that touches them.

`docs/` holds the full documentation, organised by
[Diátaxis](https://diataxis.fr/) and written in AsciiDoc. Render it locally with
`pixi run docs`, or read the sources directly:

| | |
| --- | --- |
| [Tutorials](docs/tutorials/first-ros2-route.adoc) | Your first ROS 2 route; running the same endpoint as a plugin |
| [How-to guides](docs/how-to/load-the-plugin-in-a-host.adoc) | Loading the plugin, binary payloads, late joiners, packaging, publishing, diagnosis |
| [Reference](docs/reference/configuration.adoc) | Configuration, plugin interface, pixi tasks |
| [Explanation](docs/explanation/plugin-model.adoc) | How the plugin is loaded; delivery semantics without a broker |

Start with [docs/index.adoc](docs/index.adoc) if you are not sure which you
need. The rest of this file is the short version.

## Development

The project is a [pixi](https://pixi.sh) workspace, and every task is a Python
script under `pixi-scripts/`:

```console
pixi install       # creates the environment: ROS 2 from RoboStack, plus the Rust toolchain
pixi run build     # compile the endpoint and its plugin library
pixi run test      # unit, ROS 2 integration and plugin conformance suites
pixi run check     # formatting, clippy, versions, doctests, licences, no-ROS check
pixi run demo      # run the example route and feed it with ros2 topic pub
pixi run docs      # render the documentation
pixi run conda   # build one conda package per ROS distribution
pixi run publish   # upload and index them in the program-forge registry
pixi run release   # check everything, then tag a release
pixi run sync-version         # make every manifest match Cargo.toml
```

See [docs/reference/pixi-tasks.adoc](docs/reference/pixi-tasks.adoc) for the
options each one takes.

## Requirements

Building and running this crate needs a **sourced ROS 2 installation** —
`rclrs` links `rcl` and loads message type support libraries from it:

```console
source /opt/ros/jazzy/setup.bash   # or a RoboStack/conda environment
```

`ROS_DISTRO` and `AMENT_PREFIX_PATH` must be set, and the ROS libraries must be
on the runtime loader path. Supported distributions are the ones `rclrs` knows:
`humble`, `jazzy`, `kilted` and `rolling`. This crate needs no `protoc`.

## Configuration

Register the endpoint before any route starts:

```rust
mq_bridge_ros2::register()?;
```

Then address it by name. Every field has a default, so `ros2: {}` is valid:

```yaml
input:
  ros2:
    topic: "/orders/new"                    # optional; route name by default
    message_type: "std_msgs/msg/String"     # optional; this is the default
    payload_field: "data"                   # optional; this is the default
    node: "ingest_bridge"                   # optional; mq_bridge_<route>_in/_out by default
    namespace: "/ingest"                    # optional; "/" by default
    domain_id: 0                            # optional; ROS_DOMAIN_ID by default
    inbox_capacity: 1000                    # input only; messages buffered for the route
    first_message_wait_ms: 250              # input only; idle wait before a drain ends
    qos:                                    # optional
      reliability: reliable                 # reliable | best_effort | system_default
      durability: transient_local           # volatile | transient_local | system_default
      history: keep_last                    # keep_last | keep_all
      depth: 100
      require_subscribers: false            # wait for a matched reader before send
```

The long form `custom: { name: ros2, config: {...} }` is the same endpoint, for
configs validated against `mq-bridge.schema.json`.

The endpoint publishes a JSON schema of this configuration, so hosts render a
form for it and a URI maps onto it with typed values. The URI path is the
topic, without its leading `/`: `ros2:///chatter?message_type=std_msgs/msg/String`
names the topic `chatter`, resolved relative to the namespace — the same as
`/chatter` under the default namespace `/`. `qos` is nested and therefore only
settable from YAML or JSON, not from URI parameters.

Names are checked, not repaired. A name the endpoint *derives* from the route
name is sanitised, because route names routinely contain characters ROS forbids
(`round-trip-9f1c` becomes the node `mq_bridge_round_trip_9f1c_in`). A name written
out in the configuration is rejected if it is not a valid ROS 2 name, so
`topic: "order-new"` is an error rather than a silent rewrite.

Because a bad name, an unknown message type and an unusable payload field can
never start working, they are reported as permanent errors and the route stops
instead of reconnecting forever.

### How a payload maps onto a message

The endpoint carries an opaque payload in one field of the configured message
type, named by `payload_field`. That field has to be one of:

| Declared field type | Payload |
| --- | --- |
| `string`, `string<=N` | must be valid UTF-8; `<=N` is length checked |
| `uint8[]`, `byte[]`, `char[]` | raw bytes (unbounded sequence) |
| `uint8[<=N]`, `byte[<=N]`, `char[<=N]` | raw bytes, length checked |
| `uint8[N]`, `byte[N]`, `char[N]` | raw bytes, payload length must equal `N` exactly |

Anything else is rejected when the endpoint is created, with an error naming the
type it found. The field is validated against the real message definition, so a
typo is caught at startup rather than on the first message.

The default, `std_msgs/msg/String` with `payload_field: data`, is what most ROS 2
graphs use for opaque text. For payloads that are not text, use
`std_msgs/msg/UInt8MultiArray`.

### Quality of service

`qos` sets the three DDS policies a bridge has a basis to choose; deadline,
lifespan and liveliness are left at the ROS defaults. The publisher and the
subscription of one route use the same profile, so a route always matches
itself. Note that a `best_effort` publisher and a `reliable` subscription do
**not** match at all — that is DDS, not this endpoint.

`durability` is the ROS 2 answer to "where does a reader start":

* `volatile` (the default) delivers only what is published while the
  subscription is matched. A publisher that starts sending before discovery has
  matched a reader loses those samples, which is the most common surprise when
  moving a route from a broker to ROS 2.
* `transient_local` makes the publisher retain its last `depth` samples and
  deliver them to a subscription that matches later. Both sides must ask for it.

There is no broker, so retention lives in the *publisher*: a publishing process
that exits takes its retained samples with it. `transient_local` lets a late
subscriber catch up with a **running** publisher; it is not a durable log.

`qos.require_subscribers` is `false` by default. Set it to `true` when the
route must wait for a compatible reader before `send_batch` succeeds. A missing
reader then produces a retryable result, allowing mq-bridge retry middleware
to hold the source message until discovery completes. With the default
`false`, ROS output remains fire-and-forget and a successful send does not mean
that a reader processed the message.

### What ROS 2 does not provide

Two habits from broker-backed endpoints do not survive the move:

* **No acknowledgement, so no redelivery.** DDS sends nothing back from a reader
  to a writer. The batch commit callback still enforces one disposition per
  message, but a `Nack` is accepted rather than honoured — the message is
  already gone. A route that needs redelivery needs a broker.
* **No metadata.** A ROS 2 message carries only the fields its type declares.
  Metadata on a published message is dropped rather than smuggled somewhere a
  consumer would not look, and a received message carries only `ros2_topic` and
  `ros2_message_type`.

Because nothing is acknowledged, the endpoint declares itself at-most-once
(`x-mqb-acknowledges: false`), and mq-bridge refuses a route from it that asks
for `required_delivery: at_least_once`.

Consumers buffer between the ROS callback and the route in an inbox of
`inbox_capacity` messages, so a briefly stalled output does not lose messages at
the small DDS queue depth and `keep_all` cannot grow without bound. The default is
1000, or `qos.depth` under `keep_last` when that is larger; an explicit
`inbox_capacity` below that depth is rejected. When the inbox is full the oldest
message is discarded. Discards are summarised in one warning when the consumer
closes and reported in its `status()` (`pending`, `capacity` and
`details.discarded`). For a route with a slow output, raise `inbox_capacity`.

A draining route (`exit_on_empty`) ends when no message arrives within
`first_message_wait_ms` (default 250 ms). DDS discovery has to match the
subscription to its publishers first, which can take longer than that, so a
drain that must see a fresh publisher should raise it, for example to 2000.

## Example

With any ROS 2 publisher on the topic:

```console
cargo run --features example-app --example ros2_to_file
```

The runnable route is in `examples/ros2_to_file.yaml`. Its topic is omitted
intentionally, so it resolves to the route name `ros2_to_file`, and

```console
ros2 topic pub /ros2_to_file std_msgs/msg/String "{data: hello}"
```

feeds it.

## Use it from any mq-bridge process

The crate also builds a `cdylib` — the same endpoint as a native plugin — so a
host that never compiled against it can load it at runtime:

```rust
mq_bridge::plugin::load_endpoint_plugin("./libmq_bridge_ros2.so")?;
```

Python and Node.js users install two independent packages; neither reimplements
ROS 2, both ship this library and hand its path to mq-bridge's generic loader.

```console
pip install mq-bridge mq-bridge-ros2
```

```python
import mq_bridge_ros2

mq_bridge_ros2.register()   # once, before starting routes
```

```console
npm install mq-bridge mq-bridge-ros2
```

```javascript
import { register } from "mq-bridge-ros2";

register(); // once, before starting routes
```

The configuration is the same in every language (`ros2:`). See
[PLUGINS.md](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md) for
how loading, versioning and the ABI work.

### With `mqb` or mq-bridge-app, without code

`mqb` and the desktop app find an installed plugin by its endpoint name:
putting `libmq_bridge_ros2.so` (Linux) or `libmq_bridge_ros2.dylib` (macOS) into
a plugin directory — for example one listed in `MQB_PLUGIN_DIR`, or the conda
package installing it under `$CONDA_PREFIX/lib/mq-bridge` — is the whole
installation step:

```console
mqb copy 'ros2:///chatter?message_type=std_msgs/msg/String' 'mqtt://localhost:1883/robot'
```

A program embedding mq-bridge loads it by name instead of by path:

```rust
mq_bridge::plugin::load_installed_plugin("ros2")?;   // Rust
```

```python
mq_bridge.load_installed_plugin("ros2")              # Python
```

```javascript
loadInstalledPlugin("ros2");                         // Node.js
```

Either way a ROS 2 environment has to be sourced in the process at run time,
and it has to be the distribution the library was built for (`humble` or
`jazzy`, named in the conda build string): the library links, or with
`ros-shim` resolves by `dlopen`, that distribution's `rcl` libraries, whose ABI
differs between distributions.

### Packaging

The primary artifacts are conda packages, one per ROS distribution. The
distributions are a variant axis in `packaging/conda/variants.yaml`, so one command
builds them all:

```console
pixi run conda
```

```text
build/conda/linux-64/mq-bridge-ros2-0.5.0-ros2_humble_hee40719_0.conda
build/conda/linux-64/mq-bridge-ros2-0.5.0-ros2_jazzy_h2a6e838_0.conda
```

`humble` and `jazzy` are built and tested. The endpoint's source is
distribution-agnostic, so adding another is a line in `packaging/conda/variants.yaml` rather
than a port — provided `rclrs` supports it (`humble`, `jazzy`, `kilted`,
`rolling`) and RoboStack publishes a channel for it.

Python publishes one platform wheel per target under the same distribution
name. The npm release is a single package containing all staged binaries under
`node/prebuilds/`. Build on each target, merge those directories, then pack once:

```console
pip install "mq-bridge[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 --out dist
mq-bridge-package-plugin
mq-bridge-package-plugin --pack --out npm
```

Every build needs a sourced ROS 2 installation, and an artifact is specific to
**both** the platform and the ROS distribution, because the `rcl` ABI differs
between distributions. A conda build string can say which; a wheel's platform
tag cannot, so a `jazzy`-built wheel installs cleanly on `humble` and fails at
load time. See
[docs/how-to/package-the-plugin.adoc](docs/how-to/package-the-plugin.adoc).

The `ros-shim` feature forwards to `rclrs/use_ros_shim`, which resolves ROS
symbols by `dlopen` at run time rather than linking them. It needs the
distribution as a compiler flag:

```console
RUSTFLAGS='--cfg ros_distro="jazzy"' cargo check --features ros-shim
```

That is useful for type-checking without a ROS installation. It is **not** a way
to build the plugin without one: `rclrs` vendors several message packages whose
generated code links real ROS libraries, so producing the `cdylib` still
requires them.

The package is versioned independently of `mq-bridge`; the range of
`mq-bridge` versions it works with is declared in each manifest (currently
`>=0.4.19, <2`). Update every ecosystem manifest together before tagging a
release:

```console
python scripts/set_version.py 0.5.1   # write one version into every manifest
pixi run sync-version --check         # verify they agree
```

That covers `Cargo.toml`, both npm manifests, `python/pyproject.toml` and the
conda recipe's `context.version`. CI checks they stay synchronized, and
`pixi run check` includes the same check.

## Tests

Unit tests need nothing but the toolchain and cover the parts that are pure
data: name sanitisation and validation, the QoS mapping, which message fields
can carry a payload, the batching timing, and the consumer inbox's discard
policy.

```console
cargo test --lib
```

The end-to-end tests are ignored by default and need a sourced ROS 2
installation. There is no broker to start — ROS 2 is peer to peer, so a
publisher and a consumer in one process discover each other over loopback:

```console
cargo test --test integration -- --ignored --nocapture
cargo test --test plugin -- --ignored --nocapture
```

The first covers a publisher/consumer round trip that verifies payload order and
then invokes the batch commit callback, `transient_local` delivery to a
subscription created *after* the publish, a non-UTF-8 payload through
`std_msgs/msg/UInt8MultiArray`, and that an unknown message type and an unusable
payload field both fail permanently. The second runs mq-bridge's endpoint
conformance suite twice — once against the directly linked factory, once against
the factory loaded from the compiled plugin — and requires the results to match.
Both use a dedicated `domain_id` so they cannot reach a real ROS graph.
