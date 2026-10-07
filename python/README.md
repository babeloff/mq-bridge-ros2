# mq-bridge-ros2 (Python)

A ROS 2 endpoint for [mq-bridge-py](https://pypi.org/project/mq-bridge-py/),
shipped as a native plugin. The package contains no Python implementation of
ROS 2 — it bundles the compiled Rust endpoint (built on
[`rclrs`](https://github.com/ros2-rust/ros2_rust)) and registers it with
mq-bridge, so Python, Node.js and Rust all run the same code and the same
delivery semantics.

Based on the
[mq-bridge plugin template](https://github.com/marcomq/mq-bridge/tree/main/examples/plugin-template).

```console
pip install mq-bridge-py mq-bridge-ros2
```

A ROS 2 installation has to be sourced in the environment before the
interpreter starts; the plugin resolves `rcl` and its message type support
libraries from it, and pip cannot install it for you. This is why it is not
listed as a dependency.

```python
import mq_bridge
import mq_bridge_ros2

mq_bridge_ros2.register()   # call once, before starting routes

route = mq_bridge.Route.from_str("""
ros2_to_file:
  input:
    custom:
      name: ros2
      config:
        topic: "/orders/new"
        message_type: "std_msgs/msg/String"
        payload_field: "data"
        qos:
          durability: transient_local
          depth: 100
  output:
    file:
      path: "orders.jsonl"
""")
route.start()
```

Every config field is optional: the topic defaults to the route name, the
message type to `std_msgs/msg/String` and the payload field to `data`. See the
root `README.md` for the full configuration, the payload field mapping, and the
two things ROS 2 does not provide (no redelivery, no metadata).

`register()` returns the endpoint name (`ros2`) and is a no-op when called
again. It raises `ImportError` if mq-bridge is missing and `FileNotFoundError`
if the wheel does not carry a library for this platform.

The two packages are independent: mq-bridge has no ROS dependency, and neither
package forces an upgrade of the other. A plugin is native code with the same
privileges as the interpreter — install it as you would any other native
dependency.

The generic wheel builder is supplied by mq-bridge:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 --out dist
```

## Testing

`python/tests/` exercises the endpoint the way Python actually loads it — as a
native plugin through the ABI — so it complements, rather than repeats, the
directly linked Rust tests. It covers registration, packaging and the
configuration surface; delivery semantics are covered by
`cargo test --test integration`, because a round trip needs a publisher and a
subscription alive at the same time and ROS 2 has no broker to hold messages in
between.

```console
pip install mq-bridge-py mq-bridge-ros2 pytest
pytest python/tests -v
```

Every test skips rather than fails when the packages are missing or no ROS 2
installation is sourced, so the file is safe to collect anywhere.

One trap is worth knowing: a wheel is a **compiled artifact**, so an installed
`mq-bridge-ros2` is easily older than this checkout, and a fix you just made
here will not be in it. The tests probe for that and skip with instructions
instead of reporting a confusing `unknown field` failure. To test what you just
wrote, rebuild and reinstall first:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 --out python/dist
pip install --force-reinstall python/dist/*.whl
```

## Building the wheel

The generic builder shipped by mq-bridge builds the Rust `cdylib`, stages it
into the package, and tags the wheel for the host platform:

```console
python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 --out dist
```

Run it once per operating system and architecture you publish for. Every build
needs a sourced ROS 2 installation, and a wheel is specific to both the platform
and the ROS distribution, because the `rcl` ABI differs between distributions.
