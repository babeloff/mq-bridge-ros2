# mq-bridge-ros2 (Node.js)

A ROS 2 endpoint for [mq-bridge](https://www.npmjs.com/package/mq-bridge),
shipped as a native plugin. The package contains no JavaScript implementation of
ROS 2 — it loads the compiled Rust endpoint (built on
[`rclrs`](https://github.com/ros2-rust/ros2_rust)) into mq-bridge, so Node.js,
Python and Rust all run the same code and the same delivery semantics.

Derived from
[mq-bridge-pulsar](https://github.com/marcomq/mq-bridge-pulsar).

```console
npm install mq-bridge mq-bridge-ros2
```

A ROS 2 installation has to be sourced in the environment before the process
starts; the plugin resolves `rcl` and its message type support libraries from
it, and npm cannot install it for you.

```javascript
import { Route } from "mq-bridge";
import { register } from "mq-bridge-ros2";

register(); // call once, before starting routes

const route = Route.fromStr(`
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
`);
route.start();
route.join(); // block until the route stops
```

Every config field is optional: the topic defaults to the route name, the
message type to `std_msgs/msg/String` and the payload field to `data`.

`qos.durability` (optional, `volatile` by default) decides whether a
subscription can see samples published before it matched. `transient_local` on
both sides lets a late subscriber catch up with a **running** publisher — ROS 2
has no broker, so retention lives in the publisher and a publisher that exits
takes its samples with it. See the root `README.md` for the full rules, the
payload field mapping, and the two things ROS 2 does not provide (no
redelivery, no metadata).

`register()` returns the endpoint name (`ros2`) and is a no-op when called
again. `mq-bridge` selects the current platform's library from this package's
`prebuilds/` directory using the shared plugin-package convention.

A plugin is native code with the same privileges as the Node process — install
it as you would any other native dependency.

## Building the package

The packaging command shipped by `mq-bridge` builds the Rust `cdylib` and stages
it under the current platform tag:

```console
mq-bridge-package-plugin
```

Run it on each supported target and merge the resulting `node/prebuilds/`
directories. Then create the single tarball that is published to npm:

```console
mq-bridge-package-plugin --pack --out npm
```

Every build needs a sourced ROS 2 installation, and a binary is specific to both
the platform and the ROS distribution, because the `rcl` ABI differs between
distributions.
