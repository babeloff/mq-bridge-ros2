# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## 0.5.0 - Unreleased

### Added

- `inbox_capacity` consumer option: how many received messages are buffered
  between the ROS callback and the route. Defaults to 1000, or to `qos.depth`
  under `keep_last` when that is larger; a smaller explicit value is rejected.
  ([#3](https://github.com/babeloff/mq-bridge-ros2/issues/3))
- `first_message_wait_ms` consumer option: how long a draining
  (`exit_on_empty`) route waits for the first message before treating the topic
  as idle. The default stays at 250 ms; raise it when DDS discovery takes longer.
  ([#2](https://github.com/babeloff/mq-bridge-ros2/issues/2))
- Consumer `status()` reports `pending`, `capacity` and `details.discarded`.
- The endpoint publishes a JSON schema of its configuration, so hosts can map a
  URI path to `topic` (`ros2:///chatter`) and see that a route from ROS 2 is
  at-most-once.
- `pixi run test-wheel` builds the Python wheel and tests it against the oldest
  supported and the latest mq-bridge host, failing on any skipped test. CI runs
  it per ROS distribution, and the release runs it on every published wheel.
- Python tests for a publish-and-receive round trip and for inbox overflow.

### Changed

- Supported mq-bridge hosts widened to `>=0.4.19, <2` for the crate, the npm
  peer dependency and the conda package; tested against 0.4.19 and 1.6.1.
- The Python host is now the PyPI package `mq-bridge` (formerly
  `mq-bridge-py`). On conda-forge it is still called `mq-bridge-py`.
- **Behaviour:** the consumer inbox is now bounded by `inbox_capacity`. Before,
  it held `qos.depth` messages under `keep_last` and was unbounded under
  `keep_all`; `keep_all` routes now discard the oldest message beyond 1000.
  ([#3](https://github.com/babeloff/mq-bridge-ros2/issues/3))
- The example route addresses the endpoint as `ros2:` instead of
  `custom: { name: ros2 }`.

### Fixed

- The released conda package depended on `mq-bridge`, which does not exist on
  conda-forge; it now depends on `mq-bridge-py >=0.4.19,<2`.
- `pixi run test --python` failed because ROS's `launch_testing` pytest plugin
  does not load under a current pytest, and it reported success when every test
  was skipped.
