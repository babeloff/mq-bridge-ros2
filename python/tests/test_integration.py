"""Tests for the ROS 2 endpoint as Python loads it: a native plugin.

This is the `cdylib` path — `register()` hands the bundled library to
mq-bridge's generic loader — so it exercises the plugin ABI, the packaging and
the configuration surface rather than delivery semantics.

    pip install mq-bridge-py mq-bridge-ros2     # or a locally built wheel
    pytest python/tests -v

Every test skips (rather than fails) when the packages are missing or no ROS 2
installation is sourced, so the file is safe to collect anywhere.

## Why there is no publish-then-drain test here

The Pulsar suite this is derived from published with one route and drained with
another, later. ROS 2 has no broker, so that cannot work: samples retained for
a late-joining subscription are held by the *publisher*, and a publishing route
that has exited takes them with it. A round trip therefore needs a publisher and
a subscription alive at the same time, which is what
`cargo test --test integration` does — it covers ordering, `transient_local`
backlog delivery and non-UTF-8 payloads against a real middleware. Duplicating
that here would only re-test rclrs.
"""

import json
import os
import uuid
from pathlib import Path

import pytest

mq_bridge = pytest.importorskip("mq_bridge", reason="mq-bridge-py is not installed")
mq_bridge_ros2 = pytest.importorskip(
    "mq_bridge_ros2", reason="mq-bridge-ros2 is not installed"
)

# A domain of its own, so nothing these tests create can reach a real graph.
TEST_DOMAIN_ID = 89


def _ros2_is_sourced() -> bool:
    """Whether a ROS 2 installation is available to the plugin.

    The plugin resolves `rcl` and the message type support libraries through the
    ROS environment, so without it every route fails for a reason that has
    nothing to do with this package.
    """
    return bool(os.environ.get("ROS_DISTRO") and os.environ.get("AMENT_PREFIX_PATH"))


requires_ros2 = pytest.mark.skipif(
    not _ros2_is_sourced(),
    reason="no ROS 2 installation is sourced (ROS_DISTRO and AMENT_PREFIX_PATH are unset)",
)


@pytest.fixture(scope="session", autouse=True)
def registered():
    """Registration is process-global, so do it once for the whole session."""
    assert mq_bridge_ros2.register() == "ros2"
    return "ros2"


@pytest.fixture
def topic() -> str:
    """A fresh topic per test. ROS names allow no hyphens."""
    return f"mq_bridge_pytest_{uuid.uuid4().hex[:10]}"


def _ros2_endpoint(topic: str, **extra) -> str:
    """The `custom` form is how every non-Rust host addresses a plugin."""
    config = {"topic": topic, "domain_id": TEST_DOMAIN_ID, **extra}
    lines = "\n".join(f"        {k}: {json.dumps(v)}" for k, v in config.items())
    return f"    custom:\n      name: ros2\n      config:\n{lines}"


def _supports_qos() -> bool:
    """Whether the *installed* plugin knows the `qos` field.

    A wheel is a compiled artifact, so `pip install mq-bridge-ros2` can easily
    be older than this checkout. Probing beats a confusing `unknown field`
    failure that looks like a bug in the endpoint rather than a stale install.
    """
    try:
        mq_bridge.Route.from_str(
            f"""
input:
{_ros2_endpoint("mq_bridge_pytest_probe", qos={"durability": "transient_local"})}
output:
  file: {{ path: "/dev/null" }}
exit_on_empty: true
"""
        ).run()
    except Exception as error:  # noqa: BLE001 - any failure is inspected below
        return "unknown field `qos`" not in str(error)
    return True


requires_qos = pytest.mark.skipif(
    _ros2_is_sourced() and not _supports_qos(),
    reason="the installed mq-bridge-ros2 predates `qos`; rebuild the wheel from this "
    "checkout: python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 "
    "--out python/dist",
)


def test_register_is_idempotent(registered):
    """Calling it again is a no-op, not the 'already registered' error."""
    assert mq_bridge_ros2.register() == "ros2"


def test_library_path_points_at_a_real_file():
    assert Path(mq_bridge_ros2.library_path()).is_file()


@requires_ros2
@requires_qos
def test_a_drain_of_an_idle_topic_ends_without_a_publisher(tmp_path, topic):
    """A subscription with nothing to read must end, not hang.

    This is the whole of `exit_on_empty` on a ROS 2 input: an idle topic yields
    an empty batch, which ends the route. It is also the cheapest end-to-end
    proof that the plugin really created a node and a subscription through the
    ABI, because reaching an empty batch means `rcl` accepted both.
    """
    out = tmp_path / "out.jsonl"
    mq_bridge.Route.from_str(
        f"""
input:
{_ros2_endpoint(topic, qos={"durability": "transient_local", "depth": 100})}
output:
  file: {{ path: "{out}", format: json }}
exit_on_empty: true
"""
    ).run()

    assert not out.exists() or out.read_text() == ""


@requires_ros2
def test_a_rejected_config_surfaces_as_an_error(tmp_path, topic):
    """A config the endpoint rejects must reach the caller, not hang.

    Scope: this proves the error *surfaces*. It does not prove the ABI status
    was classified as permanent, because `run()` on a drain route also raises
    via the startup timeout when the failure is merely retryable — so this test
    passes either way. The classification itself is asserted where it is
    observable: `plugin::endpoint` unit tests in the mq-bridge repo, and the
    directly linked path in `tests/integration.rs`.
    """
    route = mq_bridge.Route.from_str(
        f"""
input:
{_ros2_endpoint(topic, definitely_not_a_field="x")}
output:
  file: {{ path: "{tmp_path / 'never.jsonl'}" }}
exit_on_empty: true
"""
    )
    with pytest.raises(Exception, match="unknown field|invalid ROS 2 endpoint configuration"):
        route.run()


@requires_ros2
def test_an_invalid_ros_name_is_rejected_rather_than_repaired(tmp_path):
    """Hyphens are legal in a route name and illegal in a ROS topic name.

    The endpoint sanitises names it *derives* from the route name but never
    rewrites one spelled out in the configuration, so this has to fail.
    """
    route = mq_bridge.Route.from_str(
        f"""
input:
{_ros2_endpoint("not-a-valid-ros-topic")}
output:
  file: {{ path: "{tmp_path / 'never.jsonl'}" }}
exit_on_empty: true
"""
    )
    with pytest.raises(Exception, match="valid ROS 2 topic name|invalid ROS 2 endpoint"):
        route.run()
