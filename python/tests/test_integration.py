"""Tests for the ROS 2 endpoint as Python loads it: a native plugin.

This is the `cdylib` path — `register()` hands the bundled library to
mq-bridge's generic loader — so it exercises the plugin ABI, the packaging, the
configuration surface and the data path across the ABI.

    pixi run test-wheel                         # builds, then tests two hosts
    pytest python/tests -v                      # against what is installed

Every test skips (rather than fails) when the packages are missing or no ROS 2
installation is sourced, so the file is safe to collect anywhere. `test-wheel`
turns any skip into a failure.

The round trips use a long-lived `Publisher` and `Consumer`, not two routes:
ROS 2 has no broker, samples retained for a late joiner are held by the
publisher, and a publishing route that has exited takes them with it.
"""

import json
import os
import time
import uuid
from pathlib import Path

import pytest

mq_bridge = pytest.importorskip("mq_bridge", reason="mq-bridge is not installed")
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


def _plugin_matches_checkout() -> bool:
    """Whether the *installed* plugin knows the newest config fields.

    A wheel is a compiled artifact, so `pip install mq-bridge-ros2` can easily
    be older than this checkout. Probing beats a confusing `unknown field`
    failure that looks like a bug in the endpoint rather than a stale install.
    """
    probe = _ros2_endpoint(
        "mq_bridge_pytest_probe",
        qos={"durability": "transient_local"},
        inbox_capacity=1000,
        first_message_wait_ms=100,
    )
    try:
        mq_bridge.Route.from_str(
            f"""
input:
{probe}
output:
  file: {{ path: "{os.devnull}" }}
exit_on_empty: true
"""
        ).run()
    except Exception as error:  # noqa: BLE001 - any failure is inspected below
        return "unknown field" not in str(error)
    return True


requires_current_plugin = pytest.mark.skipif(
    _ros2_is_sourced() and not _plugin_matches_checkout(),
    reason="the installed mq-bridge-ros2 is older than this checkout; rebuild the wheel: "
    "python -m mq_bridge.plugin_packaging --package python/mq_bridge_ros2 "
    "--out python/dist",
)


def _live_endpoint(topic: str, node: str, **extra) -> dict:
    """`transient_local` with `keep_all`, so nothing depends on discovery timing."""
    config = {
        "topic": topic,
        "node": node,
        "domain_id": TEST_DOMAIN_ID,
        "qos": {"durability": "transient_local", "history": "keep_all"},
        **extra,
    }
    return {"custom": {"name": "ros2", "config": config}}


def _poll(consumer, count: int, timeout_s: float = 30.0) -> list:
    received = []
    deadline = time.monotonic() + timeout_s
    while len(received) < count and time.monotonic() < deadline:
        batch = consumer.poll(max=count - len(received), timeout_ms=500)
        if batch:
            consumer.commit()
            received.extend(batch)
    return received


def test_register_is_idempotent(registered):
    """Calling it again is a no-op, not the 'already registered' error."""
    assert mq_bridge_ros2.register() == "ros2"


def test_library_path_points_at_a_real_file():
    assert Path(mq_bridge_ros2.library_path()).is_file()


@requires_ros2
@requires_current_plugin
def test_a_round_trip_through_the_plugin_keeps_payloads_and_order(topic):
    publisher = mq_bridge.Publisher.from_config(_live_endpoint(topic, "pytest_out"))
    with mq_bridge.Consumer.from_config(_live_endpoint(topic, "pytest_in")) as consumer:
        publisher.send_batch([b"one", b"two", b"three"])
        received = _poll(consumer, 3)

    assert [message.payload for message in received] == [b"one", b"two", b"three"]


@requires_ros2
@requires_current_plugin
def test_a_full_inbox_keeps_the_newest_messages_and_reports_the_rest(topic):
    """Nothing is polled while five messages arrive, so three must be discarded."""
    publisher = mq_bridge.Publisher.from_config(_live_endpoint(topic, "pytest_out"))
    consumer_config = _live_endpoint(topic, "pytest_in", inbox_capacity=2)
    with mq_bridge.Consumer.from_config(consumer_config) as consumer:
        publisher.send_batch([b"one", b"two", b"three", b"four", b"five"])

        deadline = time.monotonic() + 30
        status = consumer.status()
        while status["details"].get("discarded", 0) < 3 and time.monotonic() < deadline:
            time.sleep(0.05)
            status = consumer.status()

        assert status["capacity"] == 2
        assert status["pending"] == 2
        assert status["details"]["discarded"] == 3
        assert [message.payload for message in _poll(consumer, 2)] == [b"four", b"five"]


@requires_ros2
@requires_current_plugin
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
