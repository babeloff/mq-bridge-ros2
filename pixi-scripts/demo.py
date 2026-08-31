#!/usr/bin/env python3
"""Runs the example route and feeds it, so the endpoint can be seen working.

This is the shortest honest end-to-end demonstration: an mq-bridge route with a
`ros2` input and a file output, fed by the ordinary `ros2 topic pub` command
that any ROS user already has. If messages land in the output file, the endpoint
subscribed, converted `std_msgs/msg/String` payloads and handed them to the
route.

The example binary is built and run directly rather than through `cargo run`,
so that stopping the demo stops the route rather than only its parent cargo.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

from _common import ROOT, TaskError, require_tool, ros_environment, run, target_directory, task

ROUTE = "ros2_to_file"
OUTPUT_FILE = ROOT / "ros2-messages.jsonl"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=int, default=5, help="how many messages to publish")
    parser.add_argument(
        "--settle",
        type=float,
        default=3.0,
        help="seconds to allow for DDS discovery before publishing",
    )
    arguments = parser.parse_args(argv)

    require_tool("ros2", install_hint="Run this through `pixi run demo`, which activates ROS 2.")
    env = ros_environment()

    run(
        ["cargo", "build", "--features", "example-app", "--example", ROUTE],
        env=env,
    )
    binary = target_directory(env) / "debug" / "examples" / ROUTE
    if not binary.is_file():
        raise TaskError(f"the example binary was not produced at {binary}")

    # A stale file would make a broken demo look like it worked.
    OUTPUT_FILE.unlink(missing_ok=True)

    print(f"\nstarting the route from examples/{ROUTE}.yaml")
    route = subprocess.Popen([str(binary)], cwd=str(ROOT), env=env)
    try:
        # The example asks for volatile durability, matching what `ros2 topic
        # pub` offers, which means the subscription has to exist *and* be
        # matched before anything is published or the samples go nowhere.
        print(f"waiting {arguments.settle}s for the subscription to be discovered")
        time.sleep(arguments.settle)
        if route.poll() is not None:
            raise TaskError(f"the route exited early with status {route.returncode}")

        run(
            [
                "ros2",
                "topic",
                "pub",
                "--times",
                str(arguments.messages),
                f"/{ROUTE}",
                "std_msgs/msg/String",
                "{data: hello from pixi run demo}",
            ],
            env=env,
        )
        # The route batches, so give it a moment to flush to the file.
        time.sleep(2.0)
    finally:
        route.terminate()
        try:
            route.wait(timeout=10)
        except subprocess.TimeoutExpired:
            route.kill()
            route.wait()

    return summarise(arguments.messages)


def summarise(expected: int) -> int:
    if not OUTPUT_FILE.is_file():
        raise TaskError(
            f"nothing was written to {OUTPUT_FILE.name}.\n"
            "The usual cause is a QoS mismatch: `ros2 topic pub` offers volatile\n"
            "durability, so a subscription that requests transient_local never\n"
            "matches it at all. See docs/how-to/diagnose-a-failed-plugin-load.adoc."
        )

    lines = [line for line in OUTPUT_FILE.read_text().splitlines() if line.strip()]
    print(f"\n{OUTPUT_FILE.name} holds {len(lines)} message(s), expected {expected}")
    for line in lines[:3]:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            print(f"  {line}")
            continue
        payload = record.get("payload")
        metadata = record.get("metadata", {})
        print(f"  payload={payload!r} topic={metadata.get('ros2_topic')!r}")

    if not lines:
        raise TaskError("the route produced an empty file, so nothing was delivered")
    if len(lines) < expected:
        # Not a failure: a volatile publisher's first samples are legitimately
        # lost if discovery had not finished, which is the point being shown.
        print(
            "\nFewer messages than published. With volatile durability the samples sent\n"
            "before discovery completed are genuinely gone; raise --settle, or ask for\n"
            "transient_local on both sides. See docs/explanation/delivery-semantics.adoc."
        )
    return 0


if __name__ == "__main__":
    task(main)
