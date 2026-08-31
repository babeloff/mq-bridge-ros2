#!/usr/bin/env python3
"""Runs the test suites.

Three of them, and they cost very different amounts:

* unit tests need only the toolchain, and cover the parts that are pure data —
  name validation, the QoS mapping, which message fields can carry a payload,
  the batching timing and the inbox discard policy;
* the integration tests talk to a real middleware, with no broker to start,
  because ROS 2 is peer to peer;
* the conformance suite runs mq-bridge's endpoint checks twice, against the
  linked factory and against the compiled plugin, and requires identical
  results. That is the one that proves the plugin ABI round trip.

The Rust test binaries link the ROS libraries, so none of them can even load
without a ROS 2 installation. Selecting `--unit` does not avoid that.
"""

from __future__ import annotations

import argparse

from _common import ROOT, ros_environment, report, run, task

SUITES = ("unit", "integration", "conformance")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for suite in SUITES:
        parser.add_argument(f"--{suite}", action="store_true", help=f"run only the {suite} suite")
    parser.add_argument(
        "--python",
        action="store_true",
        help="also run the Python plugin tests (they skip unless the wheel is installed)",
    )
    parser.add_argument("--node", action="store_true", help="also run the npm package smoke test")
    arguments = parser.parse_args(argv)

    selected = [suite for suite in SUITES if getattr(arguments, suite)]
    if not selected:
        selected = list(SUITES)

    env = ros_environment()
    steps: list[tuple[str, bool]] = []

    commands = {
        "unit": ["cargo", "test", "--lib"],
        # `--ignored` is how the end-to-end tests are reached: they are marked
        # ignored so that a bare `cargo test` stays a fast, local-only run.
        "integration": ["cargo", "test", "--test", "integration", "--", "--ignored", "--nocapture"],
        "conformance": ["cargo", "test", "--test", "plugin", "--", "--ignored", "--nocapture"],
    }
    for suite in selected:
        status = run(commands[suite], env=env, check=False)
        steps.append((f"{suite} tests", status == 0))

    if arguments.python:
        status = run(["python", "-m", "pytest", "python/tests", "-v"], env=env, check=False)
        steps.append(("python plugin tests", status == 0))

    if arguments.node:
        node = ROOT / "node"
        run(["npm", "install", "--no-package-lock"], env=env, cwd=node)
        status = run(["npm", "test"], env=env, cwd=node, check=False)
        steps.append(("npm package smoke test", status == 0))

    return report(steps)


if __name__ == "__main__":
    task(main)
