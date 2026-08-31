#!/usr/bin/env python3
"""Runs the checks that guard a release, without running the test suites.

The last one is the interesting one: it type checks the crate with the
`ros-shim` feature and no ROS installation in the environment. That is the
configuration a contributor without ROS is in, and it is worth keeping working
even though it cannot produce the plugin library — `rclrs` vendors message
packages whose generated code links real ROS libraries, so only a
non-linking check gets that far.
"""

from __future__ import annotations

import argparse

from _common import ROOT, ros_environment, report, run, shim_environment, task


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fix", action="store_true", help="rewrite formatting instead of checking it")
    arguments = parser.parse_args(argv)

    env = ros_environment()
    steps: list[tuple[str, bool]] = []

    if arguments.fix:
        run(["cargo", "fmt"], env=env)
        steps.append(("formatting applied", True))
    else:
        status = run(["cargo", "fmt", "--check"], env=env, check=False)
        steps.append(("formatting", status == 0))

    status = run(
        ["cargo", "clippy", "--all-targets", "--", "-D", "warnings"], env=env, check=False
    )
    steps.append(("clippy", status == 0))

    # Cargo.toml is the source of truth; the npm, Python and conda manifests
    # have to agree with it or a release ships mismatched versions.
    status = run(["python", "scripts/set_version.py", "--check"], env=env, check=False)
    steps.append(("version sync", status == 0))

    # The only non-trivial pure logic in the task scripts is version bumping,
    # and it is covered by doctests. Run from the script directory so the
    # modules can import their `_common` sibling.
    status = run(
        ["python", "-m", "doctest", "bump_version.py"],
        env=env,
        cwd=ROOT / "pixi-scripts",
        check=False,
    )
    steps.append(("task script doctests", status == 0))

    status = run(
        ["cargo", "check", "--features", "ros-shim", "--all-targets"],
        env=shim_environment(),
        check=False,
    )
    steps.append(("type check without ROS", status == 0))

    return report(steps)


if __name__ == "__main__":
    task(main)
