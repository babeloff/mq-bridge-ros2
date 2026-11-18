#!/usr/bin/env python3
"""Tests the Python wheel as a user installs it, against two mq-bridge hosts.

The wheel goes into fresh virtual environments, one per host version: the
oldest that Cargo.toml declares support for, and the latest on PyPI. This is
the only suite that crosses the plugin ABI between *different* mq-bridge
versions; every Rust suite links exactly one.

A skipped test fails the run. The test file skips when the wheel or ROS 2 is
missing, and a run that tested nothing must not report success.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tomllib
import xml.etree.ElementTree as ElementTree
from pathlib import Path

from _common import BUILD_DIR, ROOT, TaskError, report, ros_environment, run, task

WORK = BUILD_DIR / "python-tests"


def lowest_supported_host() -> str:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    requirement = manifest["dependencies"]["mq-bridge"]["version"]
    match = re.search(r">=\s*([0-9][0-9.]*)", requirement)
    if not match:
        raise TaskError(f"no lower bound in the mq-bridge requirement {requirement!r}")
    return match.group(1)


def virtual_environment(name: str, packages: list[str], env: dict[str, str]) -> Path:
    path = WORK / f"venv-{name}"
    run([sys.executable, "-m", "venv", "--clear", path], env=env)
    python = path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    run([python, "-m", "pip", "install", "--quiet", *packages, "pytest"], env=env)
    return python


def build_wheel(python: Path, env: dict[str, str]) -> Path:
    out = WORK / "dist"
    shutil.rmtree(out, ignore_errors=True)
    # The packager looks for the library at `<root>/target/release/` literally,
    # ignoring the per-distribution `CARGO_TARGET_DIR`; see package.py.
    wheel_env = dict(env)
    wheel_env["CARGO_TARGET_DIR"] = str(ROOT / "target")
    run(
        [python, "-m", "mq_bridge.plugin_packaging", "--package", "python/mq_bridge_ros2", "--out", out],
        env=wheel_env,
    )
    wheels = list(out.glob("*.whl"))
    if len(wheels) != 1:
        raise TaskError(f"expected one wheel in {out}, found {wheels}")
    return wheels[0]


def pytest_passes(python: Path, label: str, env: dict[str, str]) -> bool:
    results = WORK / f"pytest-{label}.xml"
    status = run(
        [python, "-m", "pytest", ROOT / "python" / "tests", "-v", "-rs", "-p", "no:cacheprovider",
         f"--junitxml={results}"],
        env=env,
        cwd=WORK,
        check=False,
    )
    if status != 0:
        return False
    root = ElementTree.parse(results).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    tests, skipped = int(suite.get("tests", 0)), int(suite.get("skipped", 0))
    if skipped or not tests:
        print(f"{skipped} of {tests} test(s) skipped: a run that skips proves nothing", flush=True)
        return False
    return True


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, help="test this wheel instead of building one")
    arguments = parser.parse_args(argv)

    env = ros_environment()
    # The ROS activation puts its own site-packages first, and the pytest plugins
    # in it (`launch_testing`) fail to load under a current pytest.
    env.pop("PYTHONPATH", None)
    WORK.mkdir(parents=True, exist_ok=True)

    floor = lowest_supported_host()
    hosts = {
        "latest": virtual_environment("latest", ["mq-bridge[plugin-packaging]"], env),
        floor: virtual_environment(floor, [f"mq-bridge=={floor}"], env),
    }
    wheel = arguments.wheel.resolve() if arguments.wheel else build_wheel(hosts["latest"], env)

    steps: list[tuple[str, bool]] = []
    for label, python in hosts.items():
        run([python, "-m", "pip", "install", "--quiet", "--force-reinstall", "--no-deps", wheel], env=env)
        steps.append((f"wheel against mq-bridge {label}", pytest_passes(python, label, env)))
    return report(steps)


if __name__ == "__main__":
    task(main)
