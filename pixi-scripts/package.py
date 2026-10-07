#!/usr/bin/env python3
"""Builds the distributable plugin packages.

The conda package is the one this project is released as; `--wheel` also builds
the Python wheel, which is the same plugin library staged into the importable
package instead.

An artifact is specific to one platform *and* one ROS distribution, because the
`rcl` ABI differs between distributions and nothing can make a single artifact
serve two. The conda side handles that with a variant axis: `packaging/conda/variants.yaml`
lists the distributions, and one build produces one package per entry with the
distribution in its build string.

The Python wheel cannot do the same, because a wheel's platform tag has no field
for it, so `--wheel` builds for whichever distribution is active in the current
environment and says so.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from _common import (
    BUILD_DIR,
    CONDA_OUTPUT as OUTPUT,
    RECIPE,
    ROOT,
    VARIANTS,
    TaskError,
    require_tool,
    ros_environment,
    run,
    task,
    variant_distros,
)


def build_channels(distro: str) -> list[str]:
    """Channels for one distribution's build.

    Deliberately not the channels from `pixi.toml`: that list describes the
    *development* environment, which holds one distribution, while packages are
    built for several.

    One distribution per invocation, and never two RoboStack channels at once.
    Most ROS packages are distribution-prefixed and would be unambiguous, but
    `ros2-distro-mutex` is not: it exists in every RoboStack channel with a
    distribution-specific build string, and is what stops two distributions
    being mixed in one environment. With both channels present and strict
    channel priority, the mutex resolves from whichever channel comes first and
    every other distribution then fails to solve.

    Keeping each build to a single RoboStack channel respects that guard instead
    of defeating it, and makes each build hermetic.
    """
    return [f"https://prefix.dev/robostack-{distro}", "conda-forge"]


def active_distro() -> str:
    """The distribution the current environment provides, for the wheel build."""
    import os

    return os.environ.get("ROS_DISTRO") or "unknown"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wheel", action="store_true", help="also build the Python wheel for this platform"
    )
    parser.add_argument(
        "--conda",
        action="store_true",
        help="build only the conda package (the default when no flag is given)",
    )
    parser.add_argument(
        "--distro",
        action="append",
        metavar="NAME",
        help="build only this ROS distribution, repeatable; defaults to every one "
        "listed in packaging/conda/variants.yaml",
    )
    arguments = parser.parse_args(argv)

    if not RECIPE.is_file():
        raise TaskError(f"no recipe at {RECIPE.relative_to(ROOT)}")

    env = ros_environment()
    build_conda = arguments.conda or not arguments.wheel

    if build_conda:
        require_tool(
            "rattler-build",
            install_hint="It is a pixi dependency, so run this through `pixi run package`.",
        )

        available = variant_distros()
        selected = arguments.distro or available
        unknown = [distro for distro in selected if distro not in available]
        if unknown:
            raise TaskError(
                f"{', '.join(unknown)} is not listed in {VARIANTS.relative_to(ROOT)}; "
                f"it has {', '.join(available)}"
            )

        print(f"\nbuilding for ROS 2: {', '.join(selected)}")
        OUTPUT.mkdir(parents=True, exist_ok=True)
        # The output directory accumulates: a variant or version change alters
        # the build string, so yesterday's package sits beside today's. Only
        # what this run produced should be reported as its result.
        started = time.time()

        # One invocation per distribution rather than one expansion of the
        # variant axis, because each needs its own channel — see
        # `build_channels`. `--variant` pins the axis to this distribution so
        # rattler-build does not expand variants.yaml's full list again.
        for distro in selected:
            channels: list[str] = []
            for channel in build_channels(distro):
                channels += ["--channel", channel]
            run(
                [
                    "rattler-build",
                    "build",
                    "--recipe",
                    str(RECIPE),
                    "--output-dir",
                    str(OUTPUT),
                    "--variant",
                    f"ros_distro={distro}",
                    *channels,
                ],
                env=env,
            )
        # rattler-build keeps failed builds under `broken/`, and reporting one of
        # those as an artifact would be worse than reporting nothing.
        packages = [
            package
            for package in OUTPUT.rglob("*.conda")
            if "broken" not in package.relative_to(OUTPUT).parts
        ]
        built = sorted(p for p in packages if p.stat().st_mtime >= started)
        stale = len(packages) - len(built)

        print("\nconda package(s):")
        for package in built:
            print(f"  {package.relative_to(ROOT)}")
        if len(built) != len(selected):
            raise TaskError(
                f"built {len(built)} package(s) for {len(selected)} distribution(s); "
                "one of the builds produced nothing"
            )
        if stale:
            print(
                f"  ({stale} package(s) from earlier runs are also in "
                f"{OUTPUT.relative_to(ROOT)})"
            )

    if arguments.wheel:
        # The builder is mq-bridge's, not this project's: it compiles the
        # cdylib, stages it next to mq-bridge-plugin.json inside the importable
        # package and applies the platform tag.
        #
        # It looks for the library at `<root>/target/release/` literally, so it
        # honours neither `CARGO_TARGET_DIR` nor a `build.target-dir` set in a
        # cargo configuration file. On a machine that points target-dir at a
        # shared cache — a common setting — cargo succeeds and the packager then
        # reports "cargo produced no shared plugin library". Pinning the
        # variable to what it expects is the whole fix.
        wheel_env = dict(env)
        wheel_env["CARGO_TARGET_DIR"] = str(ROOT / "target")
        run(
            [
                "python",
                "-m",
                "mq_bridge.plugin_packaging",
                "--package",
                "python/mq_bridge_ros2",
                "--out",
                str(BUILD_DIR / "wheel"),
            ],
            env=wheel_env,
        )
        print("\npython wheel(s):")
        for wheel in sorted(Path(BUILD_DIR / "wheel").glob("*.whl")):
            print(f"  {wheel.relative_to(ROOT)}")
        # Worth saying every time, because the filename does not: a wheel built
        # here installs cleanly on a machine running a different distribution
        # and then fails when the library is loaded.
        print(
            f"  built against ROS 2 {active_distro()}, which the wheel's platform tag\n"
            "  cannot express — publish per distribution under separate names."
        )

    return 0


if __name__ == "__main__":
    task(main)
