#!/usr/bin/env python3
"""Compiles the endpoint and the loadable plugin library.

`Cargo.toml` declares both an `rlib` and a `cdylib`, so one build produces the
library a Rust program links directly *and* the `libmq_bridge_ros2.so` that
mq-bridge hosts, the Python wheel and the npm package load at run time.
"""

from __future__ import annotations

import argparse

from _common import ros_environment, run, target_directory, task


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release", action="store_true", help="build optimised, as a release artifact is"
    )
    parser.add_argument(
        "--example",
        action="store_true",
        help="also build the runnable example route (needs the example-app feature)",
    )
    arguments = parser.parse_args(argv)

    profile = ["--release"] if arguments.release else []
    env = ros_environment()

    run(["cargo", "build", *profile], env=env)
    if arguments.example:
        run(
            [
                "cargo",
                "build",
                *profile,
                "--features",
                "example-app",
                "--example",
                "ros2_to_file",
            ],
            env=env,
        )

    profile_dir = target_directory(env) / ("release" if arguments.release else "debug")
    # The `cdylib` is what every non-Rust host loads, so it is worth naming
    # explicitly rather than leaving the reader to find it.
    libraries = sorted(
        path
        for path in profile_dir.glob("*mq_bridge_ros2.*")
        if path.suffix in {".so", ".dylib", ".dll"}
    )
    print("\nloadable plugin library:")
    for library in libraries:
        print(f"  {library}")
    if not libraries:
        print(f"  none found in {profile_dir} — does Cargo.toml still declare a cdylib?")
    return 0


if __name__ == "__main__":
    task(main)
