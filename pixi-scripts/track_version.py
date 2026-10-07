#!/usr/bin/env python3
"""Tracks the latest published `mq-bridge` version.

    pixi run track-version            # read crates.io and update every manifest
    pixi run track-version --dry-run  # report the version without writing

The endpoint is released alongside the core library, so its package version
must follow `mq-bridge` instead of being bumped independently. crates.io is the
source used here because the Rust dependency and the plugin ABI are anchored
there; npm and Python package versions are propagated from the same result.
"""

from __future__ import annotations

import argparse
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from _common import ROOT, TaskError, run, task

CRATES_API = "https://crates.io/api/v1/crates/mq-bridge"


def latest_version() -> str:
    """Return the latest stable `mq-bridge` version from crates.io."""
    request = Request(
        CRATES_API,
        headers={"User-Agent": "mq-bridge-ros2-version-tracker"},
    )
    try:
        with urlopen(request, timeout=15) as response:
            data = json.load(response)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
        raise TaskError(f"could not read the latest mq-bridge version from crates.io: {error}") from error

    version = data.get("crate", {}).get("max_version")
    if not isinstance(version, str) or not version:
        raise TaskError("crates.io returned no stable mq-bridge version")
    return version


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="report the upstream version without writing"
    )
    arguments = parser.parse_args(argv)

    version = latest_version()
    print(f"mq-bridge latest: {version}")
    if arguments.dry_run:
        print("dry run, nothing written")
        return 0

    run(["python", "scripts/set_version.py", version], cwd=ROOT)
    print(f"\ntracked mq-bridge {version} in every manifest.")
    print("Rebuild before publishing, so the artifacts carry the tracked version:")
    print("  pixi run conda && pixi run publish")
    return 0


if __name__ == "__main__":
    task(main)
