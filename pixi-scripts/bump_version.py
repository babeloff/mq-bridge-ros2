#!/usr/bin/env python3
"""Raises the package version and propagates it to every manifest.

    pixi run bump-version patch        # 0.1.0 -> 0.1.1
    pixi run bump-version minor        # 0.1.0 -> 0.2.0
    pixi run bump-version major        # 0.1.0 -> 1.0.0
    pixi run bump-version 0.4.0-rc.1   # or say it outright

`Cargo.toml` is the source of truth, so the new version is computed from what is
there and then written to all of them by `scripts/set_version.py` — the same
code CI checks with, so there is one implementation of the rule.
"""

from __future__ import annotations

import argparse
import re

from _common import ROOT, TaskError, package_version, run, task

#: Enough of semver for a version this project would use. The suffix group
#: covers a pre-release (`-rc.1`) or build metadata (`+ros2`).
VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)([-+][0-9A-Za-z.\-+]*)?$")

BUMPS = ("major", "minor", "patch")


def next_version(current: str, bump: str) -> str:
    """The version `bump` selects, given `current`.

    >>> next_version("0.1.0", "patch")
    '0.1.1'
    >>> next_version("0.1.0", "minor")
    '0.2.0'
    >>> next_version("0.1.9", "major")
    '1.0.0'
    >>> next_version("1.2.3", "minor")
    '1.3.0'
    >>> next_version("1.2.3", "patch")
    '1.2.4'

    A pre-release is *released* rather than incremented, because that is what a
    pre-release is: `0.2.0-rc.1` is a candidate for `0.2.0`, so the patch bump
    that follows it is `0.2.0` itself.

    >>> next_version("0.2.0-rc.1", "patch")
    '0.2.0'
    >>> next_version("0.2.0-rc.1", "minor")
    '0.3.0'
    >>> next_version("0.2.0-rc.1", "major")
    '1.0.0'

    Build metadata is dropped the same way.

    >>> next_version("0.1.0+ros2", "patch")
    '0.1.0'

    Anything that is not a bump keyword is taken as the version to use.

    >>> next_version("0.1.0", "1.0.0")
    '1.0.0'
    >>> next_version("0.1.0", "0.4.0-rc.1")
    '0.4.0-rc.1'
    """
    if bump not in BUMPS:
        if not VERSION.fullmatch(bump):
            raise TaskError(
                f"{bump!r} is neither a version nor one of {', '.join(BUMPS)}"
            )
        return bump

    found = VERSION.fullmatch(current)
    if not found:
        raise TaskError(f"the version in Cargo.toml, {current!r}, is not a version I can bump")
    major, minor, patch = (int(found.group(index)) for index in (1, 2, 3))
    prerelease = found.group(4)

    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    # A pre-release already carries the patch number it is a candidate for.
    return f"{major}.{minor}.{patch}" if prerelease else f"{major}.{minor}.{patch + 1}"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bump",
        metavar="BUMP",
        help=f"one of {', '.join(BUMPS)}, or an explicit version such as 0.4.0",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="report the new version without writing it"
    )
    arguments = parser.parse_args(argv)

    current = package_version()
    new = next_version(current, arguments.bump)
    if new == current:
        raise TaskError(f"the version is already {current}; nothing to bump")

    print(f"{current} -> {new}")
    if arguments.dry_run:
        print("dry run, nothing written")
        return 0

    run(["python", "scripts/set_version.py", new], cwd=ROOT)
    print(f"\nversion is now {new} in every manifest.")
    print("Rebuild before publishing, so the artifacts carry the new version:")
    print("  pixi run package && pixi run publish")
    return 0


if __name__ == "__main__":
    task(main)
