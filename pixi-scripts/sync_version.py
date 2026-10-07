#!/usr/bin/env python3
"""Makes every manifest agree with the version in `Cargo.toml`.

    pixi run sync-version            # write Cargo.toml's version to the rest
    pixi run sync-version --check    # only report whether they already agree

The tracked `mq-bridge` version is written to `Cargo.toml`, and the manifests
kept in step with it are:

* `node/package.json`
* `node/package-lock.json` (two entries)
* `python/pyproject.toml`
* `packaging/conda/recipe.yaml` (`context.version`, which the conda package version
  references)

Use this after editing `Cargo.toml` by hand, or when a merge has left one
manifest behind. To update from upstream, use `pixi run follow-version`.

The work is done by `scripts/set_version.py`, which is also what CI runs, so
there is one implementation of the rule.
"""

from __future__ import annotations

import argparse

from _common import ROOT, package_version, run, task


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report disagreement and fail, without writing anything",
    )
    arguments = parser.parse_args(argv)

    if arguments.check:
        run(["python", "scripts/set_version.py", "--check"], cwd=ROOT)
        return 0

    # Setting the version Cargo.toml already holds is what makes this a *sync*:
    # every other manifest is rewritten to match, and Cargo.toml is unchanged.
    version = package_version()
    print(f"propagating {version} from Cargo.toml")
    run(["python", "scripts/set_version.py", version], cwd=ROOT)
    return 0


if __name__ == "__main__":
    task(main)
