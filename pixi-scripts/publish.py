#!/usr/bin/env python3
"""Publishes the built conda packages to an S3-backed conda channel.

The default target is the project's MinIO registry at
`https://program-forge.invalid`. Override any of it for another registry.

Two steps, both required:

1. **upload** the `.conda` files into the channel's platform subdirectory;
2. **index** the channel, which regenerates `repodata.json`.

Uploading alone is not publishing. A conda channel *is* its `repodata.json`:
without a regenerated index the packages are sitting in the bucket and no
client can see them. `rattler-build upload` does not index, so this runs
`rattler-index` afterwards.

Only packages matching the version in `Cargo.toml` are published. The build
directory accumulates — a version or variant change alters the filename, so
yesterday's packages are still there — and publishing those by accident would
be worse than publishing nothing.

Credentials are never passed on the command line, where they would land in
shell history and in the process list. `rattler-build` and `rattler-index` read
them from the environment or from the rattler credentials file; this script only
checks that one of those will work and says so.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from _common import (
    CONDA_OUTPUT,
    ROOT,
    TaskError,
    package_version,
    require_tool,
    run,
    task,
    variant_distros,
)

#: The project's registry. A MinIO deployment, so path-style addressing:
#: virtual-host style would require a wildcard DNS entry per bucket.
DEFAULT_ENDPOINT_URL = "https://program-forge.invalid"
DEFAULT_CHANNEL = "s3://program-forge/conda"
#: MinIO ignores the region, but the AWS SDK requires one to be set.
DEFAULT_REGION = "us-east-1"

PACKAGE_NAME = "mq-bridge-ros2"


def packages_to_publish(version: str) -> list[Path]:
    """The built packages for the current version, newest run included.

    `broken/` holds failed builds; publishing one of those would be the worst
    possible outcome of this task.
    """
    if not CONDA_OUTPUT.is_dir():
        raise TaskError(
            f"{CONDA_OUTPUT.relative_to(ROOT)} does not exist — run `pixi run package` first"
        )

    found = sorted(
        path
        for path in CONDA_OUTPUT.rglob(f"{PACKAGE_NAME}-{version}-*.conda")
        if "broken" not in path.relative_to(CONDA_OUTPUT).parts
    )
    if not found:
        others = sorted(
            path.name
            for path in CONDA_OUTPUT.rglob(f"{PACKAGE_NAME}-*.conda")
            if "broken" not in path.relative_to(CONDA_OUTPUT).parts
        )
        detail = f"; the directory holds {', '.join(others)}" if others else ""
        raise TaskError(
            f"no packages for version {version} in {CONDA_OUTPUT.relative_to(ROOT)}"
            f"{detail}.\nRun `pixi run package` to build them."
        )
    return found


def credential_source() -> str | None:
    """Where the S3 credentials will come from, or `None` if nowhere.

    Checked rather than required, because there is more than one legitimate
    place to keep them and hard-failing on the environment would break the
    others. Reported so that "access denied" is never a mystery about *which*
    credentials were tried.
    """
    if os.environ.get("S3_ACCESS_KEY_ID") and os.environ.get("S3_SECRET_ACCESS_KEY"):
        return "S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY in the environment"
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the environment"

    for candidate in (
        Path(os.environ.get("RATTLER_AUTH_FILE", "")) if os.environ.get("RATTLER_AUTH_FILE") else None,
        Path.home() / ".rattler" / "credentials.json",
    ):
        if candidate and candidate.is_file():
            try:
                entries = json.loads(candidate.read_text())
            except json.JSONDecodeError:
                continue
            if any(key.startswith("s3://") for key in entries):
                return f"an s3:// entry in {candidate}"
    return None


def s3_options(arguments: argparse.Namespace) -> list[str]:
    """The S3 connection flags, credentials deliberately excluded."""
    return [
        "--endpoint-url",
        arguments.endpoint_url,
        "--region",
        arguments.region,
        "--addressing-style",
        arguments.addressing_style,
    ]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--channel",
        default=os.environ.get("S3_CHANNEL", DEFAULT_CHANNEL),
        help=f"channel URL in the bucket (default: {DEFAULT_CHANNEL})",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("S3_ENDPOINT_URL", DEFAULT_ENDPOINT_URL),
        help=f"S3 endpoint of the registry (default: {DEFAULT_ENDPOINT_URL})",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("S3_REGION", DEFAULT_REGION),
        help=f"ignored by MinIO but required by the SDK (default: {DEFAULT_REGION})",
    )
    parser.add_argument(
        "--addressing-style",
        default=os.environ.get("S3_ADDRESSING_STYLE", "path"),
        choices=["path", "virtual-host"],
        help="MinIO needs path-style unless it has wildcard DNS (default: path)",
    )
    parser.add_argument(
        "--force", action="store_true", help="replace packages that are already in the channel"
    )
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="skip regenerating repodata.json; only useful when indexing once after "
        "publishing several projects",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be published, and the exact commands, without contacting "
        "the registry",
    )
    arguments = parser.parse_args(argv)

    version = package_version()
    packages = packages_to_publish(version)
    distros = variant_distros()

    print(f"package:  {PACKAGE_NAME} {version}")
    print(f"registry: {arguments.endpoint_url}")
    print(f"channel:  {arguments.channel}")
    print(f"publishing {len(packages)} package(s):")
    for package in packages:
        print(f"  {package.relative_to(ROOT)}")

    # A missing distribution is nearly always a stale build directory rather
    # than an intent, and it would publish a half-set nobody notices.
    missing = [distro for distro in distros if not any(distro in p.name for p in packages)]
    if missing:
        raise TaskError(
            f"no package for ROS 2 {', '.join(missing)} at version {version}. "
            "Run `pixi run package` to build every distribution, or publish "
            "deliberately with a narrower selection."
        )

    upload = [
        "rattler-build",
        "upload",
        "s3",
        "--channel",
        arguments.channel,
        *s3_options(arguments),
        *(["--force"] if arguments.force else []),
        *[str(package) for package in packages],
    ]
    index = [
        "rattler-index",
        "s3",
        arguments.channel,
        *s3_options(arguments),
    ]

    if arguments.dry_run:
        print("\ndry run, nothing contacted. The commands would be:\n")
        for command in (upload, *([] if arguments.no_index else [index])):
            print("  " + " ".join(command))
        source = credential_source()
        print(f"\ncredentials: {source or 'NONE FOUND — the upload would fail'}")
        return 0

    source = credential_source()
    if source is None:
        raise TaskError(
            "no S3 credentials found. Provide them in one of:\n"
            "  * S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY in the environment\n"
            "  * AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment\n"
            "  * an s3:// entry in ~/.rattler/credentials.json\n"
            "Use --dry-run to check everything else without them."
        )
    print(f"credentials: {source}")

    require_tool(
        "rattler-build",
        install_hint="It is a pixi dependency, so run this through `pixi run publish`.",
    )
    run(upload)

    if arguments.no_index:
        print(
            "\nUploaded but NOT indexed. The packages are in the bucket and no client can\n"
            "see them until `rattler-index s3` regenerates repodata.json."
        )
        return 0

    require_tool(
        "rattler-index",
        install_hint="It is a pixi dependency, so run this through `pixi run publish`.",
    )
    run(index)

    print(f"\npublished {PACKAGE_NAME} {version} to {arguments.channel}")
    print("Consume it by adding the channel to a pixi manifest:")
    print(f"  channels = [\"{arguments.channel}\", \"conda-forge\"]")
    return 0


if __name__ == "__main__":
    task(main)
