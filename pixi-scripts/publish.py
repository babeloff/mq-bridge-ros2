#!/usr/bin/env python3
"""Publishes the built conda packages to an S3-backed conda channel.

The target is not hardcoded. It is read from pixi's global configuration:
`[s3-options.<bucket>]` states the S3 endpoint for a bucket, and `[mirrors]` for
the canonical channel is the fallback. Either way the registry's location has
one spelling, in a file deployed from a dotfiles repository. Override with
`--endpoint-url` and `--channel` for a different registry.

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

#: The registry's canonical identity. Deliberately unresolvable — the `.invalid`
#: TLD is reserved by RFC 2606 — because it is a *name*, not a location. Readers
#: never fetch it: pixi's `[mirrors]` substitutes a real URL first. This is what
#: a consumer puts in its channel list.
CANONICAL_CHANNEL = "https://program-forge.invalid"

#: Where pixi's global configuration lives, and so where the mirror that gives
#: the canonical name a location is declared.
PIXI_CONFIG = Path(
    os.environ.get("PIXI_HOME") or (Path.home() / ".pixi")
) / "config.toml"

#: Used only when the mirror cannot be read. Writing needs a real endpoint, and
#: the canonical name cannot supply one, so this fails honestly rather than
#: pretending.
FALLBACK_ENDPOINT_URL = ""
FALLBACK_CHANNEL = "s3://program-forge"
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
            f"{CONDA_OUTPUT.relative_to(ROOT)} does not exist — run `pixi run conda` first"
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
            f"{detail}.\nRun `pixi run conda` to build them."
        )
    return found


def _pixi_config() -> dict:
    """pixi's global configuration, or an empty mapping if unreadable."""
    if not PIXI_CONFIG.is_file():
        return {}
    import tomllib

    try:
        return tomllib.loads(PIXI_CONFIG.read_text())
    except (tomllib.TOMLDecodeError, OSError):
        return {}


def _mirror_bucket(config: dict) -> tuple[str, str, str] | None:
    """Endpoint, bucket and the entry they came from, from pixi's `[mirrors]`.

    Reads go over plain HTTP through the mirror, so its URL has to be split into
    an S3 endpoint and the bucket it addresses:
    `http://host:19000/program-forge` becomes `http://host:19000` plus
    `program-forge`.
    """
    from urllib.parse import urlsplit

    mirrors = config.get("mirrors", {})
    # pixi normalises the key with a trailing slash; accept either spelling.
    entries: list[str] = []
    for key in (CANONICAL_CHANNEL, f"{CANONICAL_CHANNEL}/"):
        entries += mirrors.get(key, [])

    for entry in entries:
        parts = urlsplit(entry)
        bucket = parts.path.strip("/")
        # A bucket is one path segment. Anything deeper is a sub-path channel,
        # which is not what this registry uses.
        if parts.scheme in ("http", "https") and bucket and "/" not in bucket:
            return f"{parts.scheme}://{parts.netloc}", bucket, entry
    return None


def registry_target() -> tuple[str, str, str] | None:
    """Where to write, and where that answer came from.

    The location of the registry is declared once, in pixi's global
    configuration, which is deployed from a dotfiles repository. Reading it here
    rather than repeating the URL leaves one spelling to keep correct.

    `[s3-options.<bucket>]` is preferred, because it states the S3 endpoint for
    a bucket outright — exactly the question a publish asks. `[mirrors]` is the
    fallback: it answers a *read* question, its entries are ordered by read
    preference, and its first entry could legitimately become a read-only remote
    while the local registry is down. Deriving a write target from it is an
    inference; `[s3-options]` is a statement.

    Neither publishing tool reads this file itself — `rattler-build upload s3`
    requires the endpoint as an argument even when given `--config-file`, and
    `rattler-index s3` takes no config file at all — so the values are read here
    and passed on as flags.
    """
    config = _pixi_config()
    if not config:
        return None

    options = config.get("s3-options", {})
    mirror = _mirror_bucket(config)

    def endpoint_of(bucket: str) -> str | None:
        url = options.get(bucket, {}).get("endpoint-url")
        # pixi normalises with a trailing slash, which the S3 client does not want.
        return url.rstrip("/") if isinstance(url, str) and url else None

    # Tie the two together when both are present: the bucket the mirror
    # addresses is the one being published to.
    if mirror:
        endpoint = endpoint_of(mirror[1])
        if endpoint:
            return endpoint, mirror[1], f"[s3-options.{mirror[1]}] in {PIXI_CONFIG.name}"

    # No mirror to cross-check against, but one unambiguous block.
    if len(options) == 1:
        bucket = next(iter(options))
        endpoint = endpoint_of(bucket)
        if endpoint:
            return endpoint, bucket, f"[s3-options.{bucket}] in {PIXI_CONFIG.name}"

    if mirror:
        return (
            mirror[0],
            mirror[1],
            f"mirror {mirror[2]} in {PIXI_CONFIG.name} "
            f"(no [s3-options.{mirror[1]}] block)",
        )
    return None


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
    target = registry_target()
    default_endpoint = target[0] if target else FALLBACK_ENDPOINT_URL
    default_channel = f"s3://{target[1]}" if target else FALLBACK_CHANNEL

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--channel",
        default=os.environ.get("S3_CHANNEL", default_channel),
        help=f"channel to write to (default: {default_channel})",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("S3_ENDPOINT_URL", default_endpoint),
        help="S3 endpoint of the registry (default: pixi's [s3-options] for the "
        "bucket, falling back to its [mirrors] entry)",
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

    if not arguments.endpoint_url:
        raise TaskError(
            f"no S3 endpoint. {CANONICAL_CHANNEL} is a name, not a location, and no\n"
            f"mirror for it was found in {PIXI_CONFIG}.\n"
            "Add one, or pass --endpoint-url."
        )

    print(f"package:  {PACKAGE_NAME} {version}")
    print(f"registry: {arguments.endpoint_url}")
    if target:
        print(f"          from {target[2]}")
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
            "Run `pixi run conda` to build every distribution, or publish "
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
    # A consumer names the canonical channel, never this s3:// URL. pixi's
    # mirror turns the name into a location and fetches over plain HTTP, so a
    # reader needs no S3 credentials and no knowledge of where the registry
    # currently lives.
    print("Consume it by naming the canonical channel in a pixi manifest:")
    print(f'  channels = ["{CANONICAL_CHANNEL}", "conda-forge"]')
    print(f"  (pixi rewrites that through [mirrors] in {PIXI_CONFIG})")
    return 0


if __name__ == "__main__":
    task(main)
