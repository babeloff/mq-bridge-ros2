#!/usr/bin/env python3
"""Tags a release, after checking everything the release depends on.

This task does not publish. Pushing a tag matching `[0-9]*` is what triggers
`.github/workflows/release.yml`, which builds on five platforms and publishes
the crate, the npm package and the wheels. The job here is to establish that the
commit being tagged deserves to be released, and then to create the tag.

    pixi run release --dry-run    # run every check, tag nothing
    pixi run release              # check, then create the tag locally
    pixi run release --push       # ...and push it, which starts publishing

The push is a separate flag because it is the irreversible step: crates.io and
PyPI do not allow a published version to be replaced, only yanked.

The version comes from `Cargo.toml` and is never chosen here — use
`pixi run track-version` first to follow the latest `mq-bridge` release.
"""

from __future__ import annotations

import argparse
import subprocess

from _common import (
    ROOT,
    TaskError,
    package_version,
    pixi_environments,
    report,
    ros_environment,
    run,
    task,
)

#: `release.yml` triggers on tags starting with a digit, so the tag is the bare
#: version. A `v` prefix would not match and the release would never start.
BRANCH = "main"


def capture(*command: str) -> str:
    completed = subprocess.run(
        command, cwd=str(ROOT), capture_output=True, text=True, check=False
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def working_tree_is_clean() -> tuple[bool, str]:
    dirty = capture("git", "status", "--porcelain", "--untracked-files=all")
    if dirty:
        return False, f"{len(dirty.splitlines())} uncommitted change(s); a tag must name a commit"
    return True, "clean"


def branch_is_publishable() -> tuple[bool, str]:
    """On the release branch, and identical to its remote.

    A tag push carries any commits the remote lacks, so releasing from an
    unpushed branch would publish code nobody has reviewed.
    """
    branch = capture("git", "rev-parse", "--abbrev-ref", "HEAD")
    if branch != BRANCH:
        return False, f"on {branch!r}, expected {BRANCH!r}"
    subprocess.run(["git", "fetch", "--quiet", "origin"], cwd=str(ROOT), check=False)
    local = capture("git", "rev-parse", "HEAD")
    remote = capture("git", "rev-parse", f"origin/{BRANCH}")
    if not remote:
        return False, f"origin/{BRANCH} is unknown"
    if local != remote:
        ahead = capture("git", "rev-list", "--count", f"origin/{BRANCH}..HEAD")
        behind = capture("git", "rev-list", "--count", f"HEAD..origin/{BRANCH}")
        return False, f"{ahead} ahead, {behind} behind origin/{BRANCH}"
    return True, f"{BRANCH} at {local[:7]}, level with origin"


def tag_is_free(tag: str) -> tuple[bool, str]:
    if capture("git", "tag", "--list", tag):
        return False, f"{tag} already exists locally"
    if capture("git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}"):
        return False, f"{tag} already exists on origin"
    return True, f"{tag} is free"


def ci_is_green() -> tuple[bool, str]:
    """Whether CI concluded successfully for exactly this commit.

    Advisory rather than assumed: a missing or unauthenticated `gh` should not
    block a release, but tagging a commit CI has not blessed should be a
    deliberate act.
    """
    head = capture("git", "rev-parse", "HEAD")
    runs = capture(
        "gh", "run", "list", "--workflow=ci.yml", "--limit", "20",
        "--json", "headSha,conclusion,status",
    )
    if not runs:
        return False, "could not ask gh (not installed, not authenticated, or offline)"

    import json

    for entry in json.loads(runs):
        if entry.get("headSha") == head:
            if entry.get("status") != "completed":
                return False, f"CI is {entry.get('status')} for {head[:7]}"
            ok = entry.get("conclusion") == "success"
            return ok, f"CI {entry.get('conclusion')} for {head[:7]}"
    return False, f"no CI run found for {head[:7]}"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--push",
        action="store_true",
        help="push the tag, which starts the publishing workflow",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="run every check and create no tag"
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="skip the test suites, which are the slow part; the other checks still run",
    )
    parser.add_argument(
        "--allow-red-ci",
        action="store_true",
        help="tag even though CI has not reported success for this commit",
    )
    arguments = parser.parse_args(argv)

    version = package_version()
    tag = version
    print(f"releasing {version} as tag {tag}\n")

    steps: list[tuple[str, bool]] = []

    for name, (ok, detail) in [
        ("working tree clean", working_tree_is_clean()),
        ("branch publishable", branch_is_publishable()),
        ("tag unused", tag_is_free(tag)),
    ]:
        print(f"  {name}: {detail}")
        steps.append((name, ok))

    ci_ok, ci_detail = ci_is_green()
    print(f"  ci green: {ci_detail}")
    steps.append(("ci green", ci_ok or arguments.allow_red_ci))

    env = ros_environment()
    steps.append(
        ("check", run(["python", "pixi-scripts/check.py"], env=env, check=False) == 0)
    )

    if arguments.skip_tests:
        print("\nskipping the test suites at your request")
    else:
        # Every environment, because a release ships one artifact per ROS
        # distribution and each is built from this same commit.
        for environment in pixi_environments():
            status = run(
                ["pixi", "run", "-e", environment, "test"], env=env, check=False
            )
            steps.append((f"tests ({environment})", status == 0))

    if report(steps) != 0:
        raise TaskError("not releasing: fix the failures above, or override deliberately")

    if arguments.dry_run:
        print(f"\ndry run, no tag created. It would be:\n  git tag -a {tag}")
        return 0

    run(["git", "tag", "-a", tag, "-m", f"{ROOT.name} {version}"])
    print(f"\ncreated tag {tag}")

    if not arguments.push:
        print("Not pushed. Pushing is what starts publishing, so it is a separate step:")
        print(f"  git push origin {tag}          # or re-run with --push")
        return 0

    run(["git", "push", "origin", tag])
    print(f"\npushed {tag}; release.yml is now building")

    # The conda packages are not part of that workflow, and a release that
    # forgets them leaves the registry a version behind.
    print("\nThe conda packages are published separately:")
    print("  pixi run conda && pixi run publish")
    return 0


if __name__ == "__main__":
    task(main)
