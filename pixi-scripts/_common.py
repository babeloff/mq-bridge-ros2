"""Shared helpers for the pixi task scripts.

Every task in this directory needs the same two things: a way to run a
subprocess and report its failure without a Python traceback, and the
environment a ROS 2 build needs. Both live here so the task scripts stay short
enough to read in one go.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = ROOT / "build"
RECIPE = ROOT / "recipes" / "recipe.yaml"
VARIANTS = ROOT / "recipes" / "variants.yaml"
CONDA_OUTPUT = BUILD_DIR / "conda"

#: The distribution the `ros-shim` type check targets. `rclrs` selects its `rcl`
#: bindings from this at compile time, so it has to name a real distribution
#: even when no ROS installation is present.
SHIM_ROS_DISTRO = "jazzy"


class TaskError(RuntimeError):
    """A failure worth reporting as a message rather than a traceback."""


def ros_environment(*, require: bool = True) -> dict[str, str]:
    """The environment a cargo build or test run needs to reach ROS 2.

    `rclrs` links `rcl` and loads message type support libraries through the
    ROS installation, so `ROS_DISTRO` and `AMENT_PREFIX_PATH` must be set. In a
    pixi environment the ROS activation scripts set both.

    The library search path needs help even so: the conda environment's `lib`
    directory holds `librcl.so` and the type support libraries, but nothing puts
    it on the loader path, so a test binary that links fine still fails to
    start. Adding it here is what makes `pixi run test` work.

    Pass ``require=False`` for a task that deliberately runs without ROS.
    """
    env = dict(os.environ)

    if require:
        missing = [name for name in ("ROS_DISTRO", "AMENT_PREFIX_PATH") if not env.get(name)]
        if missing:
            raise TaskError(
                "no ROS 2 installation is active: "
                + ", ".join(missing)
                + " is unset.\n"
                "Run this through pixi (`pixi run <task>`), which activates the ROS\n"
                "environment, or source a ROS 2 installation first."
            )

    prefix = env.get("CONDA_PREFIX")
    if prefix:
        library_dir = str(Path(prefix) / "lib")
        for variable in ("LD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"):
            existing = env.get(variable, "")
            if library_dir not in existing.split(os.pathsep):
                env[variable] = os.pathsep.join(filter(None, [library_dir, existing]))

    # A target directory per ROS distribution.
    #
    # `rclrs`' build script reads `ROS_DISTRO` and `AMENT_PREFIX_PATH` but
    # declares no `cargo:rerun-if-env-changed` for either, so cargo does not
    # treat a change of distribution as a reason to rerun it. Sharing one target
    # directory across distributions can therefore link one distribution's
    # bindings against another's libraries, and the result fails at load time
    # rather than at build time.
    #
    # This matters more now that `pixi run -e humble test` makes switching a
    # single flag.
    #
    # The parent directory is whatever cargo would have used anyway, which is
    # not necessarily `./target`: `build.target-dir` in a cargo configuration
    # file commonly points at a shared cache, and defaulting to `./target` here
    # would silently override that. Only cargo can report the effective value,
    # so it is asked. A shared cache therefore keeps working and simply gains a
    # subdirectory per distribution.
    distro = env.get("ROS_DISTRO")
    if distro:
        base = env.get("CARGO_TARGET_DIR")
        if not base:
            try:
                base = str(target_directory(env))
            except TaskError:
                # No cargo to ask — a task that does not need it, such as
                # publishing. Leave the setting alone rather than guessing.
                base = None
        if base and Path(base).name != distro:
            env["CARGO_TARGET_DIR"] = str(Path(base) / distro)

    return env


def shim_environment() -> dict[str, str]:
    """The environment for type checking without a ROS installation.

    `rclrs`' build script cannot discover the distribution from an environment
    that has no ROS in it, so the `ros-shim` feature requires it as a compiler
    flag instead.
    """
    env = dict(os.environ)
    # Deliberately dropped: with either of these set, `rclrs` takes the
    # non-shim path and tries to link the real libraries.
    for name in ("ROS_DISTRO", "AMENT_PREFIX_PATH"):
        env.pop(name, None)
    flag = f'--cfg ros_distro="{SHIM_ROS_DISTRO}"'
    existing = env.get("RUSTFLAGS", "")
    env["RUSTFLAGS"] = f"{existing} {flag}".strip()
    return env


def variant_distros() -> list[str]:
    """The ROS distributions `recipes/variants.yaml` builds for.

    The single source of truth: it drives the variant axis rattler-build
    expands, the channels each build resolves against, and what a publish
    expects to find. Adding a distribution is a one-line change in one file.
    """
    import yaml

    variants = yaml.safe_load(VARIANTS.read_text()) or {}
    distros = variants.get("ros_distro") or []
    if not distros:
        raise TaskError(
            f"{VARIANTS.relative_to(ROOT)} lists no `ros_distro` values, so there is "
            "nothing to build"
        )
    return [str(distro) for distro in distros]


def package_version() -> str:
    """The version in Cargo.toml, which is the source of truth for all of them."""
    import tomllib

    return tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]


def target_directory(env: dict[str, str] | None = None) -> Path:
    """Where cargo actually writes its output.

    Not necessarily `./target`: `CARGO_TARGET_DIR` and `build.target-dir` both
    move it, and pointing it at a shared cache is a common setting. Asking cargo
    beats guessing, because a wrong guess makes a task report that it built
    nothing when it built everything.
    """
    import json

    completed = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise TaskError(f"`cargo metadata` failed: {completed.stderr.strip()}")
    return Path(json.loads(completed.stdout)["target_directory"])


def run(
    command: Sequence[str | Path],
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    check: bool = True,
) -> int:
    """Runs `command`, echoing it first so a failure can be reproduced by hand."""
    parts = [str(part) for part in command]
    print(f"\n$ {' '.join(shlex.quote(part) for part in parts)}", flush=True)
    completed = subprocess.run(parts, env=env, cwd=str(cwd or ROOT))
    if check and completed.returncode != 0:
        raise TaskError(f"`{parts[0]}` failed with exit status {completed.returncode}")
    return completed.returncode


def require_tool(name: str, *, install_hint: str) -> None:
    """Fails with something actionable when a task's tool is missing."""
    from shutil import which

    if which(name) is None:
        raise TaskError(f"`{name}` is not on PATH. {install_hint}")


def report(steps: Iterable[tuple[str, bool]]) -> int:
    """Prints a summary of a multi-step task and returns a process exit status."""
    results = list(steps)
    print("\n" + "-" * 60)
    for name, ok in results:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}")
    failed = [name for name, ok in results if not ok]
    if failed:
        print(f"\n{len(failed)} of {len(results)} steps failed")
        return 1
    print(f"\nall {len(results)} steps passed")
    return 0


def task(entry: Callable[[list[str]], int | None]) -> None:
    """Runs a task's entry point, turning a `TaskError` into a clean message."""
    try:
        raise SystemExit(entry(sys.argv[1:]) or 0)
    except TaskError as error:
        print(f"\nerror: {error}", file=sys.stderr)
        raise SystemExit(1)
    except KeyboardInterrupt:
        raise SystemExit(130)
