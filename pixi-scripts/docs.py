#!/usr/bin/env python3
"""Renders the AsciiDoc documentation under `docs/`.

The tree follows Diataxis, so the four sections answer four different
questions and are deliberately not merged. `--check` validates the sources
without writing anything, which is what a CI job wants.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from _common import BUILD_DIR, ROOT, TaskError, require_tool, run, task

DOCS = ROOT / "docs"
OUTPUT = BUILD_DIR / "docs"

#: `xref:some/path.adoc#anchor[text]`. Only inter-document references are of
#: interest here; a bare `<<anchor>>` is asciidoctor's own business.
XREF = re.compile(r"xref:([^\[\]#]+\.adoc)(#[^\[\]]*)?\[")
TAG = re.compile(r"tag::([^\]]+)\[\]")
INCLUDE_TAG = re.compile(r"include::([^\[]+)\[tag=([^\]]+)\]")


def sources() -> list[Path]:
    found = sorted(DOCS.rglob("*.adoc"))
    if not found:
        raise TaskError(f"no .adoc files found under {DOCS.relative_to(ROOT)}")
    return found


def check_cross_references(files: list[Path]) -> None:
    """Verifies that every `xref:` to another document points at a real file.

    Asciidoctor does not do this: an inter-document reference is turned into a
    link without checking that the target exists, so a renamed page leaves a
    dead link and a clean build. Since the whole point of `--check` is to catch
    that before publishing, it has to be checked here.
    """
    broken: list[str] = []
    for source in files:
        for match in XREF.finditer(source.read_text()):
            target = (source.parent / match.group(1)).resolve()
            if not target.is_file():
                broken.append(f"  {source.relative_to(DOCS)} -> {match.group(1)}")

    if broken:
        raise TaskError(
            "cross-references point at files that do not exist:\n" + "\n".join(sorted(broken))
        )
    print("cross-references resolve")


def check_source_includes(files: list[Path]) -> None:
    """Checks that every documented source tag has one reference include.

    Source excerpts are deliberately kept in the reference pages rather than
    copied into prose.
    This check catches both an unreferenced tagged object and an include that
    points at a tag that no longer exists.
    """
    source_files = [
        path
        for directory in (ROOT / "src", ROOT / "examples")
        for path in directory.rglob("*")
        if path.is_file()
    ]
    tags = {
        name: path
        for path in source_files
        for name in TAG.findall(path.read_text())
    }
    includes = [
        (name, source, include_path)
        for source in files
        for include_path, name in INCLUDE_TAG.findall(source.read_text())
    ]
    implementation = DOCS / "reference" / "implementation.adoc"
    counts = {
        name: sum(included == name for included, _, _ in includes)
        for name in tags
    }
    implementation_counts = {
        name: sum(
            included == name and source == implementation
            for included, source, _ in includes
        )
        for name in tags
    }
    errors = [
        f"  {name} in {path.relative_to(ROOT)} has {implementation_counts[name]} implementation-reference includes"
        for name, path in tags.items()
        if implementation_counts[name] != 1
    ]
    errors.extend(
        f"  {name} in {path.relative_to(ROOT)} has only {counts[name]} total reference includes"
        for name, path in tags.items()
        if counts[name] < 2
    )
    errors.extend(
        f"  {name} in {source.relative_to(ROOT)} has no matching source tag"
        for name, source, _ in includes
        if name not in tags
    )
    if errors:
        raise TaskError("source tags and reference includes do not match:\n" + "\n".join(errors))
    print(f"source tags resolve ({len(tags)} tagged block(s))")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate the sources without writing HTML",
    )
    arguments = parser.parse_args(argv)

    require_tool(
        "asciidoctor",
        install_hint="It is a pixi dependency, so run this through `pixi run docs`.",
    )

    files = sources()
    print(f"{len(files)} AsciiDoc source(s) under {DOCS.relative_to(ROOT)}")
    check_cross_references(files)
    check_source_includes(files)

    def convert(destination: Path) -> None:
        # `--failure-level=WARN` is what makes this a check rather than a
        # render: a broken cross-reference between two pages is a warning, not
        # an error, and quietly producing a page with a dead link is exactly the
        # failure worth catching.
        #
        # `--source-dir` with `--destination-dir` mirrors the source tree into
        # the output, so the relative cross-references between sections keep
        # resolving.
        run(
            [
                "asciidoctor",
                "--failure-level=WARN",
                "--source-dir",
                str(DOCS),
                "--destination-dir",
                str(destination),
                "--attribute",
                "toc=left",
                "--attribute",
                "sectanchors",
                "--attribute",
                "idprefix=",
                "--attribute",
                "idseparator=-",
                *[str(path) for path in files],
            ]
        )

    if arguments.check:
        # A full conversion, because that is what surfaces the warnings, into a
        # directory that is then discarded. `--out-file` cannot be used for this
        # because asciidoctor rejects it when given more than one input.
        from tempfile import TemporaryDirectory

        with TemporaryDirectory(prefix="mq-bridge-ros2-docs-") as scratch:
            convert(Path(scratch))
        print("\ndocumentation sources are valid")
        return 0

    convert(OUTPUT)
    print(f"\nrendered to {OUTPUT.relative_to(ROOT)}")
    print(f"open {(OUTPUT / 'index.html').relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    task(main)
