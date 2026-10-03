#!/usr/bin/env python3
"""Rename the template: `rename.py kafka2 my-github-user`."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP = {"target", "node_modules", ".git"}

if len(sys.argv) != 3 or not re.fullmatch(r"[a-z][a-z0-9_]*", sys.argv[1]):
    raise SystemExit(f"usage: {Path(sys.argv[0]).name} ENDPOINT_NAME GITHUB_OWNER  (name: [a-z][a-z0-9_]*)")
name, owner = sys.argv[1], sys.argv[2]
# Package names use dashes, the library and Python module underscores.
replacements = (
    ("mq-bridge-myendpoint", f"mq-bridge-{name.replace('_', '-')}"),
    ("MqBridgeMyendpoint", "MqBridge" + name.title().replace("_", "")),
    ("Myendpoint", name.title().replace("_", "")),
    ("myendpoint", name),
    ("your-github-user", owner),
)

for path in sorted(ROOT.rglob("*")):
    if not path.is_file() or SKIP & set(path.parts) or path == Path(__file__).resolve():
        continue
    try:
        text = path.read_text()
    except UnicodeDecodeError:
        continue
    updated = text
    for old, new in replacements:
        updated = updated.replace(old, new)
    if updated != text:
        path.write_text(updated)
        print(f"updated {path.relative_to(ROOT)}")

package = ROOT / "python" / "mq_bridge_myendpoint"
if package.exists():
    package.rename(package.with_name(f"mq_bridge_{name}"))
