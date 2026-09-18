#!/usr/bin/env python3
"""Regenerates THIRD_PARTY_NOTICES from what the current build actually links.

The audit unit is the built artifact, not go.mod: Go build tags and Cargo
features change the linked set, so this reads `go list -deps` and
`cargo metadata` for the curated build rather than the declared requirements.
"""
import json
import os
import re
import subprocess
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOMODCACHE = os.path.join(ROOT, "target", "go-mod-cache")
TARGET = "aarch64-apple-darwin"

LICENSE_TEXTS = {
    "ISC": "ISC License - see the crate's own LICENSE file for the copyright line.",
    "MPL-2.0": "Mozilla Public License 2.0 - https://mozilla.org/MPL/2.0/",
}


# Licenses that must stop the build rather than appear in the notices table.
# MPL-2.0 is absent deliberately: it is file-level copyleft that the notice
# already documents an obligation for.
NEEDS_REVIEW = {"EPL-2.0"}


def escape_module(path):
    return "".join("!" + c.lower() if c.isupper() else c for c in path)


def classify(text):
    head = text[:4000]
    if "Apache License" in head and "Version 2.0" in head:
        return "Apache-2.0"
    if "Mozilla Public License" in head:
        return "MPL-2.0"
    # Eclipse projects offer EPL-2.0 or EDL-1.0; take the permissive arm, but
    # only when the dual grant is actually stated -- bare EPL-2.0 is weak
    # copyleft and must not be swept in silently.
    if "Eclipse Public License" in head:
        if "Eclipse Distribution License" in head:
            return "EPL-2.0 OR EDL-1.0"
        return "EPL-2.0"
    if "Permission is hereby granted, free of charge" in head:
        return "MIT"
    if "Redistribution and use in source and binary forms" in head:
        return "BSD-3-Clause" if "Neither the name" in head else "BSD-2-Clause"
    if "Permission to use, copy, modify, and" in head:
        return "ISC"
    return "UNKNOWN"


# The stock Apache-2.0 text names no holder, and its prose wraps onto lines
# that begin with "copyright notice ..."; only accept a real notice.
NOTICE = re.compile(r"(?i)^copyright\s+(\(c\)|©|\d{4})")


def copyright_of(text, spdx):
    if spdx in ("Apache-2.0", "EPL-2.0 OR EDL-1.0"):
        return ""
    for line in text.splitlines():
        stripped = line.strip().lstrip("#/* ").strip()
        if NOTICE.match(stripped) and "yyyy" not in stripped.lower():
            return stripped
    return ""


def go_modules():
    env = dict(os.environ)
    env["GOMODCACHE"] = GOMODCACHE
    env["GOCACHE"] = os.path.join(ROOT, "target", "go-build-cache")
    out = subprocess.run(
        ["go", "list", "-deps", "-f", "{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{end}}", "."],
        cwd=os.path.join(ROOT, "go-bridge"), env=env, capture_output=True, text=True, check=True,
    ).stdout
    rows = []
    for line in sorted(set(out.split("\n"))):
        parts = line.split()
        if len(parts) != 2:
            continue
        path, version = parts
        if path.startswith("github.com/marcomq/"):
            continue
        directory = os.path.join(GOMODCACHE, escape_module(path) + "@" + version)
        candidates = []
        for name in sorted(os.listdir(directory)):
            if re.match(r"(?i)^(licen[sc]e|copying)", name) and os.path.isfile(os.path.join(directory, name)):
                candidates.append(os.path.join(directory, name))
        # Projects that are dual-licensed per file keep the texts in licenses/
        # instead of a single root file; Redpanda Connect is one.
        nested = os.path.join(directory, "licenses")
        if not candidates and os.path.isdir(nested):
            preferred = os.path.join(nested, "Apache-2.0.txt")
            if os.path.isfile(preferred):
                candidates.append(preferred)
        if not candidates:
            rows.append((path, version, "UNKNOWN", ""))
            continue
        text = open(candidates[0], errors="replace").read()
        spdx = classify(text)
        rows.append((path, version, spdx, copyright_of(text, spdx)))
    return rows


def rust_crates():
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--filter-platform", TARGET],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout)
    packages = {p["id"]: p for p in meta["packages"]}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    root = next(i for i, p in packages.items() if p["name"] == "mq-bridge-redpanda")
    seen, stack = set(), [root]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for dep in nodes[current]["deps"]:
            if any(k["kind"] in (None, "build") for k in dep["dep_kinds"]):
                stack.append(dep["pkg"])
    rows = []
    for i in sorted(seen):
        p = packages[i]
        if p["name"] == "mq-bridge-redpanda":
            continue
        rows.append((p["name"], p["version"], p.get("license") or "UNKNOWN", ""))
    return rows


def table(rows):
    lines = []
    for name, version, spdx, holder in rows:
        suffix = f" — {holder}" if holder else ""
        lines.append(f"- `{name}` {version} — {spdx}{suffix}")
    return "\n".join(lines)


def summary(rows):
    return "\n".join(f"- {count} × {spdx}" for spdx, count in Counter(r[2] for r in rows).most_common())


def main():
    go = go_modules()
    rust = rust_crates()
    unknown = [r for r in go + rust if r[2] == "UNKNOWN"]
    if unknown:
        print("license could not be identified:", unknown, file=sys.stderr)
        return 1
    # Copyleft that a permissive redistribution cannot absorb on its own terms.
    # Reaching one is a decision for a human, not a row in a generated table.
    copyleft = [r for r in go + rust if r[2] in NEEDS_REVIEW]
    if copyleft:
        print("copyleft license needs review before shipping:", copyleft, file=sys.stderr)
        return 1
    body = TEMPLATE.format(
        go_count=len(go), rust_count=len(rust),
        go_summary=summary(go), rust_summary=summary(rust),
        go_table=table(go), rust_table=table(rust),
    )
    with open(os.path.join(ROOT, "THIRD_PARTY_NOTICES"), "w") as handle:
        handle.write(body)
    print(f"wrote THIRD_PARTY_NOTICES: {len(go)} Go modules, {len(rust)} Rust crates")
    return 0


TEMPLATE = open(os.path.join(ROOT, "scripts", "third-party-notices.tmpl")).read()

if __name__ == "__main__":
    sys.exit(main())
