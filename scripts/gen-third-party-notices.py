#!/usr/bin/env python3
"""Regenerates THIRD_PARTY_NOTICES from what the current build actually links.

The audit unit is the built artifact, not go.mod: Go build tags and Cargo
features change the linked set, so this reads `go list -deps` and
`cargo metadata` for the curated build rather than the declared requirements.

The emitted file is self-contained. Every license text is read from the module
cache or the registry checkout that produced the build and embedded in an
appendix, so a recipient with no toolchain can answer "what am I allowed to do
with this binary" without following a path off the page.
"""
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The linked set is platform-dependent -- `keyring` reaches a macOS keychain
# module, `procfs` is Linux-only -- and releases cover all of these, so the
# notices are their union with each module annotated. Pinning GOOS, GOARCH and
# CGO_ENABLED rather than inheriting the host's also makes the output
# byte-identical anywhere, which is what lets CI gate the file with a diff.
# Windows is absent because it is never built; a Windows artifact would need
# its own run with that platform added here.
RELEASE_PLATFORMS = (
    ("darwin", "arm64", "aarch64-apple-darwin"),
    ("darwin", "amd64", "x86_64-apple-darwin"),
    ("linux", "amd64", "x86_64-unknown-linux-gnu"),
    ("linux", "arm64", "aarch64-unknown-linux-gnu"),
)
GO_PLATFORMS = tuple((goos, goarch) for goos, goarch, _ in RELEASE_PLATFORMS)

# Importing the curated catalogue is what pulls the connectors in, and it sits
# behind cgo. A cross-GOOS `go list` with CGO_ENABLED=0 drops it and reports a
# tidy closure of almost nothing, which would silently shrink these notices.
SENTINEL = "github.com/redpanda-data/connect/v4"

# Eclipse projects keep the dual grant in LICENSE and the texts themselves in
# separate files a licen[sc]e/copying glob never matches.
ECLIPSE_TEXTS = ("edl-v10", "epl-v20")

LICENSE_FILE = re.compile(r"(?i)^(licen[sc]e|copying)")

# Licenses that must stop the build rather than appear in the notices table.
# MPL-2.0 is absent deliberately: it is file-level copyleft that the notice
# discharges with a per-module source URL.
NEEDS_REVIEW = {"EPL-2.0"}

# Texts shipped because upstream ships them, not because this distribution
# relies on them. Unexplained, an appendix group header reads as a claim.
NOT_ELECTED = {
    "EPL-2.0": "Reproduced because github.com/eclipse/paho.mqtt.golang ships it. This\n"
               "distribution elects that project's EDL-1.0 arm instead; see \"Eclipse dual\n"
               "licensing\" above. Nothing here is distributed under EPL-2.0.",
}

# The stock Apache-2.0 text names no holder, and its prose wraps onto lines
# that begin with "copyright notice ..."; only accept a real notice.
NOTICE = re.compile(r"(?i)^copyright\s+(\(c\)|©|\d{4})")


def go_mod_cache():
    """Where the toolchain keeps modules, so licences are read from what built."""
    return subprocess.run(
        ["go", "env", "GOMODCACHE"], capture_output=True, text=True, check=True
    ).stdout.strip()


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


def copyright_of(text, spdx):
    if spdx in ("Apache-2.0", "EPL-2.0 OR EDL-1.0"):
        return ""
    for line in text.splitlines():
        stripped = line.strip().lstrip("#/*> ").strip()
        if NOTICE.match(stripped) and "yyyy" not in stripped.lower():
            return stripped
    return ""


class Texts:
    """Interns license texts so each distinct one is emitted exactly once.

    Most MIT and BSD files differ only in their copyright notice, which every
    entry already carries. Keying on the text with those notices removed folds
    a two-hundred-copy scroll into a handful of appendix entries without
    dropping a word any recipient is owed.
    """

    def __init__(self):
        self.entries = {}
        self.tags = {}

    def key(self, text):
        lines = []
        for line in text.splitlines():
            stripped = line.strip().lstrip("#/*> ").strip()
            if NOTICE.match(stripped):
                continue
            normalized = " ".join(stripped.split())
            if normalized:
                lines.append(normalized)
        return hashlib.sha256("\n".join(lines).encode()).hexdigest()

    def intern(self, text, spdx, provenance):
        key = self.key(text)
        self.entries.setdefault(key, (spdx, provenance, text.strip("\n")))
        return key

    def finalize(self):
        """Numbers the texts in the order the appendix prints them."""
        order = sorted(self.entries, key=lambda k: (self.entries[k][0].lower(), self.entries[k][1]))
        self.tags = {key: f"L{i:03d}" for i, key in enumerate(order, 1)}
        return order

    def tag(self, key):
        return self.tags[key]

    def render(self):
        rule = "-" * 80
        blocks, group = [], None
        for key in self.finalize():
            spdx, provenance, text = self.entries[key]
            if spdx != group:
                group = spdx
                count = sum(1 for k in self.entries if self.entries[k][0] == spdx)
                plural = "text" if count == 1 else "texts"
                banner = f"{'=' * 80}\n{spdx} — {count} {plural}\n{'=' * 80}"
                if spdx in NOT_ELECTED:
                    banner += "\n\n" + NOT_ELECTED[spdx]
                blocks.append(banner)
            blocks.append(
                f"{rule}\n[{self.tag(key)}] {spdx} — as distributed with {provenance}\n{rule}\n\n{text}\n"
            )
        return "\n\n".join(blocks)


def license_paths(directory):
    """Every file in a dependency's root that carries a license text."""
    found = []
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if os.path.isfile(path) and LICENSE_FILE.match(name):
            found.append(path)
    for name in ECLIPSE_TEXTS:
        path = os.path.join(directory, name)
        if os.path.isfile(path):
            found.append(path)
    # Projects that are dual-licensed per file keep the texts in licenses/
    # instead of a single root file; Redpanda Connect is one.
    nested = os.path.join(directory, "licenses")
    if not found and os.path.isdir(nested):
        preferred = os.path.join(nested, "Apache-2.0.txt")
        if os.path.isfile(preferred):
            found.append(preferred)
    return found


def read(path):
    with open(path, errors="replace") as handle:
        return handle.read()


def texts_of(paths):
    """Each license file's text paired with the license that file itself is.

    A licen[sc]e/copying glob also matches pointer stubs -- a one-line "see
    LICENSE", a logo credit, a bare SPDX expression -- that carry no grant.
    Drop them when a real text survives; when none does, the caller treats the
    dependency as having no license file, which fails the run.
    """
    loaded = [(text, classify(text)) for text in (read(path) for path in paths)]
    recognized = [entry for entry in loaded if entry[1] != "UNKNOWN"]
    return recognized or loaded


def first_copyright(loaded):
    """The holder from whichever file names one.

    A dual-licensed dependency usually ships LICENSE-APACHE first, and the
    stock Apache text names nobody; the MIT or BSD arm beside it is where the
    attribution the entry owes actually lives.
    """
    for text, spdx in loaded:
        holder = copyright_of(text, spdx)
        if holder:
            return holder
    return ""


def platform_note(present, universe, label):
    """Names the platforms linking a dependency, when not all of them do."""
    if present == set(universe):
        return ""
    return " (" + ", ".join(label(p) for p in sorted(present)) + " only)"


def go_note(present):
    oses = {goos for goos, _ in present}
    if present == {p for p in GO_PLATFORMS if p[0] in oses}:
        return "" if oses == {g for g, _ in GO_PLATFORMS} else f" ({'/'.join(sorted(oses))} only)"
    return platform_note(present, GO_PLATFORMS, lambda p: f"{p[0]}/{p[1]}")


def go_closure(goos, goarch):
    out = subprocess.run(
        ["go", "list", "-deps", "-f", "{{if .Module}}{{.Module.Path}} {{.Module.Version}}{{end}}", "."],
        cwd=os.path.join(ROOT, "go-bridge"), capture_output=True, text=True, check=True,
        env=dict(os.environ, GOOS=goos, GOARCH=goarch, CGO_ENABLED="1"),
    ).stdout
    modules = set()
    for line in out.split("\n"):
        parts = line.split()
        if len(parts) == 2 and not parts[0].startswith("github.com/marcomq/"):
            modules.add((parts[0], parts[1]))
    if not any(path == SENTINEL for path, _version in modules):
        raise SystemExit(
            f"{SENTINEL} is not in the {goos}/{goarch} closure, so the connector "
            "catalogue was not scanned.\nThe bridge needs cgo: set CGO_ENABLED=1 "
            "and a cross-compiler, or generate on the target platform."
        )
    return modules


def go_modules(texts):
    cache = go_mod_cache()
    platforms = {}
    for goos, goarch in GO_PLATFORMS:
        for module in go_closure(goos, goarch):
            platforms.setdefault(module, set()).add((goos, goarch))
    rows, missing = [], []
    for path, version in sorted(platforms):
        directory = os.path.join(cache, escape_module(path) + "@" + version)
        found = license_paths(directory)
        if not found:
            missing.append(f"{path} {version} ({directory})")
            continue
        loaded = texts_of(found)
        spdx = loaded[0][1]
        if spdx == "UNKNOWN":
            missing.append(f"{path} {version} (no recognizable license text in {directory})")
            continue
        tags = [texts.intern(text, file_spdx, f"{path} {version}") for text, file_spdx in loaded]
        rows.append((path, version, spdx, first_copyright(loaded), tags,
                     go_note(platforms[(path, version)])))
    return rows, missing


def rust_closure(target):
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--filter-platform", target],
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
    return {(packages[i]["name"], packages[i]["version"],
             packages[i].get("license") or "UNKNOWN",
             os.path.dirname(packages[i]["manifest_path"]))
            for i in seen if packages[i]["name"] != "mq-bridge-redpanda"}


def rust_crates(texts):
    platforms = {}
    for _goos, _goarch, target in RELEASE_PLATFORMS:
        for crate in rust_closure(target):
            platforms.setdefault(crate, set()).add(target)
    targets = {t for _o, _a, t in RELEASE_PLATFORMS}
    rows, missing = [], []
    for name, version, spdx, directory in sorted(platforms):
        found = license_paths(directory)
        if not found:
            missing.append(f"{name} {version} ({directory})")
            continue
        provenance = f"{name} {version}"
        loaded = texts_of(found)
        tags = [texts.intern(text, file_spdx, provenance) for text, file_spdx in loaded]
        note = platform_note(platforms[(name, version, spdx, directory)], targets, str)
        rows.append((name, version, spdx, first_copyright(loaded), tags, note))
    return rows, missing


def mpl_sources(go_rows):
    """MPL-2.0 §3.2 wants a location, so cite the version-exact proxy zip.

    The module proxy is immutable per version; a VCS tag is not.
    """
    lines = []
    for path, version, spdx, _holder, _tags, _note in go_rows:
        if spdx != "MPL-2.0":
            continue
        url = f"https://proxy.golang.org/{escape_module(path)}/@v/{version}.zip"
        lines.append(f"- {path} {version}\n  {url}")
    return "\n".join(lines)


def table(rows, texts):
    lines = []
    for name, version, spdx, holder, keys, note in rows:
        cites = " ".join(f"[{texts.tag(k)}]" for k in keys)
        suffix = f" — {holder}" if holder else ""
        lines.append(f"- `{name}` {version}{note} — {spdx} {cites}{suffix}")
    return "\n".join(lines)


def summary(rows):
    return "\n".join(f"- {count} × {spdx}" for spdx, count in Counter(r[2] for r in rows).most_common())


def main():
    texts = Texts()
    go, go_missing = go_modules(texts)
    rust, rust_missing = rust_crates(texts)
    # A dependency whose license text cannot be read is the exact case a human
    # must look at, so it fails the run rather than vanishing from the file.
    if go_missing or rust_missing:
        print("no license file found for:", *(go_missing + rust_missing), sep="\n  ", file=sys.stderr)
        return 1
    unknown = [r[:3] for r in go + rust if r[2] == "UNKNOWN"]
    if unknown:
        print("license could not be identified:", unknown, file=sys.stderr)
        return 1
    # Copyleft that a permissive redistribution cannot absorb on its own terms.
    # Reaching one is a decision for a human, not a row in a generated table.
    copyleft = [r[:3] for r in go + rust if r[2] in NEEDS_REVIEW]
    if copyleft:
        print("copyleft license needs review before shipping:", copyleft, file=sys.stderr)
        return 1
    # render() numbers the texts, so it has to run before any table cites one.
    license_texts = texts.render()
    body = TEMPLATE.format(
        go_count=len(go), rust_count=len(rust),
        go_summary=summary(go), rust_summary=summary(rust),
        go_table=table(go, texts), rust_table=table(rust, texts),
        mpl_sources=mpl_sources(go), license_texts=license_texts,
        platform_scope="\n".join(
            f"  {goos + '/' + goarch:<14} (Rust target {target})"
            for goos, goarch, target in RELEASE_PLATFORMS),
    )
    with open(os.path.join(ROOT, "THIRD_PARTY_NOTICES"), "w") as handle:
        handle.write(body)
    print(f"wrote THIRD_PARTY_NOTICES: {len(go)} Go modules, {len(rust)} Rust crates, "
          f"{len(texts.entries)} distinct license texts across "
          f"{len(RELEASE_PLATFORMS)} release platforms")
    return 0


TEMPLATE = open(os.path.join(ROOT, "scripts", "third-party-notices.tmpl")).read()

if __name__ == "__main__":
    sys.exit(main())
