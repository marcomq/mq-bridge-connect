#!/usr/bin/env python3
"""Fails if the Go sibling links any Redpanda Community License source.

The allowlist in go-bridge/components.allow is a claim; this is the check. It
asks the Go toolchain which packages the bridge actually compiles -- for this
GOOS/GOARCH, with build constraints applied -- and reads every file in that
closure. A package that upstream taints in a later release is caught here
rather than by a licence audit after release.

Run it for the host platform, and with GOOS set for every platform shipped.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRIDGE = os.path.join(ROOT, "go-bridge")

# The header upstream stamps on enterprise files, from licenses/rcl_header.go.txt.
MARKER = "under the Redpanda Community"

# Importing this is what pulls the whole curated catalogue in. If it is missing
# from the closure, the scan covered something other than the shipped build.
SENTINEL = "github.com/marcomq/mq-bridge-connect/go-bridge/internal/components"


def linked_packages():
    """The compiled file set of every package the bridge links."""
    proc = subprocess.run(
        ["go", "list", "-deps", "-json", "."],
        cwd=BRIDGE,
        capture_output=True,
        text=True,
        check=True,
    )
    decoder = json.JSONDecoder()
    text, index, packages = proc.stdout, 0, []
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        package, index = decoder.raw_decode(text, index)
        packages.append(package)
    return packages


def main():
    goos = os.environ.get("GOOS") or subprocess.run(
        ["go", "env", "GOOS"], capture_output=True, text=True, check=True
    ).stdout.strip()

    tainted = []
    scanned = 0
    packages = linked_packages()

    # A cross-GOOS `go list` defaults CGO_ENABLED to 0, which drops the cgo
    # entrypoint and with it every component package -- and would then report a
    # clean scan of nothing at all. Refuse rather than reassure.
    if not any(package.get("ImportPath") == SENTINEL for package in packages):
        raise SystemExit(
            f"{SENTINEL} is not in the {goos} closure, so no component was "
            "scanned.\n"
            "The bridge needs cgo: set CGO_ENABLED=1 and a cross-compiler, or "
            "run this check on the target platform."
        )

    for package in packages:
        directory = package.get("Dir")
        if not directory:
            continue
        # Only what the compiler sees: no _test.go, no constraint-excluded files.
        names = package.get("GoFiles", []) + package.get("CgoFiles", [])
        for name in names:
            path = os.path.join(directory, name)
            try:
                with open(path, encoding="utf-8", errors="replace") as handle:
                    head = handle.read(4000)
            except OSError as error:
                raise SystemExit(f"could not read {path}: {error}")
            scanned += 1
            if MARKER in head:
                tainted.append((package["ImportPath"], name))

    if tainted:
        print(f"RCL-licensed source is linked into the {goos} build:", file=sys.stderr)
        for import_path, name in sorted(tainted):
            print(f"  {import_path}/{name}", file=sys.stderr)
        print(
            "\nRemove the offending component from go-bridge/components.allow and "
            "regenerate with `go generate ./...`.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print(f"{goos}: {scanned} linked Go files, no RCL header")


if __name__ == "__main__":
    main()
