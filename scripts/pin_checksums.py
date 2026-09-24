#!/usr/bin/env python3
"""Pin the release archives' sha256 for the packages that download them.

Usage: pin_checksums.py VERSION DIR

DIR holds the release's `*.tar.gz.sha256` files, e.g. from
`gh release download vX.Y.Z -p '*.sha256' -D DIR`. Writes `checksums.sha256`
(read by build.rs) and `node/checksums.json` (read by node/install.js).
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
}


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    version = sys.argv[1].removeprefix("v")
    prefix = f"mq-bridge-connect-{version}-"
    by_target = {}
    for sha_file in sorted(Path(sys.argv[2]).glob("*.tar.gz.sha256")):
        digest, name = sha_file.read_text().split()
        name = name.lstrip("*")
        if name.startswith(prefix):
            by_target[name.removeprefix(prefix).removesuffix(".tar.gz")] = (digest, name)
    if set(by_target) != TARGETS:
        raise SystemExit(f"expected archives for {sorted(TARGETS)}, found {sorted(by_target)}")

    lines = [f"{digest}  {name}\n" for digest, name in sorted(by_target.values(), key=lambda v: v[1])]
    (ROOT / "checksums.sha256").write_text("".join(lines))
    checksums = {target: digest for target, (digest, _) in sorted(by_target.items())}
    (ROOT / "node" / "checksums.json").write_text(json.dumps(checksums, indent=2) + "\n")
    print("".join(lines), end="")


if __name__ == "__main__":
    main()
