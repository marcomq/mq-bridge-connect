#!/bin/sh
# Installs the mq-bridge-connect plugin from a GitHub release where mqb finds it.
#   curl -fsSL https://raw.githubusercontent.com/marcomq/mq-bridge-connect/main/install.sh | sh
# Optional: a version as argument (`| sh -s -- 0.2.1`), MQB_CONNECT_DIR for another directory.
set -eu

REPO=https://github.com/marcomq/mq-bridge-connect
DIR=${MQB_CONNECT_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/mq-bridge/plugins}

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) TARGET=aarch64-apple-darwin EXT=dylib ;;
    Linux-x86_64) TARGET=x86_64-unknown-linux-gnu EXT=so ;;
    Linux-aarch64) TARGET=aarch64-unknown-linux-gnu EXT=so ;;
    *) echo "No build for $(uname -s) $(uname -m)." >&2 && exit 1 ;;
esac

VERSION=${1:-$(curl -fsSLo /dev/null -w '%{url_effective}' "$REPO/releases/latest")}
VERSION=${VERSION##*/}
VERSION=${VERSION#v}
NAME=mq-bridge-connect-$VERSION-$TARGET
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

cd "$TMP"
curl -fsSLO "$REPO/releases/download/v$VERSION/$NAME.tar.gz"
curl -fsSLO "$REPO/releases/download/v$VERSION/$NAME.tar.gz.sha256"
if command -v sha256sum >/dev/null; then sha256sum -c "$NAME.tar.gz.sha256"; else shasum -a 256 -c "$NAME.tar.gz.sha256"; fi
tar xzf "$NAME.tar.gz"

# The plugin loads its Go sibling from beside itself, and the licences of the
# statically linked code require THIRD_PARTY_NOTICES to travel with both.
FILES="libmq_bridge_connect.$EXT libmq_bridge_connect_go.$EXT THIRD_PARTY_NOTICES LICENSE-MIT LICENSE-APACHE"
for F in $FILES; do
    [ -s "$NAME/$F" ] || { echo "Archive is missing $F." >&2; exit 1; }
done

mkdir -p "$DIR"
for F in $FILES; do
    # Copy, then rename: overwriting a loaded library can crash a running mqb.
    cp "$NAME/$F" "$DIR/.$F.new" && mv -f "$DIR/.$F.new" "$DIR/$F"
done
echo "Installed mq-bridge-connect $VERSION to $DIR"
