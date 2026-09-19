#!/usr/bin/env sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target_dir=${CARGO_TARGET_DIR:-"$repo_dir/target"}
profile=${PROFILE:-debug}

case "$(uname -s)" in
    Darwin)
        rust_library="libmq_bridge_redpanda.dylib"
        go_library="libmq_bridge_redpanda_go.dylib"
        ;;
    Linux)
        rust_library="libmq_bridge_redpanda.so"
        go_library="libmq_bridge_redpanda_go.so"
        ;;
    *)
        echo "unsupported Unix platform: $(uname -s)" >&2
        exit 2
        ;;
esac

mkdir -p "$target_dir/$profile"
(
    cd "$repo_dir/go-bridge"
    go build -buildmode=c-shared -o "$target_dir/$profile/$go_library" .
)
(
    cd "$repo_dir"
    cargo build --locked --lib --bin phase0_smoke
)

# The conformance suite needs a live broker, so CI runs it in its own job; this
# script covers what a machine without one can check.
(
    cd "$repo_dir"
    cargo test --locked --lib --test data_path
)

run=1
while [ "$run" -le 3 ]; do
    "$target_dir/$profile/phase0_smoke" \
        "$target_dir/$profile/$rust_library" \
        "$target_dir/$profile/$go_library"
    run=$((run + 1))
done
