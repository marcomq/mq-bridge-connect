#!/usr/bin/env sh
# Measures what the Rust<->Go boundary costs, by running each pipeline twice:
# once entirely inside Go (`nativebench`) and once with mq-bridge owning one end
# (`examples/throughput.rs`). Both link the same Benthos engine and the same
# component set, so the difference between the two numbers is the boundary.
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target_dir=${CARGO_TARGET_DIR:-"$repo_dir/target"}
GOCACHE=${GOCACHE:-"$target_dir/go-build-cache"}
GOMODCACHE=${GOMODCACHE:-"$target_dir/go-mod-cache"}
export GOCACHE GOMODCACHE

messages=${MESSAGES:-200000}
batch=${BATCH:-500}
# Counted in source batches, so it is independent of the batch size mq-bridge
# asks for.
max_in_flight=${MAX_IN_FLIGHT:-64}
repeats=${REPEATS:-3}

case "$(uname -s)" in
    Darwin) go_library="libmq_bridge_redpanda_go.dylib" ;;
    Linux)  go_library="libmq_bridge_redpanda_go.so" ;;
    *) echo "unsupported Unix platform: $(uname -s)" >&2; exit 2 ;;
esac

# The Go library carries no debug/release distinction, so whichever profile built
# it last is both the newest ABI and the same optimized code.
if [ -z "${MQ_BRIDGE_REDPANDA_GO_LIBRARY:-}" ]; then
    built=
    for profile in release debug; do
        [ -f "$target_dir/$profile/$go_library" ] &&
            built="$built $target_dir/$profile/$go_library"
    done
    # shellcheck disable=SC2086
    MQ_BRIDGE_REDPANDA_GO_LIBRARY=$(ls -t $built 2>/dev/null | head -n 1)
fi
if [ -z "$MQ_BRIDGE_REDPANDA_GO_LIBRARY" ] || [ ! -f "$MQ_BRIDGE_REDPANDA_GO_LIBRARY" ]; then
    echo "no Go library found; run scripts/phase0-smoke.sh first" >&2
    exit 1
fi
export MQ_BRIDGE_REDPANDA_GO_LIBRARY
echo "go library: $MQ_BRIDGE_REDPANDA_GO_LIBRARY" >&2

echo "building..." >&2
(cd "$repo_dir/go-bridge" && go build -o "$target_dir/release/nativebench" ./internal/nativebench)
(cd "$repo_dir" && cargo build --locked --release --example throughput)

native="$target_dir/release/nativebench"
plugin="$target_dir/release/examples/throughput"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM

payload=$(awk 'BEGIN { line = ""; for (i = 0; i < 256; i++) line = line "x"; print line }')
source_file="$work/source.txt"
yes "$payload" 2>/dev/null | head -n "$messages" > "$source_file"

generate="input:
  generate:
    count: $messages
    interval: \"\"
    batch_size: $batch
    mapping: 'root = \"$payload\"'
"
printf '%soutput:\n  drop: {}\n' "$generate" > "$work/generate-drop.yaml"
printf '%soutput:\n  file:\n    path: %s\n' "$generate" "$work/native-out.txt" > "$work/generate-file.yaml"
printf 'input:\n  file:\n    paths: [ "%s" ]\noutput:\n  drop: {}\n' "$source_file" > "$work/file-drop.yaml"

# Best of `repeats`: throughput noise is one-sided, so the fastest run is the
# one least contaminated by whatever else the machine was doing.
best() {
    lowest=
    attempt=1
    while [ "$attempt" -le "$repeats" ]; do
        # Without this a failed run yields an empty time, and the table below
        # divides by it rather than reporting that the run never happened.
        if ! value=$("$@") || [ -z "$value" ]; then
            echo "benchmark run failed: $*" >&2
            exit 1
        fi
        lowest=$(awk -v a="$lowest" -v b="$value" \
            'BEGIN { if (a == "" || b + 0 < a + 0) print b; else print a }')
        attempt=$((attempt + 1))
    done
    printf '%s' "$lowest"
}

row() {
    label=$1
    native_seconds=$2
    plugin_seconds=$3
    awk -v label="$label" -v n="$native_seconds" -v p="$plugin_seconds" -v m="$messages" \
        'BEGIN { printf "%-26s %12.0f %12.0f %11.2fx\n", label, m / n, m / p, p / n }'
}

echo >&2
printf '%-26s %12s %12s %12s\n' "scenario" "native/s" "bridged/s" "cost"
printf -- '---------------------------------------------------------------------\n'

generate_drop=$(best "$native" -config "$work/generate-drop.yaml")
generate_file=$(best "$native" -config "$work/generate-file.yaml")
file_drop=$(best "$native" -config "$work/file-drop.yaml")

row "generate -> mq-bridge" "$generate_drop" \
    "$(best "$plugin" generate-consume "$messages" "$batch" "$max_in_flight")"
row "file -> mq-bridge" "$file_drop" \
    "$(best "$plugin" file-consume "$messages" "$batch" "$max_in_flight" "$source_file")"
row "mq-bridge -> drop" "$generate_drop" \
    "$(best "$plugin" publish-drop "$messages" "$batch" "$max_in_flight")"
row "mq-bridge -> file" "$generate_file" \
    "$(best "$plugin" publish-file "$messages" "$batch" "$max_in_flight" "$work/plugin-out.txt")"

echo
echo "$messages messages of 256 B, batches of $batch, max_in_flight $max_in_flight, best of $repeats."
