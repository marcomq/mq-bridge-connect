#!/usr/bin/env sh
# Holds the `redpanda` endpoint to mq-bridge's native endpoint on the same
# broker, through the `mqb` CLI. Per transport it runs four pairings — the queue
# is filled by one implementation and drained by the other — each on a fresh
# queue or stream. Every pairing must deliver exactly the messages sent, and
# carry the metadata they were sent with. Throughput is printed per phase.
#
# Needs `mqb` 0.4.13+ on PATH, jq, and the brokers from mq-bridge's
# tests/integration/docker-compose (nats, amqp, redis). MQTT is absent: its
# topics keep nothing for a subscriber that is not yet connected, so a queue
# cannot be filled first and drained after.
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target_dir=${CARGO_TARGET_DIR:-"$repo_dir/target"}
messages=${MESSAGES:-100000}
batch=${BATCH:-500}
nats=${NATS:-127.0.0.1:4222}
amqp=${AMQP:-127.0.0.1:5672}
redis=${REDIS:-127.0.0.1:6379}

case "$(uname -s)" in
    Darwin) ext=dylib ;;
    Linux)  ext=so ;;
    *) echo "unsupported Unix platform: $(uname -s)" >&2; exit 2 ;;
esac
plugin="$target_dir/release/libmq_bridge_redpanda.$ext"

echo "building..." >&2
(cd "$repo_dir/go-bridge" && go build -buildmode=c-shared -o "$target_dir/release/libmq_bridge_redpanda_go.$ext" .)
(cd "$repo_dir" && cargo build --locked --release --lib)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT INT TERM
# The file source reads the envelope its sink writes, so each message carries a
# metadata entry of its own for the drain side to prove it survived.
seq -f 'message-%07g' 1 "$messages" > "$work/payloads.txt"
awk '{ printf "{\"message_id\":\"00000000-0000-7000-8000-%012d\",\"payload\":\"%s\",\"metadata\":{\"equivalence\":\"%s\"}}\n", NR, $0, $0 }' \
    "$work/payloads.txt" > "$work/seed.jsonl"
: > "$work/empty.txt"

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }

# Runs one route to completion and prints its wall time. `mqb` 0.4.13 keeps
# running after an `exit_on_empty` route completes, so the log is watched for it.
# Given an output file, a drain is done once that holds every message, so the
# consumer's wait for an empty poll is not counted.
run_route() {
    config=$1
    output=${2:-}
    log="$config.log"
    start=$(now)
    mqb --color never --no-ui --no-metrics --plugin "$plugin" --config "$config" > "$log" 2>&1 &
    pid=$!
    deadline=$(( $(date +%s) + 300 ))
    until grep -q 'completed gracefully' "$log" ||
        { [ -n "$output" ] && [ -f "$output" ] && [ "$(wc -l < "$output")" -ge "$messages" ]; }; do
        if ! kill -0 "$pid" 2>/dev/null || [ "$(date +%s)" -gt "$deadline" ]; then
            kill "$pid" 2>/dev/null || true
            echo "route did not complete: $config" >&2
            tail -20 "$log" >&2
            exit 1
        fi
        sleep 0.05
    done
    end=$(now)
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    awk -v s="$start" -v e="$end" 'BEGIN { printf "%.3f", e - s }'
}

# $1 transport, $2 native|redpanda, $3 in|out, $4 run id
endpoint() {
    case "$1:$2:$3" in
        nats:native:*)
            printf 'nats: { url: "nats://%s", stream: "%s", subject: "%s.data" }' "$nats" "$4" "$4" ;;
        nats:redpanda:*)
            printf 'custom: { name: redpanda, config: { connector: nats_jetstream, urls: ["nats://%s"], subject: "%s.data", output: { metadata: { include_patterns: [".*"] } } } }' "$nats" "$4" ;;
        amqp:native:*)
            printf 'amqp: { url: "amqp://guest:guest@%s/%%2f", queue: "%s" }' "$amqp" "$4" ;;
        amqp:redpanda:*)
            printf 'custom: { name: redpanda, config: { connector: amqp_0_9, urls: ["amqp://guest:guest@%s/"], input: { queue: "%s", queue_declare: { enabled: true, durable: true } }, output: { exchange: "", key: "%s" } } }' "$amqp" "$4" "$4" ;;
        redis:native:*)
            printf 'redis_streams: { url: "redis://%s", stream: "%s", read_from_start: true }' "$redis" "$4" ;;
        # mq-bridge keeps the body in a `payload` field; Benthos defaults to `body`.
        redis:redpanda:*)
            printf 'custom: { name: redpanda, config: { connector: redis_streams, url: "redis://%s", body_key: payload, input: { streams: ["%s"], consumer_group: equivalence, create_streams: true, start_from_oldest: true }, output: { stream: "%s" } } }' "$redis" "$4" "$4" ;;
    esac
}

# $1 file, $2 input endpoint, $3 output endpoint
route() {
    printf 'routes:\n  equivalence:\n    exit_on_empty: true\n    batch_size: %s\n    input:\n      %s\n    output:\n      %s\n' \
        "$batch" "$2" "$3" > "$1"
}

failures=0
printf '%-15s %-22s %9s %9s %12s %12s\n' transport "fill -> drain" received metadata "fill msg/s" "drain msg/s"
printf -- '-------------------------------------------------------------------------------------\n'
for transport in nats amqp redis; do
    for pairing in native:native native:redpanda redpanda:native redpanda:redpanda; do
        producer=${pairing%%:*}
        consumer=${pairing##*:}
        run="mqb-equivalence-$(date +%s)-$transport-$producer-$consumer"
        out="$work/$run.jsonl"

        # A native publisher creates what the Redpanda output assumes exists:
        # the JetStream stream, and the queue the default exchange routes to.
        route "$work/$run-setup.yaml" "file: { path: \"$work/empty.txt\" }" "$(endpoint "$transport" native out "$run")"
        run_route "$work/$run-setup.yaml" > /dev/null
        route "$work/$run-fill.yaml" "file: { path: \"$work/seed.jsonl\" }" "$(endpoint "$transport" "$producer" out "$run")"
        fill=$(run_route "$work/$run-fill.yaml")
        route "$work/$run-drain.yaml" "$(endpoint "$transport" "$consumer" in "$run")" "file: { path: \"$out\" }"
        drain=$(run_route "$work/$run-drain.yaml" "$out")

        received=$(wc -l < "$out" | tr -d ' ')
        carried=$(jq -r 'select(.metadata.equivalence == .payload) | 1' "$out" | wc -l | tr -d ' ')
        if ! jq -r .payload "$out" | sort | cmp -s - "$work/payloads.txt"; then
            echo "FAIL $transport $pairing: payloads differ from what was sent" >&2
            failures=$((failures + 1))
        fi
        if [ "$carried" -ne "$messages" ]; then
            echo "FAIL $transport $pairing: metadata on $carried of $messages messages" >&2
            failures=$((failures + 1))
        fi
        awk -v t="$transport" -v p="$producer -> $consumer" -v r="$received" -v c="$carried" \
            -v m="$messages" -v f="$fill" -v d="$drain" \
            'BEGIN { printf "%-15s %-22s %9d %9d %12.0f %12.0f\n", t, p, r, c, m / f, m / d }'
    done
done

echo
echo "$messages messages, route batch_size $batch; times include process start and connection."
[ "$failures" -eq 0 ]
