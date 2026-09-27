# Performance

Measured on macOS arm64 (8 cores, 8 GB), release build, plugin ABI 1.2. First
the fixed costs of the approach, then throughput through the `mqb` CLI, then
against a native pipeline.

| | |
| :--- | ---: |
| Rust cdylib | 3.0 MiB |
| Go sibling library (stripped) | 218 MiB |
| `mqb copy` start to finish, 1 row, without / with the plugin (warm page cache) | 0.09 s / 0.18 s |
| Peak RSS of a 1M-row `mqb copy`, without / with the plugin | 29 MiB / 196 MiB |
| Rust→Go call, steady state | ~300 ns |
| Rust→Go call, first on a new OS thread | ~5.5 µs (worst seen 30 µs) |
| `ResourceBuilder` build + close | ~24 µs |

The last three rows were measured on the earlier three-package build and not
re-measured since; they describe the call mechanism, which has not changed.
Three things follow.

**Size dominates.** The Go sibling is ~70× the Rust plugin, and it accounts for
most of the resident memory. It is nearly all Redpanda Connect and its
transitive dependencies, and it grows with the allowlist, not with usage.

**Loading costs ~90 ms warm.** A cold page cache has to fault in the 218 MiB
image first, so the first start after a boot is slower; that was ~1.4 s for the
45 MiB image of the early build and has not been re-measured.

**Cross the boundary per batch, never per message.** ~300 ns steady-state is
cheap against real network I/O but not against nothing. The sharper edge is the
first call from an OS thread the Go runtime has not seen: 10–50× the steady
cost. `mq-bridge`'s tokio worker pool is fixed, so that amortises — but code
that calls into Go from `spawn_blocking` threads would pay it repeatedly. This
is why the private ABI is defined per batch.

## Through `mqb`

1 000 000 short JSON lines, `{"id":N,"perf_test":true}` (29 MiB), run with the
installed `mqb` 0.4.15 and the plugin loaded: `mqb copy --drain` at its defaults
(`--batch-size 1024 --concurrency 4`), `format=raw` on both files. The rate is the one `mqb`
reports, as mq-bridge's own figures are, so it leaves out process start; best
of five.

| Route | rows/s |
| :--- | ---: |
| `file` → `file` — native, reference | 4 878 049 |
| `file` → `connect+drop` — Redpanda sink | 3 246 753 |
| `connect+generate` → `file` — Redpanda source | 1 941 748 |
| `\|connect` with `noop` — the middleware boundary alone | 934 579 |
| `\|connect_mapping` `meta checked = "yes"` | 543 478 |
| `\|connect_dedupe` on the payload, memory cache | 515 464 |
| `\|connect_mapping` dropping odd `this.id` | 398 406 |
| `\|connect_mapping` `root = this` | 383 142 |
| `\|connect_mapping` `root = this.merge({"seen": true})` | 274 725 |
| `\|connect_mapping` merge into `connect+drop` instead | 584 795 |
| `\|connect_mapping` merge into `connect+drop`, `--concurrency 1` | 271 003 |

Middleware rows run `file` → `file` unless they say otherwise, and each wrote
all 1 000 000 rows but the filter, which kept its 500 000. A `file` sink keeps
order, so those rows run on one worker whatever `--concurrency` says; into
`connect+drop` the same merge uses all four. Wall clock including process start
is lower: 3.01M rows/s native, 2.31M into the Redpanda sink, 837k through
`noop`.

**Endpoints cost little.** A Redpanda sink adds ~0.1 µs a row over native, and
a Redpanda source feeds a file at 1.94M rows/s.

**A middleware costs ~0.9 µs a message to cross, and its processor costs the
rest.** The route already crosses once per batch, so there is no boundary cost
left for batching to amortize; handing the processor the whole batch instead of
one message at a time saves a few percent. What helps CPU-bound work is more cores,
through route `concurrency`.

**JSON goes through go-json.** Benthos parses and serializes JSON with
`encoding/json`, which is most of what a mapping over `this` costs. Once a chain
hands back parsed JSON, the plugin parses JSON objects and arrays itself with
[go-json](https://github.com/goccy/go-json), hands Benthos the value, and either
serializes the result with go-json or, for a message the chain left unchanged,
returns the original bytes. That takes `root = this` from 311k to 383k rows/s
and the merge from 230k to 271k, measured back to back. A chain that only reads JSON and keeps the
message, like the filter above, gives no sign that it parsed and stays on
Benthos' parser. Whatever go-json would write differently from
`encoding/json` — floats with an exponent, invalid UTF-8, anything but plain
JSON types — is left to Benthos.

## Throughput against a native pipeline

[`scripts/benchmark.sh`](../scripts/benchmark.sh) runs every pipeline twice: once
entirely inside Go ([`nativebench`](../go-bridge/internal/nativebench/main.go)), and
once with mq-bridge owning an end
([`examples/throughput.rs`](../examples/throughput.rs)). Both link the same Benthos
engine and the same component set, so what separates the two numbers is the
boundary and nothing else. macOS arm64 on AC power, 200 000 messages of 256 B,
batches of 500, `max_in_flight: 64`, connect v4.110.0; best of five, and the
second column of costs is an independent repeat of the whole run:

| scenario | native | bridged | cost | repeat |
| :--- | ---: | ---: | ---: | ---: |
| `generate` → mq-bridge | 1 913 656 msg/s | 1 778 347 msg/s | 1.08× | 1.06× |
| `file` → mq-bridge | 231 759 msg/s | 208 984 msg/s | 1.11× | 1.14× |
| mq-bridge → `drop` | 1 913 656 msg/s | 3 598 740 msg/s | 0.53× | 0.51× |
| mq-bridge → `file` | 151 342 msg/s | 107 548 msg/s | 1.41× | 1.36× |

Absolute numbers track the machine, so read the `cost` column, not the first
two.

**The plugin cannot be faster than Redpanda Connect.** It is Redpanda Connect,
plus a boundary. The rows under 1.00× are not a win: their baseline fabricates
every message with a Bloblang mapping, which the publisher is instead handed for
free. What those rows show is that the publisher boundary disappears into the
noise, not that anything got faster.

**mq-bridge → `file` got slower.** Before plugin ABI 1.2 it ran at 0.94× and
1.02×; the cause of the change has not been tracked down yet.

**The `file` → mq-bridge row is a tuning artefact, not a boundary cost.** `file` emits one
message per batch, and `max_in_flight` counts batches, so 64 there means 64
messages in flight rather than 32 000 — and every one of those round-trips
through mq-bridge before the source may refill. Raising it to 512 turned that row
into **0.95×** (252 727 bridged against 239 047 native, before ABI 1.2) and leaves the others
where they are. Any source that does not batch wants a far higher
`max_in_flight` than one that does; the cost is memory, since a parked message
is a resident message.

Timings need a quiet machine, but allocation is deterministic and says the same
thing. Per message, measured in Go alone with `go test -bench`
([`stream_bench_test.go`](../go-bridge/stream_bench_test.go)), without cgo or
Rust: a native `drop` allocates 1 189 B, the bridged sink 1 528 B. The boundary
is the 339 B difference, and it is one blob per batch and nothing per message —
encoding a batch of 500 allocates once, whether or not the messages carry
metadata. Three things had to go for that to hold:

- Sizing the blob from a fixed 256 B cost 1 373 B per message, because `append`
  reached 130 KB by doubling and copying eleven times. It is now sized from what
  the last blob measured.
- Rendering metadata through `fmt.Sprint` cost an allocation per message as soon
  as a connector set a number — `file` sets a mod time, Kafka an offset and a
  partition. The digits now go straight into the blob.
- Decoding copied every payload out of the blob. Payloads are now slices of it
  (`Bytes` is reference-counted), so a batch of 500 costs one allocation on the
  Rust side instead of 501.

**Two properties of the sink are load-bearing**, and both are worth knowing
before changing it. It is a `service.BatchOutput` rather than a single-message
`service.Output`, because Benthos breaks a batch bound for the latter into a
separate blocked goroutine per message: that costs **5.2×** in scheduler
contention alone, enough that a CPU profile is 60% `runtime.lock2` under
`selectgo` with almost no work in it. Per-message nack granularity survives the
change through `service.BatchError` — see [Semantics](../README.md#semantics). And
[`collect`](../go-bridge/stream.go) hands a partly filled batch over as soon as every
in-flight slot is parked, instead of waiting out `batchLinger` for messages that
cannot arrive until mq-bridge commits; that is worth **14×** to any source
emitting one message per batch, `file` among them.
`TestASourceOfSingleMessageBatchesDoesNotWaitOutTheLinger` holds the line.

## Against mq-bridge's native endpoints

[`scripts/equivalence.sh`](../scripts/equivalence.sh) holds the `connect` endpoint
to the endpoint mq-bridge ships for the same broker, through the `mqb` CLI. For
NATS JetStream, RabbitMQ (AMQP 0.9) and Redis Streams it fills a fresh queue with
one implementation and drains it with the other, in all four pairings:

```sh
sh scripts/equivalence.sh     # needs mqb 0.4.13+, jq, and mq-bridge's nats/amqp/redis compose brokers
```

**The results are the same.** Every pairing delivers exactly the messages sent,
payloads and metadata alike, so either implementation can read what the other
wrote. MQTT is left out: a topic keeps nothing for a subscriber that has not
connected yet, so it cannot be filled first and drained afterwards.

**Publishing performs on par with the native endpoints.** Draining through the
plugin is slower. On NATS the gap is modest. On AMQP and Redis Streams it is
large with the connectors' defaults, which fetch a handful of messages at a
time: `amqp_0_9` defaults to `prefetch_count: 10` and `redis_streams` to
`limit: 10`. Raising them closes most of the gap:

```yaml
config: { connector: amqp_0_9, prefetch_count: 1000, … }
config: { connector: redis_streams, limit: 500, … }
```

`mqb` 0.4.13 leaves a headless run up after its `exit_on_empty` routes
complete, so the script stops each route once it has finished, and the times
include process start and connection.
