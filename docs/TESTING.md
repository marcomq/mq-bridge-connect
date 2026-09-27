# Testing status

**Messages cross the boundary in both directions**, at 1.94M msg/s in and
3.25M out through `mqb`. A `connect` input consumes through a Redpanda Connect connector and
a `connect` output publishes through one; payloads and metadata survive both
ways, Bloblang processors run in between, and an mq-bridge nack rejects the one
source message it belongs to.

**What has been tested.** Two sources: the acceptance gate
`mq_bridge::plugin::conformance` ([`tests/conformance.rs`](../tests/conformance.rs)),
which runs against live brokers on every push in CI and holds each connector
to what its transport actually guarantees; and round trips run by hand with
the released 0.1.0 (Homebrew) and `mqb` 0.4.14 on macOS arm64, through
`mqb copy` and the mq-bridge MCP server, 20 messages each way with non-ASCII
payloads. The conformance suite also passes locally against those binaries.

| Connector | Conformance checks (CI) | Manual out / in | Metadata across the broker |
| :--- | :--- | :---: | :--- |
| `beanstalkd` | work queue: `round_trip`, `nack_redelivers`, `uncommitted_batch_redelivers` | – | n/a (jobs carry none) |
| `nats_jetstream` | persistent stream: `round_trip`, `nack_redelivers`, `uncommitted_batch_redelivers` | ✓ / ✓ | not sent unless the output's `metadata` filter is set |
| `amqp_0_9` (RabbitMQ 4.1) | routed queue: `round_trip`, `metadata_preserved`, nack redelivery | ✓ / ✓ | ✓ |
| `redis_streams` | consumer group: `round_trip`, `metadata_preserved`, nack redelivery | ✓ / ✓ | ✓ |
| `mqtt` (Mosquitto 2.0) | QoS 1 topic: `round_trip`, nack redelivery | ✓ / ✓ | – |
| `redis_list` | work queue, no redelivery: `round_trip` | – | – |
| `nats` (core) | – | ✓ / ✓ | as `nats_jetstream` |
| `redis_pubsub` | – | ✓ / ✓ | – |
| `amqp_1` (RabbitMQ 4.1) | – | ✓ / ✓ | no: RabbitMQ rejects plain annotation keys, so form A excludes all metadata by default (see [`amqp_1`](../README.md#connector-notes)) |
| `nsq` | – | ✓ / ✓ | – (no headers in NSQ) |
| `mongodb` | – | ✓ / ✓ | via `document_map` |
| `sql_insert` / `sql_select` (SQLite) | – | ✓ / ✓ | `@kind` usable in `args_mapping` |
| `file` | – | ✓ / ✓ | n/a; `pipeline.processors` ran in between |
| `generate` | – | – / ✓ | Bloblang `meta` reaches mq-bridge |

Five independent transports now prove redelivery for real, rather than through
a scripted source: all five return a message that was explicitly rejected, and
two of them — `beanstalkd` and `nats_jetstream` — also return one abandoned
without an acknowledgement. A missing broker is a hard
failure under CI, so the gate cannot quietly degrade into a skip, and each
test asserts the exact set of checks its transport supports — a check that
stops applying fails rather than vanishing.

**Linux and macOS both build, link and pass clippy** — the earlier link
failure is fixed and CI is green on both.

**What still gates the next status, and why you should test first.** The
table covers 27 of the 114 endpoint components, six connectors of them in CI,
so the connector you are about to use may well be one of the other 87. The
risk is no longer that this does not build — it is that your connector has
never been run against a live broker through this boundary. Run the
conformance suite against yours before you rely on it. Windows is documented
but never built in CI.

**The suite shares one configuration between input and output.** Connectors
whose directions take different fields — `mqtt` (`topics` vs `topic`),
`amqp_0_9` (`queue` vs `exchange`), `redis_streams` (`streams` vs `stream`) —
pass it through form A's `input`/`output` blocks. Neither NATS connector can
be checked for metadata there: the output's `metadata` filter would have to
be configured, and the input rejects a field it does not define. Core `nats`
is left out of the suite because it drops anything published before the
subscription exists; it was verified by hand instead.

What *is* verified: 14 Go tests (race-clean) and 21 Rust tests, covering
acknowledgement granularity, batch splitting and aggregation, release-once
semantics, the wire format, configuration, and `socket_server` / `socket` over
loopback TCP. The loader contract holds across 150 consecutive
load/probe/panic-recovery cycles.

## Beyond the tested connectors

The data path is exercised by [`tests/data_path.rs`](../tests/data_path.rs)
(`socket_server` / `socket` over loopback TCP) and by the Go tests in
[`go-bridge/stream_test.go`](../go-bridge/stream_test.go), which drive
acknowledgement through the same code the ABI calls. Against real brokers,
only the connectors in the "What has been tested" table at the top have been
run: six through the conformance suite in CI, the rest by hand. Nothing has been run against
S3, Pulsar or the cloud and database connectors beyond SQLite and MongoDB, and
connector-specific behaviour — authentication, partitioning, redelivery
timing — is unverified outside those runs.

`mq_bridge::plugin::conformance` is the acceptance gate, and it passes as
[`tests/conformance.rs`](../tests/conformance.rs), each connector held to the
checks its transport supports. It runs in CI against broker containers; locally
each test skips unless its broker's environment variable is set
(`MQ_BRIDGE_CONNECT_BEANSTALKD`, `…_NATS`, `…_REDIS`, `…_AMQP`, `…_MQTT`).
