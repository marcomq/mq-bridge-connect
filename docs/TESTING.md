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
table covers 27 of the 114 endpoint components and the
[round trips](#endpoint-round-trips) bring that to 81, all of them in CI,
so the connector you are about to use may well be one of the other 33. The
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
S3 or the cloud and database connectors beyond SQLite, PostgreSQL and MongoDB, and
connector-specific behaviour — authentication, partitioning, redelivery
timing — is unverified outside those runs.

`mq_bridge::plugin::conformance` is the acceptance gate, and it passes as
[`tests/conformance.rs`](../tests/conformance.rs), each connector held to the
checks its transport supports. It runs in CI against broker containers; locally
each test skips unless its broker's environment variable is set
(`MQ_BRIDGE_CONNECT_BEANSTALKD`, `…_NATS`, `…_REDIS`, `…_AMQP`, `…_MQTT`).

## Endpoint round trips

[`tests/endpoints/run.py`](../tests/endpoints/run.py) is the fast way to try a
connector or a processor middleware: it needs no compose file and compiles
nothing. It drives the `mqb` CLI with whichever plugin is installed (or
`--plugin <path>`). Brokers are throwaway containers on random ports, or a
local binary when one is on `PATH`; a case without a broker runs anywhere.

```sh
python3 tests/endpoints/run.py            # everything runnable on this machine
python3 tests/endpoints/run.py redis nats # cases whose name matches
python3 tests/endpoints/run.py connect_ processor_  # middlewares, no Docker needed
```

The cases live in [`tests/endpoints/cases.toml`](../tests/endpoints/cases.toml).

| Kind | Cases | What is checked |
| :--- | :--- | :--- |
| Connector, no broker | `file`, `socket` → `socket_server`, `http_client` → `http_server`, `websocket` → `http_server`, `http_server` → `http_client` and `websocket`, `subprocess`, `stdout` → `stdin`, `file` → `csv`, `sql_insert` → `sql_select` and `sql_raw` (SQLite), `nanomsg`, and the wrappers `broker`, `fallback` → `sequence`, `retry` → `batched`, `drop_on` → `read_until`, `dynamic`, `reject_errored` | ten messages out through the output and back through the input |
| Connector, live broker | `beanstalkd`, `redis_streams`, `redis_pubsub`, `redis_list`, `nats`, `nats_jetstream`, `nats_kv`, `amqp_0_9`, `amqp_1`, `mqtt`, `nsq`, `pulsar`, `mongodb`, `sql_insert` → `sql_select` and `sql_raw` (PostgreSQL), `cache` → `redis_scan`, `redis_hash`, `sftp`, `azure_blob_storage`, `azure_queue_storage`, `azure_table_storage` (Azurite), `nats_stream`, `cassandra`, `gcp_cloud_storage` and `gcp_pubsub` (emulators), `qdrant`, `questdb`, `cockroachdb_changefeed` | the same; metadata too for `redis_streams` and `amqp_0_9` |
| Middleware | all 18 `connect_*` and the `connect` chain | the rewritten payloads, dropped messages, and that a failing processor or a fan-out rejects the batch |
| Processor | 31 more, through the `connect` chain (`processor_*` cases; the list is in the [README](../README.md#test-coverage)) | the rewritten payload |

A connector case uses the template format of Redpanda Connect's own
integration tests, so covering another connector is usually pasting the
`output:` / `input:` template from
`internal/impl/<connector>/integration_test.go` upstream and naming an image.
Where a case departs from upstream, a comment above it says why.

CI runs the suite in the build job against the library built there; on Linux a
broker that cannot start fails the job, on macOS the runner has no Docker and
the broker cases are skipped.

It checks delivery, not redelivery or acknowledgement — that is the
conformance suite's job. The schema registry behind
`connect_schema_registry_decode` and `_encode` is a stub in the runner that
serves one Avro schema.

### Known failures

* **The `switch` output does not deliver.** With a `switch` output — one case,
  with or without a `check`, a `file` child — the child is never opened, no
  message is confirmed, and the publish fails after `publish_timeout` (30s)
  with `delivery not confirmed`. Seen with plugin 0.1.1 under `mqb` 0.4.18.
  The other wrapping
  outputs (`broker` with `fan_out`, `fallback`, `retry`, `drop_on`) pass. The
  case is in `cases.toml`, commented out; the `switch` *processor* is
  unaffected.

* **Two outputs cannot take a JSON number — upstream, not the plugin.**
  `redis_hash` with `walk_json_object: true` fails with `can't marshal
  json.Number`, and `qdrant` with `payload_mapping: root = this` fails with
  `invalid type: json.Number`. Redpanda Connect decodes JSON numbers as
  `json.Number` by default (`internal/message/util.go` in benthos), and
  neither the go-redis nor the Qdrant client accepts that type, so the same
  config fails in Redpanda Connect itself. Setting `BENTHOS_USE_NUMBER=false`
  in the environment of the host process makes upstream decode to `float64`,
  and `redis_hash` then passes (verified; `qdrant` not re-run with it). That
  loses integer precision above 2^53, so the cases work around it in the
  config instead: explicit `fields` for `redis_hash`, a rebuilt payload for
  `qdrant`.

  A read of the other linked connectors for the same pattern — a value taken
  from the message and handed to a client library — found these, of which
  only the first was run:

  | Component | Field | Expected effect |
  | :--- | :--- | :--- |
  | `amqp_1` output | `application_properties_map` | fails, `marshal not implemented for json.Number` (verified) |
  | `azure_cosmosdb` output | `partition_keys_map`, patch `increment` | fails, `unsupported partition key type` / `expected patch value to be int64` |
  | `cypher` output | `args_mapping` | no error, but the number is stored as a string |
  | `gcp_bigquery_select` input and processor | `args_mapping` | no error, but the parameter is sent as a string |
  | `sql_*` | `args_mapping` | sent as a string; SQLite and PostgreSQL coerce it and the round trips pass, a stricter driver may not |

  `cassandra`, `questdb`, the `redis` processors, `pinecone` and `cyborgdb`
  vectors convert the type themselves; `mongodb`, `couchbase`, `timeplus`,
  Elasticsearch and OpenSearch send JSON, where it serialises as a number.
  Converting in the mapping avoids it where it was tried (`qdrant`):
  `.number()` gives a float, `.number().round()` an integer.

* **Not yet explained, both seen with `qdrant`.** `id: root = this.id` fails
  with `context was undefined, unable to reference id`, while
  `root = json("id")` works. And with `grpc_host: localhost:<port>` the output
  hangs without an error when only the IPv4 port is mapped; `127.0.0.1` works.
