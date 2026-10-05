# mq-bridge-connect

[![CI](https://github.com/marcomq/mq-bridge-connect/actions/workflows/ci.yml/badge.svg)](https://github.com/marcomq/mq-bridge-connect/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Status](https://img.shields.io/badge/status-early%20(broker%20verified%20in%20CI)-orange)

A Redpanda Connect **connector** compatibility plugin for
[`mq-bridge`](https://github.com/marcomq/mq-bridge).

The goal is to reuse Redpanda Connect's connector implementations as `mq-bridge`
endpoints **without** running a Redpanda Connect pipeline. `mq-bridge` stays the
engine — routing, batching, middleware, retries, DLQ and transformations remain
its job. Redpanda supplies only the I/O components.

> **Unofficial.** This is an independent community project. It is not affiliated
> with, endorsed by or supported by Redpanda Data, Inc. or the Benthos project.
> Please do not report issues with this plugin to them if this just affects the mq-bridge usage. Use this repository's
> [issue tracker](https://github.com/marcomq/mq-bridge-connect/issues) instead.
> "Redpanda" and "Benthos" are trademarks of their respective owners and are
> used here only to describe compatibility.

> ## ⚠️ Status: early — perform your own testing before you deploy
>
> Messages cross the boundary in both directions, payloads and metadata survive
> both ways, and an mq-bridge nack rejects the one source message it belongs to.
> 81 of the 124 endpoint components are round-tripped on every push, 52 of them
> against a live broker, and six connectors are also held to the conformance
> suite's redelivery checks. The one you are about to use may well be among the
> other 43: [add a case for it](#test-coverage) before you rely on it. See
> [docs/TESTING.md](docs/TESTING.md) for exactly what has been tested and how.

## When this is worth it

mq-bridge owns one end of every stream, so the plugin is only ever half a route.
That is also the rule for when it earns its place.

**Reach is the point.** 56 inputs, 68 outputs and 86 processors that mq-bridge
has no connector for, plus mq-bridge's own route model — retry, DLQ,
deduplication, encryption, transform, switch, observability — over sinks that
never had it.

**The boundary is not what limits a route.** Through `mqb`, a file of short JSON
lines drains into a Redpanda sink at **3.25M rows/s** and a Redpanda source feeds a file at **1.94M rows/s**
([Performance](docs/PERFORMANCE.md)); what limits a route is the connector at the
other end. A Redpanda processor as a middleware is a different matter: it is
priced by the processor, 270k–540k rows/s for a Bloblang mapping on one worker.

**Do not use it for a Redpanda source into a Redpanda sink.** You would pay the
consumer boundary
([Performance](docs/PERFORMANCE.md)), a 218 MiB library and a Go
runtime pinned in the process for its lifetime, and gain nothing at all. Run
Redpanda Connect.

## How it works

`mq-bridge` loads a small Rust plugin (`libmq_bridge_connect`), which loads a Go
`c-shared` sibling (`libmq_bridge_connect_go`) holding the Redpanda Connect
components. The two must sit in the same directory. Batches, never single
messages, cross a private C ABI between them. See
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for the architecture, the build and
the test suite.

## Install

Prebuilt for macOS arm64, Linux x86_64/arm64 and Windows x86_64:

```console
brew install marcomq/tap/mq-bridge-connect
conda install -c marcomq mq-bridge-connect
```

Either puts both libraries, and the third-party notices, where mq-bridge looks
for them; a route can then name the `connect` endpoint with no further setup.

For the Python and Node.js bindings of mq-bridge, call `register()` from the
language package before starting routes:

```console
pip install mq-bridge mq-bridge-connect  # wheel includes both libraries
npm install mq-bridge mq-bridge-connect     # downloads them on first use, see node/README.md
```

From Rust, depend on the crate and use `ConnectFactory` directly
([`examples/quickstart.rs`](examples/quickstart.rs)):

```console
cargo add mq-bridge mq-bridge-connect
```

Cargo compiles only the Rust side. The build script downloads the Go library
for the build target from this version's GitHub release, verifies it against
the sha256 pinned in the published crate (a mismatch fails the build), and puts
it next to the binaries, tests and examples under `target/`. `cargo run` and
`cargo test` then work with no setup.

- `MQ_BRIDGE_CONNECT_GO_LIBRARY=/abs/path/to/libmq_bridge_connect_go.so` uses
  your own library instead: at build time it skips the download, at runtime it
  overrides every other location. It is not checksummed.
- `MQ_BRIDGE_CONNECT_DOWNLOAD_URL` fetches the archives from a mirror; the
  checksum still applies.
- `default-features = false, features = ["plugin"]` turns the download off.
- Offline, the build succeeds with a warning; set the variable at runtime.

To ship a program, copy `libmq_bridge_connect_go.*` (`mq_bridge_connect_go.dll`)
from `target/<profile>/` next to the executable, together with
`THIRD_PARTY_NOTICES` (in the crate and in every release archive).
The release archives are the same files for a manual install — see
[packaging/INSTALL.md](packaging/INSTALL.md).

### Docker

The [`mq-bridge-app`](https://github.com/marcomq/mq-bridge/tree/main/apps/mq-bridge-app)
image (`ghcr.io/marcomq/mq-bridge-app`) has no plugins built in, but it searches
`/usr/local/lib/mq-bridge` for them, so installing the plugin is copying a
Linux release archive there. The image is Debian 12 based, which the Linux
archives (built against glibc 2.35) run on.

Download and unpack the archive for the image's architecture
(`aarch64-unknown-linux-gnu` on arm64):

```sh
curl -fsSLO https://github.com/marcomq/mq-bridge-connect/releases/download/v0.1.0/mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu.tar.gz
tar xzf mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu.tar.gz
```

**Build a derived image** — the recommended way:

```dockerfile
FROM ghcr.io/marcomq/mq-bridge-app:latest
COPY mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu/*.so /usr/local/lib/mq-bridge/
COPY mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu/THIRD_PARTY_NOTICES \
     mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu/LICENSE-* \
     /usr/share/licenses/mq-bridge-connect/
```

`COPY` leaves the files owned by root, which is what plugin discovery requires:
a library it finds on its own is loaded only if it and every directory above it
belong to root or to the user running mq-bridge (`nonroot` here). A route can
then name `connect` with no further setup:

```sh
docker build -t mq-bridge-app-connect .
docker run --rm mq-bridge-app-connect copy 'connect+mqtt://broker:1883/orders' 'file:///app/orders.jsonl'
```

**Or mount the directory** without building an image. A bind mount keeps the
host's file owner, which discovery refuses, so name the plugin by path —
a library loaded by path is not ownership-checked:

```sh
docker run --rm -v "$PWD/mq-bridge-connect-0.1.0-x86_64-unknown-linux-gnu:/plugins/connect:ro" \
    ghcr.io/marcomq/mq-bridge-app:latest \
    --plugin /plugins/connect/libmq_bridge_connect.so copy 'connect+mqtt://broker:1883/orders' 'file:///app/orders.jsonl'
```

The Go sibling is found beside the plugin, so mount the whole directory, not
the one file. Either way, keep `THIRD_PARTY_NOTICES` in the image or the mount:
it is part of the distribution (see [packaging/INSTALL.md](packaging/INSTALL.md)).
The first `connect` route loads a 218 MiB library, so give it a generous
startup timeout (see [Known issues](#known-issues)).

Building the plugin yourself: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md).

## Examples

Four ways to drive the same connector, in [`examples/`](examples/), plus the
benchmark harness. None of them needs a broker: they run a Redpanda Connect
`generate` input into a `file` output, so the only thing exercised is the
boundary.

| Example | What it shows |
| :--- | :--- |
| [`quickstart.rs`](examples/quickstart.rs) | The shortest crossing: create a consumer, receive one batch, commit it. ~50 lines. |
| [`route.rs`](examples/route.rs) | A full route — both config forms, both directions, middleware, a handler, clean shutdown. |
| [`mqb-route.yaml`](examples/mqb-route.yaml) | The same route for the `mqb` CLI / server and the desktop UI, with no code at all. |
| [`python_route.py`](examples/python_route.py) | The same route from Python, loading the plugin at runtime. |
| [`throughput.rs`](examples/throughput.rs) | The benchmark harness behind [docs/PERFORMANCE.md](docs/PERFORMANCE.md). |

`mqb` and Python load the plugin, which finds the Go sibling beside it:

```sh
MQB_PLUGIN_DIR=$PWD/target/debug mqb --config examples/mqb-route.yaml
python examples/python_route.py
```

The Rust examples need `MQ_BRIDGE_CONNECT_GO_LIBRARY` set; see
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md#running-the-examples).

## Configuring an endpoint

mq-bridge owns one end of every stream and Redpanda Connect owns the other: a
`connect` **input** is a Benthos stream whose output is mq-bridge, and a
`connect` **output** is one whose input is mq-bridge. A configuration that also
declares the end mq-bridge owns is rejected, rather than quietly bypassing the
route's retries, DLQ and observability.

There are two configuration forms, and the first compiles into the second, so
they cannot drift apart ([`src/config.rs`](src/config.rs)).

**Form A — one connector.** Name it with `connector`; everything else is that
component's own configuration.

```json
{ "custom": { "name": "connect", "config": {
    "connector": "mqtt",
    "urls": ["tcp://localhost:1883"],
    "topics": ["orders"]
} } }
```

Most connectors name the same thing differently in each direction, and Benthos
rejects a field the direction does not define. Put those in an `input` or
`output` block; the one matching this endpoint is merged in and the other is
dropped, so a single configuration describes both ends of a route.

```json
{ "custom": { "name": "connect", "config": {
    "connector": "amqp_0_9",
    "urls": ["amqp://guest:guest@localhost:5672/"],
    "input":  { "queue": "orders" },
    "output": { "exchange": "", "key": "orders" }
} } }
```

A block is optional, and a field inside one overrides the same field outside it
— which is how `mqtt` gives each direction its own `client_id`, as a broker
disconnects the older session when two clients present the same one. A connector
that genuinely has a field called `input` or `output` needs form B.

`publish_timeout` and `logger` are settings of the stream, not of the
component, and form A moves them to the top level of the document (see below).

**From a URI.** The scheme names the component after a `+`, the spelling
`git+ssh://` and `postgresql+psycopg2://` made familiar, and the rest of the URI
is read as that component's:

```sh
mqb copy 'connect+mqtt://localhost:1883/orders' 'file:///tmp/out.jsonl'
```

That is the same configuration as the first example above. The authority is the
broker's address and the path is the topic, queue or subject; each is written
into whichever field the component names it by, in this direction:

| Component | Address | Reading | Writing |
| :--- | :--- | :--- | :--- |
| `mqtt` | `urls: [tcp://…]` | `topics` | `topic` |
| `amqp_0_9` | `urls: [amqp://…]` | `queue` | `key`, with the default exchange |
| `amqp_1` | `urls: [amqp://…]` | `source_address` | `target_address` |
| `nats`, `nats_jetstream` | `urls: [nats://…]` | `subject` | `subject` |
| `pulsar` | `url: pulsar://…` | `topics` | `topic` |
| `redis_streams` | `url: redis://…` | `streams` | `stream` |
| `redis_pubsub` | `url: redis://…` | `channels` | `channel` |
| `redis_list` | `url: redis://…` | `key` | `key` |

Every other field is a query parameter, and any field given by its own name wins
over what the URI would have filled. A query value arrives as a string; one given
to a field the component declares as a bool or a number is converted, so
`?start_from_oldest=true` works. A URI with no authority, such as
`connect+generate://?count=100`, sets no address. A component outside the table is configured
by its own field names, and keeps `address` and `topic` as its own — `beanstalkd`
takes an address and `nsq` a topic, so translating them there would break a
configuration that works today.

A URI scheme may hold only letters, digits, `+`, `-` and `.`
([RFC 3986](https://www.rfc-editor.org/rfc/rfc3986#section-3.1)), so a component
whose name contains `_` is spelled with `-`: `connect+amqp-0-9://`. No
component name contains a `-`, which is what makes the way back unambiguous.

**Form B — a Redpanda Connect configuration**, minus the end mq-bridge owns.
This is the form to reach for if you already know Redpanda Connect.

```json
{ "custom": { "name": "connect", "config": { "yaml":
    "input:\n  mqtt:\n    urls: [tcp://localhost:1883]\n    topics: [orders]\npipeline:\n  processors:\n    - mapping: 'meta ingested_at = now()'\n"
} } }
```

Form B accepts `input` or `output` (whichever this endpoint owns), `pipeline`
with `threads` and `processors`, `logger`, the three `*_resources` sections,
`max_in_flight` and, for an output, `publish_timeout`. Any other top-level key is an error naming what is accepted —
an ignored key would be configuration the user believes is in effect.

`max_in_flight` (default 64) is how many source **batches** may sit
unacknowledged inside the plugin at once. It is the backpressure, and it is also
what lets the source keep working while mq-bridge handles the previous batch. See
[Ordering](#semantics) before lowering it.

It counts batches, not messages, so the right value depends on what the source
produces. A connector that batches is already well served by 64; one that emits
a message at a time — `file` does, without a `batching` policy — is held to 64
messages in flight and loses about a fifth of its throughput to the round trip.
Raise it for those, and see
[Throughput](#throughput-against-a-native-pipeline) for what it recovers.
A `file` source that has to keep its line order needs `max_in_flight: 1` instead.

`publish_timeout` (default `30s`, output only) bounds how long a send waits for
the component to confirm delivery. Benthos retries a failing output internally
and indefinitely — an unreachable broker, or one that rejects the message — so
without a bound the route would block forever and look healthy. When it elapses
the send fails as retryable, so mq-bridge's `retry` and `dlq` middlewares take
over. Benthos may still deliver the timed-out batch later, so a retry can
duplicate it. `0s` waits forever.

**Logging.** Without a `logger` block, Benthos warnings and errors go to
stderr — they are what name the cause of a send that never completes, such as
`connection refused`. A `logger` block replaces that with Benthos's own logger,
which writes to stdout unless it is given a `file`; `logger: { level: off }`
silences it.

### Connector notes

* **`amqp_1`.** Benthos writes metadata as AMQP message annotations, and AMQP
  1.0 reserves every annotation key without an `x-` prefix. RabbitMQ 4.x
  enforces that by dropping the connection. Form A therefore defaults an
  `amqp_1` output to `metadata: { exclude_prefixes: [""] }`; set `metadata`
  yourself to change it, and in form B set it yourself. `application_properties_map`
  can carry fields, but the `amqp_1` input does not turn application properties
  back into metadata.
* **`nats_jetstream`, `amqp_0_9`.** Benthos does not create streams or queues.
  An AMQP 0.9 publish to a queue that does not exist is dropped by the broker
  without an error.

### Schema and validation

The plugin describes its configuration to the host as a JSON Schema (plugin ABI
1.1), so a host can render a form for it and give a URI's values their types.
Only the keys the two forms own are described — `connector`, `address`, `topic`,
`yaml`, `input` and `output` — because form A's remaining fields belong to
whichever component `connector` names, and the host cannot know those.

The first three carry the `x-mqb-uri` annotations that make the URI form work:
`connector` takes the scheme's part after the `+`, `address` the authority and
`topic` the path. They are ordinary configuration keys too, so the same
shorthand is available in JSON and YAML.

Set `MQB_PLUGIN_VALIDATE_CONFIG=1` and the host checks a route's configuration
against that schema before the endpoint is opened. It is off by default, and the
checks that matter here — the two forms excluding each other, stray keys beside
`yaml` — are made by the plugin itself either way.

## Processors as middlewares

Processors also run as mq-bridge middlewares, on any endpoint — a native
`kafka` input as much as a `connect` one. Each processor below is its own
middleware, `connect_<processor>`, configured exactly as the processor is:

```yaml
input:
  kafka: { url: localhost:9092, topic: orders }
  middlewares:
    - connect_mapping: 'root = this.merge({"received_at": now()})'
    - connect_dedupe:
        key: '${! meta("kafka_key") }'
        cache: { redis: { url: redis://localhost:6379 } }
```

| Middleware | What it is for |
| :--- | :--- |
| `connect_mapping`, `connect_mutation`, `connect_bloblang` | Bloblang rewrites; `deleted()` drops a message |
| `connect_jq`, `connect_jmespath`, `connect_grok`, `connect_parse_log` | Reshaping and parsing |
| `connect_json_schema` | Validation |
| `connect_avro`, `connect_msgpack` | Encoding |
| `connect_http`, `connect_branch`, `connect_cached`, `connect_javascript` | Enrichment and lookups |
| `connect_dedupe` | Deduplication against a cache, which may be shared (Redis, Memcached) across instances |
| `connect_log` | Logging each message |

`connect_dedupe` and `connect_cached` take their `cache` inline, as a cache
component, or as the label of a cache resource. In a URI (`mqb`) the middleware
is written `|connect-<processor>?field=value`; a Bloblang processor takes its
mapping as a parameter named after itself, `|connect-mapping?mapping=...`.

Every middleware is one call into Go per batch. To run several processors in
one call, and to share resources between them, use the `connect` middleware:

```yaml
middlewares:
  - connect:
      processors:
        - mapping: 'root = this.payload'
        - dedupe: { cache: seen, key: '${! json("id") }' }
      cache_resources:
        - { label: seen, memory: { default_ttl: 5m } }
```

It accepts any processor, not only the ones listed, as long as the processor
keeps, rewrites or drops each message. One that splits a message
(`unarchive`, `split`) or merges several (`archive`, `group_by`) fails the
batch and belongs in a connect endpoint's `pipeline`. A message a processor
marks as failed fails its batch as retryable, so mq-bridge's `retry` and `dlq`
decide what happens next. Metadata the processors change is carried back; the
message ID is kept.

The middlewares are registered when the plugin loads, so a route that uses them
without a `connect` endpoint needs a host that loads its plugins up front, as
`mqb`'s `plugins:` list does.

**Reach for them for what mq-bridge lacks, not for speed.** Crossing into Go
and back costs about 0.9 µs per message, and the processor's own work comes on
top: ~1.5 µs for `root = this`, ~2.6 µs for a `merge`
([Performance](docs/PERFORMANCE.md#through-mqb)). Processing is CPU-bound, so it scales across route
workers — 2.2× with `mqb`'s default of four — when the sink takes batches in any
order; `file` keeps order, which holds the chain to one worker.

## Test coverage

Two suites run in CI. The **round trips**
([`tests/endpoints/`](tests/endpoints/)) publish through a component's output
and read back through its input, using the `mqb` CLI and the templates of
Redpanda Connect's own integration tests. The **conformance suite**
([`tests/conformance.rs`](tests/conformance.rs)) is mq-bridge's acceptance
gate, which also checks that a rejected or abandoned message comes back.

| Component | Round trip | Conformance | Against |
| :--- | :---: | :---: | :--- |
| `amqp_0_9` | ✓ with metadata | ✓ | RabbitMQ 4.1 |
| `amqp_1` | ✓ | – | RabbitMQ 4.1 |
| `beanstalkd` | ✓ | ✓ | beanstalkd |
| `mongodb` | ✓ | – | MongoDB 7 |
| `mqtt` | ✓ | ✓ | Mosquitto 2.0 |
| `nats` | ✓ | – | NATS 2.10 |
| `nats_jetstream` | ✓ | ✓ | NATS 2.10 |
| `nats_kv` | ✓ | – | NATS 2.10 |
| `nats_stream` | ✓ | – | NATS Streaming 0.25 |
| `nsq` | ✓ | – | nsqd 1.2 |
| `pulsar` | ✓ | – | Pulsar 3.3 |
| `redis_list` | ✓ | ✓ | Redis 7 |
| `redis_pubsub` | ✓ | – | Redis 7 |
| `redis_streams` | ✓ with metadata | ✓ | Redis 7 |
| `cache` (output) → `redis_scan` | ✓ | – | Redis 7 |
| `redis_hash` (output) | ✓ read back with the `redis` processor | – | Redis 7 |
| `azure_blob_storage` | ✓ | – | Azurite 3.34 |
| `azure_queue_storage` | ✓ | – | Azurite 3.34 |
| `azure_table_storage` | ✓ | – | Azurite 3.34 |
| `cassandra` | ✓ | – | Cassandra 5.0 |
| `gcp_cloud_storage` | ✓ | – | fake-gcs-server 1.52 |
| `gcp_pubsub` | ✓ | – | Pub/Sub emulator |
| `qdrant` (output) | ✓ read back over REST | – | Qdrant 1.17 |
| `questdb` (output) | ✓ read back over HTTP | – | QuestDB 8.0 |
| `sql_raw` → `cockroachdb_changefeed` (input) | ✓ | – | CockroachDB 24.3 |
| `sftp` | ✓ | – | OpenSSH (atmoz/sftp) |
| `sql_insert` → `sql_select` | ✓ | – | PostgreSQL 16, SQLite |
| `sql_raw` | ✓ | – | PostgreSQL 16, SQLite |
| `file` | ✓ | – | no broker |
| `file` → `csv` | ✓ | – | no broker |
| `http_client` → `http_server` | ✓ | – | no broker |
| `http_server` (output) → `http_client` (input) | ✓ | – | no broker |
| `http_server` (output) → `websocket` (input) | ✓ | – | no broker |
| `websocket` (output) → `http_server` | ✓ | – | no broker |
| `nanomsg` | ✓ | – | no broker |
| `socket` → `socket_server` | ✓ | – | no broker |
| `subprocess` | ✓ | – | no broker |
| `stdout` → `stdin` | ✓ | – | no broker |
| `broker` | ✓ | – | no broker, wrapping `file` |
| `fallback` → `sequence` | ✓ | – | no broker, wrapping `file` |
| `retry` → `batched` | ✓ | – | no broker, wrapping `file` |
| `drop_on` → `read_until` | ✓ | – | no broker, wrapping `file` |
| `dynamic` | ✓ | – | no broker, wrapping `file` |
| `reject_errored` (output) | ✓ | – | no broker, wrapping `file` |
| `generate` (input) | feeds every round trip | – | no broker |

That is 81 of the 124 inputs and outputs. Everything else — S3 and the other
cloud connectors, Elasticsearch, OpenSearch and the rest — has not
been run through this boundary.

All 16 [processor middlewares](#processors-as-middlewares) and the `connect`
chain are run end to end, and through the chain 31 more processors, 47 of the
86 linked:

| Processors | Run as | What is checked |
| :--- | :--- | :--- |
| `mapping`, `mutation`, `bloblang`, `jq`, `jmespath`, `grok`, `parse_log` | `connect_<processor>` | the rewritten payload; `deleted()` drops the message, `throw()` rejects the batch |
| `json_schema` | `connect_json_schema` | a valid message passes, an invalid one rejects the batch |
| `avro`, `msgpack` | `connect_<processor>` | encode, then decode back |
| `http`, `branch`, `cached`, `javascript` | `connect_<processor>` | the enriched payload; `cached` answers a repeated key from its cache |
| `dedupe`, `log` | `connect_<processor>` | a repeated key is dropped; a logged message passes unchanged |
| `compress`, `decompress`, `bounds_check`, `select_parts`, `noop`, `sleep`, `metric` | `connect` chain | the payload after the chain; `bounds_check` drops a short message |
| `try`, `catch`, `try_catch`, `switch`, `for_each`, `while`, `retry`, `parallel`, `processors`, `workflow` | `connect` chain | the payload their child processors produce |
| `string_split`, `text_chunker`, `sync_response` | `connect` chain | the split payload as an array; a text shorter than one chunk, and a synced message, pass unchanged |
| `archive`, `group_by`, `group_by_value` | `connect` chain | a batch of one message only: the archived array, the payload of the matching group |
| `cache`, `rate_limit` | `connect` chain | with a `cache_resources` / `rate_limit_resources` entry; the caches `memory`, `lru`, `ttlru`, `ristretto` and `multilevel` |
| `command`, `subprocess` | `connect` chain | the payload piped through `tr` and `cat` |
| `wasm` | `connect` chain | the payload uppercased by a module that reads and rewrites it |
| `sql_raw`, `sql_insert`, `sql_select` | `connect` chain | rows written to and read from SQLite |

`unarchive` and `insert_part` are run only to show that a processor adding
messages rejects the batch, as `text_chunker` does on a text longer than one
chunk. The other 37, among them `redis`, `mongodb`, `nats_kv`,
`nats_request_reply` and `split`, are not run.

To cover another component, paste its `output:` / `input:` template from
upstream into [`tests/endpoints/cases.toml`](tests/endpoints/cases.toml), name
an image, and run `python3 tests/endpoints/run.py <name>`. It needs `mqb` and
Docker, and compiles nothing ([docs/TESTING.md](docs/TESTING.md#endpoint-round-trips)).

## Curated components

Only the Redpanda Connect packages that reach no Redpanda Community License
code are linked — 56 inputs, 68 outputs and 86 processors, from the allowlist in
[`go-bridge/components.allow`](go-bridge/components.allow). The aggregate
`public/components/all` bundle, and with it `kafka`, `snowflake` and
`redpanda`, is excluded. How the allowlist is built and checked, and the
third-party licenses it brings in, are in [docs/LICENSING.md](docs/LICENSING.md).

## Performance

The plugin adds 3 MiB of Rust and a 218 MiB Go library, ~90 ms to a warm start
and ~170 MiB of resident memory. Through `mqb`, a Redpanda sink takes 3.25M
rows/s and a Redpanda source feeds 1.94M rows/s; a processor middleware costs
~0.9 µs a message to cross plus the processor's own work. Numbers, methodology
and the comparison against native pipelines and mq-bridge's own endpoints:
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## Known issues

* **The Go runtime is loaded once and never unloaded.** Go does not support
  `dlclose` of a `c-shared` library, so the mapping — and its memory — stays
  resident until the process exits. Allow only **one** Go-runtime plugin per
  process ([golang/go#65050](https://github.com/golang/go/issues/65050)).
* **Hosts must install signal handlers before loading the plugin.** Go adds
  `SA_ONSTACK` only to handlers that exist when the library loads, and
  `tokio::signal` does not set it. Register SIGINT/SIGTERM handlers first; `mqb`
  does.
* **Most connectors have never been run** against a live broker through this
  boundary — see [docs/TESTING.md](docs/TESTING.md).
* **Two libraries plus `THIRD_PARTY_NOTICES`, in one directory.** The notices
  are required by the licenses of statically linked code; keep them with the
  libraries in any repackaging ([`packaging/INSTALL.md`](packaging/INSTALL.md)).
* **The first route can outlast a 5 s startup timeout.** The Go library loads
  when the first `connect` endpoint is created. Over the mq-bridge MCP server,
  give the first route `startup_timeout_ms: 30000`.
* **No crash isolation.** Deferred `recover` contains ordinary panics only;
  fatal Go runtime errors, native crashes and OOM take the host down with them.
  Only a subprocess deployment of Redpanda Connect gives real isolation.

## Semantics

* **At-least-once, per message.** A source batch is held inside its output write
  until mq-bridge has reported a disposition for every message in it. If all are
  `Ack`ed the write returns `nil`; if any is `Nack`ed it returns a
  `service.BatchError` naming **exactly those messages**, and the source
  redelivers only them. `TestANackRejectsOneMessageOfASourceBatch` asserts it
  against a real multi-message source batch.
  A source that cannot associate the error with its own batch falls back to
  redelivering all of it — at-least-once is preserved either way.
* **An uncommitted batch is nacked, not dropped.** Closing a stream rejects
  everything handed to mq-bridge that was never committed, so the source
  redelivers it.
* **Ordering is not preserved across in-flight batches.** Up to `max_in_flight`
  source batches are written concurrently and have no order between them. Set
  `max_in_flight: 1` for a source whose order matters — at the cost of the
  pipelining that overlaps mq-bridge's work with the source's.
* **`Reply` is not supported.** A Benthos source has nowhere to put a reply, so
  `MessageDisposition::Reply` acknowledges and the reply payload is dropped.
* **A send is bounded by `publish_timeout`.** A send the output never confirms
  fails as retryable after it, and may still be delivered later.
* **Whole-batch output results.** Redpanda `BatchError` can report partial
  success; plugin ABI v1 reports a publish batch all-or-nothing. A publish call
  returns only once Benthos has delivered the whole batch, so success means
  delivered rather than queued — but retrying can duplicate already-written items.
* **Bytes and string metadata only.** Redpanda's `any`-typed metadata is
  coerced to strings; Go message contexts are not translated.
* **Secrets must be indirect.** Custom endpoint config is not covered by
  mq-bridge's secret extractor, and form B makes inline secrets tempting. Use
  environment or file references.

Bloblang and processors **are** supported, through form B's `pipeline` and as
[middlewares](#processors-as-middlewares). Explicit
non-goals: buffers, the HTTP management API, per-connector DLLs, and any claim of
exactly-once or zero-copy behaviour.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

`mq-bridge` itself is MIT. Dual-licensing this plugin is deliberate: the
distributed artifact links Apache-2.0 Go code (Redpanda Connect) alongside MIT
Go code (Benthos), and the Apache-2.0 option carries the express patent grant
that pairing warrants.

Choosing a license arm covers **this repository's own code only**. Bundled
third-party code keeps its own terms, so any binary distribution must still ship
`THIRD_PARTY_NOTICES` with the Apache-2.0 and MIT attributions of everything
actually linked. Because Go build tags change the linked set, the audit unit is
the built artifact, not `go.mod`.

The distributed artifact is **not** purely MIT/Apache-2.0: ten linked Go
modules are weak copyleft (nine MPL-2.0, one EPL-2.0/EDL-1.0 used under
EDL-1.0), none GPL, LGPL or AGPL. See
[docs/LICENSING.md](docs/LICENSING.md#third-party-licenses).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
