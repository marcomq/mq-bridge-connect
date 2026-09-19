# mq-bridge-redpanda

[![CI](https://github.com/marcomq/mq-bridge-redpanda/actions/workflows/ci.yml/badge.svg)](https://github.com/marcomq/mq-bridge-redpanda/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Status](https://img.shields.io/badge/status-early%20(loopback%20verified)-orange)

A Redpanda Connect **connector** compatibility plugin for
[`mq-bridge`](https://github.com/marcomq/mq-bridge).

The goal is to reuse Redpanda Connect's connector implementations as `mq-bridge`
endpoints **without** running a Redpanda Connect pipeline. `mq-bridge` stays the
engine — routing, batching, middleware, retries, DLQ and transformations remain
its job. Redpanda supplies only the I/O components.

> ## ⚠️ Status: early — do not deploy
>
> **Messages cross the boundary in both directions**, at 1.83M msg/s in and
> 3.66M out. A `redpanda` input consumes through a Redpanda Connect connector and
> a `redpanda` output publishes through one; payloads and metadata survive both
> ways, Bloblang processors run in between, and an mq-bridge nack rejects the one
> source message it belongs to.
>
> **The acceptance gate now passes against a real broker.**
> `mq_bridge::plugin::conformance` ([`tests/conformance.rs`](tests/conformance.rs))
> runs `round_trip`, `nack_redelivers` and `uncommitted_batch_redelivers` against
> a live beanstalkd, green five times consecutively. That is the first connector
> that talks to a broker, and it exercises redelivery for real rather than
> through a scripted source. `metadata_preserved` is skipped because a beanstalkd
> job is a body and nothing else; boundary metadata is covered by other tests.
>
> **What still gates the next status.** No Linux build has ever succeeded — the
> first CI run failed to link, and the fix in this tree is unverified until CI
> runs again. `beanstalkd` is also one connector; treat the other 50 inputs and
> 62 outputs as unverified.
>
> What *is* verified: 13 Go tests (race-clean) and 17 Rust tests, covering
> acknowledgement granularity, batch splitting and aggregation, release-once
> semantics, the wire format, configuration, and `socket_server` / `socket` over
> loopback TCP. The loader contract holds across 150 consecutive
> load/probe/panic-recovery cycles.

## When this is worth it

mq-bridge owns one end of every stream, so the plugin is only ever half a route.
That is also the rule for when it earns its place.

**Reach is the point.** 51 inputs, 63 outputs and 68 processors that mq-bridge
has no connector for, plus mq-bridge's own route model — retry, DLQ,
deduplication, encryption, transform, switch, observability — over sinks that
never had it.

**Speed can be, too, where mq-bridge owns the faster end.** Reading a local file,
mq-bridge moves **694 773 msg/s** against Redpanda Connect's **238 851 msg/s**
for the same 200 000 lines, and the boundary into a Redpanda sink is free
(`mq-bridge → file` measured 182 165 msg/s against 171 996 native). So a
`file → <redpanda sink>` route through mq-bridge beats the same route inside
Redpanda Connect whenever the sink can absorb more than 239k/s; when the sink is
the bottleneck it is a wash, never a loss. That is **one connector on one
workload, measured with two different harnesses** — a reason to measure your own
pair, not a general claim that either tool is faster. The Redpanda figures are
from the run in [Throughput](#throughput-against-a-native-pipeline); the
mq-bridge one predates it and was not re-measured alongside, so treat the ratio
as indicative.

**Do not use it for a Redpanda source into a Redpanda sink.** You would pay the
consumer boundary
([Throughput](#throughput-against-a-native-pipeline)), a 253 MiB library and a Go
runtime pinned in the process for its lifetime, and gain nothing at all. Run
Redpanda Connect.

## Architecture

```text
mq-bridge
  └─ native plugin ABI 1.0
      └─ Rust cdylib: libmq_bridge_redpanda
          ├─ mq-bridge plugin SDK (runtime, handles, panic boundary)
          ├─ CanonicalMessage / disposition translation
          └─ private batch C ABI, resolved at runtime
              └─ Go c-shared sibling: libmq_bridge_redpanda_go
                  ├─ Benthos StreamBuilder, one stream per endpoint
                  ├─ batch parking with per-message dispositions
                  └─ curated Redpanda component imports
```

Two artifacts ship together in one directory. The Rust plugin locates the Go
sibling **relative to its own absolute path** ([`src/sibling.rs`](src/sibling.rs)),
never via the working directory or the system library search path.
`MQ_BRIDGE_REDPANDA_GO_LIBRARY` overrides that with an absolute path, for tests.

The private Rust↔Go ABI ([`go-bridge/bridge.h`](go-bridge/bridge.h)) is versioned
independently of mq-bridge's public plugin ABI: a `struct_size` plus
major/minor pair, C scalar types only, explicit ownership, and one call per
*batch* — never per message. A batch crosses as one length-prefixed blob
([`src/wire.rs`](src/wire.rs), [`go-bridge/wire.go`](go-bridge/wire.go)): one
allocation and one free per crossing, and no base64 of binary payloads.

### Why a separate Go library

Redpanda Connect is Go. Linking a Go `c-archive` into a `dlopen`ed Rust cdylib
hits static-TLS problems ([golang/go#48596](https://github.com/golang/go/issues/48596)),
and reimplementing mq-bridge's plugin vtable in Go would duplicate handle
management, buffer pairing, status mapping and panic containment that the Rust
SDK already provides. A `c-shared` sibling keeps mq-bridge's tested ABI on the
Rust side and a small private ABI in between.

## Requirements

| Tool | Version |
| :--- | :--- |
| Rust | 1.85+ |
| Go | 1.26.6+ (cgo enabled — needs a working C toolchain) |
| `mq-bridge` | checked out at `../mq-bridge` (path dependency) |

Pinned upstreams: Benthos `v4.78.0`, Redpanda Connect `v4.107.2`.

Cross-compilation is **not** a supported build path: cgo is disabled by default
for cross builds and needs a target C compiler and sysroot. Build on native
runners per target.

## Build and verify

```sh
sh scripts/phase0-smoke.sh      # Linux / macOS
```

```powershell
scripts\phase0-smoke.ps1        # Windows
```

The script builds the Go `c-shared` library and the Rust cdylib into the same
`target/<profile>/` directory, runs `cargo test`, then runs
[`phase0_smoke`](src/bin/phase0_smoke.rs), which:

1. opens the Go library, builds and closes an empty Benthos resource manager;
2. asserts an ordinary Go panic is recovered at the export boundary and
   surfaced as a status plus a diagnostic string, not a process abort;
3. loads the Rust cdylib through `mq_bridge::plugin::load_endpoint_plugin` and
   checks the advertised endpoint name and capabilities.

The Go side has its own tests, which need the module cache environment the
script sets up. Run them under the race detector — the sink parks source batches
across goroutines, so that is the check that matters:

```sh
cd go-bridge && go test -race ./...
```

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs all of it on every
push: `gofmt`, `go vet` and `go test -race`; `cargo fmt`, `cargo clippy -D
warnings` and the smoke script on both Linux and macOS; and the conformance
suite against a real beanstalkd broker.

The conformance suite needs a broker and skips without one. To run it locally:

```sh
docker run --rm -p 11300:11300 schickling/beanstalkd   # or: beanstalkd -l 127.0.0.1 -p 11300
MQ_BRIDGE_REDPANDA_BEANSTALKD=127.0.0.1:11300 cargo test --test conformance -- --nocapture
```

The build uses the Go toolchain's own caches (`go env GOCACHE`, `GOMODCACHE`).
They are shared with every other Go project and trimmed automatically; an
earlier revision pointed them under `target/`, which gave each checkout a
private ~26 GB copy that nothing ever reclaimed. `go clean -cache -modcache`
frees them.

[`scripts/benchmark.sh`](scripts/benchmark.sh) measures the boundary against an
identical pipeline that never leaves Go — see
[Throughput](#throughput-against-a-native-pipeline).

## Configuring an endpoint

mq-bridge owns one end of every stream and Redpanda Connect owns the other: a
`redpanda` **input** is a Benthos stream whose output is mq-bridge, and a
`redpanda` **output** is one whose input is mq-bridge. A configuration that also
declares the end mq-bridge owns is rejected, rather than quietly bypassing the
route's retries, DLQ and observability.

There are two configuration forms, and the first compiles into the second, so
they cannot drift apart ([`src/config.rs`](src/config.rs)).

**Form A — one connector.** Name it with `connector`; everything else is that
component's own configuration.

```json
{ "custom": { "name": "redpanda", "config": {
    "connector": "mqtt",
    "urls": ["tcp://localhost:1883"],
    "topics": ["orders"]
} } }
```

**Form B — a Redpanda Connect configuration**, minus the end mq-bridge owns.
This is the form to reach for if you already know Redpanda Connect.

```json
{ "custom": { "name": "redpanda", "config": { "yaml":
    "input:\n  mqtt:\n    urls: [tcp://localhost:1883]\n    topics: [orders]\npipeline:\n  processors:\n    - mapping: 'meta ingested_at = now()'\n"
} } }
```

Form B accepts `input` or `output` (whichever this endpoint owns), `pipeline`
with `threads` and `processors`, `logger`, the three `*_resources` sections, and
`max_in_flight`. Any other top-level key is an error naming what is accepted —
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

## Curated components

Components are linked from an explicit allowlist,
[`go-bridge/components.allow`](go-bridge/components.allow) — the 54
`public/components` packages of Redpanda Connect that reach no file carrying a
Redpanda Community License header and can be linked into a shared library, out
of the 79 that exist. The allowlist is
data: [`scripts/gen-components.py`](scripts/gen-components.py) turns it into
`go-bridge/internal/components`, which both the bridge and the catalog tool
import, so they cannot drift apart.

Enumerating the registered components in the built library gives:

| Registered | Count |
| :--- | ---: |
| Inputs | 51 |
| Outputs | 63 |
| Processors | 68 |

Reproduce with `go run ./internal/catalogtool` from [`go-bridge/`](go-bridge/),
which walks the Benthos global environment of the library as built.

`tigerbeetle` is the 55th RCL-free package and is excluded on every platform:
it ships prebuilt static archives, and neither can go into a `c-shared` object.
The macOS one has members that are not 8-byte aligned, which only the deprecated
`-ld_classic` accepts. The Linux one carries `R_X86_64_TPOFF32` initial-exec TLS
relocations, which a shared object cannot hold — `ld` says "recompile with
`-fPIC`", and since the archive is prebuilt, we cannot.

The taint is transitive and cannot be guessed from a package name: `snowflake`,
`kafka`, `aws` and `redpanda` are all excluded, while `gcp` is included because
its RCL code is confined to a `gcp/enterprise` subpackage that
`components/gcp` never imports. Two upstream files carry most of the blast
radius — `internal/serviceaccount/oauth2.go` and `internal/license`.

That the boundary holds is checked rather than asserted:
[`scripts/check-rcl.py`](scripts/check-rcl.py) asks the Go toolchain for the
package closure the compiler actually compiles, reads every file in it, and
fails on an RCL header. It runs in CI, so an upstream release that taints a
package we link is caught at the next push rather than after shipping. At
connect v4.110.0 it reads 20,741 linked files and finds none; the same scan of
the `all` bundle finds 193.

The aggregate `public/components/all` is RCL-tainted, so the "everything" bundle
is not usable under Apache-2.0 terms. `public/bundle/free` does not exist in the
published module; it is generated at build time and cannot be imported.

The allowlist links **351 third-party Go modules**, none carrying an RCL header
— see [`THIRD_PARTY_NOTICES`](THIRD_PARTY_NOTICES), which is generated from what
the build actually links, and the non-recursive verification command it
documents. RCL-free is not the same as permissive: see
[Third-party licenses](#third-party-licenses).

## Performance

Measured on macOS arm64, release build. First the fixed costs of the approach —
load time and per-call overhead — then throughput against a native pipeline.

| | |
| :--- | ---: |
| Rust cdylib | 1.7 MiB |
| Go sibling library | 45.2 MiB |
| Go runtime resident set | ~7 MiB |
| First load (cold page cache) | ~1400 ms |
| Load (warm page cache) | ~4.7 ms |
| Rust→Go call, steady state | ~300 ns |
| Rust→Go call, first on a new OS thread | ~5.5 µs (worst seen 30 µs) |
| `ResourceBuilder` build + close | ~24 µs |

Three things follow.

> **These figures predate the full allowlist.** They were measured against the
> earlier three-package build (1 input, 2 outputs). Only two have been re-measured
> against the full allowlist: the Go sibling is **253 MiB** release, and
> peak RSS is **113 MiB** — though that reading comes from the smoke test, which
> does eight load/unload cycles, so it is not a steady-state number. Load time
> and call latency have not been re-measured.

**Size dominates.** The Go sibling is ~150× the Rust plugin. It is nearly all
Redpanda Connect and its transitive dependencies, and it grows with the
allowlist, not with usage.

**Cold start is disk-bound.** ~1.4 s on first load versus ~4.7 ms warm is the
cost of faulting in a 45 MiB image. It is paid once per machine boot, but it is
paid during `mq-bridge` startup.

**Cross the boundary per batch, never per message.** ~300 ns steady-state is
cheap against real network I/O but not against nothing. The sharper edge is the
first call from an OS thread the Go runtime has not seen: 10–50× the steady
cost. `mq-bridge`'s tokio worker pool is fixed, so that amortises — but code
that calls into Go from `spawn_blocking` threads would pay it repeatedly. This
is why the private ABI is defined per batch.

### Throughput against a native pipeline

[`scripts/benchmark.sh`](scripts/benchmark.sh) runs every pipeline twice: once
entirely inside Go ([`nativebench`](go-bridge/internal/nativebench/main.go)), and
once with mq-bridge owning an end
([`examples/throughput.rs`](examples/throughput.rs)). Both link the same Benthos
engine and the same component set, so what separates the two numbers is the
boundary and nothing else. macOS arm64 on AC power, 200 000 messages of 256 B,
batches of 500, `max_in_flight: 64`, connect v4.110.0; best of five, and the
second column of costs is an independent repeat of the whole run:

| scenario | native | bridged | cost | repeat |
| :--- | ---: | ---: | ---: | ---: |
| `generate` → mq-bridge | 1 976 285 msg/s | 1 826 117 msg/s | 1.08× | 1.07× |
| `file` → mq-bridge | 238 851 msg/s | 199 004 msg/s | 1.20× | 1.19× |
| mq-bridge → `drop` | 1 976 285 msg/s | 3 664 413 msg/s | 0.54× | 0.56× |
| mq-bridge → `file` | 171 996 msg/s | 182 165 msg/s | 0.94× | 1.02× |

Absolute numbers track the machine, so read the `cost` column, not the first
two.

**The plugin cannot be faster than Redpanda Connect.** It is Redpanda Connect,
plus a boundary. The rows under 1.00× are not a win: their baseline fabricates
every message with a Bloblang mapping, which the publisher is instead handed for
free. What those rows show is that the publisher boundary disappears into the
noise, not that anything got faster.

**The `file` row is a tuning artefact, not a boundary cost.** `file` emits one
message per batch, and `max_in_flight` counts batches, so 64 there means 64
messages in flight rather than 32 000 — and every one of those round-trips
through mq-bridge before the source may refill. Raising it to 512 turns that row
into **0.95×** (252 727 bridged against 239 047 native) and leaves the others
where they are. Any source that does not batch wants a far higher
`max_in_flight` than one that does; the cost is memory, since a parked message
is a resident message.

Timings need a quiet machine, but allocation is deterministic and says the same
thing. Per message, measured in Go alone with `go test -bench`
([`stream_bench_test.go`](go-bridge/stream_bench_test.go)), without cgo or
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
change through `service.BatchError` — see [Semantics](#semantics). And
[`collect`](go-bridge/stream.go) hands a partly filled batch over as soon as every
in-flight slot is parked, instead of waiting out `batchLinger` for messages that
cannot arrive until mq-bridge commits; that is worth **14×** to any source
emitting one message per batch, `file` among them.
`TestASourceOfSingleMessageBatchesDoesNotWaitOutTheLinger` holds the line.

## Known issues

### The Go runtime is loaded once and never unloaded

Go does not support `dlclose` of a `c-shared` library. Dropping and reloading
one aborts the process with `fatal error: morestack on g0` — measured at 27
failures in 120 runs before this was fixed.

[`GoLibrary`](src/go_library.rs) therefore wraps its handle in `ManuallyDrop`:
the mapping stays resident for the life of the process, and dropping a
`GoLibrary` releases only the Rust-side handle. `mq-bridge` already retains
loaded plugins, so this matches host behaviour rather than fighting it. The
smoke test asserts the contract — it loads eight handles, keeps them alive,
re-probes them, drops them all at once, and reopens — and passes 150/150.

The cost is honest: **memory is never reclaimed**, and a process that loads the
plugin keeps the Go runtime until it exits.

Related: [golang/go#65050](https://github.com/golang/go/issues/65050) reports
corruption with multiple Go `c-shared` runtimes on macOS. Until that is
understood, allow only **one** Go-runtime plugin per process.

### One broker connector has been run

The data path is exercised by [`tests/data_path.rs`](tests/data_path.rs)
(`socket_server` / `socket` over loopback TCP) and by the Go tests in
[`go-bridge/stream_test.go`](go-bridge/stream_test.go), which drive
acknowledgement through the same code the ABI calls. Beyond that only
`beanstalkd` has faced a real broker, through the conformance suite below.
Nothing has been run against Kafka, MQTT or S3, and connector-specific
behaviour — authentication, partitioning, redelivery timing — is unverified for
every connector but that one.

`mq_bridge::plugin::conformance` is the acceptance gate, and it passes as
[`tests/conformance.rs`](tests/conformance.rs). It shares one configuration
between input and output, so it needs a connector whose two directions take the
same fields: `beanstalkd` takes only `address` and genuinely redelivers what a
consumer rejects. Three of the four checks apply — `metadata_preserved` does
not, because a beanstalkd job carries no metadata to preserve. It runs in CI
against a broker container, and skips locally unless
`MQ_BRIDGE_REDPANDA_BEANSTALKD` is set.

Deployment also requires **two files in the same directory**. The Rust plugin
resolves its sibling from its own absolute path, so this is robust, but it does
mean the artifact is not a single file.

### Crash isolation

Every in-process option shares fate with the host. Deferred `recover` contains
ordinary panics only; fatal Go runtime errors, native crashes and OOM remain
process-fatal for `mq-bridge` itself. Only a subprocess deployment of Redpanda
Connect gives real isolation.

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
* **Whole-batch output results.** Redpanda `BatchError` can report partial
  success; plugin ABI v1 reports a publish batch all-or-nothing. A publish call
  returns only once Benthos has delivered the whole batch, so success means
  delivered rather than queued — but retrying can duplicate already-written items.
* **Bytes and string metadata only.** Redpanda's `any`-typed metadata is
  coerced to strings; Go message contexts are not translated.
* **Secrets must be indirect.** Custom endpoint config is not covered by
  mq-bridge's secret extractor, and form B makes inline secrets tempting. Use
  environment or file references.

Bloblang and processors **are** supported, through form B's `pipeline`. Explicit
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

### Third-party licenses

The distributed artifact is **not** purely MIT/Apache-2.0. Every linked Redpanda
Connect component is Apache-2.0 — that is what the allowlist guarantees — but ten
of the 353 linked Go modules are weak copyleft. None is GPL, LGPL or AGPL, so
nothing obliges you to open your own source.

Nine are MPL-2.0, which is file-level copyleft: distributing a binary that
includes them requires making the source of *those files* available to
recipients, under MPL-2.0, and saying where. The unmodified upstream sources at
the pinned versions satisfy this.

| Module | Reached through |
| :--- | :--- |
| `hashicorp/golang-lru/v2` | `pure` → Benthos base |
| `hashicorp/golang-lru/arc/v2` | `pure` → Benthos base |
| `go-sql-driver/mysql` | `sql` |
| `hashicorp/go-retryablehttp` | `sql` → `databricks-sql-go` |
| `hashicorp/go-cleanhttp` | `sql` → `databricks-sql-go` |
| `hashicorp/go-uuid` | `sql` → `trino-go-client` → `gokrb5` |
| `certifi/gocertifi` | `spicedb` → `authzed/grpcutil` |
| `r3labs/diff/v3` | `changelog` |
| `cyphar/filepath-securejoin` | `git` → `go-git` |

The tenth is `github.com/eclipse/paho.mqtt.golang`, behind the `mqtt` connector,
offered under either EPL-2.0 or EDL-1.0. This distribution relies on the EDL-1.0
arm, a BSD-3-Clause equivalent carrying no copyleft obligation.

**The MPL-2.0 obligation cannot be trimmed away.** `golang-lru` arrives through
`public/components/pure`, which Benthos requires as its base component set.
Dropping `sql`, `mqtt`, `spicedb`, `changelog` and `git` would clear the other
eight, at the cost of five connector families, and still leave it.

The Rust side is entirely permissive: MIT/Apache-2.0 throughout, plus
`libloading` (ISC), `memchr` (Unlicense OR MIT) and `unicode-ident`
(`(MIT OR Apache-2.0) AND Unicode-3.0` — attribution, not copyleft).

Counts come from `THIRD_PARTY_NOTICES`; the paths were traced with
`go mod why -m <module>` from [`go-bridge/`](go-bridge/).
[`scripts/gen-third-party-notices.py`](scripts/gen-third-party-notices.py) fails
the build on an unrecognised license or on a bare EPL-2.0. MPL-2.0 passes
deliberately, because the notice documents its obligation rather than hiding it.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
