# Development

## Architecture

```text
mq-bridge
  └─ native plugin ABI 1.1
      └─ Rust cdylib: libmq_bridge_connect
          ├─ mq-bridge plugin SDK (runtime, handles, panic boundary)
          ├─ CanonicalMessage / disposition translation
          └─ private batch C ABI, resolved at runtime
              └─ Go c-shared sibling: libmq_bridge_connect_go
                  ├─ Benthos StreamBuilder, one stream per endpoint
                  ├─ batch parking with per-message dispositions
                  └─ curated Redpanda component imports
```

Two artifacts ship together in one directory. The Rust plugin locates the Go
sibling **relative to its own absolute path** ([`src/sibling.rs`](../src/sibling.rs)),
never via the working directory or the system library search path.
`MQ_BRIDGE_CONNECT_GO_LIBRARY` overrides that with an absolute path, for tests.
A crates.io build falls back to the copy its build script fetched
([Install](../README.md#install)).

The private Rust↔Go ABI ([`go-bridge/bridge.h`](../go-bridge/bridge.h)) is versioned
independently of mq-bridge's public plugin ABI: a `struct_size` plus
major/minor pair, C scalar types only, explicit ownership, and one call per
*batch* — never per message. A batch crosses as one length-prefixed blob
([`src/wire.rs`](../src/wire.rs), [`go-bridge/wire.go`](../go-bridge/wire.go)): one
allocation and one free per crossing, and no base64 of binary payloads.

## Why a separate Go library

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
| `mq-bridge` | `0.4.13` from crates.io (plugin ABI 1.1) |

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
[`phase0_smoke`](../src/bin/phase0_smoke.rs), which:

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

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs all of it on every
push: `gofmt`, `go vet` and `go test -race`; `cargo fmt`, `cargo clippy -D
warnings` and the smoke script on both Linux and macOS; and the conformance
suite against beanstalkd, NATS JetStream and Redis.

Each connector's test skips when its broker is absent, so you can run one
without the others. NATS and Redis come from the host repository's compose
files, which CI reuses rather than duplicating, so the broker versions stay in
step with the ones mq-bridge itself tests against:

```sh
docker run --rm -d -p 11300:11300 schickling/beanstalkd
docker compose -f ../mq-bridge/tests/integration/docker-compose/nats.yml up -d --wait
docker compose -f ../mq-bridge/tests/integration/docker-compose/redis.yml up -d --wait

MQ_BRIDGE_CONNECT_BEANSTALKD=127.0.0.1:11300 \
MQ_BRIDGE_CONNECT_NATS=127.0.0.1:4222 \
MQ_BRIDGE_CONNECT_REDIS=127.0.0.1:6379 \
    cargo test --test conformance -- --nocapture
```

The JetStream test takes about 30s: an uncommitted batch only returns once
`ack_wait` expires, and that is an input-only field the shared config cannot
shorten.

## Versioning

`Cargo.toml` is the source of truth for the package version. Update every
ecosystem manifest together before tagging a release:

```console
python3 scripts/set_version.py 0.1.1
```

`python3 scripts/set_version.py --check` verifies that the Cargo, npm and Python
versions match. The [release workflow](../.github/workflows/release.yml) runs that
check and also requires the tag to match the version.

## Running the examples

The Rust examples run as ordinary binaries, so they do not sit next to the Go
sibling the way the cdylib does. Point at it explicitly:

```sh
cargo build --lib
(cd go-bridge && go build -buildmode=c-shared \
    -o ../target/debug/libmq_bridge_connect_go.dylib .)   # .so on Linux

MQ_BRIDGE_CONNECT_GO_LIBRARY=$PWD/target/debug/libmq_bridge_connect_go.dylib \
    cargo run --example quickstart
```

`mqb` and Python load the plugin instead, which finds the Go sibling beside it:

```sh
MQB_PLUGIN_DIR=$PWD/target/debug mqb --config examples/mqb-route.yaml
python examples/python_route.py
```

The build uses the Go toolchain's own caches (`go env GOCACHE`, `GOMODCACHE`).
They are shared with every other Go project. `GOCACHE` periodically removes
build data that has gone unused; `GOMODCACHE` is never pruned automatically. An
earlier revision pointed them under `target/`, which gave each checkout a
private ~26 GB copy that nothing ever reclaimed. `go clean -cache -modcache`
frees them.
