# mq-bridge-redpanda

[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
![Status](https://img.shields.io/badge/status-prototype%20(phase%200)-orange)

A Redpanda Connect **connector** compatibility plugin for
[`mq-bridge`](https://github.com/marcomq/mq-bridge).

The goal is to reuse Redpanda Connect's connector implementations as `mq-bridge`
endpoints **without** running a Redpanda Connect pipeline. `mq-bridge` stays the
engine — routing, batching, middleware, retries, DLQ and transformations remain
its job. Redpanda supplies only the I/O components.

> ## ⚠️ Status: prototype only — do not deploy
>
> This repository is at **Phase 0** (release-gate spike). It builds a Rust plugin
> and a Go sibling library, loads them, and proves the Benthos `ResourceBuilder`
> API is reachable across the FFI boundary. **It moves no messages yet**: there is
> no consumer, no publisher, and no connector configuration.
>
> The Phase 0 gate is **green**: the smoke test passes, including 150 consecutive
> runs of the load/probe/panic-recovery cycle without a single abort.

## Architecture

```text
mq-bridge
  └─ native plugin ABI 1.0
      └─ Rust cdylib: libmq_bridge_redpanda
          ├─ mq-bridge plugin SDK (runtime, handles, panic boundary)
          ├─ CanonicalMessage / disposition translation   (not yet implemented)
          └─ private batch C ABI, resolved at runtime
              └─ Go c-shared sibling: libmq_bridge_redpanda_go
                  ├─ Benthos ResourceBuilder / Resources
                  └─ curated Redpanda component imports
```

Two artifacts ship together in one directory. The Rust plugin locates the Go
sibling **relative to its own absolute path** ([`src/sibling.rs`](src/sibling.rs)),
never via the working directory or the system library search path.
`MQ_BRIDGE_REDPANDA_GO_LIBRARY` overrides that with an absolute path, for tests.

The private Rust↔Go ABI ([`go-bridge/bridge.h`](go-bridge/bridge.h)) is versioned
independently of mq-bridge's public plugin ABI: a `struct_size` plus
major/minor pair, C scalar types only, explicit ownership, and one call per
*batch* — never per message.

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
`target/<profile>/` directory, then runs [`phase0_smoke`](src/bin/phase0_smoke.rs),
which:

1. opens the Go library, builds and closes an empty Benthos resource manager;
2. asserts an ordinary Go panic is recovered at the export boundary and
   surfaced as a status plus a diagnostic string, not a process abort;
3. loads the Rust cdylib through `mq_bridge::plugin::load_endpoint_plugin` and
   checks the advertised endpoint name and capabilities.

Go build and module caches are redirected under `target/` so the repository
stays self-contained.

## Curated components

Only an explicit allowlist is imported ([`go-bridge/main.go`](go-bridge/main.go)):
`beanstalkd`, `elasticsearch/v8`, and `pure`. Enumerating the registered
components in the built library gives:

| Registered | Count | Of which are real connectors |
| :--- | ---: | :--- |
| Inputs | 8 | **1** — `beanstalkd` |
| Outputs | 14 | **2** — `beanstalkd`, `elasticsearch_v8` |
| Processors | 41 | 0 (not exposed; see non-goals) |

Reproduce with `go run ./internal/catalogtool` from
[`go-bridge/`](go-bridge/internal/catalogtool/main.go), which walks the Benthos
global environment of the library as built.

So the current build exposes **two connectors**. Everything else is Benthos
pipeline plumbing pulled in by `pure` — `broker`, `switch`, `retry`, `fallback`,
`resource`, `drop_on` and friends. That plumbing duplicates work `mq-bridge`
already owns, and [non-goals](#planned-semantics-not-yet-implemented) say not to
expose it, so a later phase must filter the catalogue rather than surface
whatever happens to be registered. `pure` itself cannot simply be dropped:
Benthos requires it as the base component set.

For scale, `connect/v4` ships **78** component packages under
`public/components/`, which is the practical upper bound on what this approach
could reach. Getting there is a licensing and dependency exercise, not a
technical one.

The aggregate `public/bundle/free` is deliberately **not** imported. It pulls
453 module requirements at the pinned snapshot, and some packages reached
through `public/components/community` (`cohere`, `openai`, `ollama`) carry
Redpanda Community/enterprise license headers over Apache-2.0 implementation
files. That contradiction has to be resolved with upstream before anything
broad is redistributed.

The allowlist keeps the linked set to **63 third-party Go modules**, none of
which carry an RCL header — see [`THIRD_PARTY_NOTICES`](THIRD_PARTY_NOTICES),
which is generated from what the build actually links.

## Performance

Measured on macOS arm64, release build. Phase 0 moves no messages, so these are
the fixed costs of the approach, not throughput.

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

**Size dominates.** The Go sibling is 27× the Rust plugin, for two connectors.
It is nearly all Redpanda Connect and its transitive dependencies, and it grows
with the allowlist, not with usage.

**Cold start is disk-bound.** ~1.4 s on first load versus ~4.7 ms warm is the
cost of faulting in a 45 MiB image. It is paid once per machine boot, but it is
paid during `mq-bridge` startup.

**Cross the boundary per batch, never per message.** ~300 ns steady-state is
cheap against real network I/O but not against nothing. The sharper edge is the
first call from an OS thread the Go runtime has not seen: 10–50× the steady
cost. `mq-bridge`'s tokio worker pool is fixed, so that amortises — but code
that calls into Go from `spawn_blocking` threads would pay it repeatedly. This
is why the private ABI is defined per batch.

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

### Usability: two connectors, no data path

Configuring a `redpanda` endpoint today fails with an explicit Phase 0
diagnostic. The plugin advertises both directions to pin the ABI shape, which
would otherwise contradict the generic "does not support" error `mq-bridge`
returns by default.

Deployment also requires **two files in the same directory**. The Rust plugin
resolves its sibling from its own absolute path, so this is robust, but it does
mean the artifact is not a single file.

### Crash isolation

Every in-process option shares fate with the host. Deferred `recover` contains
ordinary panics only; fatal Go runtime errors, native crashes and OOM remain
process-fatal for `mq-bridge` itself. Only a subprocess deployment of Redpanda
Connect gives real isolation.

## Planned semantics (not yet implemented)

These are the contracts the vertical slice will have to honour, recorded here so
they are not discovered late:

* **At-least-once, with duplicates.** Benthos `AckFunc` acknowledges a whole
  batch; `mq-bridge` records a disposition per message. Any `Nack` must reject
  the entire source batch, so successful siblings can be redelivered. Use
  idempotent destinations.
* **Whole-batch output results.** Redpanda `BatchError` can report partial
  success; plugin ABI v1 reports a publish batch all-or-nothing. Retrying can
  duplicate already-written items.
* **Bytes and string metadata only.** Structured values and Go message contexts
  are not translated; Redpanda's `any`-typed metadata is coerced to strings.
* **Secrets must be indirect.** Custom endpoint config is not currently covered
  by mq-bridge's secret extractor, so use environment or file references —
  never inline secrets.

Explicit non-goals: Bloblang, processors, buffers, streams, the HTTP management
API, per-connector DLLs, and any claim of exactly-once or zero-copy behaviour.

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

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
