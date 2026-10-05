# Components and licensing

Components are linked from an explicit allowlist,
[`go-bridge/components.allow`](../go-bridge/components.allow) — 52
`public/components` packages of Redpanda Connect, out of the 79 that exist, and
19 implementation packages linked [without their public
wrapper](#implementation-packages-linked-directly). Every Redpanda Connect file
they reach carries the Apache-2.0 header. The allowlist is
data: [`scripts/gen-components.py`](../scripts/gen-components.py) turns it into
`go-bridge/internal/components` and `go-bridge/unwrapped`, which both the bridge
and the catalog tool import, so they cannot drift apart.

Enumerating the registered components in the built library gives:

| Registered | Count |
| :--- | ---: |
| Inputs | 56 |
| Outputs | 68 |
| Processors | 86 |

Reproduce with `go run ./internal/catalogtool` from [`go-bridge/`](../go-bridge/),
which walks the Benthos global environment of the library as built.

`tigerbeetle` is RCL-free and is excluded on every platform:
it ships prebuilt static archives, and neither can go into a `c-shared` object.
The macOS one has members that are not 8-byte aligned, which only the deprecated
`-ld_classic` accepts. The Linux one carries `R_X86_64_TPOFF32` initial-exec TLS
relocations, which a shared object cannot hold — `ld` says "recompile with
`-fPIC`", and since the archive is prebuilt, we cannot.

The taint is transitive and cannot be guessed from a package name: `snowflake`,
`kafka` and `redpanda` are all excluded, while `gcp` is included because
its RCL code is confined to a `gcp/enterprise` subpackage that
`components/gcp` never imports. Two upstream files carry most of the blast
radius — `internal/serviceaccount/oauth2.go` and `internal/license`.

That the boundary holds is checked rather than asserted:
[`scripts/check-rcl.py`](../scripts/check-rcl.py) asks the Go toolchain for the
package closure the compiler actually compiles and reads every file in it. It
fails on an RCL header anywhere, and on any Redpanda Connect file that does not
carry the Apache-2.0 header — so a third licence, or no header at all, fails
too. It runs in CI, so an upstream release that taints a package we link is
caught at the next push rather than after shipping. At connect v4.110.0 it
reads 21,686 linked files and passes.

The second rule is why `confluent` is not linked. Its Schema Registry code
imports `internal/impl/protobuf/common`, where four compiled files carry a
Business Source License header instead. The `protobuf` processor is out for the
same reason.

## Implementation packages linked directly

Upstream keeps each connector in `internal/impl/<name>` and exposes it through a
one-line `public/components/<name>/package.go` that imports it. For some
connectors the implementation is Apache-2.0 throughout and only the way in is
not:

| Connector | Why the public wrapper is not used |
| :--- | :--- |
| `openai`, `ollama`, `cohere` | the wrapper file itself carries the RCL header |
| `aws` | the wrapper also imports `dynamodb`, `kafka/aws`, `mysql/aws` and `postgresql/aws`, which reach RCL code |
| `pure/extended` | the wrapper also imports `protobuf`, which reaches the BSL files above |
| `timeplus` | the wrapper file carries no licence header at all |

For these the wrapper is never imported. [`go-bridge/unwrapped`](../go-bridge/unwrapped/)
is a file of our own that imports the Apache-2.0 implementation packages
instead: `openai`, `ollama`, `cohere`, `timeplus`, `awk`, `html`, `jsonpath`,
`lang`, `parquet`, `xml`, the AWS `bedrock`, `cloudwatch`, `kinesis`, `lambda`,
`s3`, `sns` and `sqs` packages, and the AWS credential hooks for `mongodb` and
`opensearch`. AWS `dynamodb` stays out: five of its eight files are RCL.

Go only lets code inside the upstream module path import its `internal`
packages, so `go-bridge/unwrapped` is a nested module named
`github.com/redpanda-data/connect/v4/mqbridge/unwrapped`, mapped to the local
directory by a `replace` in `go-bridge/go.mod`. That is a toolchain visibility
rule rather than a licence term, and upstream can change the layout in any
release; the check above is what would catch a newly tainted file.

The aggregate `public/components/all` is RCL-tainted, so the "everything" bundle
is not usable under Apache-2.0 terms. `public/bundle/free` does not exist in the
published module; it is generated at build time and cannot be imported.

The allowlist links several hundred third-party Go modules, none carrying an RCL
header. [`THIRD_PARTY_NOTICES`](../THIRD_PARTY_NOTICES) has the exact counts, the
per-license breakdown and the non-recursive verification command; it is
generated from what the build actually links, so it is the only place those
numbers are maintained. RCL-free is not the same as permissive: see
[Third-party licenses](#third-party-licenses).

## Third-party licenses

The distributed artifact is **not** purely MIT/Apache-2.0. Every linked Redpanda
Connect component is Apache-2.0 — that is what the allowlist guarantees — but
eleven linked Go modules are weak copyleft. None is GPL, LGPL or AGPL, so nothing
obliges you to license a larger work that includes the binaries under copyleft
terms. The MPL-2.0 and Apache-2.0 patent grants do end for anyone who brings
certain patent litigation over the covered code. See `THIRD_PARTY_NOTICES` for
the counts.

Ten are MPL-2.0, which is file-level copyleft: distributing a binary that
includes them requires making the source of *those files* available to
recipients, under MPL-2.0, and saying where. They are linked unmodified at the
pinned versions, and `THIRD_PARTY_NOTICES` names a version-exact Go module proxy
URL for each, which is what discharges the obligation.

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
| `gosimple/slug` | `lang` |

The eleventh is `github.com/eclipse/paho.mqtt.golang`, behind the `mqtt` connector,
offered under either EPL-2.0 or EDL-1.0. This distribution relies on the EDL-1.0
arm, a BSD-3-Clause equivalent carrying no copyleft obligation.

**The MPL-2.0 obligation cannot be trimmed away.** `golang-lru` arrives through
`public/components/pure`, which Benthos requires as its base component set.
Dropping `sql`, `mqtt`, `spicedb`, `changelog`, `git` and `lang` would clear the
other nine, at the cost of six connector families, and still leave it.

The Rust side is entirely permissive: MIT/Apache-2.0 throughout, plus ISC,
Unlicense OR MIT, and the ICU crates' Unicode-3.0 — attribution, not copyleft.

The linked set differs per platform, so the notices are the union across every
released platform and mark any module that is not linked on all of them.

Counts and licenses come from `THIRD_PARTY_NOTICES`; the paths were traced with
`go mod why -m <module>` from [`go-bridge/`](../go-bridge/).
[`scripts/gen-third-party-notices.py`](../scripts/gen-third-party-notices.py) fails
the build on an unrecognised license or on a bare EPL-2.0. MPL-2.0 passes
deliberately, because the notice documents its obligation rather than hiding it.
