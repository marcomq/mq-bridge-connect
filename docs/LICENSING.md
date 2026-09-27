# Components and licensing

Components are linked from an explicit allowlist,
[`go-bridge/components.allow`](../go-bridge/components.allow) — the 54
`public/components` packages of Redpanda Connect that reach no file carrying a
Redpanda Community License header and can be linked into a shared library, out
of the 79 that exist. The allowlist is
data: [`scripts/gen-components.py`](../scripts/gen-components.py) turns it into
`go-bridge/internal/components`, which both the bridge and the catalog tool
import, so they cannot drift apart.

Enumerating the registered components in the built library gives:

| Registered | Count |
| :--- | ---: |
| Inputs | 51 |
| Outputs | 63 |
| Processors | 68 |

Reproduce with `go run ./internal/catalogtool` from [`go-bridge/`](../go-bridge/),
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
[`scripts/check-rcl.py`](../scripts/check-rcl.py) asks the Go toolchain for the
package closure the compiler actually compiles, reads every file in it, and
fails on an RCL header. It runs in CI, so an upstream release that taints a
package we link is caught at the next push rather than after shipping. At
connect v4.110.0 it reads 20,741 linked files and finds none; the same scan of
the `all` bundle finds 193.

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
Connect component is Apache-2.0 — that is what the allowlist guarantees — but ten
linked Go modules are weak copyleft. None is GPL, LGPL or AGPL, so nothing
obliges you to license a larger work that includes the binaries under copyleft
terms. The MPL-2.0 and Apache-2.0 patent grants do end for anyone who brings
certain patent litigation over the covered code. See `THIRD_PARTY_NOTICES` for
the counts.

Nine are MPL-2.0, which is file-level copyleft: distributing a binary that
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

The tenth is `github.com/eclipse/paho.mqtt.golang`, behind the `mqtt` connector,
offered under either EPL-2.0 or EDL-1.0. This distribution relies on the EDL-1.0
arm, a BSD-3-Clause equivalent carrying no copyleft obligation.

**The MPL-2.0 obligation cannot be trimmed away.** `golang-lru` arrives through
`public/components/pure`, which Benthos requires as its base component set.
Dropping `sql`, `mqtt`, `spicedb`, `changelog` and `git` would clear the other
eight, at the cost of five connector families, and still leave it.

The Rust side is entirely permissive: MIT/Apache-2.0 throughout, plus ISC,
Unlicense OR MIT, and the ICU crates' Unicode-3.0 — attribution, not copyleft.

The linked set differs per platform, so the notices are the union across every
released platform and mark any module that is not linked on all of them.

Counts and licenses come from `THIRD_PARTY_NOTICES`; the paths were traced with
`go mod why -m <module>` from [`go-bridge/`](../go-bridge/).
[`scripts/gen-third-party-notices.py`](../scripts/gen-third-party-notices.py) fails
the build on an unrecognised license or on a bare EPL-2.0. MPL-2.0 passes
deliberately, because the notice documents its obligation rather than hiding it.
