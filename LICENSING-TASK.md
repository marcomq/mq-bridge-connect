# Task: close the license-compliance gaps in the distributed artifact

This repo's licensing **documentation** is in good shape. What is missing is the
part that travels with the binary. `THIRD_PARTY_NOTICES` describes obligations
accurately but does not discharge several of them, and nothing in CI or in
packaging enforces or ships it.

Work through the findings below in order; 1–3 are blocking for any public
release, 4–5 are hygiene. Each names the evidence so you can re-verify rather
than trust this file — regenerate and re-read before acting, since the counts
and module lists move with every upstream bump.

Two things to hold on to, because they are settled and re-litigating them wastes
the session:

- **The audit unit is the built artifact**, not `go.mod` or `Cargo.toml`. Build
  tags and Cargo features change the linked set. Everything below is about what
  `go list -deps` over `go-bridge/` and `cargo metadata` actually link at the
  pinned versions.
- **The MPL-2.0 obligation cannot be trimmed away and no one should try.**
  `hashicorp/golang-lru` arrives through `public/components/pure`, which Benthos
  requires as its base component set. Dropping `sql`, `mqtt`, `spicedb`,
  `changelog` and `git` would clear eight of the nine MPL modules at the cost of
  five connector families and still leave `golang-lru`. The goal is to *comply*
  cleanly, not to reach a permissive-only artifact.

None of this is legal advice. It is the engineering work needed before counsel
can sign anything off.

---

## 1. Permissive license texts are not distributed (blocking)

`THIRD_PARTY_NOTICES`, under "Full license texts":

> For BSD-2-Clause, BSD-3-Clause, ISC, MPL-2.0, EPL-2.0/EDL-1.0 and
> Unicode-3.0, and for the exact copyright holders of every entry, see the
> LICENSE file inside each dependency's own source tree; the Go module cache
> (`go env GOMODCACHE`) and the Cargo registry checkout retain them verbatim.

A recipient of the two shared libraries has no module cache and no registry
checkout. MIT, BSD-2-Clause, BSD-3-Clause and ISC each require the copyright
notice and the permission text to *accompany* the distribution — pointing at
where the builder could find them does not satisfy that. Part A's own counts put
178 modules in this group (107 MIT, 50 BSD-3-Clause, 19 BSD-2-Clause, 2 ISC),
plus the Rust side in Part B.

**Fix:** make `scripts/gen-third-party-notices.py` embed the texts so the file is
self-contained.

- Read each dependency's `LICENSE`/`COPYING`/`LICENCE` from the module cache and
  the Cargo registry checkout at generation time. A module with no license file
  found is a generation failure, not a silent omission — that is exactly the case
  a human must look at.
- Most texts are byte-identical apart from the copyright line. Group them: emit
  each distinct text once in an appendix, and give each module entry its own
  copyright line plus a pointer to the appendix entry. That keeps the file
  reviewable instead of a 200-copy MIT scroll, while still shipping every text.
- Keep the per-module copyright line verbatim from upstream. Several entries in
  the current file already carry one (`hashicorp/go-uuid`, `cyphar/filepath-securejoin`);
  most do not, and the ones that do not are the compliance gap.

Verify: the emitted file, read on a machine with no Go or Rust toolchain, should
answer "what am I allowed to do with this binary" without following a single
path.

## 2. MPL-2.0 source availability names no location (blocking)

`THIRD_PARTY_NOTICES`, "Mozilla Public License 2.0 obligations":

> distributing a binary that includes them requires making the source of *those
> files* available to recipients, under MPL-2.0, and saying where. The
> unmodified upstream sources at the pinned versions satisfy this.

The obligation is stated correctly and then not discharged: the file never says
*where*. MPL-2.0 §3.2 requires informing recipients how to obtain the Source
Code Form, and relying on "it is on the internet somewhere" is the thing that
sentence is meant to rule out.

**Fix:** emit a resolvable URL per MPL-2.0 module at the exact pinned version,
and state plainly that this is where the Source Code Form is available. The nine
modules, as currently linked:

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

The Go module proxy gives an immutable, version-exact source zip
(`https://proxy.golang.org/<escaped module path>/@v/<version>.zip`), which is a
better citation than a VCS tag that can move. Generate it, do not hand-write it:
the module set changes with every upstream bump.

Also add the standing sentence that a fork or patch of any of these obliges the
distributor to publish the modified files — the current text has it, keep it.

State the `paho.mqtt.golang` election as it stands (EDL-1.0 arm, a BSD-3-Clause
equivalent) and, per finding 1, ship the EDL-1.0 text. That election itself is
sound; only the missing text is a gap.

## 3. Nothing ships the notices file with the artifact (blocking)

`.github/workflows/` contains only `ci.yml`. There is no release or packaging
workflow, so there is no step that places `THIRD_PARTY_NOTICES`, `LICENSE-MIT`
and `LICENSE-APACHE` next to the two libraries. The README's deployment section
tells users to put **two files** in a directory — which is precisely a
distribution with no notices attached.

**Fix:** whatever produces a downloadable artifact must package the notices with
it. That includes every channel:

- a release archive: the two libraries plus `THIRD_PARTY_NOTICES`,
  `LICENSE-MIT`, `LICENSE-APACHE`
- a Homebrew formula or conda package: install the notices alongside the
  library, not only into the source tarball
- the README's deployment instructions: say the notices file travels with the
  libraries, so a hand-copied deployment stays compliant

mq-bridge now resolves a plugin by the endpoint name a route asks for — it looks
for `libmq_bridge_redpanda.{so,dylib}` / `mq_bridge_redpanda.dll` on a search
path covering `MQB_PLUGIN_DIR`, the running binary's prefix, `$CONDA_PREFIX`,
`$HOMEBREW_PREFIX` and `~/.local/share/mq-bridge/plugins`, under both
`lib/mq-bridge` and plain `lib`. That makes a packaged install the expected way
in — so the package is the place the notices have to land, and a formula that
installs only the two libraries is the failure mode to design against. See
`docs/PLUGINS.md` in the mq-bridge repo.

## 4. No staleness gate on the generated notices (hygiene)

`ci.yml:37` runs `python3 scripts/check-rcl.py`, so an upstream release that
taints the allowlist with a Redpanda Community License header fails the build.
Nothing runs `scripts/gen-third-party-notices.py`, so the compliance artifact
itself can drift silently — a `go.sum` bump, a `components.allow` edit or a
`Cargo.lock` change can add a module, or a copyleft one, with the committed file
still describing the previous build.

**Fix:** a CI job that regenerates the file and fails on a diff, next to the RCL
check. It has to be a diff gate rather than a regenerate-and-commit step: the
point is that a human reads a new license before it ships.

Note that this gate is what makes findings 1 and 2 hold over time. Without it,
the embedded texts and MPL URLs are correct exactly once.

## 5. The module count disagrees with itself (hygiene)

`README.md` says "ten of the 353 linked Go modules are weak copyleft" in the
third-party section, and "The allowlist links **351 third-party Go modules**"
about thirty lines earlier. `THIRD_PARTY_NOTICES` Part A says 351, and its
per-license breakdown sums to 351 (163 + 107 + 50 + 19 + 9 + 2 + 1).

**Fix:** 353 is the stale one. Better than correcting it by hand: have the README
cite `THIRD_PARTY_NOTICES` for counts instead of repeating them, so there is one
number in one generated place. Two hand-maintained copies of a generated figure
will diverge again on the next bump.

---

## Verification

Before calling this done:

```console
python3 scripts/check-rcl.py                      # still clean
python3 scripts/gen-third-party-notices.py        # regenerates
git diff --exit-code THIRD_PARTY_NOTICES          # gate passes on a fresh run
```

Then read the regenerated `THIRD_PARTY_NOTICES` start to finish as a recipient
would, with no toolchain and no network, and check that it answers:

- what every linked module is, at what version, under what license
- the full text of each of those licenses
- where to obtain the Source Code Form for each MPL-2.0 module
- which arm of `paho.mqtt.golang`'s dual license this distribution elects

And confirm the file is present in every artifact a user can download.
