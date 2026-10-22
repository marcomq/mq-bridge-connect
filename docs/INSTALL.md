# Installing mq-bridge-connect

The short version is in the [README](../README.md#install).

The plugin is a pair of shared libraries, not a program. You need:

1. **mq-bridge** itself: the `mqb` command line tool, or the `mq-bridge`
   library for Python, Node.js or Rust.
2. **This plugin**: two libraries that must sit in the same directory.

   | Platform | Plugin (loaded by mq-bridge) | Go sibling (loaded by the plugin) |
   | :--- | :--- | :--- |
   | macOS | `libmq_bridge_connect.dylib` | `libmq_bridge_connect_go.dylib` |
   | Linux | `libmq_bridge_connect.so` | `libmq_bridge_connect_go.so` |
   | Windows | `mq_bridge_connect.dll` | `mq_bridge_connect_go.dll` |

   `THIRD_PARTY_NOTICES` belongs with them; every package below installs it.

Prebuilt for:

| Platform | Homebrew | Conda | pip / npm | Archive target |
| :--- | :--- | :--- | :--- | :--- |
| macOS Apple Silicon (arm64) | yes | yes | yes | `aarch64-apple-darwin` |
| Linux x86_64 (glibc ≥ 2.35) | yes | yes | yes | `x86_64-unknown-linux-gnu` |
| Linux arm64 (glibc ≥ 2.35) | yes | yes | yes | `aarch64-unknown-linux-gnu` |
| Windows x64 | – | yes | yes | `x86_64-pc-windows-msvc` |

There is no build for Intel macOS or musl (Alpine); build it yourself, see
[docs/DEVELOPMENT.md](DEVELOPMENT.md). Check the glibc version with
`ldd --version`.

## Step 1: install `mqb`

Skip this if you only use the library from Python, Node.js or Rust.

```sh
brew install marcomq/tap/mq-bridge-app                    # macOS arm64, Linux
conda install -c marcomq -c conda-forge mq-bridge-app     # Windows, macOS arm64, Linux x86_64
cargo binstall mq-bridge-app                              # prebuilt binary via cargo
```

The plugin's Homebrew and conda packages do **not** pull in `mqb`; install it
separately. Check with `mqb --version`. Other ways to install are in
mq-bridge's [install guide](https://marcomq.github.io/mq-bridge/INSTALL.html).

## Step 2: install the plugin

Pick **one** of the following. Homebrew, conda and the archive put the
libraries where `mqb` searches for plugins, so afterwards a route that names
`connect` just works: no `plugins:` entry and no `--plugin` flag.

**Homebrew** (macOS arm64, Linux):

```sh
brew install marcomq/tap/mq-bridge-connect
```

This installs both libraries into `$(brew --prefix)/lib/mq-bridge/`. Update
later with `brew update && brew upgrade mq-bridge-connect`.

**Conda** (Windows, macOS arm64, Linux), into the same environment as
`mq-bridge-app`:

```sh
conda install -c marcomq -c conda-forge mq-bridge-connect
```

This installs both libraries into `$CONDA_PREFIX/lib/mq-bridge/`, or
`%CONDA_PREFIX%\Library\bin\` on Windows.

**Install script** (macOS arm64, Linux, no package manager). It downloads the
release archive for this machine, checks its sha256 and copies both libraries
and `THIRD_PARTY_NOTICES` into `~/.local/share/mq-bridge/plugins`:

```sh
curl -fsSL https://raw.githubusercontent.com/marcomq/mq-bridge-connect/main/install.sh | sh
curl -fsSL https://raw.githubusercontent.com/marcomq/mq-bridge-connect/main/install.sh | sh -s -- 0.2.1   # a fixed version
```

Set `MQB_CONNECT_DIR` for another directory; then point `MQB_PLUGIN_DIR` at it
as described below. Running the script again updates the plugin in place.
Read [install.sh](../install.sh) first if you prefer, or do the same steps by
hand:

**Release archive** (any supported platform, no package manager). Pick the
latest version from the [releases page](https://github.com/marcomq/mq-bridge-connect/releases)
and your platform's target from the table above:

```sh
VERSION=0.2.1
TARGET=aarch64-apple-darwin
BASE=https://github.com/marcomq/mq-bridge-connect/releases/download/v$VERSION
curl -fLO "$BASE/mq-bridge-connect-$VERSION-$TARGET.tar.gz"
curl -fLO "$BASE/mq-bridge-connect-$VERSION-$TARGET.tar.gz.sha256"
shasum -a 256 -c "mq-bridge-connect-$VERSION-$TARGET.tar.gz.sha256"   # Linux: sha256sum -c
mkdir -p ~/.local/share/mq-bridge/plugins
tar xzf "mq-bridge-connect-$VERSION-$TARGET.tar.gz" --strip-components=1 \
    -C ~/.local/share/mq-bridge/plugins
```

The archive holds one folder; `--strip-components=1` puts its files directly
into the plugin directory, because `mqb` does not search subdirectories.
`~/.local/share/mq-bridge/plugins` (or `$XDG_DATA_HOME/mq-bridge/plugins`) is
always searched. On Windows, extract the archive with the same `tar` command
into `%APPDATA%\mq-bridge\plugins` instead. The directory must belong to you
(or root) and must not be world-writable, or `mqb` refuses the library. A
`mqb` running as root only loads root-owned libraries.

To keep the libraries somewhere else, either point `MQB_PLUGIN_DIR` at their
directory (`:`-separated list, `;` on Windows):

```sh
export MQB_PLUGIN_DIR=/opt/mq-bridge/plugins
```

or name the plugin file in the config, or with `mqb --plugin <path>`. The Go
sibling is still found next to it:

```yaml
plugins:
  - "/opt/mq-bridge/plugins/libmq_bridge_connect.so"
```

The full search order is in mq-bridge's
[plugin docs](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md#installing-a-plugin-by-name).

**Python** (3.9 or newer): the wheel contains both libraries, so nothing else
is needed. Call `register()` once, before the first route that uses `connect`:

```sh
pip install mq-bridge mq-bridge-connect
```

```python
import mq_bridge_connect
mq_bridge_connect.register()   # returns "connect"
```

A complete example is in [python/README.md](../python/README.md).

**Node.js** (18 or newer): the libraries (about 70 MB per platform) are too
large for npm, so `register()` downloads them on first use into a cache
(`~/.cache`, `~/Library/Caches` or `%LOCALAPPDATA%`, under
`mq-bridge-connect/`) and checks the sha256. A Homebrew or conda install is
used instead if present.

```sh
npm install mq-bridge mq-bridge-connect
npx mq-bridge-connect      # optional: download now, e.g. in a Docker build
```

```js
import { register } from "mq-bridge-connect";
register();   // returns "connect"
```

Details and environment variables are in [node/README.md](../node/README.md).

**Rust**: add the crate and register the factory once at startup:

```sh
cargo add mq-bridge mq-bridge-connect
```

```rust
use std::sync::Arc;
use mq_bridge::extensions::register_endpoint_factory;
use mq_bridge_connect::ConnectFactory;

register_endpoint_factory("connect", Arc::new(ConnectFactory::default()))?;
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
`THIRD_PARTY_NOTICES` (in the crate and in every release archive). See
[`examples/quickstart.rs`](../examples/quickstart.rs) and
[`examples/route.rs`](../examples/route.rs).

## Step 3: check that the plugin loads

```sh
mqb --plugin "$(brew --prefix)/lib/mq-bridge/libmq_bridge_connect.dylib" describe connect
```

Use the path the plugin was installed to (`.so` on Linux,
`mq_bridge_connect.dll` on Windows). It prints the endpoint's fields, starting
with `endpoint type   connect (extension)`. An error here means the file is for
another platform or the Go sibling is not next to it. `mqb list` and
`mqb describe connect` without `--plugin` do not show the plugin: discovery
runs only when a route names `connect`, and then logs
`loaded plugin name=connect version=… path=…`.

## Quick start: run a route

No broker needed: the Redpanda Connect `generate` input fabricates the
messages. Create `hello.yaml`:

```yaml
routes:
  hello:
    exit_on_empty: true          # stop once the generator is done
    startup_timeout_ms: 30000    # loading the 218 MiB library can exceed the 5 s default
    input:
      custom:
        name: connect
        config:
          connector: generate
          count: 5
          interval: ""
          mapping: 'root = {"id": counter(), "text": "hello"}'
    output:
      file: { path: "hello.jsonl", format: raw }
```

Start it:

```sh
mqb --config hello.yaml --no-ui
```

The log shows `loaded plugin name=connect …`, then `Every route has drained.`
and `mqb` exits. `hello.jsonl` holds five lines:

```json
{"id":1,"text":"hello"}
```

Relative paths are resolved from the directory `mqb` is started in. Without
`exit_on_empty` the route keeps running; stop it with Ctrl-C. Without
`--no-ui`, `mqb` asks on a terminal whether to start the web UI.

The same thing as a one-off command, without a config file:

```sh
mqb copy --drain 'connect+generate://?count=3&interval=&mapping=root.id%20%3D%20counter()' \
    "file://$PWD/out.jsonl"
```

It prints `copied 3 rows in …`. A real connector looks the same, e.g. MQTT to a
file (runs until Ctrl-C):

```sh
mqb copy 'connect+mqtt://localhost:1883/orders' "file://$PWD/orders.jsonl"
```

See [Configuring an endpoint](../README.md#configuring-an-endpoint) for the configuration
forms and [`examples/mqb-route.yaml`](../examples/mqb-route.yaml) for a route with
middlewares.

## Troubleshooting

| Message | Cause and fix |
| :--- | :--- |
| `Custom endpoint factory 'connect' not found: no libmq_bridge_connect.dylib in …` | The plugin was not found. The message lists every directory searched; install there, or set `MQB_PLUGIN_DIR` / `--plugin`. |
| the Go library cannot be loaded | `libmq_bridge_connect_go.*` is not next to the plugin. Copy both, or set `MQ_BRIDGE_CONNECT_GO_LIBRARY` to its absolute path. |
| `did not become ready within 5000ms` | The first `connect` endpoint loads a 218 MiB library. Set `startup_timeout_ms: 30000` on the route. |
| a library is refused, naming a directory | That directory is world-writable or belongs to another user. |
| `File line is valid JSON but not a record envelope` | The input is plain JSON lines: add `format: raw` to the `file` endpoint. |

## Docker

The [`mq-bridge-app`](https://github.com/marcomq/mq-bridge/tree/main/apps/mq-bridge-app)
image (`ghcr.io/marcomq/mq-bridge-app`) has no plugins built in, but it searches
`/usr/local/lib/mq-bridge` for them, so installing the plugin is copying a
Linux release archive there. The image is Debian 12 based, which the Linux
archives (built against glibc 2.35) run on.

Download and unpack the archive for the image's architecture
(`aarch64-unknown-linux-gnu` on arm64):

```sh
VERSION=0.2.1
TARGET=x86_64-unknown-linux-gnu
curl -fLO "https://github.com/marcomq/mq-bridge-connect/releases/download/v$VERSION/mq-bridge-connect-$VERSION-$TARGET.tar.gz"
mkdir -p connect
tar xzf "mq-bridge-connect-$VERSION-$TARGET.tar.gz" --strip-components=1 -C connect
```

**Build a derived image**, the recommended way. `Dockerfile` next to the
`connect` folder:

```dockerfile
FROM ghcr.io/marcomq/mq-bridge-app:latest
COPY connect/*.so /usr/local/lib/mq-bridge/
COPY connect/THIRD_PARTY_NOTICES connect/LICENSE-* /usr/share/licenses/mq-bridge-connect/
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
host's file owner, which discovery refuses, so name the plugin by path; a
library loaded by path is not ownership-checked:

```sh
docker run --rm -v "$PWD/connect:/plugins/connect:ro" \
    ghcr.io/marcomq/mq-bridge-app:latest \
    --plugin /plugins/connect/libmq_bridge_connect.so copy 'connect+mqtt://broker:1883/orders' 'file:///app/orders.jsonl'
```

The Go sibling is found beside the plugin, so mount the whole directory, not
the one file. Either way, keep `THIRD_PARTY_NOTICES` in the image or the mount:
it is part of the distribution (see [packaging/INSTALL.md](../packaging/INSTALL.md)).

Building the plugin yourself: [docs/DEVELOPMENT.md](DEVELOPMENT.md).
