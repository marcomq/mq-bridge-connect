# mq-bridge-connect (Python)

An unofficial Redpanda Connect compatibility plugin for
[mq-bridge-py](https://pypi.org/project/mq-bridge-py/). It lets mq-bridge routes
use Redpanda Connect (Benthos) connectors as endpoints. The wheel contains no
Python implementation: it bundles the compiled Rust plugin and its Go sibling
library and registers them with mq-bridge.

> **Unofficial.** Not affiliated with, endorsed by or supported by Redpanda
> Data, Inc. or the Benthos project. Report issues at
> <https://github.com/marcomq/mq-bridge-connect/issues>.

```console
pip install mq-bridge-py mq-bridge-connect
```

```python
import mq_bridge
import mq_bridge_connect

mq_bridge_connect.register()   # call once, before starting routes

route = mq_bridge.Route.from_str("""
generate_to_file:
  input:
    custom:
      name: connect
      config:
        connector: generate
        count: 10
        interval: ""
        mapping: "root.id = counter()"
  output:
    file:
      path: "out.jsonl"
""")
route.start()
```

`register()` returns the endpoint name (`connect`) and is a no-op when called
again. It raises `ImportError` if mq-bridge is missing and `FileNotFoundError`
if the wheel does not carry a library for this platform. See the
[repository README](https://github.com/marcomq/mq-bridge-connect#configuring-an-endpoint)
for the endpoint configuration.

A plugin is native code with the same privileges as the interpreter — install
it as you would any other native dependency.

## Licences

The two libraries statically link several hundred third-party modules.
`THIRD_PARTY_NOTICES`, installed in the wheel's `.dist-info/licenses/`, carries
the full text of every licence involved.

## Building the wheel

The release workflow builds it; to reproduce one locally, stage both libraries
and the licence files into this directory, then build and tag the wheel:

```console
cargo build --release --lib
(cd go-bridge && go build -buildmode=c-shared -trimpath \
    -o ../python/mq_bridge_connect/libmq_bridge_connect_go.dylib .)   # .so on Linux
cp target/release/libmq_bridge_connect.dylib python/mq_bridge_connect/
cp THIRD_PARTY_NOTICES LICENSE-MIT LICENSE-APACHE python/
python -m build --wheel --outdir python/dist python
python -m wheel tags --platform-tag macosx_11_0_arm64 --remove python/dist/*.whl
```
