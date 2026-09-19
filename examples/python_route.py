"""The same Redpanda connector from Python, via the mq-bridge plugin loader.

The Python binding never compiled this endpoint in; it loads the shared library
at runtime and the endpoint is then addressable by the name the plugin exports
(`redpanda`), exactly as in Rust or in an `mqb` config.

    pip install mq-bridge-py
    cargo build --lib
    (cd go-bridge && go build -buildmode=c-shared \
        -o ../target/debug/libmq_bridge_redpanda_go.dylib .)   # .so on Linux
    python examples/python_route.py

A plugin is native code with the interpreter's privileges, not a sandboxed
script; load only libraries you trust.
"""

import pathlib
import platform
import time

import mq_bridge

REPO = pathlib.Path(__file__).resolve().parent.parent
SUFFIX = ".dylib" if platform.system() == "Darwin" else ".so"
PLUGIN = REPO / "target" / "debug" / f"libmq_bridge_redpanda{SUFFIX}"
OUTPUT = REPO / "target" / "python-route-output.jsonl"

# The Go sibling is resolved next to the Rust library, so both must be in
# target/debug. Loading twice is a no-op; a second library claiming the name
# `redpanda` is rejected rather than silently replacing the first.
mq_bridge.load_endpoint_plugin(str(PLUGIN))

CONFIG = f"""
routes:
  redpanda_demo:
    input:
      custom:
        name: redpanda
        config:
          connector: generate
          count: 10
          interval: ""
          mapping: "root.id = counter()"
    output:
      custom:
        name: redpanda
        config:
          yaml: |
            output:
              file:
                path: {OUTPUT}
      middlewares:
        - retry:
            max_attempts: 3
            initial_interval_ms: 100
    batch_size: 5
"""


def handle(message):
    """Runs between the two connectors, once per message.

    Returning bytes publishes them with the source message's metadata; return
    None to ack without publishing. The message itself is read-only.
    """
    payload = message.payload.decode()
    print("handling:", payload)
    return f'{{"seen":{payload}}}'.encode()


def main() -> None:
    OUTPUT.unlink(missing_ok=True)

    route = mq_bridge.Route.from_yaml_str(CONFIG, "redpanda_demo").with_handler(handle)
    # start() deploys on a background thread and returns; run() blocks instead.
    route.start()
    try:
        time.sleep(2)
    finally:
        route.stop()
        route.join()

    print(f"\n{OUTPUT}:")
    print(OUTPUT.read_text(), end="")


if __name__ == "__main__":
    main()
