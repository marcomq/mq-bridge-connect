//! A complete route, with every part of the plugin's surface in one place:
//! both configuration forms, both directions, mq-bridge middleware wrapped
//! around a Redpanda connector, a handler in between, and a clean shutdown.
//!
//! The route reads through a Redpanda Connect `generate` input, transforms each
//! message in Rust, and writes through a Redpanda Connect `file` output. Both
//! ends are Redpanda connectors; mq-bridge supplies everything between them.
//!
//! Nothing here needs a broker. Run it the same way as `quickstart`:
//!
//! ```text
//! MQ_BRIDGE_CONNECT_GO_LIBRARY=$PWD/target/debug/libmq_bridge_connect_go.dylib \
//!     cargo run --example route
//! ```

use std::sync::Arc;
use std::time::Duration;

use mq_bridge::extensions::register_endpoint_factory;
use mq_bridge::{stop_route, CanonicalMessage, Handled, Route};
use mq_bridge_connect::ConnectFactory;
use serde_json::json;

const ROUTE: &str = "connect-demo";
const OUTPUT_PATH: &str = "target/route-example-output.jsonl";

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let _ = std::fs::remove_file(OUTPUT_PATH);

    // Linking the crate lets you register the factory directly. A process that
    // did not compile it in loads the same code from the shared library with
    // `mq_bridge::plugin::load_endpoint_plugin`; both end up as the endpoint
    // named `connect`, so the route config below is identical either way.
    register_endpoint_factory("connect", Arc::new(ConnectFactory::default()))?;

    let route: Route = serde_json::from_value(route_config())?;
    let route = route.with_handler(|mut message: CanonicalMessage| async move {
        let payload = String::from_utf8_lossy(&message.payload).into_owned();
        message.set_payload_str(format!(r#"{{"seen":{payload}}}"#));
        Ok(Handled::Publish(message))
    });

    route.deploy(ROUTE).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_route(ROUTE).await;

    let written = std::fs::read_to_string(OUTPUT_PATH)?;
    println!(
        "{} line(s) written to {OUTPUT_PATH}:",
        written.lines().count()
    );
    print!("{written}");
    Ok(())
}

/// The same shape a YAML config file uses, so this is also the reference for
/// what to write under `routes:` for `mqb` or for the Python and Node bindings.
fn route_config() -> serde_json::Value {
    json!({
        "input": {
            // Form A: `connector` names the Redpanda Connect component and the
            // remaining fields are that component's own configuration.
            "custom": {
                "name": "connect",
                "config": {
                    "connector": "generate",
                    "count": 10,
                    "interval": "",
                    "mapping": r#"root.id = counter()"#,
                },
            },
        },
        "output": {
            // Form B: a Redpanda Connect document, minus the `input` section
            // that mq-bridge owns. Use it when you need processors, or any
            // stream-level field that form A cannot express.
            "custom": {
                "name": "connect",
                "config": {
                    // `max_in_flight` is the connector's own concurrency, not
                    // mq-bridge's: above 1 the sink may write out of source order.
                    "yaml": format!(
                        "max_in_flight: 4\n\
                         pipeline:\n  processors:\n    - bloblang: 'root = this'\n\
                         output:\n  file:\n    path: {OUTPUT_PATH}\n"
                    ),
                },
            },
            // mq-bridge middleware wraps the connector without the connector
            // knowing: reach that a Redpanda pipeline has no equivalent for.
            // Both are publisher-side; `retry` is ignored on an input endpoint.
            "middlewares": [
                { "retry": { "max_attempts": 3, "initial_interval_ms": 100 } },
                // Anything the sink permanently rejects lands here instead of
                // stalling the route.
                { "dlq": { "endpoint": { "file": { "path": "target/route-example-dlq.jsonl" } } } },
            ],
        },
        // Batching is mq-bridge's, not the connector's: the source batch is
        // split or aggregated to this size and acknowledged as one unit.
        "batch_size": 5,
        // A route config defaults to 1; `mqb copy` and MCP default to 4, where
        // whole batches may reach the sink out of source order.
        "concurrency": 1,
    })
}
