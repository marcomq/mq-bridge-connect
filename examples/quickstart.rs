//! The shortest program that pulls messages through a Redpanda Connect
//! connector. No broker: Benthos `generate` fabricates the messages, so the
//! only thing under test is the boundary itself.
//!
//! The factory finds the Go sibling library next to the Rust one, which is not
//! where `cargo run --example` puts this binary, so point at it explicitly:
//!
//! ```text
//! cargo build --lib
//! (cd go-bridge && go build -buildmode=c-shared \
//!     -o ../target/debug/libmq_bridge_redpanda_go.dylib .)   # .so on Linux
//! MQ_BRIDGE_REDPANDA_GO_LIBRARY=$PWD/target/debug/libmq_bridge_redpanda_go.dylib \
//!     cargo run --example quickstart
//! ```

use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge_redpanda::RedpandaFactory;
use serde_json::json;

const MESSAGES: usize = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let factory = RedpandaFactory::default();

    // Form A: name a connector, and every other field goes to it. mq-bridge
    // owns the other end of the stream, so there is no `output` to write.
    let config = json!({
        "connector": "generate",
        "count": MESSAGES,
        "interval": "",
        "mapping": r#"root.greeting = "hello from Redpanda Connect""#,
    });

    let mut consumer = factory.create_consumer("quickstart", &config).await?;
    // Without this the consumer waits for messages the `generate` input will
    // never produce, because `count` has already been reached.
    consumer.set_exit_on_empty(true);

    let batch = consumer.receive_batch(MESSAGES).await?;
    for message in &batch.messages {
        println!("{}", String::from_utf8_lossy(&message.payload));
    }

    // Nothing is acknowledged until the commit runs, and each disposition
    // applies to the message at the same index.
    let dispositions = vec![MessageDisposition::Ack; batch.messages.len()];
    (batch.commit)(dispositions).await?;

    consumer.close().await?;
    Ok(())
}
