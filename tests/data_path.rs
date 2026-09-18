//! End-to-end checks that messages actually cross the Rust↔Go boundary.
//!
//! The pair is `socket_server` (input) and `socket` (output) over loopback TCP:
//! two real Redpanda Connect connectors, framed with the `lines` codec both ends
//! default to, and no external broker. The full `mq_bridge::plugin::conformance`
//! suite needs a connector whose input and output take the same configuration,
//! so it runs against a real broker rather than here.

use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessageDisposition};
use mq_bridge::CanonicalMessage;
use mq_bridge_redpanda::RedpandaFactory;
use serde_json::json;

fn go_library() -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let name = if cfg!(target_os = "windows") {
        "mq_bridge_redpanda_go.dll"
    } else if cfg!(target_os = "macos") {
        "libmq_bridge_redpanda_go.dylib"
    } else {
        "libmq_bridge_redpanda_go.so"
    };
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(profile)
        .join(name)
}

/// The test binary lives in `target/<profile>/deps`, so sibling resolution would
/// look one directory too deep.
fn factory() -> RedpandaFactory {
    let library = go_library();
    assert!(
        library.exists(),
        "{} is missing; build it with `sh scripts/phase0-smoke.sh` first",
        library.display()
    );
    std::env::set_var("MQ_BRIDGE_REDPANDA_GO_LIBRARY", &library);
    RedpandaFactory::default()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to reserve a loopback port")
        .local_addr()
        .expect("reserved socket has no address")
        .port()
}

/// Reading half. The `mapping` processor stamps metadata inside Benthos, so the
/// assertion covers the Go→Rust metadata path rather than a connector's own keys
/// (`socket_server` sets none).
fn listening(port: u16) -> serde_json::Value {
    json!({ "yaml": format!(
        "input:\n  socket_server:\n    network: tcp\n    address: 127.0.0.1:{port}\n\
         pipeline:\n  processors:\n    - mapping: 'meta seen_by = \"benthos\"'\n"
    )})
}

/// Writing half. The mapping folds mq-bridge metadata into the payload, so what
/// arrives proves the Rust→Go metadata path survived the wire format — a socket
/// carries bytes only.
fn connecting(port: u16) -> serde_json::Value {
    json!({ "yaml": format!(
        "output:\n  socket:\n    network: tcp\n    address: 127.0.0.1:{port}\n\
         pipeline:\n  processors:\n    - mapping: 'root = content().string() + \"|\" + @origin'\n"
    )})
}

async fn drain(
    consumer: &mut dyn MessageConsumer,
    expected: usize,
) -> Vec<(String, std::collections::HashMap<String, String>)> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut received = Vec::new();
    while received.len() < expected && Instant::now() < deadline {
        let batch = consumer
            .receive_batch(expected)
            .await
            .expect("receive_batch failed");
        let count = batch.messages.len();
        received.extend(batch.messages.iter().map(|message| {
            (
                message.get_payload_str().to_string(),
                message.metadata.clone(),
            )
        }));
        (batch.commit)(vec![MessageDisposition::Ack; count])
            .await
            .expect("commit failed");
    }
    received
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn messages_cross_the_boundary_in_both_directions() {
    let factory = factory();
    let port = free_port();

    let mut consumer = factory
        .create_consumer("redpanda-round-trip", &listening(port))
        .await
        .expect("failed to create the consumer");
    let publisher = factory
        .create_publisher("redpanda-round-trip", &connecting(port))
        .await
        .expect("failed to create the publisher");

    let sent: Vec<CanonicalMessage> = (0..8)
        .map(|index| {
            let mut message = CanonicalMessage::from(format!("payload-{index}"));
            message
                .metadata
                .insert("origin".to_owned(), "mq-bridge".to_owned());
            message
        })
        .collect();
    publisher
        .send_batch(sent.clone())
        .await
        .expect("send_batch failed");

    let received = drain(&mut *consumer, sent.len()).await;
    let mut payloads: Vec<String> = received.iter().map(|(body, _)| body.clone()).collect();
    payloads.sort();

    // The trailing `|mq-bridge` is the metadata this side sent, folded into the
    // payload by the publisher's mapping.
    let mut expected: Vec<String> = sent
        .iter()
        .map(|message| format!("{}|mq-bridge", message.get_payload_str()))
        .collect();
    expected.sort();
    assert_eq!(payloads, expected);

    let metadata = &received[0].1;
    assert_eq!(
        metadata.get("seen_by").map(String::as_str),
        Some("benthos"),
        "metadata set inside Benthos did not reach mq-bridge: {metadata:?}"
    );

    consumer.close().await.expect("consumer close failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_configuration_that_owns_both_ends_is_rejected() {
    let factory = factory();
    let rejected = factory
        .create_consumer(
            "redpanda-both-ends",
            &json!({ "yaml": "input:\n  generate:\n    mapping: root = \"x\"\noutput:\n  drop: {}\n" }),
        )
        .await;
    let message = rejection(rejected, "a consumer must not accept an `output`");
    assert!(
        message.contains("mq-bridge owns that end"),
        "unexpected error: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_connector_fails_at_construction() {
    let factory = factory();
    let rejected = factory
        .create_consumer(
            "redpanda-unknown",
            &json!({ "connector": "not_a_connector" }),
        )
        .await;
    let message = rejection(rejected, "an unknown connector must not produce a consumer");
    assert!(
        message.contains("not_a_connector"),
        "unexpected error: {message}"
    );
}

/// `MessageConsumer` is not `Debug`, so `expect_err` cannot report the success case.
fn rejection<T>(result: anyhow::Result<T>, expectation: &str) -> String {
    match result {
        Ok(_) => panic!("{expectation}"),
        Err(error) => format!("{error:#}"),
    }
}
