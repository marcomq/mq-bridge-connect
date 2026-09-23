//! End-to-end checks that messages actually cross the Rust↔Go boundary.
//!
//! The pair is `socket_server` (input) and `socket` (output) over loopback TCP:
//! two real Redpanda Connect connectors, framed with the `lines` codec both ends
//! default to, and no external broker. The full `mq_bridge::plugin::conformance`
//! suite needs a connector whose input and output take the same configuration,
//! so it runs against a real broker rather than here.

use std::net::TcpListener;
use std::time::{Duration, Instant};

use mq_bridge::errors::ConsumerError;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessageDisposition};
use mq_bridge::CanonicalMessage;
use serde_json::json;

mod common;
use common::factory;

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
        .create_consumer("connect-round-trip", &listening(port))
        .await
        .expect("failed to create the consumer");
    let publisher = factory
        .create_publisher("connect-round-trip", &connecting(port))
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
            "connect-both-ends",
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
            "connect-unknown",
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

/// A source that produces one batch of `count` messages, so a smaller
/// `receive_batch` has to split it.
fn generating(count: usize) -> serde_json::Value {
    json!({ "yaml": format!(
        "input:\n  generate:\n    count: {count}\n    interval: \"\"\n    batch_size: {count}\n\
         \x20   mapping: 'root = \"payload\"'\n"
    )})
}

/// Covers three things the unit tests cannot: that a source batch larger than
/// mq-bridge asked for is split across calls, that no call exceeds the requested
/// size, and that a drained stream reports the end rather than hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_source_batch_is_split_and_then_the_stream_ends() {
    const TOTAL: usize = 100;
    const CHUNK: usize = 10;

    let factory = factory();
    let mut consumer = factory
        .create_consumer("connect-split", &generating(TOTAL))
        .await
        .expect("failed to create the consumer");
    consumer.set_exit_on_empty(true);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut received = 0;
    let mut ended = false;
    while Instant::now() < deadline {
        match consumer.receive_batch(CHUNK).await {
            Ok(batch) => {
                let count = batch.messages.len();
                assert!(
                    count <= CHUNK,
                    "receive_batch({CHUNK}) returned {count} messages"
                );
                received += count;
                (batch.commit)(vec![MessageDisposition::Ack; count])
                    .await
                    .expect("commit failed");
            }
            Err(ConsumerError::EndOfStream) => {
                ended = true;
                break;
            }
            Err(error) => panic!("receive_batch failed: {error}"),
        }
    }

    assert_eq!(received, TOTAL, "drained {received} of {TOTAL} messages");
    assert!(ended, "the drained stream never reported the end");
    consumer.close().await.expect("consumer close failed");
}

/// The commit closure counts the dispositions it is handed, so a caller that
/// miscounts is told rather than silently acknowledging the wrong messages.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_with_the_wrong_number_of_dispositions_is_rejected() {
    let factory = factory();
    let mut consumer = factory
        .create_consumer("connect-miscount", &generating(4))
        .await
        .expect("failed to create the consumer");

    let batch = consumer
        .receive_batch(4)
        .await
        .expect("receive_batch failed");
    let count = batch.messages.len();
    assert!(count > 0, "the source produced nothing to commit");

    let error = (batch.commit)(vec![MessageDisposition::Ack; count + 1])
        .await
        .expect_err("a commit with too many dispositions must fail");
    assert!(
        format!("{error:#}").contains("dispositions"),
        "unexpected error: {error:#}"
    );

    consumer.close().await.expect("consumer close failed");
}
