//! The acceptance gate: mq-bridge's own endpoint conformance suite, run against
//! real brokers.
//!
//! The suite shares one configuration between the input and the output. That
//! rules out the `yaml` form entirely — it names the section mq-bridge owns,
//! which is the opposite one in each direction. Connectors whose two
//! directions disagree on field names use the `input`/`output` blocks of form
//! A instead: `mqtt` reads `topics` and writes `topic`, `amqp_0_9` reads a
//! `queue` and writes to an `exchange`, `redis_streams` reads `streams` and
//! writes `stream`.
//!
//! Each connector is held to what its transport actually guarantees:
//!
//! | Connector | Transport | Redelivery | Metadata |
//! | :-- | :-- | :-- | :-- |
//! | `beanstalkd` | work queue | full — TTR returns an abandoned job | no — a job is a body |
//! | `nats_jetstream` | persistent stream | full — on nack, and on `ack_wait` expiry | no — see below |
//! | `redis_list` | work queue | none — the pop already removed it | no — body only |
//! | `amqp_0_9` | queue | nack only — no acknowledgement deadline | yes |
//! | `redis_streams` | consumer group | nack only — a pending entry needs an explicit claim | yes |
//! | `mqtt` | QoS 1 topic | nack only — replayed in process | no — v3.1.1 has no user properties |
//!
//! "Full" means both redelivery checks; "nack only" means a rejected message
//! comes back but a batch abandoned without a commit does not, because the
//! transport has no acknowledgement deadline to expire. `ConformanceOptions`
//! turns both checks on together, so those three run the suite without them
//! and are held to `nack_is_redelivered` below instead.
//!
//! Neither NATS connector can carry metadata here: the output's `metadata`
//! field is an *include* filter that writes nothing until configured, and the
//! one shared config cannot configure it, because the input rejects a
//! `metadata` field it does not define. `amqp_0_9` and `redis_streams` use an
//! *exclude* filter, which carries everything by default.
//!
//! Core `nats` is deliberately absent. It is fire-and-forget pub/sub with no
//! buffering, so anything published before the subscription reaches the server
//! is lost — a race the suite would hit intermittently rather than a property
//! it could check.
//!
//! Set the environment variable named in each test to run it. Without a broker
//! there is nothing to check, so the test reports a skip and returns — never
//! under CI, where a skip would make a missing broker look like a pass.

use std::time::Duration;

use mq_bridge::plugin::conformance::{self, ConformanceOptions};
use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge::{CanonicalMessage, SentBatch};
use serde_json::{json, Value};

mod common;
use common::factory;

const FULL_SUITE: &[&str] = &[
    "round_trip",
    "nack_redelivers",
    "uncommitted_batch_redelivers",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_beanstalkd() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_BEANSTALKD") else {
        return;
    };
    let mut options = ConformanceOptions::new(
        "connect-conformance-beanstalkd",
        json!({ "connector": "beanstalkd", "address": address }),
    );
    // A beanstalkd job is a body and nothing else, and the connector's output
    // writes `msg.AsBytes()` alone. That is the transport's limit, not the
    // boundary's — metadata fidelity across the FFI is covered by `data_path.rs`
    // and by the Go-side tests.
    options.expect_metadata = false;

    run(options, FULL_SUITE).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_nats_jetstream() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_NATS") else {
        return;
    };
    // The stream outlives the run, and a fresh ephemeral consumer starts at the
    // beginning of it, so a fixed subject would replay every earlier run.
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is before the epoch")
        .as_millis();
    let subject = format!("mqb.conformance.{run_id}");
    create_stream(&address, &format!("mqb_conformance_{run_id}"), &subject).await;

    let mut options = ConformanceOptions::new(
        "connect-conformance-nats-jetstream",
        json!({
            "connector": "nats_jetstream",
            "urls": [format!("nats://{address}")],
            "subject": subject,
        }),
    );
    options.expect_metadata = false;
    // An uncommitted batch comes back only once `ack_wait` expires, and that
    // defaults to 30s. It is an input-only field, so the shared config cannot
    // shorten it; wait it out instead.
    options.receive_timeout = Duration::from_secs(45);

    run(options, FULL_SUITE).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_redis_list() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_REDIS") else {
        return;
    };
    let mut options = ConformanceOptions::new(
        "connect-conformance-redis-list",
        json!({
            "connector": "redis_list",
            "url": format!("redis://{address}"),
            "key": "mq-bridge-conformance",
        }),
    );
    options.expect_metadata = false;
    // `BLPOP` already removed the element and the connector's ack function is a
    // no-op, so a rejected message is gone either way.
    options.expect_redelivery = false;

    run(options, &["round_trip"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_amqp_0_9() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_AMQP") else {
        return;
    };
    // The two directions disagree on every routing field, which is what the
    // per-direction blocks are for: the input consumes a queue, the output
    // publishes to an exchange. The default exchange routes by name, so a key
    // equal to the queue lands the message in it.
    let queue = format!("mqb-conformance-{}", run_id());
    declare_queue(&address, &queue).await;
    let config = json!({
            "connector": "amqp_0_9",
            "urls": [format!("amqp://guest:guest@{address}/")],
            "input": {
                "queue": queue,
                "queue_declare": { "enabled": true, "durable": true },
            },
            "output": { "exchange": "", "key": queue },
    });
    let mut options = ConformanceOptions::new("connect-conformance-amqp-0-9", config.clone());
    options.expect_redelivery = false;

    run(options, &["round_trip", "metadata_preserved"]).await;
    nack_is_redelivered("connect-amqp-0-9-nack", &config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_redis_streams() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_REDIS") else {
        return;
    };
    // `streams` reads a list and `stream` writes one name, so the shared form
    // cannot express this connector at all without the per-direction blocks.
    let stream = format!("mqb-conformance-{}", run_id());
    let config = json!({
            "connector": "redis_streams",
            "url": format!("redis://{address}"),
            "body_key": "body",
            "input": {
                "streams": [stream],
                "consumer_group": "mqb-conformance",
                "create_streams": true,
                "start_from_oldest": true,
            },
            "output": { "stream": stream },
    });
    let mut options = ConformanceOptions::new("connect-conformance-redis-streams", config.clone());
    options.expect_redelivery = false;

    run(options, &["round_trip", "metadata_preserved"]).await;
    nack_is_redelivered("connect-redis-streams-nack", &config).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_mqtt() {
    let Some(address) = address("MQ_BRIDGE_CONNECT_MQTT") else {
        return;
    };
    // A broker disconnects the older session when a second client presents the
    // same id, so the two ends need different ones. That is a per-direction
    // field whose name is shared, which the blocks handle as readily as the
    // `topics`/`topic` split.
    let id = run_id();
    let topic = format!("mqb/conformance/{id}");
    let config = json!({
            "connector": "mqtt",
            "urls": [format!("tcp://{address}")],
            "qos": 1,
            "input": {
                "topics": [topic],
                "client_id": format!("mqb-conformance-in-{id}"),
                "clean_session": false,
            },
            "output": {
                "topic": topic,
                "client_id": format!("mqb-conformance-out-{id}"),
            },
    });
    // Benthos connects lazily, so without this the publisher reaches the broker
    // before the subscription does and the first messages are dropped. A QoS 1
    // session opened with `clean_session: false` makes the broker hold the
    // subscription, and queue messages, while the client is away.
    subscribe_first(&config).await;

    let mut options = ConformanceOptions::new("connect-conformance-mqtt", config.clone());
    // MQTT carries no user properties under v3.1.1, which is what this client
    // speaks, so a payload is all that crosses.
    options.expect_metadata = false;
    options.expect_redelivery = false;

    run(options, &["round_trip"]).await;
    nack_is_redelivered("connect-mqtt-nack", &config).await;
}

/// A per-run suffix, so a broker that outlives the run cannot replay an earlier
/// one into it.
fn run_id() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is before the epoch")
        .as_millis()
}

/// Creates the JetStream stream the shared config cannot ask for: `stream` and
/// `create_stream` are input-only fields, so they go through the `yaml` form,
/// which is direction-specific. Benthos connects lazily, so this has to read
/// before the stream exists.
async fn create_stream(address: &str, stream: &str, subject: &str) {
    let yaml = format!(
        "input:\n  nats_jetstream:\n    urls: [ \"nats://{address}\" ]\n    \
         stream: {stream}\n    subject: {subject}\n    create_stream: true\n"
    );
    let mut consumer = factory()
        .create_consumer("conformance-bootstrap", &json!({ "yaml": yaml }))
        .await
        .unwrap_or_else(|error| panic!("could not open {stream}: {error:#}"));
    consumer.set_exit_on_empty(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer.receive_batch(1)).await;
    consumer
        .close()
        .await
        .unwrap_or_else(|error| panic!("could not close the bootstrap consumer: {error:#}"));
}

/// A nacked message comes back.
///
/// The shared suite turns both redelivery checks on together, and these three
/// transports guarantee only this half. None has an acknowledgement deadline,
/// so a batch abandoned without a commit stays checked out until the consumer
/// disconnects, where beanstalkd's TTR and JetStream's `ack_wait` hand it back
/// on a timer. Rejecting the message explicitly returns it on all three.
async fn nack_is_redelivered(route: &str, config: &Value) {
    let factory = factory();
    let mut consumer = factory
        .create_consumer(route, config)
        .await
        .unwrap_or_else(|error| panic!("{route}: could not open the consumer: {error:#}"));
    let publisher = factory
        .create_publisher(route, config)
        .await
        .unwrap_or_else(|error| panic!("{route}: could not open the publisher: {error:#}"));

    let payload = format!("nack-redelivery-{}", run_id());
    match publisher
        .send_batch(vec![CanonicalMessage::from(payload.as_str())])
        .await
    {
        Ok(SentBatch::Ack) => {}
        Ok(SentBatch::Partial { failed, .. }) if failed.is_empty() => {}
        other => panic!("{route}: publishing failed: {other:?}"),
    }
    publisher
        .flush()
        .await
        .unwrap_or_else(|error| panic!("{route}: flushing failed: {error:#}"));

    let first = take_one(route, &mut *consumer, &payload, MessageDisposition::Nack).await;
    assert_eq!(first, payload, "{route}: received an unexpected message");
    let second = take_one(route, &mut *consumer, &payload, MessageDisposition::Ack).await;
    assert_eq!(
        second, payload,
        "{route}: a nacked message was not redelivered"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|error| panic!("{route}: could not close the consumer: {error:#}"));
}

/// Receives until `payload` arrives, settling every batch with `disposition`.
/// Anything else on the transport is acknowledged so it cannot mask the answer.
async fn take_one(
    route: &str,
    consumer: &mut dyn mq_bridge::traits::MessageConsumer,
    payload: &str,
    disposition: MessageDisposition,
) -> String {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        let batch = match consumer.receive_batch(1).await {
            Ok(batch) => batch,
            Err(error) => panic!("{route}: receive_batch failed: {error:#}"),
        };
        let found = batch
            .messages
            .iter()
            .find(|message| message.get_payload_str() == payload)
            .map(|message| message.get_payload_str().to_string());
        let settle = if found.is_some() {
            disposition.clone()
        } else {
            MessageDisposition::Ack
        };
        let count = batch.messages.len();
        (batch.commit)(vec![settle; count])
            .await
            .unwrap_or_else(|error| panic!("{route}: commit failed: {error:#}"));
        if let Some(found) = found {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{route}: `{payload}` never arrived");
}

/// Registers the MQTT subscription before anything publishes to the topic, by
/// opening the session once and letting it close. The broker keeps a
/// `clean_session: false` session, so the suite's own consumer resumes it.
async fn subscribe_first(config: &Value) {
    let mut consumer = factory()
        .create_consumer("conformance-bootstrap", config)
        .await
        .unwrap_or_else(|error| panic!("could not open the MQTT session: {error:#}"));
    consumer.set_exit_on_empty(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer.receive_batch(1)).await;
    consumer
        .close()
        .await
        .unwrap_or_else(|error| panic!("could not close the bootstrap consumer: {error:#}"));
}

/// Declares the queue before anything publishes to it. Benthos connects lazily,
/// and the default exchange drops a message with no matching queue without
/// erroring, so a first consumer has to have run to completion.
async fn declare_queue(address: &str, queue: &str) {
    let mut consumer = factory()
        .create_consumer(
            "conformance-bootstrap",
            &json!({
                "connector": "amqp_0_9",
                "urls": [format!("amqp://guest:guest@{address}/")],
                "queue": queue,
                "queue_declare": { "enabled": true, "durable": true },
            }),
        )
        .await
        .unwrap_or_else(|error| panic!("could not declare {queue}: {error:#}"));
    consumer.set_exit_on_empty(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer.receive_batch(1)).await;
    consumer
        .close()
        .await
        .unwrap_or_else(|error| panic!("could not close the bootstrap consumer: {error:#}"));
}

/// `None` means the broker is absent and the test should report a skip.
fn address(variable: &str) -> Option<String> {
    match std::env::var(variable) {
        Ok(address) => Some(address),
        Err(_) => {
            // A silent skip would let a green run stand in for a gate that never ran.
            assert!(
                std::env::var_os("CI").is_none(),
                "{variable} must be set in CI: this suite is the acceptance gate, and a \
                 skip would make a missing broker look like a pass"
            );
            eprintln!("skipped: set {variable}=host:port to run this connector");
            None
        }
    }
}

/// Fails if the suite ran a different set of checks than the transport claims,
/// so a check that quietly stops applying is caught rather than celebrated.
async fn run(options: ConformanceOptions, expected: &[&str]) {
    let route = options.route_name.clone();
    let passed = conformance::run(&factory(), options)
        .await
        .unwrap_or_else(|error| panic!("the conformance suite failed for {route}: {error:#}"));
    assert_eq!(
        passed, expected,
        "{route} did not run the checks this transport supports"
    );
}
