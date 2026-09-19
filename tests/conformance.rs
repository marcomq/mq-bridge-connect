//! The acceptance gate: mq-bridge's own endpoint conformance suite, run against
//! real brokers.
//!
//! The suite shares one configuration between the input and the output, so it
//! needs a connector whose two directions take the same fields. That rules out
//! the `yaml` form entirely — it names the section mq-bridge owns, which is the
//! opposite one in each direction — and it rules out every connector whose two
//! directions disagree: `mqtt` reads `topics` and writes `topic`, `amqp_0_9`
//! reads a `queue` and writes to an `exchange`, `redis_streams` reads `streams`
//! and writes `stream`. Those need a direction-aware configuration form, not a
//! different test.
//!
//! Each connector is held to what its transport actually guarantees:
//!
//! | Connector | Transport | Redelivery | Metadata |
//! | :-- | :-- | :-- | :-- |
//! | `beanstalkd` | work queue | yes — a released job returns to the ready queue | no — a job is a body |
//! | `nats_jetstream` | persistent stream | yes — on nack, and on `ack_wait` expiry | no — see below |
//! | `redis_list` | work queue | no — the ack is a no-op; the pop already removed it | no — body only |
//!
//! Neither NATS connector can carry metadata here: the output's `metadata`
//! field is an *include* filter that writes nothing until configured, and the
//! one shared config cannot configure it, because the input rejects a
//! `metadata` field it does not define.
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
use mq_bridge::traits::CustomEndpointFactory;
use serde_json::json;

mod common;
use common::factory;

const FULL_SUITE: &[&str] = &[
    "round_trip",
    "nack_redelivers",
    "uncommitted_batch_redelivers",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_beanstalkd() {
    let Some(address) = address("MQ_BRIDGE_REDPANDA_BEANSTALKD") else {
        return;
    };
    let mut options = ConformanceOptions::new(
        "redpanda-conformance-beanstalkd",
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
    let Some(address) = address("MQ_BRIDGE_REDPANDA_NATS") else {
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
        "redpanda-conformance-nats-jetstream",
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
    let Some(address) = address("MQ_BRIDGE_REDPANDA_REDIS") else {
        return;
    };
    let mut options = ConformanceOptions::new(
        "redpanda-conformance-redis-list",
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
