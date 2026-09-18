//! The acceptance gate: mq-bridge's own endpoint conformance suite, run against
//! a real broker.
//!
//! The suite shares one configuration between the input and the output, so it
//! needs a connector whose two directions take the same fields. `beanstalkd`
//! takes only `address`, and it redelivers what a consumer rejects, so all four
//! checks apply — `round_trip`, `metadata_preserved`, `nack_redelivers` and
//! `uncommitted_batch_redelivers`.
//!
//! Set `MQ_BRIDGE_REDPANDA_BEANSTALKD=host:port` to run it. Without a broker
//! there is nothing to check, so the test reports that and returns.

use mq_bridge::plugin::conformance::{self, ConformanceOptions};
use serde_json::json;

mod common;
use common::factory;

const ADDRESS: &str = "MQ_BRIDGE_REDPANDA_BEANSTALKD";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_conformance_suite_passes_against_beanstalkd() {
    let Ok(address) = std::env::var(ADDRESS) else {
        eprintln!("skipped: set {ADDRESS}=host:port to run the conformance suite");
        return;
    };

    let factory = factory();
    let options = ConformanceOptions::new(
        "redpanda-conformance",
        json!({ "connector": "beanstalkd", "address": address }),
    );

    let passed = conformance::run(&factory, options)
        .await
        .expect("the conformance suite failed");
    assert_eq!(
        passed.len(),
        4,
        "expected all four checks to run, got {passed:?}"
    );
}
