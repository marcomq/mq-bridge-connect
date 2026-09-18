//! The acceptance gate: mq-bridge's own endpoint conformance suite, run against
//! a real broker.
//!
//! The suite shares one configuration between the input and the output, so it
//! needs a connector whose two directions take the same fields. `beanstalkd`
//! takes only `address`, and it redelivers what a consumer rejects, so three of
//! the four checks apply — `round_trip`, `nack_redelivers` and
//! `uncommitted_batch_redelivers`.
//!
//! `metadata_preserved` does not: a beanstalkd job is a body and nothing else,
//! and the connector's output writes `msg.AsBytes()` alone. That is the
//! transport's limit, not the boundary's — metadata fidelity across the FFI is
//! covered by `data_path.rs` and by the Go-side tests.
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
        // A silent skip would let a green run stand in for a gate that never ran.
        assert!(
            std::env::var_os("CI").is_none(),
            "{ADDRESS} must be set in CI: this suite is the acceptance gate, and a \
             skip would make a missing broker look like a pass"
        );
        eprintln!("skipped: set {ADDRESS}=host:port to run the conformance suite");
        return;
    };

    let factory = factory();
    let mut options = ConformanceOptions::new(
        "redpanda-conformance",
        json!({ "connector": "beanstalkd", "address": address }),
    );
    options.expect_metadata = false;

    let passed = conformance::run(&factory, options)
        .await
        .expect("the conformance suite failed");
    assert_eq!(
        passed,
        [
            "round_trip",
            "nack_redelivers",
            "uncommitted_batch_redelivers"
        ],
        "the suite did not run the checks this transport supports"
    );
}
