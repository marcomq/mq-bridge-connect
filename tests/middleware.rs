//! Redpanda processors as mq-bridge middlewares, through the real Go library.

use mq_bridge::plugin::sdk::{BatchFilter, MiddlewareFactory};
use mq_bridge::CanonicalMessage;
use mq_bridge_connect::middleware::{ChainMiddleware, ProcessorMiddleware};
use mq_bridge_connect::{Dedupe, Mapping};
use serde_json::{json, Value};

mod common;

fn message(payload: &str) -> CanonicalMessage {
    let mut message = CanonicalMessage::from(payload.as_bytes().to_vec());
    message.metadata.insert("origin".into(), "test".into());
    message
}

async fn filter(factory: impl MiddlewareFactory, config: Value) -> Box<dyn BatchFilter> {
    // Resolves the Go sibling for the test binary before the middleware loads it.
    let _ = common::factory();
    factory
        .create("test", &config)
        .await
        .expect("create failed")
}

fn payloads(filtered: &[Option<CanonicalMessage>]) -> Vec<Option<String>> {
    filtered
        .iter()
        .map(|message| message.as_ref().map(|m| m.get_payload_str().to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mapping_rewrites_and_keeps_identity_and_metadata() {
    let filter = filter(
        ProcessorMiddleware::<Mapping>::default(),
        json!("root = content().uppercase()\nmeta mapped = \"yes\""),
    )
    .await;
    let sent = vec![message("a"), message("b")];
    let ids: Vec<u128> = sent.iter().map(|m| m.message_id).collect();

    let filtered = filter.on_receive(sent).await.unwrap();

    assert_eq!(payloads(&filtered), [Some("A".into()), Some("B".into())]);
    for (message, id) in filtered.iter().flatten().zip(ids) {
        assert_eq!(message.message_id, id);
        assert_eq!(
            message.metadata.get("origin").map(String::as_str),
            Some("test")
        );
        assert_eq!(
            message.metadata.get("mapped").map(String::as_str),
            Some("yes")
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_uri_style_mapping_drops_deleted_messages() {
    let filter = filter(
        ProcessorMiddleware::<Mapping>::default(),
        json!({ "mapping": "root = if content() == \"drop\" { deleted() }" }),
    )
    .await;

    let filtered = filter
        .on_send(vec![message("keep"), message("drop"), message("also")])
        .await
        .unwrap();

    assert_eq!(
        payloads(&filtered),
        [Some("keep".into()), None, Some("also".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedupe_with_an_inline_cache_drops_repeats_across_batches() {
    let filter = filter(
        ProcessorMiddleware::<Dedupe>::default(),
        json!({ "key": "${! content() }", "cache": { "memory": {} } }),
    )
    .await;

    let first = filter
        .on_receive(vec![message("a"), message("b")])
        .await
        .unwrap();
    let second = filter
        .on_receive(vec![message("b"), message("c")])
        .await
        .unwrap();

    assert_eq!(payloads(&first), [Some("a".into()), Some("b".into())]);
    assert_eq!(payloads(&second), [None, Some("c".into())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_runs_in_order_and_rejects_fan_out() {
    let chain = filter(
        ChainMiddleware,
        json!({ "processors": [
            { "mapping": "root = content() + \"1\"" },
            { "mutation": "root = content() + \"2\"" }
        ]}),
    )
    .await;
    let filtered = chain.on_receive(vec![message("x")]).await.unwrap();
    assert_eq!(payloads(&filtered), [Some("x12".into())]);

    let splitting = filter(
        ChainMiddleware,
        json!({ "processors": [ { "unarchive": { "format": "lines" } } ] }),
    )
    .await;
    let error = splitting
        .on_receive(vec![message("one\ntwo")])
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("pipeline"), "{error:#}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_message_fails_the_batch_and_bad_config_fails_creation() {
    let throwing = filter(
        ProcessorMiddleware::<Mapping>::default(),
        json!("root = throw(\"bad input\")"),
    )
    .await;
    let error = throwing.on_receive(vec![message("x")]).await.unwrap_err();
    assert!(format!("{error:#}").contains("bad input"), "{error:#}");

    let _ = common::factory();
    assert!(ChainMiddleware
        .create(
            "test",
            &json!({ "processors": [ { "no_such_processor": {} } ] })
        )
        .await
        .is_err());
}
