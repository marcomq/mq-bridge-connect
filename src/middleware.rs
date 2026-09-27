//! Redpanda processors as mq-bridge middlewares.
//!
//! Each fitting processor is its own middleware, `connect_<processor>`, whose
//! configuration is that processor's own:
//!
//! ```yaml
//! middlewares:
//!   - connect_mapping: 'root = this.merge({"at": now()})'
//!   - connect_dedupe: { key: '${! meta("id") }', cache: { memory: {} } }
//! ```
//!
//! `connect` runs a whole chain in one crossing into Go:
//!
//! ```yaml
//! middlewares:
//!   - connect:
//!       processors: [ { jq: { query: '.payload' } }, { mapping: 'root = this' } ]
//!       cache_resources: [ { label: seen, redis: { url: 'redis://localhost:6379' } } ]
//! ```
//!
//! A middleware keeps, rewrites or drops each message, so a processor that
//! splits one message into several fails the batch; it belongs in a connect
//! endpoint's `pipeline`. A message a processor marked as failed fails the
//! batch as retryable, which is what mq-bridge's `retry` and `dlq` act on.

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use mq_bridge::errors::InvalidConfig;
use mq_bridge::plugin::sdk::{BatchFilter, MiddlewareFactory};
use mq_bridge::CanonicalMessage;
use serde_json::{json, Map, Value};
use tokio::task::spawn_blocking;

use crate::{wire, GoLibrary};

/// How long Go waits for a chain's resources to shut down.
const CLOSE_TIMEOUT_MS: u32 = 5_000;

/// The label an inline `cache` object is registered under.
const INLINE_CACHE: &str = "mq_bridge_cache";

/// Processors whose whole configuration is one Bloblang string.
const STRING_CONFIGURED: &[&str] = &["mapping", "mutation", "bloblang"];

/// Processors that name a cache resource, which a named middleware may give inline.
const CACHE_USING: &[&str] = &["dedupe", "cached"];

/// One Redpanda processor exported as the `connect_<NAME>` middleware.
pub trait Processor: Send + Sync + 'static {
    const NAME: &'static str;
}

/// The `connect_<processor>` middleware for processor `P`.
pub struct ProcessorMiddleware<P>(PhantomData<P>);

impl<P> Default for ProcessorMiddleware<P> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<P: Processor> MiddlewareFactory for ProcessorMiddleware<P> {
    async fn create(&self, route_name: &str, config: &Value) -> Result<Box<dyn BatchFilter>> {
        let config = step_config(P::NAME, config).map_err(InvalidConfig)?;
        open(config)
            .await
            .with_context(|| format!("route {route_name:?}: connect_{}", P::NAME))
    }

    fn config_schema(&self) -> Option<Value> {
        Some(step_schema(P::NAME))
    }
}

/// The `connect` middleware: a chain of any processors that keep or drop.
#[derive(Debug, Default)]
pub struct ChainMiddleware;

#[async_trait]
impl MiddlewareFactory for ChainMiddleware {
    async fn create(&self, route_name: &str, config: &Value) -> Result<Box<dyn BatchFilter>> {
        let config = chain_config(config).map_err(InvalidConfig)?;
        open(config)
            .await
            .with_context(|| format!("route {route_name:?}: connect middleware"))
    }

    fn config_schema(&self) -> Option<Value> {
        Some(json!({
            "type": "object",
            "title": "Redpanda Connect processors",
            "required": ["processors"],
            "additionalProperties": false,
            "properties": {
                "processors": {
                    "type": "array",
                    "title": "Processors",
                    "description": "Redpanda Connect processors, run in order in one call. \
                                    Each must keep or drop a message, never split it.",
                    "items": { "type": "object" }
                },
                "cache_resources": {
                    "type": "array",
                    "title": "Cache resources",
                    "items": { "type": "object" }
                },
                "rate_limit_resources": {
                    "type": "array",
                    "title": "Rate limit resources",
                    "items": { "type": "object" }
                }
            }
        }))
    }
}

async fn open(config: String) -> Result<Box<dyn BatchFilter>> {
    let go = crate::go_library().map_err(|error| anyhow!(error))?;
    let handle = {
        let go = Arc::clone(&go);
        spawn_blocking(move || go.processor_open(&config))
            .await
            .context("the connect middleware task failed")??
    };
    Ok(Box::new(ProcessorFilter { go, handle }))
}

struct ProcessorFilter {
    go: Arc<GoLibrary>,
    handle: u64,
}

impl ProcessorFilter {
    async fn apply(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<Vec<Option<CanonicalMessage>>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let blob = wire::encode(&messages);
        let go = Arc::clone(&self.go);
        let handle = self.handle;
        let (kept, processed) = spawn_blocking(move || go.processor_apply(handle, &blob))
            .await
            .context("the connect middleware task failed")??;
        if kept.len() != messages.len() {
            bail!(
                "processors returned {} keep flags for {} messages",
                kept.len(),
                messages.len()
            );
        }
        let mut processed = wire::decode(Bytes::from(processed))?.into_iter();
        messages
            .into_iter()
            .zip(kept)
            .map(|(original, flag)| {
                if flag == 0 {
                    return Ok(None);
                }
                let mut message = processed
                    .next()
                    .ok_or_else(|| anyhow!("processors returned fewer messages than they kept"))?;
                message.message_id = original.message_id;
                Ok(Some(message))
            })
            .collect()
    }
}

#[async_trait]
impl BatchFilter for ProcessorFilter {
    async fn on_receive(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<Vec<Option<CanonicalMessage>>> {
        self.apply(messages).await
    }

    async fn on_send(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<Vec<Option<CanonicalMessage>>> {
        self.apply(messages).await
    }
}

impl Drop for ProcessorFilter {
    fn drop(&mut self) {
        let _ = self.go.processor_close(self.handle, CLOSE_TIMEOUT_MS);
    }
}

fn chain_config(config: &Value) -> Result<String> {
    let Value::Object(fields) = config else {
        bail!("the connect middleware needs a configuration object with `processors`");
    };
    let stray: Vec<&str> = fields
        .keys()
        .map(String::as_str)
        .filter(|key| !["processors", "cache_resources", "rate_limit_resources"].contains(key))
        .collect();
    if !stray.is_empty() {
        bail!(
            "the connect middleware takes `processors`, `cache_resources` and \
             `rate_limit_resources`, not {}",
            stray.join(", ")
        );
    }
    match fields.get("processors") {
        Some(Value::Array(steps)) if !steps.is_empty() => {}
        _ => bail!("`processors` must be a non-empty list of Redpanda Connect processors"),
    }
    Ok(serde_json::to_string(config)?)
}

fn step_config(processor: &str, config: &Value) -> Result<String> {
    // A URI carries a string configuration as a parameter named after the
    // processor: `|connect-mapping?mapping=...`.
    let mut config = match config {
        Value::Object(fields) if fields.len() == 1 && fields.contains_key(processor) => {
            fields[processor].clone()
        }
        other => other.clone(),
    };
    let mut document = Map::new();
    if CACHE_USING.contains(&processor) {
        if let Some(cache) = config.get_mut("cache").filter(|cache| cache.is_object()) {
            let mut resource = std::mem::replace(cache, Value::from(INLINE_CACHE));
            resource["label"] = Value::from(INLINE_CACHE);
            document.insert("cache_resources".into(), json!([resource]));
        }
    }
    document.insert("processors".into(), json!([{ processor: config }]));
    Ok(serde_json::to_string(&document)?)
}

fn step_schema(processor: &str) -> Value {
    let title = format!("Redpanda Connect `{processor}` processor");
    if STRING_CONFIGURED.contains(&processor) {
        return json!({
            "type": "object",
            "title": title,
            "description": "A Bloblang mapping, as this one field or given directly as a string.",
            "properties": { processor: { "type": "string", "title": "Bloblang mapping" } }
        });
    }
    let mut schema = json!({
        "type": "object",
        "title": title,
        "description": format!("The `{processor}` processor's own fields."),
    });
    if CACHE_USING.contains(&processor) {
        schema["properties"] = json!({
            "cache": {
                "type": ["string", "object"],
                "title": "Cache",
                "description": "A cache resource, such as `{ memory: {} }` or \
                                `{ redis: { url: ... } }`."
            }
        });
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(document: String) -> Value {
        serde_json::from_str(&document).unwrap()
    }

    #[test]
    fn a_string_configuration_becomes_the_processor() {
        assert_eq!(
            parsed(step_config("mapping", &json!("root = this")).unwrap()),
            json!({ "processors": [ { "mapping": "root = this" } ] })
        );
    }

    #[test]
    fn a_uri_parameter_named_after_the_processor_is_unwrapped() {
        assert_eq!(
            parsed(step_config("mapping", &json!({ "mapping": "root = this" })).unwrap()),
            json!({ "processors": [ { "mapping": "root = this" } ] })
        );
    }

    #[test]
    fn an_inline_cache_becomes_a_resource() {
        let config = json!({ "key": "${! content() }", "cache": { "memory": {} } });
        assert_eq!(
            parsed(step_config("dedupe", &config).unwrap()),
            json!({
                "cache_resources": [ { "label": INLINE_CACHE, "memory": {} } ],
                "processors": [ { "dedupe": { "key": "${! content() }", "cache": INLINE_CACHE } } ]
            })
        );
    }

    #[test]
    fn a_cache_label_is_left_alone() {
        let config = json!({ "key": "k", "cache": "shared" });
        assert_eq!(
            parsed(step_config("dedupe", &config).unwrap()),
            json!({ "processors": [ { "dedupe": config } ] })
        );
    }

    #[test]
    fn every_schema_is_one_the_host_accepts() {
        for processor in ["mapping", "dedupe", "jq"] {
            mq_bridge::support::config_schema::validate(&step_schema(processor)).unwrap();
        }
        mq_bridge::support::config_schema::validate(&ChainMiddleware.config_schema().unwrap())
            .unwrap();
    }

    #[test]
    fn a_chain_passes_through() {
        let config = json!({
            "processors": [ { "mapping": "root = this" } ],
            "cache_resources": [ { "label": "c", "memory": {} } ]
        });
        assert_eq!(parsed(chain_config(&config).unwrap()), config);
    }

    #[test]
    fn a_chain_needs_processors_and_nothing_unknown() {
        for config in [
            json!("root = this"),
            json!({}),
            json!({ "processors": [] }),
            json!({ "processors": [ { "mapping": "root = this" } ], "threads": 2 }),
        ] {
            assert!(chain_config(&config).is_err(), "{config}");
        }
    }
}
