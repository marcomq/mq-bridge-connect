//! Redpanda Connect connector compatibility plugin for mq-bridge.
//!
//! mq-bridge owns one end of every stream and Benthos owns the other: a
//! `connect` input is a Benthos stream whose output is mq-bridge, and a
//! `connect` output is one whose input is mq-bridge. See [`config`] for the two
//! configuration forms.
//!
//! The processors that keep or drop a message are also exported as
//! middlewares; see [`middleware`].

mod config;
mod consumer;
mod go_library;
#[cfg(feature = "plugin")]
pub mod middleware;
mod publisher;
mod sibling;
mod stream;
mod wire;

use std::sync::{Arc, OnceLock};

pub use go_library::{GoError, GoLibrary, ProbeError, StreamKind};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mq_bridge::errors::{ConsumerError, PublisherError};
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

/// Loads the Go sibling when the first endpoint is created, so a host that only
/// registers the plugin never starts Go. A failed load is reported by every endpoint.
#[derive(Debug, Default)]
pub struct ConnectFactory;

/// [`go_library`] off the async runtime: the first call opens the library and starts Go.
async fn go_library_blocking() -> Result<Arc<GoLibrary>, String> {
    tokio::task::spawn_blocking(go_library)
        .await
        .map_err(|error| format!("loading the Go sibling library failed: {error}"))?
}

/// One Go runtime per process, shared by the endpoint and every middleware: Go
/// cannot be unloaded, so a second load would gain nothing.
fn go_library() -> Result<Arc<GoLibrary>, String> {
    static GO: OnceLock<Result<Arc<GoLibrary>, String>> = OnceLock::new();
    GO.get_or_init(|| {
        load_go_library()
            .map(Arc::new)
            .map_err(|error| format!("{error:#}"))
    })
    .clone()
}

fn load_go_library() -> anyhow::Result<GoLibrary> {
    let path = sibling::go_library_path().context("failed to resolve the Go sibling library")?;
    unsafe { GoLibrary::open(&path) }
        .and_then(|go| go.probe().map(|()| go).map_err(anyhow::Error::from))
        .with_context(|| format!("failed to initialize {}", path.display()))
}

#[async_trait]
impl CustomEndpointFactory for ConnectFactory {
    fn config_schema(&self) -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    fn acknowledges(&self, config: &serde_json::Value) -> bool {
        config::acknowledges(config)
    }

    async fn create_consumer(
        &self,
        route_name: &str,
        config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        let go = go_library_blocking()
            .await
            .map_err(|error| anyhow::Error::new(ConsumerError::Permanent(anyhow!(error))));
        let created = match go {
            Ok(go) => consumer::create(go, config).await,
            Err(error) => Err(error),
        };
        created.map_err(|error| route_context(route_name, "consumer", error))
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        let go = go_library_blocking()
            .await
            .map_err(|error| anyhow::Error::new(PublisherError::NonRetryable(anyhow!(error))));
        let created = match go {
            Ok(go) => publisher::create(go, config).await,
            Err(error) => Err(error),
        };
        created.map_err(|error| route_context(route_name, "publisher", error))
    }
}

/// Keeps the route name in the message without re-wrapping the error, so the
/// permanent/retryable classification the constructors made survives.
fn route_context(route_name: &str, direction: &str, error: anyhow::Error) -> anyhow::Error {
    error.context(format!(
        "route {route_name:?}: failed to create the connect {direction}"
    ))
}

/// Exports the `connect` endpoint and middleware, plus one `connect_<processor>`
/// middleware per listed processor.
#[cfg(feature = "plugin")]
macro_rules! export_connect_plugins {
    ($($marker:ident => $processor:literal),* $(,)?) => {
        $(
            #[doc = concat!("The `", $processor, "` processor, as the `connect_", $processor,
                            "` middleware.")]
            #[derive(Debug, Default)]
            pub struct $marker;

            impl middleware::Processor for $marker {
                const NAME: &'static str = $processor;
            }
        )*

        mq_bridge::export_endpoint_plugins! {
            { name: "connect", factory: ConnectFactory, middleware: middleware::ChainMiddleware },
            $({
                name: concat!("connect_", $processor),
                factory: mq_bridge::plugin::sdk::NoEndpoint,
                middleware: middleware::ProcessorMiddleware<$marker>,
                capabilities: mq_bridge::plugin::sdk::CAPABILITIES_MIDDLEWARE_ONLY,
            }),*
        }
    };
}

#[cfg(feature = "plugin")]
export_connect_plugins! {
    Mapping => "mapping",
    Mutation => "mutation",
    Bloblang => "bloblang",
    Jq => "jq",
    Jmespath => "jmespath",
    Grok => "grok",
    JsonSchema => "json_schema",
    ParseLog => "parse_log",
    Avro => "avro",
    Msgpack => "msgpack",
    Javascript => "javascript",
    Http => "http",
    Branch => "branch",
    Cached => "cached",
    Dedupe => "dedupe",
    Log => "log",
}
