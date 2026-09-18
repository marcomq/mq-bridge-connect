//! Redpanda Connect connector compatibility plugin for mq-bridge.
//!
//! mq-bridge owns one end of every stream and Benthos owns the other: a
//! `redpanda` input is a Benthos stream whose output is mq-bridge, and a
//! `redpanda` output is one whose input is mq-bridge. See [`config`] for the two
//! configuration forms.

mod config;
mod consumer;
mod go_library;
mod publisher;
mod sibling;
mod stream;
mod wire;

use std::sync::Arc;

pub use go_library::{GoError, GoLibrary, ProbeError, StreamKind};

use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

#[derive(Debug)]
pub struct RedpandaFactory {
    go: Arc<GoLibrary>,
}

impl Default for RedpandaFactory {
    fn default() -> Self {
        let path = sibling::go_library_path().unwrap_or_else(|error| {
            panic!("failed to resolve the Redpanda Go sibling library: {error:#}")
        });
        let go = unsafe { GoLibrary::open(&path) }
            .and_then(|go| go.probe().map(|()| go).map_err(anyhow::Error::from))
            .unwrap_or_else(|error| panic!("failed to initialize {}: {error:#}", path.display()));
        Self { go: Arc::new(go) }
    }
}

#[async_trait]
impl CustomEndpointFactory for RedpandaFactory {
    async fn create_consumer(
        &self,
        route_name: &str,
        config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        consumer::create(Arc::clone(&self.go), config)
            .await
            .map_err(|error| route_context(route_name, "consumer", error))
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        publisher::create(Arc::clone(&self.go), config)
            .await
            .map_err(|error| route_context(route_name, "publisher", error))
    }
}

/// Keeps the route name in the message without re-wrapping the error, so the
/// permanent/retryable classification the constructors made survives.
fn route_context(route_name: &str, direction: &str, error: anyhow::Error) -> anyhow::Error {
    error.context(format!(
        "route {route_name:?}: failed to create the redpanda {direction}"
    ))
}

#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "redpanda",
    factory: RedpandaFactory,
}
