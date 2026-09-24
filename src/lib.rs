//! Redpanda Connect connector compatibility plugin for mq-bridge.
//!
//! mq-bridge owns one end of every stream and Benthos owns the other: a
//! `connect` input is a Benthos stream whose output is mq-bridge, and a
//! `connect` output is one whose input is mq-bridge. See [`config`] for the two
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

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use mq_bridge::errors::{ConsumerError, PublisherError};
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

/// A Go sibling that fails to load is reported by every endpoint the factory
/// creates, not by a panic: `Default` has no other way to fail.
#[derive(Debug)]
pub struct ConnectFactory {
    go: Result<Arc<GoLibrary>, String>,
}

impl Default for ConnectFactory {
    fn default() -> Self {
        Self {
            go: load_go_library()
                .map(Arc::new)
                .map_err(|error| format!("{error:#}")),
        }
    }
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

    async fn create_consumer(
        &self,
        route_name: &str,
        config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        let go = self
            .go
            .clone()
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
        let go = self
            .go
            .clone()
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

#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "connect",
    factory: ConnectFactory,
}
