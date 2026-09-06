//! Phase 0 shell for the mq-bridge Redpanda Connect compatibility plugin.

mod go_library;
mod sibling;

pub use go_library::{GoLibrary, ProbeError};

use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

/// Endpoint construction intentionally remains disabled during the release-gate spike.
#[derive(Debug)]
pub struct RedpandaFactory {
    _go: GoLibrary,
}

impl Default for RedpandaFactory {
    fn default() -> Self {
        let path = sibling::go_library_path().unwrap_or_else(|error| {
            panic!("failed to resolve the Redpanda Go sibling library: {error:#}")
        });
        let go = unsafe { GoLibrary::open(&path) }
            .and_then(|go| go.probe().map(|()| go).map_err(anyhow::Error::from))
            .unwrap_or_else(|error| panic!("failed to initialize {}: {error:#}", path.display()));
        Self { _go: go }
    }
}

// The plugin advertises both directions so the ABI shape is fixed early. Without
// these overrides mq-bridge answers with its generic "does not support" default,
// which contradicts the advertised capability and tells the user nothing.
#[async_trait]
impl CustomEndpointFactory for RedpandaFactory {
    async fn create_consumer(
        &self,
        route_name: &str,
        _config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        Err(unimplemented_direction(route_name, "consumer"))
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        _config: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        Err(unimplemented_direction(route_name, "publisher"))
    }
}

fn unimplemented_direction(route_name: &str, direction: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "route {route_name:?}: the redpanda plugin cannot create a {direction} yet. \
         This build is the Phase 0 release-gate spike: it loads Redpanda Connect and \
         proves the FFI boundary works, but implements no data path. It advertises both \
         directions to pin the plugin ABI, not because either is usable."
    )
}

#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "redpanda",
    factory: RedpandaFactory,
}
