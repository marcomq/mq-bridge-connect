use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use mq_bridge::errors::{InvalidConfig, PublisherError};
use mq_bridge::traits::{BoxFuture, MessagePublisher};
use mq_bridge::{CanonicalMessage, SentBatch};

use crate::go_library::StreamKind;
use crate::stream::GoStream;
use crate::{config, wire};

pub(crate) struct ConnectPublisher {
    stream: Arc<GoStream>,
}

pub(crate) async fn create(
    go: Arc<crate::GoLibrary>,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let config =
        config::stream_config(config::Direction::Publisher, value).map_err(InvalidConfig)?;
    let stream = GoStream::open(go, StreamKind::Publisher, config).await?;
    Ok(Box::new(ConnectPublisher { stream }))
}

#[async_trait]
impl MessagePublisher for ConnectPublisher {
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move { self.stream.close().await }))
    }

    /// The Go call blocks until Benthos has delivered the whole batch, so a
    /// successful return means delivered, not merely queued.
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }
        self.stream
            .publish(wire::encode(&messages))
            .await
            .map_err(PublisherError::Retryable)?;
        Ok(SentBatch::Ack)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
