use std::any::Any;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
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
        let responses = self
            .stream
            .publish(wire::encode(&messages))
            .await
            .map_err(PublisherError::Retryable)?;
        let responses = wire::decode_responses(Bytes::from(responses))
            .and_then(|responses| answer(&messages, responses))
            .map_err(PublisherError::NonRetryable)?;
        Ok(if responses.is_empty() {
            SentBatch::Ack
        } else {
            SentBatch::Partial {
                responses: Some(responses),
                failed: Vec::new(),
            }
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Gives each response the `message_id` of the request it answers, which is how
/// mq-bridge pairs a reply with its request.
fn answer(
    requests: &[CanonicalMessage],
    responses: Vec<(usize, CanonicalMessage)>,
) -> anyhow::Result<Vec<CanonicalMessage>> {
    responses
        .into_iter()
        .map(|(index, mut response)| {
            let request = requests.get(index).ok_or_else(|| {
                anyhow!(
                    "a response to request {index} of a batch of {}",
                    requests.len()
                )
            })?;
            response.message_id = request.message_id;
            Ok(response)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_response_takes_the_message_id_of_its_request() {
        let requests: Vec<_> = ["a", "b", "c"].map(CanonicalMessage::from).into();
        let answered = answer(&requests, vec![(2, "C".into()), (0, "A".into())]).unwrap();
        assert_eq!(answered[0].message_id, requests[2].message_id);
        assert_eq!(answered[1].message_id, requests[0].message_id);
        assert_eq!(&answered[0].payload[..], b"C");
    }

    #[test]
    fn a_response_to_a_request_outside_the_batch_is_an_error() {
        let requests = vec![CanonicalMessage::from("a")];
        assert!(answer(&requests, vec![(1, "B".into())]).is_err());
    }
}
