use std::any::Any;
use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use mq_bridge::errors::ConsumerError;
use mq_bridge::traits::{BatchCommitFunc, BoxFuture, MessageConsumer, MessageDisposition};
use mq_bridge::ReceivedBatch;

use crate::go_library::StreamKind;
use crate::stream::GoStream;
use crate::{config, wire};

/// How long Go waits for the first message of a batch. A live route can afford
/// to wait; a draining one gives up sooner so the empty batch ends the route.
const LIVE_WAIT_MS: u32 = 1_000;
const DRAIN_WAIT_MS: u32 = 250;

pub(crate) struct RedpandaConsumer {
    stream: Arc<GoStream>,
    exit_on_empty: bool,
}

pub(crate) async fn create(
    go: Arc<crate::GoLibrary>,
    value: &serde_json::Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let config = config::stream_config(config::Direction::Consumer, value)
        .map_err(|error| anyhow::Error::new(ConsumerError::Permanent(error)))?;
    let stream = GoStream::open(go, StreamKind::Consumer, config).await?;
    Ok(Box::new(RedpandaConsumer {
        stream,
        exit_on_empty: false,
    }))
}

#[async_trait]
impl MessageConsumer for RedpandaConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move { self.stream.close().await }))
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }
        let timeout_ms = if self.exit_on_empty {
            DRAIN_WAIT_MS
        } else {
            LIVE_WAIT_MS
        };
        let max = u32::try_from(max_messages).unwrap_or(u32::MAX);

        let Some((batch_id, blob)) = self
            .stream
            .next_batch(max, timeout_ms)
            .await
            .map_err(ConsumerError::Connection)?
        else {
            return Err(ConsumerError::EndOfStream);
        };

        let messages = wire::decode(Bytes::from(blob)).map_err(ConsumerError::Permanent)?;
        if messages.is_empty() {
            return Ok(ReceivedBatch::empty());
        }

        let expected = messages.len();
        let stream = Arc::clone(&self.stream);
        let commit: BatchCommitFunc = Box::new(move |dispositions| {
            Box::pin(async move {
                if dispositions.len() != expected {
                    bail!(
                        "Redpanda batch commit received {} dispositions for {expected} messages",
                        dispositions.len()
                    );
                }
                let encoded = dispositions.iter().map(disposition_byte).collect();
                stream.commit(batch_id, encoded).await
            })
        });
        Ok(ReceivedBatch { messages, commit })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Mirrors `MQBRP_ACK` / `MQBRP_NACK`. A Benthos source has nowhere to put a
/// reply, so `Reply` acknowledges and the reply payload is dropped; the endpoint
/// documents this rather than failing a route over it.
fn disposition_byte(disposition: &MessageDisposition) -> u8 {
    match disposition {
        MessageDisposition::Nack => 1,
        MessageDisposition::Ack | MessageDisposition::Reply(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mq_bridge::CanonicalMessage;

    #[test]
    fn only_a_nack_rejects_the_source_message() {
        assert_eq!(disposition_byte(&MessageDisposition::Ack), 0);
        assert_eq!(disposition_byte(&MessageDisposition::Nack), 1);
        assert_eq!(
            disposition_byte(&MessageDisposition::Reply(CanonicalMessage::from("reply"))),
            0
        );
    }
}
