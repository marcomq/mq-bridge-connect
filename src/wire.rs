//! The batch wire format shared with `go-bridge/wire.go`.
//!
//! One length-prefixed blob per batch: one allocation and one free per boundary
//! crossing. All integers are little-endian `u32`.
//!
//! ```text
//! u32 count
//!   u32 payload_len, payload
//!   u32 meta_count
//!     u32 key_len, key, u32 value_len, value   (x meta_count)
//! ```

use std::collections::HashMap;
use std::ops::Range;

use anyhow::{bail, Result};
use bytes::Bytes;
use mq_bridge::CanonicalMessage;

pub(crate) fn encode(messages: &[CanonicalMessage]) -> Vec<u8> {
    // Every length is O(1) to read, so the exact size is cheaper to compute than
    // the reallocation and copying that guessing it costs.
    let size = 4 + messages
        .iter()
        .map(|message| {
            8 + message.payload.len()
                + message
                    .metadata
                    .iter()
                    .map(|(key, value)| 8 + key.len() + value.len())
                    .sum::<usize>()
        })
        .sum::<usize>();
    let mut blob = Vec::with_capacity(size);
    blob.extend_from_slice(&(messages.len() as u32).to_le_bytes());
    for message in messages {
        push_bytes(&mut blob, &message.payload);
        blob.extend_from_slice(&(message.metadata.len() as u32).to_le_bytes());
        for (key, value) in &message.metadata {
            push_bytes(&mut blob, key.as_bytes());
            push_bytes(&mut blob, value.as_bytes());
        }
    }
    blob
}

/// Takes the batch buffer by value so each payload can be a slice of it rather
/// than a copy: `Bytes` is reference-counted, so a batch of 500 costs one
/// allocation here instead of 501. The whole buffer stays alive while any of its
/// messages does, which is the right trade while a batch travels together.
pub(crate) fn decode(blob: Bytes) -> Result<Vec<CanonicalMessage>> {
    if blob.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = Reader {
        blob: &blob,
        offset: 0,
    };
    let count = reader.u32()?;
    let mut messages = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let payload = reader.range()?;
        let meta_count = reader.u32()?;
        let mut metadata = HashMap::with_capacity(meta_count as usize);
        for _ in 0..meta_count {
            let key = reader.string()?;
            metadata.insert(key, reader.string()?);
        }
        let mut message = CanonicalMessage::new_bytes(blob.slice(payload), None);
        message.metadata = metadata;
        messages.push(message);
    }
    if reader.offset != blob.len() {
        bail!(
            "message batch has {} trailing bytes",
            blob.len() - reader.offset
        );
    }
    Ok(messages)
}

fn push_bytes(blob: &mut Vec<u8>, value: &[u8]) {
    blob.extend_from_slice(&(value.len() as u32).to_le_bytes());
    blob.extend_from_slice(value);
}

struct Reader<'a> {
    blob: &'a [u8],
    offset: usize,
}

impl Reader<'_> {
    fn u32(&mut self) -> Result<u32> {
        let end = self.offset + 4;
        let Some(field) = self.blob.get(self.offset..end) else {
            bail!("truncated message batch");
        };
        self.offset = end;
        Ok(u32::from_le_bytes(field.try_into().expect("4 bytes")))
    }

    /// Where the next field sits in the blob, rather than its bytes, so a caller
    /// holding the blob can slice it instead of copying out of it.
    fn range(&mut self) -> Result<Range<usize>> {
        let length = self.u32()? as usize;
        let end = self.offset + length;
        if end > self.blob.len() {
            bail!("truncated message batch");
        }
        Ok(std::mem::replace(&mut self.offset, end)..end)
    }

    fn bytes(&mut self) -> Result<&[u8]> {
        let field = self.range()?;
        Ok(&self.blob[field])
    }

    fn string(&mut self) -> Result<String> {
        Ok(String::from_utf8_lossy(self.bytes()?).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: &str, metadata: &[(&str, &str)]) -> CanonicalMessage {
        let mut message = CanonicalMessage::from(payload.as_bytes().to_vec());
        message.metadata = metadata
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        message
    }

    /// The capacity is computed, not guessed, so it must land exactly: too small
    /// reallocates on every publish, too large wastes the headroom.
    #[test]
    fn the_reserved_capacity_is_exactly_what_encoding_uses() {
        for sent in [
            vec![message("first", &[("source", "test")]), message("x", &[])],
            vec![message("", &[])],
            vec![message(&"p".repeat(4096), &[("k", &"v".repeat(500))])],
            Vec::new(),
        ] {
            let blob = encode(&sent);
            assert_eq!(blob.len(), blob.capacity(), "for {} messages", sent.len());
        }
    }

    #[test]
    fn round_trips_payloads_and_metadata() {
        let sent = vec![
            message("first", &[("source", "test"), ("kafka_key", "k1")]),
            message("second", &[]),
        ];
        let received = decode(Bytes::from(encode(&sent))).unwrap();

        assert_eq!(received.len(), 2);
        assert_eq!(received[0].payload.as_ref(), b"first");
        assert_eq!(
            received[0].metadata.get("kafka_key").map(String::as_str),
            Some("k1")
        );
        assert!(received[1].metadata.is_empty());
    }

    /// Payloads are slices of the batch buffer, not copies of it. A regression
    /// here is invisible in behaviour and costs an allocation and a copy of
    /// every payload in every batch.
    #[test]
    fn decoded_payloads_borrow_the_batch_buffer() {
        let blob = Bytes::from(encode(&[
            message("first", &[("source", "test")]),
            message("second", &[]),
        ]));
        let range = blob.as_ptr() as usize..blob.as_ptr() as usize + blob.len();

        for received in decode(blob.clone()).unwrap() {
            assert!(
                range.contains(&(received.payload.as_ptr() as usize)),
                "payload was copied out of the batch buffer"
            );
        }
    }

    #[test]
    fn round_trips_binary_payloads_untouched() {
        let sent = vec![message("", &[])];
        let mut sent = sent;
        sent[0].payload = vec![0u8, 159, 146, 150, 255].into();

        let received = decode(Bytes::from(encode(&sent))).unwrap();
        assert_eq!(received[0].payload.as_ref(), &[0u8, 159, 146, 150, 255]);
    }

    #[test]
    fn an_empty_blob_is_an_empty_batch() {
        assert!(decode(Bytes::new()).unwrap().is_empty());
        assert!(decode(Bytes::from(encode(&[]))).unwrap().is_empty());
    }

    #[test]
    fn truncated_and_overlong_blobs_are_rejected() {
        let blob = encode(&[message("payload", &[("k", "v")])]);
        assert!(decode(Bytes::from(blob[..blob.len() - 3].to_vec())).is_err());

        let mut trailing = blob.clone();
        trailing.push(0);
        assert!(decode(Bytes::from(trailing)).is_err());
    }
}
