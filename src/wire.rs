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

use anyhow::{bail, Result};
use mq_bridge::CanonicalMessage;

pub(crate) fn encode(messages: &[CanonicalMessage]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(64 * messages.len());
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

pub(crate) fn decode(blob: &[u8]) -> Result<Vec<CanonicalMessage>> {
    if blob.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = Reader { blob, offset: 0 };
    let count = reader.u32()?;
    let mut messages = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let payload = reader.bytes()?.to_vec();
        let meta_count = reader.u32()?;
        let mut metadata = HashMap::with_capacity(meta_count as usize);
        for _ in 0..meta_count {
            let key = reader.string()?;
            metadata.insert(key, reader.string()?);
        }
        let mut message = CanonicalMessage::from(payload);
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

    fn bytes(&mut self) -> Result<&[u8]> {
        let length = self.u32()? as usize;
        let end = self.offset + length;
        let Some(field) = self.blob.get(self.offset..end) else {
            bail!("truncated message batch");
        };
        self.offset = end;
        Ok(field)
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

    #[test]
    fn round_trips_payloads_and_metadata() {
        let sent = vec![
            message("first", &[("source", "test"), ("kafka_key", "k1")]),
            message("second", &[]),
        ];
        let received = decode(&encode(&sent)).unwrap();

        assert_eq!(received.len(), 2);
        assert_eq!(received[0].payload.as_ref(), b"first");
        assert_eq!(
            received[0].metadata.get("kafka_key").map(String::as_str),
            Some("k1")
        );
        assert!(received[1].metadata.is_empty());
    }

    #[test]
    fn round_trips_binary_payloads_untouched() {
        let sent = vec![message("", &[])];
        let mut sent = sent;
        sent[0].payload = vec![0u8, 159, 146, 150, 255].into();

        let received = decode(&encode(&sent)).unwrap();
        assert_eq!(received[0].payload.as_ref(), &[0u8, 159, 146, 150, 255]);
    }

    #[test]
    fn an_empty_blob_is_an_empty_batch() {
        assert!(decode(&[]).unwrap().is_empty());
        assert!(decode(&encode(&[])).unwrap().is_empty());
    }

    #[test]
    fn truncated_and_overlong_blobs_are_rejected() {
        let blob = encode(&[message("payload", &[("k", "v")])]);
        assert!(decode(&blob[..blob.len() - 3]).is_err());

        let mut trailing = blob.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
    }
}
