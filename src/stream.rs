//! A safe wrapper over the private stream ABI.
//!
//! Every call into Go blocks: `next_batch` waits for messages, `publish` waits
//! for delivery, `close` waits for the stream to stop. They therefore run on
//! `spawn_blocking` threads and never on an async executor thread. [`Drop`] is
//! the one exception and cannot be: it closes inline, so a stream dropped
//! without [`GoStream::close`] can stall its thread for up to
//! [`CLOSE_TIMEOUT_MS`]. Both endpoints close through `on_disconnect_hook`, so
//! that path is the error path, not the ordinary one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context};
use tokio::task::spawn_blocking;

use crate::go_library::{GoLibrary, StreamKind};

/// How long Go waits for a stream to stop before reporting the timeout.
const CLOSE_TIMEOUT_MS: u32 = 5_000;

/// A running Benthos stream, owned by the Go sibling and addressed by handle.
pub(crate) struct GoStream {
    go: Arc<GoLibrary>,
    /// Zero once closed, so a late call reports a closed stream instead of
    /// addressing a handle Go has already released.
    handle: AtomicU64,
}

impl GoStream {
    pub(crate) async fn open(
        go: Arc<GoLibrary>,
        kind: StreamKind,
        config: String,
    ) -> anyhow::Result<Arc<Self>> {
        let opened = {
            let go = Arc::clone(&go);
            spawn_blocking(move || go.stream_open(kind, &config))
                .await
                .context("the connect stream task failed")??
        };
        Ok(Arc::new(Self {
            go,
            handle: AtomicU64::new(opened),
        }))
    }

    pub(crate) async fn next_batch(
        self: &Arc<Self>,
        max_messages: u32,
        timeout_ms: u32,
    ) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        let handle = self.handle()?;
        let this = Arc::clone(self);
        Ok(
            spawn_blocking(move || this.go.stream_next_batch(handle, max_messages, timeout_ms))
                .await
                .context("the connect stream task failed")??,
        )
    }

    pub(crate) async fn commit(
        self: &Arc<Self>,
        batch_id: u64,
        dispositions: Vec<u8>,
    ) -> anyhow::Result<()> {
        let handle = self.handle()?;
        let this = Arc::clone(self);
        spawn_blocking(move || this.go.stream_commit(handle, batch_id, &dispositions))
            .await
            .context("the connect stream task failed")??;
        Ok(())
    }

    pub(crate) async fn publish(self: &Arc<Self>, batch: Vec<u8>) -> anyhow::Result<()> {
        let handle = self.handle()?;
        let this = Arc::clone(self);
        spawn_blocking(move || this.go.stream_publish(handle, &batch))
            .await
            .context("the connect stream task failed")??;
        Ok(())
    }

    /// Stops the stream. Messages handed to mq-bridge but never committed are
    /// nacked by Go, so the source redelivers them.
    pub(crate) async fn close(self: &Arc<Self>) -> anyhow::Result<()> {
        let handle = self.handle.swap(0, Ordering::AcqRel);
        if handle == 0 {
            return Ok(());
        }
        let this = Arc::clone(self);
        spawn_blocking(move || this.go.stream_close(handle, CLOSE_TIMEOUT_MS))
            .await
            .context("the connect stream task failed")??;
        Ok(())
    }

    fn handle(&self) -> anyhow::Result<u64> {
        match self.handle.load(Ordering::Acquire) {
            0 => bail!("the connect stream is closed"),
            handle => Ok(handle),
        }
    }
}

// A stream dropped without `close` still has to be stopped: its Go goroutine and
// parked messages would otherwise outlive it for the life of the process.
impl Drop for GoStream {
    fn drop(&mut self) {
        let handle = self.handle.swap(0, Ordering::AcqRel);
        if handle != 0 {
            let _ = self.go.stream_close(handle, CLOSE_TIMEOUT_MS);
        }
    }
}
