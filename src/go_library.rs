use std::fmt;
use std::mem::ManuallyDrop;
use std::path::Path;
use std::ptr::NonNull;

use anyhow::{anyhow, bail, Context};
use libloading::Library;

const ABI_MAJOR: u16 = 1;
const STATUS_OK: i32 = 0;
const STATUS_END_OF_STREAM: i32 = 4;
const ENTRY_SYMBOL: &[u8] = b"mqbrp_get_api_v1\0";
const PROBE_PANIC: u32 = 1;

/// Which end of a Benthos stream mq-bridge occupies. Mirrors `MQBRP_KIND_*`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    /// Benthos owns the input; mq-bridge reads what comes out.
    Consumer = 0,
    /// Benthos owns the output; mq-bridge writes what goes in.
    Publisher = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct OwnedBytes {
    ptr: *mut u8,
    len: usize,
}

type ProbeFn = unsafe extern "C" fn(u32, *mut OwnedBytes) -> i32;
type BytesFreeFn = unsafe extern "C" fn(OwnedBytes);
type StreamOpenFn = unsafe extern "C" fn(u32, *const u8, usize, *mut u64, *mut OwnedBytes) -> i32;
type StreamNextBatchFn =
    unsafe extern "C" fn(u64, u32, u32, *mut u64, *mut OwnedBytes, *mut OwnedBytes) -> i32;
type StreamCommitFn = unsafe extern "C" fn(u64, u64, *const u8, usize, *mut OwnedBytes) -> i32;
type StreamPublishFn = unsafe extern "C" fn(u64, *const u8, usize, *mut OwnedBytes) -> i32;
type StreamCloseFn = unsafe extern "C" fn(u64, u32, *mut OwnedBytes) -> i32;

#[repr(C)]
struct ApiV1 {
    struct_size: usize,
    abi_major: u16,
    abi_minor: u16,
    probe: Option<ProbeFn>,
    bytes_free: Option<BytesFreeFn>,
    stream_open: Option<StreamOpenFn>,
    stream_next_batch: Option<StreamNextBatchFn>,
    stream_commit: Option<StreamCommitFn>,
    stream_publish: Option<StreamPublishFn>,
    stream_close: Option<StreamCloseFn>,
}

/// A loaded Go runtime and its versioned private ABI table.
///
/// The library is deliberately never unloaded. Go does not support `dlclose` of a
/// `c-shared` runtime: unloading and reloading it aborts the process with
/// `fatal error: morestack on g0`. Dropping a `GoLibrary` therefore releases only
/// the handle, and the mapping stays resident until the process exits.
pub struct GoLibrary {
    api: NonNull<ApiV1>,
    _library: ManuallyDrop<Library>,
}

// The table is immutable, and all exported Go entry points are safe to call concurrently.
unsafe impl Send for GoLibrary {}
unsafe impl Sync for GoLibrary {}

impl fmt::Debug for GoLibrary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let api = unsafe { self.api.as_ref() };
        formatter
            .debug_struct("GoLibrary")
            .field("abi_major", &api.abi_major)
            .field("abi_minor", &api.abi_minor)
            .finish_non_exhaustive()
    }
}

impl GoLibrary {
    /// Loads and validates the private ABI from an explicit library path.
    ///
    /// # Safety
    ///
    /// The target must be a trusted mq-bridge-connect Go sibling library. Loading a
    /// dynamic library executes its initialization code.
    pub unsafe fn open(path: &Path) -> anyhow::Result<Self> {
        let library = unsafe { Library::new(path) }
            .with_context(|| format!("failed to load {}", path.display()))?;
        let get_api =
            unsafe { library.get::<unsafe extern "C" fn() -> *const ApiV1>(ENTRY_SYMBOL) }
                .context("missing mqbrp_get_api_v1")?;
        let api = NonNull::new(unsafe { get_api() }.cast_mut())
            .ok_or_else(|| anyhow!("mqbrp_get_api_v1 returned null"))?;
        let value = unsafe { api.as_ref() };

        if value.struct_size < std::mem::size_of::<ApiV1>() {
            bail!(
                "private ABI table is too small: got {}, need {}",
                value.struct_size,
                std::mem::size_of::<ApiV1>()
            );
        }
        if value.abi_major != ABI_MAJOR {
            bail!(
                "unsupported private ABI {}.{}, expected {}.x",
                value.abi_major,
                value.abi_minor,
                ABI_MAJOR
            );
        }
        if value.probe.is_none()
            || value.bytes_free.is_none()
            || value.stream_open.is_none()
            || value.stream_next_batch.is_none()
            || value.stream_commit.is_none()
            || value.stream_publish.is_none()
            || value.stream_close.is_none()
        {
            bail!("private ABI table contains a null required function");
        }

        Ok(Self {
            api,
            _library: ManuallyDrop::new(library),
        })
    }

    /// Builds and closes an empty Benthos resource manager in the Go sibling.
    pub fn probe(&self) -> Result<(), ProbeError> {
        self.call_probe(0)
    }

    /// Exercises the Go export boundary's panic recovery.
    pub fn probe_panic(&self) -> Result<(), ProbeError> {
        self.call_probe(PROBE_PANIC)
    }

    fn call_probe(&self, behavior: u32) -> Result<(), ProbeError> {
        let api = unsafe { self.api.as_ref() };
        let mut error = OwnedBytes::default();
        let status = unsafe { api.probe.expect("validated probe pointer")(behavior, &mut error) };
        let message = if error.ptr.is_null() || error.len == 0 {
            String::new()
        } else {
            let bytes = unsafe { std::slice::from_raw_parts(error.ptr, error.len) };
            String::from_utf8_lossy(bytes).into_owned()
        };
        unsafe { api.bytes_free.expect("validated free pointer")(error) };

        if status == 0 {
            Ok(())
        } else {
            Err(ProbeError { status, message })
        }
    }

    fn api(&self) -> &ApiV1 {
        unsafe { self.api.as_ref() }
    }

    /// Copies an owned buffer out of Go and releases the Go-side allocation.
    fn take(&self, value: OwnedBytes) -> Vec<u8> {
        let copied = if value.ptr.is_null() || value.len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(value.ptr, value.len) }.to_vec()
        };
        unsafe { self.api().bytes_free.expect("validated free pointer")(value) };
        copied
    }

    fn message(&self, error: OwnedBytes) -> String {
        String::from_utf8_lossy(&self.take(error)).into_owned()
    }

    /// Builds a Benthos stream and starts running it.
    pub fn stream_open(&self, kind: StreamKind, config: &str) -> Result<u64, GoError> {
        let mut handle = 0u64;
        let mut error = OwnedBytes::default();
        let status = unsafe {
            self.api().stream_open.expect("validated stream_open")(
                kind as u32,
                config.as_ptr(),
                config.len(),
                &mut handle,
                &mut error,
            )
        };
        self.result("stream_open", status, error).map(|()| handle)
    }

    /// Waits up to `timeout_ms` for the next batch. `Ok(None)` means the stream
    /// ended; an empty batch means the wait elapsed with nothing to report.
    pub fn stream_next_batch(
        &self,
        handle: u64,
        max_messages: u32,
        timeout_ms: u32,
    ) -> Result<Option<(u64, Vec<u8>)>, GoError> {
        let mut batch_id = 0u64;
        let mut batch = OwnedBytes::default();
        let mut error = OwnedBytes::default();
        let status = unsafe {
            self.api()
                .stream_next_batch
                .expect("validated stream_next_batch")(
                handle,
                max_messages,
                timeout_ms,
                &mut batch_id,
                &mut batch,
                &mut error,
            )
        };
        if status == STATUS_END_OF_STREAM {
            self.take(batch);
            self.take(error);
            return Ok(None);
        }
        let payload = self.take(batch);
        self.result("stream_next_batch", status, error)
            .map(|()| Some((batch_id, payload)))
    }

    /// Releases the acknowledgements parked for `batch_id`, one per message.
    pub fn stream_commit(
        &self,
        handle: u64,
        batch_id: u64,
        dispositions: &[u8],
    ) -> Result<(), GoError> {
        let mut error = OwnedBytes::default();
        let status = unsafe {
            self.api().stream_commit.expect("validated stream_commit")(
                handle,
                batch_id,
                dispositions.as_ptr(),
                dispositions.len(),
                &mut error,
            )
        };
        self.result("stream_commit", status, error)
    }

    /// Writes a batch into the stream, blocking until it is delivered or fails.
    pub fn stream_publish(&self, handle: u64, batch: &[u8]) -> Result<(), GoError> {
        let mut error = OwnedBytes::default();
        let status = unsafe {
            self.api().stream_publish.expect("validated stream_publish")(
                handle,
                batch.as_ptr(),
                batch.len(),
                &mut error,
            )
        };
        self.result("stream_publish", status, error)
    }

    /// Stops the stream and releases the handle. Anything still parked is nacked.
    pub fn stream_close(&self, handle: u64, timeout_ms: u32) -> Result<(), GoError> {
        let mut error = OwnedBytes::default();
        let status = unsafe {
            self.api().stream_close.expect("validated stream_close")(handle, timeout_ms, &mut error)
        };
        self.result("stream_close", status, error)
    }

    fn result(
        &self,
        operation: &'static str,
        status: i32,
        error: OwnedBytes,
    ) -> Result<(), GoError> {
        let message = self.message(error);
        if status == STATUS_OK {
            Ok(())
        } else {
            Err(GoError {
                operation,
                status,
                message,
            })
        }
    }
}

/// A failed call into the Go sibling, carrying the diagnostic Go produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoError {
    pub operation: &'static str,
    pub status: i32,
    pub message: String,
}

impl fmt::Display for GoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(
                formatter,
                "{} failed with status {}",
                self.operation, self.status
            )
        } else {
            write!(formatter, "{}: {}", self.operation, self.message)
        }
    }
}

impl std::error::Error for GoError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeError {
    pub status: i32,
    pub message: String,
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(formatter, "Go probe failed with status {}", self.status)
        } else {
            write!(
                formatter,
                "Go probe failed with status {}: {}",
                self.status, self.message
            )
        }
    }
}

impl std::error::Error for ProbeError {}
