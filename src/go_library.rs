use std::fmt;
use std::mem::ManuallyDrop;
use std::path::Path;
use std::ptr::NonNull;

use anyhow::{anyhow, bail, Context};
use libloading::Library;

const ABI_MAJOR: u16 = 1;
const ENTRY_SYMBOL: &[u8] = b"mqbrp_get_api_v1\0";
const PROBE_PANIC: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct OwnedBytes {
    ptr: *mut u8,
    len: usize,
}

type ProbeFn = unsafe extern "C" fn(u32, *mut OwnedBytes) -> i32;
type BytesFreeFn = unsafe extern "C" fn(OwnedBytes);

#[repr(C)]
struct ApiV1 {
    struct_size: usize,
    abi_major: u16,
    abi_minor: u16,
    probe: Option<ProbeFn>,
    bytes_free: Option<BytesFreeFn>,
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
    /// The target must be a trusted mq-bridge-redpanda Go sibling library. Loading a
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
        if value.probe.is_none() || value.bytes_free.is_none() {
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
}

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
