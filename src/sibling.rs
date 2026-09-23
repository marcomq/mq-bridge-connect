use std::path::{Path, PathBuf};

use anyhow::{bail, Context};

pub fn go_library_path() -> anyhow::Result<PathBuf> {
    if let Some(path) = std::env::var_os("MQ_BRIDGE_CONNECT_GO_LIBRARY") {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            bail!("MQ_BRIDGE_CONNECT_GO_LIBRARY must be an absolute path");
        }
        return Ok(path);
    }

    let plugin = current_library_path()?;
    let directory = plugin
        .parent()
        .context("the Rust plugin path has no parent directory")?;
    Ok(directory.join(go_library_filename()))
}

fn go_library_filename() -> &'static Path {
    #[cfg(target_os = "windows")]
    return Path::new("mq_bridge_connect_go.dll");
    #[cfg(target_os = "macos")]
    return Path::new("libmq_bridge_connect_go.dylib");
    #[cfg(all(unix, not(target_os = "macos")))]
    return Path::new("libmq_bridge_connect_go.so");
}

#[cfg(unix)]
fn current_library_path() -> anyhow::Result<PathBuf> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;

    let mut info = std::mem::MaybeUninit::<libc::Dl_info>::zeroed();
    let result = unsafe {
        libc::dladdr(
            current_library_path as *const () as *const libc::c_void,
            info.as_mut_ptr(),
        )
    };
    if result == 0 {
        bail!("dladdr could not resolve the Rust plugin path");
    }
    let info = unsafe { info.assume_init() };
    if info.dli_fname.is_null() {
        bail!("dladdr returned an empty Rust plugin path");
    }
    let path = std::ffi::OsStr::from_bytes(unsafe { CStr::from_ptr(info.dli_fname) }.to_bytes());
    let path = Path::new(path);
    std::fs::canonicalize(path)
        .with_context(|| format!("failed to canonicalize {}", path.display()))
}

#[cfg(windows)]
fn current_library_path() -> anyhow::Result<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::HMODULE;
    use windows_sys::Win32::System::LibraryLoader::{
        GetModuleFileNameW, GetModuleHandleExW, GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
        GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
    };

    let mut module: HMODULE = std::ptr::null_mut();
    let flags =
        GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT;
    let ok = unsafe {
        GetModuleHandleExW(
            flags,
            current_library_path as *const () as *const u16,
            &mut module,
        )
    };
    if ok == 0 {
        bail!("GetModuleHandleExW could not resolve the Rust plugin module");
    }

    let mut buffer = vec![0_u16; 32_768];
    let length = unsafe { GetModuleFileNameW(module, buffer.as_mut_ptr(), buffer.len() as u32) };
    if length == 0 || length as usize >= buffer.len() {
        bail!("GetModuleFileNameW could not resolve the Rust plugin path");
    }
    buffer.truncate(length as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer)))
}
