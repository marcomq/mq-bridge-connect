//! Shared setup for the integration tests.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::OnceLock;

use mq_bridge_connect::ConnectFactory;

static LIBRARY: OnceLock<()> = OnceLock::new();

pub fn go_library() -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let name = if cfg!(target_os = "windows") {
        "mq_bridge_connect_go.dll"
    } else if cfg!(target_os = "macos") {
        "libmq_bridge_connect_go.dylib"
    } else {
        "libmq_bridge_connect_go.so"
    };
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(profile)
        .join(name)
}

/// The test binary lives in `target/<profile>/deps`, so sibling resolution would
/// look one directory too deep.
pub fn factory() -> ConnectFactory {
    let library = go_library();
    assert!(
        library.exists(),
        "{} is missing; build it with `sh scripts/phase0-smoke.sh` first",
        library.display()
    );
    // `set_var` is not thread-safe, and the tests in one binary run in parallel.
    LIBRARY.get_or_init(|| std::env::set_var("MQ_BRIDGE_CONNECT_GO_LIBRARY", &library));
    ConnectFactory::default()
}
