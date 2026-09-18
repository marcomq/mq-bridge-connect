//! Shared setup for the integration tests.

#![allow(dead_code)]

use std::path::PathBuf;

use mq_bridge_redpanda::RedpandaFactory;

pub fn go_library() -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let name = if cfg!(target_os = "windows") {
        "mq_bridge_redpanda_go.dll"
    } else if cfg!(target_os = "macos") {
        "libmq_bridge_redpanda_go.dylib"
    } else {
        "libmq_bridge_redpanda_go.so"
    };
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(profile)
        .join(name)
}

/// The test binary lives in `target/<profile>/deps`, so sibling resolution would
/// look one directory too deep.
pub fn factory() -> RedpandaFactory {
    let library = go_library();
    assert!(
        library.exists(),
        "{} is missing; build it with `sh scripts/phase0-smoke.sh` first",
        library.display()
    );
    std::env::set_var("MQ_BRIDGE_REDPANDA_GO_LIBRARY", &library);
    RedpandaFactory::default()
}
