use std::path::PathBuf;

use anyhow::{bail, Context};
use mq_bridge::plugin::load_endpoint_plugin;
use mq_bridge_connect::GoLibrary;

const DIRECT_LOAD_CYCLES: usize = 8;

fn main() -> anyhow::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    let plugin = PathBuf::from(
        arguments
            .next()
            .context("usage: phase0-smoke <rust-plugin> <go-library>")?,
    );
    let go_library = PathBuf::from(
        arguments
            .next()
            .context("usage: phase0-smoke <rust-plugin> <go-library>")?,
    );
    if arguments.next().is_some() {
        bail!("usage: phase0-smoke <rust-plugin> <go-library>");
    }

    // Handles are dropped, which must not unload the Go runtime: `GoLibrary` keeps
    // the library resident for the life of the process. Unloading it aborts with
    // `morestack on g0` in roughly a fifth of runs.
    let mut loaded = Vec::with_capacity(DIRECT_LOAD_CYCLES);
    for cycle in 0..DIRECT_LOAD_CYCLES {
        let go = unsafe { GoLibrary::open(&go_library) }
            .with_context(|| format!("direct Go load cycle {cycle}"))?;
        go.probe()
            .with_context(|| format!("ResourceBuilder probe cycle {cycle}"))?;
        let panic = go
            .probe_panic()
            .expect_err("the panic probe must return an error");
        if !panic.message.contains("phase-0 probe panic") {
            bail!("panic probe returned an unexpected diagnostic: {panic}");
        }
        loaded.push(go);
    }

    // Each handle keeps answering after the later loads, then all are dropped at
    // once: the runtime must stay usable through the plugin load below.
    for (cycle, go) in loaded.iter().enumerate() {
        go.probe()
            .with_context(|| format!("re-probe of retained handle {cycle}"))?;
    }
    drop(loaded);

    let survivor = unsafe { GoLibrary::open(&go_library) }
        .context("reopening the Go library after dropping every handle")?;
    survivor
        .probe()
        .context("probing the Go library after dropping every handle")?;

    let info = load_endpoint_plugin(&plugin)
        .with_context(|| format!("failed to load Rust plugin {}", plugin.display()))?;
    if info.name != "connect" {
        bail!("loaded endpoint name {:?}, expected connect", info.name);
    }
    if !(info.supports_consumer && info.supports_publisher) {
        bail!("the Phase 0 plugin must advertise both future endpoint directions");
    }
    if info.abi_major != 1 || info.abi_minor < 1 {
        bail!(
            "plugin exports ABI {}.{}, expected 1.1 or a later 1.x",
            info.abi_major,
            info.abi_minor
        );
    }
    // The schema slot only exists from ABI 1.1, so this proves the host read it.
    if info.endpoint_schema().is_none() {
        bail!("plugin exported no configuration schema through ABI 1.1");
    }

    println!(
        "phase-0 smoke passed: {DIRECT_LOAD_CYCLES} Go reloads, panic recovery, Rust plugin {} (ABI {}.{})",
        info.version, info.abi_major, info.abi_minor
    );
    Ok(())
}
