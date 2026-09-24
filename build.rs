//! Fetches the prebuilt Go sibling library for a crates.io build.
//!
//! The release workflow pins the sha256 of every release archive in
//! `checksums.sha256` before publishing. Without that file (a git checkout)
//! nothing is downloaded and the Go library is built by hand, as the README says.

use std::env;
use std::fs;
#[cfg(feature = "download")]
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const GO_LIBRARY_ENV: &str = "MQ_BRIDGE_CONNECT_GO_LIBRARY";
const DOWNLOAD_URL_ENV: &str = "MQ_BRIDGE_CONNECT_DOWNLOAD_URL";
const CHECKSUMS: &str = "checksums.sha256";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={CHECKSUMS}");
    println!("cargo:rerun-if-env-changed={GO_LIBRARY_ENV}");
    println!("cargo:rerun-if-env-changed={DOWNLOAD_URL_ENV}");

    if !cfg!(feature = "download")
        || env::var_os(GO_LIBRARY_ENV).is_some()
        || env::var_os("DOCS_RS").is_some()
    {
        return;
    }
    let Ok(checksums) = fs::read_to_string(CHECKSUMS) else {
        return;
    };

    let version = env::var("CARGO_PKG_VERSION").unwrap();
    let target = env::var("TARGET").unwrap();
    let archive = format!("mq-bridge-connect-{version}-{target}");
    let Some(expected) = pinned_checksum(&checksums, &format!("{archive}.tar.gz")) else {
        println!(
            "cargo:warning=mq-bridge-connect has no prebuilt Go library for {target}; \
             set {GO_LIBRARY_ENV} to one you built"
        );
        return;
    };

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let library = out_dir.join(go_library_filename());
    if !library.exists() {
        let base = env::var(DOWNLOAD_URL_ENV).unwrap_or_else(|_| {
            format!("https://github.com/marcomq/mq-bridge-connect/releases/download/v{version}")
        });
        let url = format!("{base}/{archive}.tar.gz");
        if let Err(error) = fetch(&url, &expected, &out_dir, &library) {
            println!(
                "cargo:warning=mq-bridge-connect could not fetch its Go library from {url}: \
                 {error}; set {GO_LIBRARY_ENV} to the library path at runtime"
            );
            return;
        }
    }

    println!(
        "cargo:rustc-env=MQ_BRIDGE_CONNECT_GO_BUILT={}",
        library.display()
    );
    place_beside_artifacts(&out_dir, &library);
}

fn pinned_checksum(checksums: &str, file: &str) -> Option<String> {
    checksums.lines().find_map(|line| {
        let (digest, name) = line.split_once(char::is_whitespace)?;
        (name.trim().trim_start_matches('*') == file).then(|| digest.to_ascii_lowercase())
    })
}

fn go_library_filename() -> &'static str {
    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("windows") => "mq_bridge_connect_go.dll",
        Ok("macos") => "libmq_bridge_connect_go.dylib",
        _ => "libmq_bridge_connect_go.so",
    }
}

#[cfg(not(feature = "download"))]
fn fetch(_: &str, _: &str, _: &Path, _: &Path) -> Result<(), String> {
    unreachable!("main returns early without the download feature")
}

#[cfg(feature = "download")]
fn fetch(url: &str, expected: &str, out_dir: &Path, library: &Path) -> Result<(), String> {
    use sha2::{Digest, Sha256};

    let download = out_dir.join("archive.tar.gz");
    // Covers connecting and reading the body, so a stalled download only warns.
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(600)))
        .build()
        .into();
    let response = agent.get(url).call().map_err(|error| error.to_string())?;
    let mut body = response.into_body().into_reader();
    let mut file = fs::File::create(&download).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 1 << 20];
    loop {
        let read = body.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        file.write_all(&buffer[..read])
            .map_err(|error| error.to_string())?;
    }
    drop(file);

    let actual: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if actual != expected {
        let _ = fs::remove_file(&download);
        // A tampered or corrupted archive must never be built against.
        panic!("checksum mismatch for {url}: expected {expected}, got {actual}");
    }

    let result = extract(&download, library);
    let _ = fs::remove_file(&download);
    result
}

#[cfg(feature = "download")]
fn extract(download: &Path, library: &Path) -> Result<(), String> {
    let name = library.file_name().unwrap();
    let archive = fs::File::open(download).map_err(|error| error.to_string())?;
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    for entry in archive.entries().map_err(|error| error.to_string())? {
        let mut entry = entry.map_err(|error| error.to_string())?;
        if entry.path().map_err(|error| error.to_string())?.file_name() == Some(name) {
            let partial = library.with_extension("partial");
            let mut file = fs::File::create(&partial).map_err(|error| error.to_string())?;
            io::copy(&mut entry, &mut file).map_err(|error| error.to_string())?;
            return fs::rename(&partial, library).map_err(|error| error.to_string());
        }
    }
    Err(format!(
        "the archive contains no {}",
        name.to_string_lossy()
    ))
}

// `OUT_DIR` is `target/[<triple>/]<profile>/build/<pkg>-<hash>/out`. These copies
// serve executables moved elsewhere; in place they load the `OUT_DIR` library.
fn place_beside_artifacts(out_dir: &Path, library: &Path) {
    let Some(profile_dir) = out_dir.ancestors().nth(3) else {
        return;
    };
    let name = library.file_name().unwrap();
    for dir in [
        profile_dir.to_path_buf(),
        profile_dir.join("deps"),
        profile_dir.join("examples"),
    ] {
        let destination = dir.join(name);
        if fs::create_dir_all(&dir).is_err() {
            continue;
        }
        let _ = fs::remove_file(&destination);
        if fs::hard_link(library, &destination).is_err() {
            let _ = fs::copy(library, &destination);
        }
    }
}
