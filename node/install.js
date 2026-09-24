#!/usr/bin/env node
"use strict";

const crypto = require("crypto");
const fs = require("fs");
const http = require("http");
const https = require("https");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");
const { Transform } = require("stream");
const { pipeline } = require("stream/promises");

const { version } = require("./package.json");
const { library } = require("./mq-bridge-plugin.json");

const TARGETS = {
  "linux-x64": "x86_64-unknown-linux-gnu",
  "linux-arm64": "aarch64-unknown-linux-gnu",
  "darwin-arm64": "aarch64-apple-darwin",
  "win32-x64": "x86_64-pc-windows-msvc",
};
const RELEASE_URL = `https://github.com/marcomq/mq-bridge-connect/releases/download/v${version}`;
// Written by the release workflow from the archives it just built; the
// published package therefore pins exactly the bytes of its own release.
const CHECKSUMS = path.join(__dirname, "checksums.json");
const DOWNLOAD_TIMEOUT_MS = 10 * 60 * 1000;

function libraryFileName() {
  if (process.platform === "win32") return `${library}.dll`;
  if (process.platform === "darwin") return `lib${library}.dylib`;
  return `lib${library}.so`;
}

function target() {
  return TARGETS[`${process.platform}-${process.arch}`];
}

function archiveName(rustTarget) {
  return `mq-bridge-connect-${version}-${rustTarget}`;
}

function cacheRoot() {
  if (process.env.MQ_BRIDGE_CONNECT_CACHE) return process.env.MQ_BRIDGE_CONNECT_CACHE;
  const home = os.homedir();
  const base = process.platform === "win32"
    ? process.env.LOCALAPPDATA || path.join(home, "AppData", "Local")
    : process.platform === "darwin"
      ? path.join(home, "Library", "Caches")
      : process.env.XDG_CACHE_HOME || path.join(home, ".cache");
  return path.join(base, "mq-bridge-connect");
}

/** Directory the libraries of this package version are cached in, if the platform has a release. */
function cacheDir() {
  const rustTarget = target();
  return rustTarget ? path.join(cacheRoot(), archiveName(rustTarget)) : undefined;
}

function expectedChecksum(rustTarget) {
  let checksums = {};
  try {
    checksums = JSON.parse(fs.readFileSync(CHECKSUMS, "utf8"));
  } catch {}
  return checksums[rustTarget];
}

// Git Bash on Windows puts GNU tar first on PATH, which reads `C:` as a host.
function tarCommand() {
  if (process.platform !== "win32") return "tar";
  return path.join(process.env.SystemRoot || "C:\\Windows", "System32", "tar.exe");
}

// Core http(s) rather than fetch: undici can hit an internal assertion when the
// server closes the connection while the body stream is paused.
function get(url, signal, redirects = 5) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https:") ? https : http;
    client.get(url, { signal }, (response) => {
      const { statusCode, headers } = response;
      if (statusCode >= 300 && statusCode < 400 && headers.location && redirects > 0) {
        response.resume();
        resolve(get(new URL(headers.location, url).href, signal, redirects - 1));
      } else if (statusCode !== 200) {
        response.resume();
        reject(new Error(`downloading ${url} failed: HTTP ${statusCode}`));
      } else {
        resolve(response);
      }
    }).on("error", reject);
  });
}

async function download(url, destination) {
  // One deadline for headers and body; aborting destroys the request and response.
  const signal = AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS);
  const response = await get(url, signal);
  const hash = crypto.createHash("sha256");
  const hashing = new Transform({
    transform(chunk, _encoding, callback) {
      hash.update(chunk);
      callback(null, chunk);
    },
  });
  await pipeline(response, hashing, fs.createWriteStream(destination), { signal });
  return hash.digest("hex");
}

/** Download and unpack the release archive for this platform; resolves to the plugin library path. */
async function install() {
  const rustTarget = target();
  if (!rustTarget) {
    throw new Error(`no prebuilt libraries are released for ${process.platform}-${process.arch}`);
  }
  const dir = cacheDir();
  const libraryFile = path.join(dir, libraryFileName());
  if (fs.existsSync(libraryFile)) return libraryFile;

  const expected = expectedChecksum(rustTarget);
  if (!expected) {
    throw new Error(`${CHECKSUMS} pins no checksum for ${rustTarget}; only released packages can download`);
  }
  const base = process.env.MQ_BRIDGE_CONNECT_DOWNLOAD_URL || RELEASE_URL;
  const url = `${base}/${archiveName(rustTarget)}.tar.gz`;

  fs.mkdirSync(cacheRoot(), { recursive: true });
  const work = fs.mkdtempSync(path.join(cacheRoot(), ".download-"));
  try {
    const actual = await download(url, path.join(work, "archive.tar.gz"));
    if (actual !== expected) {
      throw new Error(`checksum mismatch for ${url}: expected ${expected}, got ${actual}`);
    }
    execFileSync(tarCommand(), ["-xzf", "archive.tar.gz"], { cwd: work, stdio: "inherit" });
    try {
      fs.renameSync(path.join(work, archiveName(rustTarget)), dir);
    } catch (error) {
      // Another process finished the same download first.
      if (!fs.existsSync(libraryFile)) throw error;
    }
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
  if (!fs.existsSync(libraryFile)) throw new Error(`${url} does not contain ${libraryFileName()}`);
  return libraryFile;
}

module.exports = { DOWNLOAD_TIMEOUT_MS, cacheDir, install, libraryFileName };

if (require.main === module) {
  install().then(
    (libraryFile) => console.log(libraryFile),
    (error) => {
      console.error(`mq-bridge-connect: ${error.message}`);
      process.exitCode = 1;
    },
  );
}
