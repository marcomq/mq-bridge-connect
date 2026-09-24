"use strict";

const { execFileSync } = require("child_process");
const fs = require("fs");
const path = require("path");
const mqBridge = require("mq-bridge");
const installer = require("./install");
const { name: ENDPOINT_NAME } = require("./mq-bridge-plugin.json");

const INSTALL_HINT =
  "install it with `npx mq-bridge-connect`, `conda install -c marcomq mq-bridge-connect` " +
  "or `brew install marcomq/tap/mq-bridge-connect`, or set MQ_BRIDGE_CONNECT_LIBRARY to the library path";

// conda and brew both put the two libraries side by side in these directories.
function systemLibraryDirs() {
  const dirs = [];
  const conda = process.env.CONDA_PREFIX;
  if (conda) {
    dirs.push(process.platform === "win32"
      ? path.join(conda, "Library", "bin")
      : path.join(conda, "lib", "mq-bridge"));
  }
  if (process.platform !== "win32") {
    for (const prefix of [process.env.HOMEBREW_PREFIX, "/opt/homebrew", "/home/linuxbrew/.linuxbrew", "/usr/local"]) {
      if (prefix) dirs.push(path.join(prefix, "lib", "mq-bridge"));
    }
  }
  return dirs;
}

function findLibrary() {
  if (process.env.MQ_BRIDGE_CONNECT_LIBRARY) return process.env.MQ_BRIDGE_CONNECT_LIBRARY;
  try {
    return mqBridge.pluginLibraryPath(__dirname);
  } catch {}
  const fileName = installer.libraryFileName();
  return [installer.cacheDir(), ...systemLibraryDirs()]
    .filter(Boolean)
    .map((dir) => path.join(dir, fileName))
    .find((candidate) => fs.existsSync(candidate));
}

function libraryPath() {
  const found = findLibrary();
  if (found) return found;
  if (process.env.MQ_BRIDGE_CONNECT_NO_DOWNLOAD) {
    throw new Error(`mq-bridge-connect library not found; ${INSTALL_HINT}`);
  }
  // register() is synchronous, so the download runs in a child process.
  try {
    return execFileSync(process.execPath, [require.resolve("./install")], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "inherit"],
      timeout: installer.DOWNLOAD_TIMEOUT_MS + 60 * 1000,
    }).trim();
  } catch {
    throw new Error(`mq-bridge-connect library could not be downloaded; ${INSTALL_HINT}`);
  }
}

function register() {
  return mqBridge.loadEndpointPlugin(libraryPath());
}

module.exports.ENDPOINT_NAME = ENDPOINT_NAME;
module.exports.install = installer.install;
module.exports.libraryPath = libraryPath;
module.exports.register = register;
