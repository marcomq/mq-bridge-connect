/** Endpoint name routes refer to, e.g. `{ custom: { name: "connect", config: {...} } }`. */
export const ENDPOINT_NAME: "connect";

/**
 * Absolute path of the plugin library for this platform.
 *
 * Looks at `MQ_BRIDGE_CONNECT_LIBRARY`, the download cache, and conda and
 * Homebrew installs, then downloads the release archive unless
 * `MQ_BRIDGE_CONNECT_NO_DOWNLOAD` is set. Throws if none of these succeed.
 */
export function libraryPath(): string;

/** Download this version's release archive into the cache; resolves to the library path. */
export function install(): Promise<string>;

/**
 * Register the `connect` endpoint with mq-bridge.
 *
 * Call once, before starting any route that uses it; calling it again is a
 * no-op. Returns the registered endpoint name.
 */
export function register(): string;
