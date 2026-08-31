/** Endpoint name routes refer to, e.g. `{ custom: { name: "ros2", config: {...} } }`. */
export const ENDPOINT_NAME: "ros2";

/**
 * Absolute path of the bundled plugin library for this platform.
 *
 * Throws if the package has no prebuild for the current platform.
 */
export function libraryPath(): string;

/**
 * Register the `ros2` endpoint with mq-bridge.
 *
 * Call once, before starting any route that uses it; calling it again is a
 * no-op. Returns the registered endpoint name.
 */
export function register(): string;
