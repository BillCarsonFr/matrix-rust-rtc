// Public entry point for browsers and bundlers (Vite, webpack 5, Rollup).
//
// `initAsync()` with no argument resolves the wasm next to this module via
// `new URL(..., import.meta.url)`, which every modern bundler rewrites to the
// emitted asset. Pass a source to control it yourself (a CDN URL, a Vite
// `?url` import of `@element-hq/matrix-rtc/wasm`, prefetched bytes).
export * from "./generated/matrix_rtc.js";
export { isInitialized, type WasmSource } from "./init.js";
import { makeInitAsync } from "./init.js";

/** The URL of the wasm binary as this module sees it. */
export function defaultWasmUrl(): URL {
  return new URL("./generated/wasm-bindgen/index_bg.wasm", import.meta.url);
}

/** Load and initialise the bindings. Idempotent; concurrent callers share one load. */
export const initAsync = makeInitAsync(defaultWasmUrl);
