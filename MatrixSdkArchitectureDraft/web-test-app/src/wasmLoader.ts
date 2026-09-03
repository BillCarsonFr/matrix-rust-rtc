// Vite-friendly wasm init. The ubrn-generated entrypoint (src/index.web.ts)
// imports the .wasm directly, which vite reserves for WebAssembly-ESM; the
// `?url` import gives us the served asset URL instead, which the
// wasm-bindgen init fetches.
import initAsync from "./generated/wasm-bindgen/index.js";
import wasmUrl from "./generated/wasm-bindgen/index_bg.wasm?url";
import bindings from "./generated/matrix_rtc.js";

export async function initWasm(): Promise<void> {
  await initAsync({ module_or_path: wasmUrl });
  bindings.initialize();
}
