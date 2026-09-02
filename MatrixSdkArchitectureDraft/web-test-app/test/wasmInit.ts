// Loads the wasm module in Node (vitest) — the browser entrypoint
// (src/index.web.ts) fetches the .wasm by URL, which Node cannot do.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import initAsync from "../src/generated/wasm-bindgen/index.js";
import bindings from "../src/generated/matrix_rtc.js";

let initialized = false;

export async function initWasm(): Promise<void> {
  if (initialized) return;
  const wasmPath = fileURLToPath(
    new URL("../src/generated/wasm-bindgen/index_bg.wasm", import.meta.url),
  );
  await initAsync({ module_or_path: readFileSync(wasmPath) });
  bindings.initialize();
  initialized = true;
}
