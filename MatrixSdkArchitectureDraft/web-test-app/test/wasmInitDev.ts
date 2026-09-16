// Loads the test-only build (`src/generated-dev`, `npm run build:dev`), which
// carries the `runtime-probe` feature. Only test/runtimeProbe.test.ts uses it;
// everything else runs against the published build via ./wasmInit.ts.
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export type DevBindings = typeof import("../src/generated-dev/matrix_rtc.js");

const wasmUrl = new URL("../src/generated-dev/wasm-bindgen/index_bg.wasm", import.meta.url);

/** `false` until `npm run build:dev` has produced the probe build. */
export const devBuildPresent = existsSync(fileURLToPath(wasmUrl));

let loaded: Promise<DevBindings> | undefined;

/** Initialises the dev build once and hands back its module namespace. */
export function initWasm(): Promise<DevBindings> {
  loaded ??= (async () => {
    const { default: initAsync } = await import("../src/generated-dev/wasm-bindgen/index.js");
    const bindings: DevBindings = await import("../src/generated-dev/matrix_rtc.js");
    await initAsync({ module_or_path: readFileSync(fileURLToPath(wasmUrl)) });
    bindings.default.initialize();
    return bindings;
  })();
  return loaded;
}
