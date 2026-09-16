// The one place the wasm module is loaded. `index.ts` (browsers, bundlers)
// and `index.node.ts` (the `node` export condition) each supply the default
// source; everything else is shared here.
import initWasmBindgen, { type InitInput } from "./generated/wasm-bindgen/index.js";
import bindings, { installPanicHook } from "./generated/matrix_rtc.js";

/**
 * Where to load `index_bg.wasm` from: a URL or string (fetched), a `Request`,
 * an already-fetched `Response`, raw bytes, or a compiled `WebAssembly.Module`.
 */
export type WasmSource = InitInput;

type State = "idle" | "loading" | "ready";
let state: State = "idle";
let pending: Promise<void> | undefined;

/** `true` once `initAsync` has resolved. */
export function isInitialized(): boolean {
  return state === "ready";
}

/**
 * Builds the entry point's `initAsync`: idempotent, and concurrent callers
 * share the in-flight load. A failed load resets so the next call retries.
 */
export function makeInitAsync(defaultSource: () => WasmSource | Promise<WasmSource>) {
  return async function initAsync(source?: WasmSource | Promise<WasmSource>): Promise<void> {
    if (state === "ready") return;
    if (pending) return pending;
    state = "loading";
    pending = (async () => {
      const module_or_path = source === undefined ? await defaultSource() : await source;
      await initWasmBindgen({ module_or_path });
      // Checksums and callback vtables; the generated code guards against a
      // second call, so a page reload during development is fine.
      bindings.initialize();
      // wasm32 only: report `panic!` messages to the console instead of an
      // opaque `RuntimeError: unreachable`.
      installPanicHook();
      state = "ready";
    })().catch((e) => {
      state = "idle";
      pending = undefined;
      throw e;
    });
    return pending;
  };
}
