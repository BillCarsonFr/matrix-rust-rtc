// Hand-written types for the wasm-bindgen output in `generated/wasm-bindgen/`.
//
// uniffi-bindgen-react-native drives wasm-bindgen with TypeScript output off
// and exposes no flag to turn it on, so the module ships untyped. This stub
// covers what `index.ts` / `index.node.ts` use; the generated
// `matrix_rtc.ts` is `@ts-nocheck` and needs nothing from it. The build
// copies this file next to `index.js` (both in `src/generated` and `dist`).
// Drop it once ubrn emits `index.d.ts` itself.

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly [exportName: string]: unknown;
}

export interface InitOptions {
  module_or_path?: InitInput | Promise<InitInput>;
}

export interface SyncInitOptions {
  module: BufferSource | WebAssembly.Module;
}

/** wasm-bindgen `--target web` entry: fetch/compile/instantiate and wire the imports. */
export default function initAsync(
  module_or_path?: InitOptions | InitInput | Promise<InitInput>,
): Promise<InitOutput>;

export function initSync(module: SyncInitOptions | BufferSource | WebAssembly.Module): InitOutput;

/** uniffi's out-parameter for the call status of every FFI call. */
export class RustCallStatus {
  constructor();
  free(): void;
  code: number;
  errorBuf: Uint8Array;
}
