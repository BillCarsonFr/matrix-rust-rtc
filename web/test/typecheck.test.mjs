/*
Copyright 2026 Valere Fedronic

This file is part of matrix-rust-rtc.

matrix-rust-rtc is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version. See <https://www.gnu.org/licenses/>.
*/

// Type-checks the generated declarations through a real consumer
// (types-smoke.ts). Guards the hand-written TS in the crate's ts_types.rs:
// a declaration that stops parsing, a vanished export, or a signature that
// regresses to `any`-shaped nonsense fails here.

import { describe, it, expect } from 'vitest';
import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const smokeFile = fileURLToPath(new URL('./types-smoke.ts', import.meta.url));
const dts = fileURLToPath(new URL('../pkg/browser/matrix_rtc_wasm.d.ts', import.meta.url));

describe('generated TypeScript declarations', () => {
  it('type-check through a consumer', () => {
    if (!existsSync(dts)) {
      console.warn('pkg/browser not built; run `npm run build` first');
      return;
    }
    expect(() =>
      execFileSync(
        'npx',
        ['tsc', '--noEmit', '--strict', '--target', 'es2022', '--module', 'esnext',
         // esnext lib: wasm-bindgen classes declare `[Symbol.dispose]()`.
         '--moduleResolution', 'bundler', '--lib', 'esnext,dom', smokeFile],
        { stdio: 'pipe', encoding: 'utf8' },
      ),
    ).not.toThrow();
  }, 60_000);
});
