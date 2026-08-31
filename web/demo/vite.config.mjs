// Copyright 2026 Valere Fedronic
//
// This file is part of matrix-rust-rtc.
//
// matrix-rust-rtc is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// matrix-rust-rtc is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

import { defineConfig } from 'vite';

export default defineConfig({
  // Both wasm packages load their .wasm relative to their own JS via
  // `new URL(..., import.meta.url)`; esbuild prebundling would relocate the
  // JS away from the wasm and break that.
  optimizeDeps: {
    exclude: ['matrix-rtc-wasm', '@matrix-org/matrix-sdk-crypto-wasm'],
  },
  // Dev server only (build/preview bundle the wasm as assets): the bindings
  // live in the parent package (`web/pkg/browser`), outside this package's
  // workspace root, and vite's filesystem fence 403s the `/@fs/` fetch for
  // them unless the parent is allowed.
  server: {
    fs: { allow: ['..'] },
  },
  build: {
    // Top-level await appears in matrix-sdk-crypto-wasm's module glue.
    target: 'es2022',
  },
  worker: {
    format: 'es',
  },
});
