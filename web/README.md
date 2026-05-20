# web

Browser-first JavaScript bindings for `matrix-rtc-wasm`.

This package is a thin packaging layer around the Rust wasm crate in `crates/matrix-rtc-wasm`. It uses `wasm-pack` to generate runtime bindings into `web/pkg/`, which stays uncommitted.

## Package shape

- `pkg/browser/`: browser-first wasm-pack output built with `--target web`
- `pkg/node/`: Node.js fallback output built with `--target nodejs`
- `test/`: JavaScript smoke tests for package exports and generated bindings

## Build

```bash
cd web
npm run build
```

That runs:

1. `wasm-pack build ../crates/matrix-rtc-wasm --target web`
2. `wasm-pack build ../crates/matrix-rtc-wasm --target nodejs`

## Test

```bash
cd web
npm test
```

The tests are written to skip the runtime smoke check if `pkg/` has not been generated yet.

## Notes

- Generated files are intentionally not committed.
- The package export map favors browser usage by default.
- If you want a published npm name later, a scoped name like `@matrix-org/matrix-rtc-wasm` would be a natural fit.

