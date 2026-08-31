# matrix-rtc (architecture draft skeleton)

Compilable skeleton of the plan in `../MatrixSdkArchitecture.md`.
Every method body is `todo!()` — this pins down module boundaries and
signatures, not behavior.

```sh
cargo check                                                     # core
cargo check --features uniffi                                   # bindings (native)
cargo check --features uniffi --target wasm32-unknown-unknown   # bindings (web/wasm)
```

`src/uniffi_api` is the single binding surface for **all** platforms:
Swift and Kotlin via uniffi-bindgen, React Native and web/wasm via
[uniffi-bindgen-react-native](https://github.com/jhugman/uniffi-bindgen-react-native)
— so the generated (and documented) API is identical everywhere.

The `driver` module mirrors matrix-rust-sdk's widget `MatrixDriver` so that
implementation can drop in behind a thin adapter;
`uniffi_api::MatrixDriverCallback` is the same seam as a foreign trait, so a
matrix-js-sdk-backed driver implements the identical contract on web.

`web/` holds the uniffi-generated npm package (via
uniffi-bindgen-react-native's wasm target), a demo webapp with a TS
`MatrixDriver` mock, and the acceptance test suite that drives
`ParticipationManager` through injected events — the harness for
implementing the `todo!()`s. See `web/README.md`.
