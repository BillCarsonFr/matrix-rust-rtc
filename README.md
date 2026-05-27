# matrix-rust-rtc

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Note:** This project is developed with AI assistance.

A Rust implementation of a Matrix RTC (Real-Time Communication) client SDK.

This project provides a core RTC SDK in Rust that can be used across multiple platforms, with bindings for web (via WebAssembly) and native mobile platforms (via FFI). This allows maintaining a single codebase for the core RTC functionality while enabling broad platform support.

## Workspace crates

- `crates/matrix-rtc-core`: single-session machine plus room-scoped session manager and MSC4143/MSC4354 event conversion boundary.
- `crates/matrix-rtc-wasm`: wasm bindings that accept room-scoped JS sticky event payloads.
- `web`: browser-first JavaScript package and wasm-pack build/test scaffold for the wasm bindings.
- `crates/matrix-rtc-ffi`: UniFFI-based native bindings around the same room-scoped manager API.
- `mobile/android`: Android Gradle library module and build scripts for AAR packaging.
- `mobile/ios`: iOS Swift Package and build scripts for XCFramework packaging.

## Quick Mobile Builds

To build Android AAR and iOS XCFramework with one command each:

```bash
# Prerequisites
cargo install uniffi_bindgen
cargo install cargo-ndk

# Add required Rust targets
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android

# Build Android AAR
./scripts/build-android-aar.sh

# Build iOS XCFramework
./scripts/build-ios-xcframework.sh
```

See [mobile/PACKAGING.md](mobile/PACKAGING.md) for detailed documentation, integration guides, and CI/CD setup.

## Quick Web Builds

```bash
cd web
npm run build
npm test
```

The `web/` package uses `wasm-pack` to generate browser-first bindings under `web/pkg/`.

## Manual Binding Generation

If you prefer to generate bindings manually without building the full AAR/XCFramework:

```bash
cargo build -p matrix-rtc-ffi --release

# Generate Swift bindings
uniffi-bindgen generate \
  --library target/release/libmatrix_rtc_ffi.dylib \
  --language swift \
  --out-dir ./bindings/swift

# Generate Kotlin bindings
uniffi-bindgen generate \
  --library target/release/libmatrix_rtc_ffi.so \
  --language kotlin \
  --out-dir ./bindings/kotlin
```

## Basic commands

```bash
cargo check
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Pre-commit checklist

Before committing any change, run:

```bash
cargo check
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Then run binding tasks when relevant:

- If changes touch `crates/matrix-rtc-wasm/**` or `web/**`:

```bash
cd web && npm run build
cd web && npm test
```

- If changes touch `crates/matrix-rtc-ffi/**`, `mobile/**`, or `scripts/build-*.sh`:

```bash
./scripts/build-android-aar.sh
./scripts/build-ios-xcframework.sh
```

(`./scripts/build-ios-xcframework.sh` is macOS-only.)

If a required platform/toolchain is not available locally, document the skip reason in the PR description and ensure the corresponding CI job passes before merge.

## License

Licensed under the [AGPL-3.0](LICENSE).

