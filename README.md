# matrix-rust-rtc

Initial Rust workspace for a Matrix RTC SDK skeleton.

## Workspace crates

- `crates/matrix-rtc-core`: single-session machine plus room-scoped session manager and MSC4143/MSC4354 event conversion boundary.
- `crates/matrix-rtc-wasm`: wasm bindings that accept room-scoped JS sticky event payloads.
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
