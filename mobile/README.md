# Mobile Build Setup

This directory contains build scripts and configuration for packaging the Matrix RTC FFI library for mobile platforms.

## Quick Start

### Install Build Tools

```bash
# Install Rust toolchain additions and build tools
cargo install uniffi_bindgen cargo-ndk

# Add iOS targets
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

# Add Android targets
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
```

### Build Android AAR

```bash
./scripts/build-android-aar.sh
```

Output: `mobile/android/matrixrtc/build/outputs/aar/matrixrtc-release.aar`

### Build iOS XCFramework

```bash
./scripts/build-ios-xcframework.sh
```

Output: `mobile/ios/build/MatrixRtcFFI.xcframework`

## Full Documentation

See [PACKAGING.md](./PACKAGING.md) for complete documentation including:
- Detailed build workflows
- Integration instructions for iOS and Android apps
- CI/CD examples
- Troubleshooting guides

