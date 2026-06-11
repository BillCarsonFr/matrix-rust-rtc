#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# This file is part of matrix-rust-rtc.
#
# matrix-rust-rtc is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# matrix-rust-rtc is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with matrix-rust-rtc.  If not, see <https://www.gnu.org/licenses/>.

set -e

# Build iOS XCFramework from Rust FFI crate
# Targets: aarch64-apple-ios (device) + aarch64-apple-ios-sim + x86_64-apple-ios (simulator)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/mobile/ios/build"
OUTPUT_DIR="$BUILD_DIR"
FRAMEWORK_NAME="MatrixRtcFFI"

echo "Building iOS XCFramework..."
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"

mkdir -p "$BUILD_DIR"

# Ensure required targets are installed
echo "Ensuring Rust targets are installed..."
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

# Build for each target
echo "Building for aarch64-apple-ios (device)..."
cargo build -p matrix-rtc-ffi --release --target aarch64-apple-ios

echo "Building for aarch64-apple-ios-sim..."
cargo build -p matrix-rtc-ffi --release --target aarch64-apple-ios-sim

echo "Building for x86_64-apple-ios..."
cargo build -p matrix-rtc-ffi --release --target x86_64-apple-ios

# Create universal simulator library
DEVICE_LIB="$PROJECT_ROOT/target/aarch64-apple-ios/release/libmatrix_rtc_ffi.a"
SIM_AARCH64_LIB="$PROJECT_ROOT/target/aarch64-apple-ios-sim/release/libmatrix_rtc_ffi.a"
SIM_X86_64_LIB="$PROJECT_ROOT/target/x86_64-apple-ios/release/libmatrix_rtc_ffi.a"
UNIVERSAL_SIM_LIB="$BUILD_DIR/libmatrix_rtc_ffi_sim.a"

echo "Creating universal simulator library..."
lipo -create "$SIM_AARCH64_LIB" "$SIM_X86_64_LIB" -output "$UNIVERSAL_SIM_LIB"

# Create XCFramework
echo "Creating XCFramework..."
xcodebuild -create-xcframework \
  -library "$DEVICE_LIB" \
  -library "$UNIVERSAL_SIM_LIB" \
  -output "$OUTPUT_DIR/$FRAMEWORK_NAME.xcframework"

# Generate Swift bindings
echo "Generating Swift bindings..."
SWIFT_OUT="$PROJECT_ROOT/mobile/ios/generated"
mkdir -p "$SWIFT_OUT"
cargo run -p uniffi-bindgen -- generate \
  --library "$PROJECT_ROOT/target/aarch64-apple-ios/release/libmatrix_rtc_ffi.a" \
  --language swift \
  --out-dir "$SWIFT_OUT"

echo ""
echo "✅ iOS XCFramework built successfully!"
echo ""
echo "Outputs:"
echo "  XCFramework: $OUTPUT_DIR/$FRAMEWORK_NAME.xcframework"
echo "  Swift bindings: $SWIFT_OUT"
echo ""
echo "Next steps:"
echo "1. Copy $OUTPUT_DIR/$FRAMEWORK_NAME.xcframework to your Xcode project"
echo "2. Copy Swift bindings from $SWIFT_OUT to your project"
echo "3. Link against $FRAMEWORK_NAME in your build settings"

