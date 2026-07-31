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
#
# MEDIA=1 builds the media-enabled variant (matrix-rtc-ffi `media` feature),
# which statically links libwebrtc. Consuming apps MUST add `-ObjC` to
# "Other Linker Flags" (libwebrtc's Objective-C categories get dead-stripped
# from the static archive otherwise, aborting at runtime with
# `+[NSString stringForAbslStringView:]: unrecognized selector`). See
# mobile/PACKAGING.md.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/mobile/ios/build"
OUTPUT_DIR="$BUILD_DIR"
FRAMEWORK_NAME="MatrixRtcFFI"

FEATURE_ARGS=()
if [ "${MEDIA:-0}" = "1" ]; then
    FEATURE_ARGS=(--features media)
    echo "Building iOS XCFramework (MEDIA variant: frame streams + publishing)..."
else
    echo "Building iOS XCFramework (slim signalling-only variant; MEDIA=1 for media)..."
fi
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"

mkdir -p "$BUILD_DIR"

# Ensure required targets are installed
echo "Ensuring Rust targets are installed..."
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios

# Build for each target
echo "Building for aarch64-apple-ios (device)..."
cargo build -p matrix-rtc-ffi --release --target aarch64-apple-ios "${FEATURE_ARGS[@]}"

echo "Building for aarch64-apple-ios-sim..."
cargo build -p matrix-rtc-ffi --release --target aarch64-apple-ios-sim "${FEATURE_ARGS[@]}"

DEVICE_LIB="$PROJECT_ROOT/target/aarch64-apple-ios/release/libmatrix_rtc_ffi.a"
SIM_AARCH64_LIB="$PROJECT_ROOT/target/aarch64-apple-ios-sim/release/libmatrix_rtc_ffi.a"

if [ "${MEDIA:-0}" = "1" ]; then
    # No x86_64 (Intel-Mac) simulator slice for the media variant: livekit
    # publishes no libwebrtc for it (their CI builds ios arm64 + arm64-sim
    # only), and webrtc-sys additionally mis-maps x86_64-apple-ios to a
    # nonexistent "ios-device-x64" artifact (404). Simulator support is
    # Apple Silicon only.
    echo "Skipping x86_64-apple-ios (no libwebrtc prebuilt exists for the Intel simulator)"
    SIM_LIB="$SIM_AARCH64_LIB"
else
    echo "Building for x86_64-apple-ios..."
    cargo build -p matrix-rtc-ffi --release --target x86_64-apple-ios "${FEATURE_ARGS[@]}"
    SIM_X86_64_LIB="$PROJECT_ROOT/target/x86_64-apple-ios/release/libmatrix_rtc_ffi.a"
    UNIVERSAL_SIM_LIB="$BUILD_DIR/libmatrix_rtc_ffi_sim.a"
    echo "Creating universal simulator library..."
    lipo -create "$SIM_AARCH64_LIB" "$SIM_X86_64_LIB" -output "$UNIVERSAL_SIM_LIB"
    SIM_LIB="$UNIVERSAL_SIM_LIB"
fi

# Create XCFramework
echo "Creating XCFramework..."
rm -rf "$OUTPUT_DIR/$FRAMEWORK_NAME.xcframework"
xcodebuild -create-xcframework \
  -library "$DEVICE_LIB" \
  -library "$SIM_LIB" \
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
echo "Library sizes:"
du -h "$DEVICE_LIB" "$SIM_LIB"
echo ""
echo "Next steps:"
echo "1. Copy $OUTPUT_DIR/$FRAMEWORK_NAME.xcframework to your Xcode project"
echo "2. Copy Swift bindings from $SWIFT_OUT to your project"
echo "3. Link against $FRAMEWORK_NAME in your build settings"
if [ "${MEDIA:-0}" = "1" ]; then
    echo "4. MEDIA build: add -ObjC to the app target's 'Other Linker Flags'"
    echo "   (libwebrtc's Objective-C categories are dead-stripped otherwise;"
    echo "   if that causes duplicate symbols, use -force_load on the archive)"
fi

