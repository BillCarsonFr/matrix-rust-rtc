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

# Build the iOS XCFramework from the matrix-rtc-ffi crate.
#
#   scripts/build-ios-xcframework.sh [options]
#
# Options:
#   --profile <name>    Cargo profile (default: release; the release workflow
#                       passes mobile-release, defined in the root Cargo.toml).
#   --swift-out <dir>   Where the generated Swift goes (default:
#                       mobile/ios/generated; the release workflow passes
#                       Sources/MatrixRtc, the Swift package's target).
#   --zip               Also produce MatrixRtcFFI.xcframework.zip and its
#                       SHA-256 (the value Package.swift's binaryTarget needs)
#                       next to the xcframework.
#   --keep-debug        Keep DWARF debug info in the archives. By default it is
#                       stripped (`strip -S`, symbol names stay): it is about
#                       two thirds of the archive and only reaches an app's
#                       dSYM, where crash reports still resolve to Rust
#                       function names without it.
#
# Environment:
#   MEDIA=1  builds the media-enabled variant (matrix-rtc-ffi `media` feature),
#            which statically links libwebrtc. Consuming apps MUST add `-ObjC`
#            to "Other Linker Flags" (libwebrtc's Objective-C categories get
#            dead-stripped from the static archive otherwise, aborting at
#            runtime with `+[NSString stringForAbslStringView:]: unrecognized
#            selector`). See mobile/PACKAGING.md.
#
# Targets: aarch64-apple-ios (device) + aarch64-apple-ios-sim, plus
# x86_64-apple-ios (Intel simulator) for the slim variant only.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/mobile/ios/build"
FRAMEWORK_NAME="MatrixRtcFFI"
LIB_NAME="libmatrix_rtc_ffi.a"

# uniffi-bindgen locates crates/matrix-rtc-ffi/uniffi.toml through `cargo
# metadata` of the current directory.
cd "$PROJECT_ROOT"

PROFILE="release"
SWIFT_OUT="$PROJECT_ROOT/mobile/ios/generated"
ZIP=0
STRIP=1

usage() {
    sed -n '/^# Build the iOS XCFramework/,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --profile) PROFILE="$2"; shift 2 ;;
        --swift-out)
            case "$2" in
                /*) SWIFT_OUT="$2" ;;
                *) SWIFT_OUT="$PROJECT_ROOT/$2" ;;
            esac
            shift 2 ;;
        --zip) ZIP=1; shift ;;
        --keep-debug) STRIP=0; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

case "$PROFILE" in
    dev) PROFILE_DIR="debug" ;;
    *) PROFILE_DIR="$PROFILE" ;;
esac

FEATURE_ARGS=()
DEVICE_TARGET="aarch64-apple-ios"
SIM_TARGETS=(aarch64-apple-ios-sim)
if [ "${MEDIA:-0}" = "1" ]; then
    FEATURE_ARGS=(--features media)
    echo "Building iOS XCFramework (MEDIA variant: frame streams + publishing)..."
    # No x86_64 (Intel-Mac) simulator slice for the media variant: livekit
    # publishes no libwebrtc for it (their CI builds ios arm64 + arm64-sim
    # only), and webrtc-sys additionally mis-maps x86_64-apple-ios to a
    # nonexistent "ios-device-x64" artifact (404). Simulator support is
    # Apple Silicon only.
else
    echo "Building iOS XCFramework (slim signalling-only variant; MEDIA=1 for media)..."
    SIM_TARGETS+=(x86_64-apple-ios)
fi
echo "Project root: $PROJECT_ROOT"
echo "Build directory: $BUILD_DIR"
echo "Profile: $PROFILE"

mkdir -p "$BUILD_DIR"

echo "Ensuring Rust targets are installed..."
rustup target add "$DEVICE_TARGET" "${SIM_TARGETS[@]}"

for t in "$DEVICE_TARGET" "${SIM_TARGETS[@]}"; do
    echo "Building for $t..."
    cargo build -p matrix-rtc-ffi --profile "$PROFILE" --target "$t" "${FEATURE_ARGS[@]}"
done

# The archives that go into the xcframework are copies of cargo's output, so
# stripping never touches the build cache (and never runs twice on one file).
ARCHIVES_DIR="$BUILD_DIR/archives"
rm -rf "$ARCHIVES_DIR"
prepare_archive() {
    local target="$1"
    local src="$PROJECT_ROOT/target/$target/$PROFILE_DIR/$LIB_NAME"
    local dst="$ARCHIVES_DIR/$target/$LIB_NAME"
    mkdir -p "$ARCHIVES_DIR/$target"
    cp "$src" "$dst"
    if [ "$STRIP" = "1" ]; then
        strip -S "$dst"
    fi
    printf '%s\n' "$dst"
}

# uniffi reads its metadata from the unstripped build output.
CODEGEN_LIB="$PROJECT_ROOT/target/$DEVICE_TARGET/$PROFILE_DIR/$LIB_NAME"

[ "$STRIP" = "1" ] && echo "Stripping debug info from the archives (--keep-debug to keep it)..."
DEVICE_LIB="$(prepare_archive "$DEVICE_TARGET")"
if [ "${#SIM_TARGETS[@]}" -eq 1 ]; then
    SIM_LIB="$(prepare_archive "${SIM_TARGETS[0]}")"
else
    SIM_LIB="$BUILD_DIR/libmatrix_rtc_ffi_sim.a"
    LIPO_INPUTS=()
    for t in "${SIM_TARGETS[@]}"; do
        LIPO_INPUTS+=("$(prepare_archive "$t")")
    done
    echo "Creating universal simulator library..."
    lipo -create "${LIPO_INPUTS[@]}" -output "$SIM_LIB"
fi

# ---- Bindings ---------------------------------------------------------------
# Generated before the xcframework: its header and module map go inside the
# framework so `import MatrixRtcFFI` resolves for SPM consumers. Names come
# from crates/matrix-rtc-ffi/uniffi.toml.

GEN_DIR="$BUILD_DIR/generated"
rm -rf "$GEN_DIR"
mkdir -p "$GEN_DIR"
echo "Generating Swift bindings..."
cargo run -p uniffi-bindgen -- generate \
  --library "$CODEGEN_LIB" \
  --language swift \
  --out-dir "$GEN_DIR"

for expected in "$FRAMEWORK_NAME.h" "$FRAMEWORK_NAME.modulemap" "MatrixRtc.swift"; do
    if [ ! -f "$GEN_DIR/$expected" ]; then
        echo "❌ uniffi did not generate $expected — check [bindings.swift] in crates/matrix-rtc-ffi/uniffi.toml"
        ls "$GEN_DIR"
        exit 1
    fi
done

# Headers live in a subdirectory named after the module (as
# matrix-rust-components-swift does) so that an app linking several UniFFI
# xcframeworks does not get colliding module maps.
HEADERS_DIR="$BUILD_DIR/headers"
rm -rf "$HEADERS_DIR"
mkdir -p "$HEADERS_DIR/$FRAMEWORK_NAME"
cp "$GEN_DIR/$FRAMEWORK_NAME.h" "$HEADERS_DIR/$FRAMEWORK_NAME/"
cp "$GEN_DIR/$FRAMEWORK_NAME.modulemap" "$HEADERS_DIR/$FRAMEWORK_NAME/module.modulemap"

# ---- XCFramework ------------------------------------------------------------

XCFRAMEWORK="$BUILD_DIR/$FRAMEWORK_NAME.xcframework"
echo "Creating XCFramework..."
rm -rf "$XCFRAMEWORK"
xcodebuild -create-xcframework \
  -library "$DEVICE_LIB" -headers "$HEADERS_DIR" \
  -library "$SIM_LIB" -headers "$HEADERS_DIR" \
  -output "$XCFRAMEWORK"

# ---- Swift sources ----------------------------------------------------------

mkdir -p "$SWIFT_OUT"
# Everything here is generated (the headers are inside the xcframework now), so
# drop leftovers from an older layout or a previous release.
rm -f "$SWIFT_OUT"/*.swift "$SWIFT_OUT"/*.h "$SWIFT_OUT"/*.modulemap
cp "$GEN_DIR"/*.swift "$SWIFT_OUT/"

# ---- Zip + checksum ---------------------------------------------------------

if [ "$ZIP" = "1" ]; then
    ZIP_PATH="$XCFRAMEWORK.zip"
    rm -f "$ZIP_PATH" "$ZIP_PATH.sha256"
    echo "Zipping XCFramework..."
    (cd "$BUILD_DIR" && ditto -c -k --sequesterRsrc --keepParent "$FRAMEWORK_NAME.xcframework" "$FRAMEWORK_NAME.xcframework.zip")
    # SwiftPM's binaryTarget checksum is the plain SHA-256 of the zip
    # (`swift package compute-checksum` gives the same value).
    CHECKSUM="$(shasum -a 256 "$ZIP_PATH" | cut -d' ' -f1)"
    printf '%s\n' "$CHECKSUM" > "$ZIP_PATH.sha256"
fi

echo ""
echo "✅ iOS XCFramework built successfully!"
echo ""
echo "Outputs:"
echo "  XCFramework: $XCFRAMEWORK"
echo "  Swift bindings: $SWIFT_OUT/MatrixRtc.swift"
if [ "$ZIP" = "1" ]; then
    echo "  Zip: $ZIP_PATH ($(du -h "$ZIP_PATH" | cut -f1))"
    echo "  Checksum: $CHECKSUM"
fi
echo ""
echo "Library sizes (pre-link static archives as shipped, see mobile/PACKAGING.md):"
du -h "$DEVICE_LIB" "$SIM_LIB"
echo ""
echo "Local integration: copy mobile/ios/Debug-Package.swift over Package.swift"
echo "at the repo root and add the repo as a local Swift package."
if [ "${MEDIA:-0}" = "1" ]; then
    echo "MEDIA build: add -ObjC to the app target's 'Other Linker Flags'"
    echo "(libwebrtc's Objective-C categories are dead-stripped otherwise;"
    echo "if that causes duplicate symbols, use -force_load on the archive)."
fi
