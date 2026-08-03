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

# Build Android AAR from Rust FFI crate
# Supports arm64-v8a, armeabi-v7a, and x86_64 ABIs
#
# MEDIA=1 builds the media-enabled variant (matrix-rtc-ffi `media` feature):
# participants + frame streams + publishing. This compiles libwebrtc (needs
# the NDK's C++ toolchain and network access for the prebuilt download on
# first build) and bundles libwebrtc.jar into the AAR. Expect the .so to grow
# by roughly 8-15 MB per ABI — see mobile/PACKAGING.md.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ANDROID_MODULE_ROOT="$PROJECT_ROOT/mobile/android/matrixrtc"
JNI_LIBS_DIR="$ANDROID_MODULE_ROOT/src/main/jniLibs"
MODULE_LIBS_DIR="$ANDROID_MODULE_ROOT/libs"

FEATURE_ARGS=()
if [ "${MEDIA:-0}" = "1" ]; then
    FEATURE_ARGS=(--features media)
    echo "Building Android AAR (MEDIA variant: frame streams + publishing)..."
else
    echo "Building Android AAR (slim signalling-only variant; MEDIA=1 for media)..."
fi
echo "Project root: $PROJECT_ROOT"
echo "Android module: $ANDROID_MODULE_ROOT"

# Check if cargo-ndk is installed
if ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
fi

if [ "${MEDIA:-0}" = "1" ]; then
    # webrtc-sys-build runs llvm-readelf over libwebrtc.a (JNI symbol
    # export) and locates the NDK by joining "toolchains/llvm/prebuilt/..."
    # onto ANDROID_NDK_HOME *verbatim* — no discovery, unlike gradle or
    # cargo-ndk. Accept both common conventions (a versioned NDK dir, or
    # the ndk/ parent of versioned dirs) and normalise to a versioned dir
    # for THIS BUILD ONLY; your environment stays as it is.
    resolve_ndk() {
        candidate="$1"
        [ -n "$candidate" ] && [ -d "$candidate" ] || return 1
        if [ -d "$candidate/toolchains" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
        # A parent directory of versioned NDK installs: pick the highest.
        latest="$(ls "$candidate" 2>/dev/null | sort -V | tail -1)"
        if [ -n "$latest" ] && [ -d "$candidate/$latest/toolchains" ]; then
            printf '%s\n' "$candidate/$latest"
            return 0
        fi
        return 1
    }

    NDK_RESOLVED=""
    for candidate in \
        "${ANDROID_NDK_HOME:-}" \
        "${ANDROID_NDK_ROOT:-}" \
        "${ANDROID_HOME:+$ANDROID_HOME/ndk}" \
        "${ANDROID_SDK_ROOT:+$ANDROID_SDK_ROOT/ndk}" \
        "$HOME/Library/Android/sdk/ndk" \
        "$HOME/Android/Sdk/ndk"; do
        if NDK_RESOLVED="$(resolve_ndk "$candidate")"; then
            break
        fi
        NDK_RESOLVED=""
    done
    if [ -z "$NDK_RESOLVED" ]; then
        echo "❌ MEDIA=1 needs the Android NDK and none was found."
        echo "   Checked ANDROID_NDK_HOME, ANDROID_NDK_ROOT, ANDROID_HOME/ndk,"
        echo "   ANDROID_SDK_ROOT/ndk, and the default SDK locations."
        exit 1
    fi
    export ANDROID_NDK_HOME="$NDK_RESOLVED"

    case "$(uname -s)" in
        Darwin) HOST_TAG="darwin-x86_64" ;;   # also arm64 macs: NDK keeps this dir name
        Linux)  HOST_TAG="linux-x86_64" ;;
        *)      HOST_TAG="" ;;
    esac
    READELF="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$HOST_TAG/bin/llvm-readelf"
    if [ -n "$HOST_TAG" ] && [ ! -x "$READELF" ]; then
        echo "❌ llvm-readelf not found at:"
        echo "   $READELF"
        echo "   The resolved NDK looks incomplete; reinstall it or point"
        echo "   ANDROID_NDK_HOME at another NDK (versioned dir or ndk/ parent"
        echo "   both work)."
        exit 1
    fi
    echo "Using NDK: $ANDROID_NDK_HOME"
fi

# Ensure required targets are installed
echo "Ensuring Rust targets are installed..."
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android

# Build native libraries for all ABIs
echo "Building native libraries with cargo-ndk..."
cargo ndk \
  -t arm64-v8a \
  -t armeabi-v7a \
  -t x86_64 \
  -o "$JNI_LIBS_DIR" \
  build -p matrix-rtc-ffi --release "${FEATURE_ARGS[@]}"

# Assert the Java->C++ direction survived linking. matrix-rtc-ffi's build.rs
# re-emits libwebrtc's --undefined/--version-script link args (webrtc-sys emits
# them from an rlib, where cargo drops them), but if that ever regresses the
# .so still builds and installs perfectly — and then aborts the host process
# with SIGABRT the first time libwebrtc constructs its PeerConnectionFactory,
# because DefaultVideoEncoderFactory's native methods are missing. A runtime
# abort in someone else's app is a terrible place to discover this, so fail the
# build here instead.
if [ "${MEDIA:-0}" = "1" ] && [ -x "$READELF" ]; then
    echo "Verifying libwebrtc JNI symbols are exported..."
    while IFS= read -r so; do
        jni_count="$("$READELF" --dyn-syms "$so" | grep -c 'Java_livekit_org_webrtc' || true)"
        if [ "$jni_count" -eq 0 ]; then
            echo "❌ $so exports no Java_livekit_org_webrtc* symbols."
            echo "   libwebrtc's Java classes would have no native implementation,"
            echo "   aborting the app on the first session. Check that"
            echo "   crates/matrix-rtc-ffi/build.rs ran configure_jni_symbols()"
            echo "   for this target (it is gated on the media feature)."
            exit 1
        fi
        echo "  $(basename "$(dirname "$so")"): $jni_count JNI symbols"
    done < <(find "$JNI_LIBS_DIR" -name 'libmatrix_rtc_ffi.so')
fi

# libwebrtc's Java classes: the native library up-calls into them, so the
# media AAR must ship the jar (it is architecture-independent). Where
# webrtc-sys leaves it depends on how it was built:
#   1. our target dir       — only when webrtc-sys is a path dep (livekit's
#                             own workspace layout; its get_output_path()
#                             assumes CARGO_MANIFEST_DIR/../target),
#   2. the cargo registry   — where that broken relative path actually lands
#                             for a crates.io build,
#   3. the downloaded libwebrtc bundle (scratch dir) — always present, the
#                             file the build copied from in the first place.
find_webrtc_jar() {
    candidate="$PROJECT_ROOT/target/aarch64-linux-android/release/libwebrtc.jar"
    if [ -f "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    cargo_home="${CARGO_HOME:-$HOME/.cargo}"
    candidate="$(find "$cargo_home/registry/src" -maxdepth 6 \
        -path '*/target/*-linux-android*/release/libwebrtc.jar' 2>/dev/null | head -1)"
    if [ -n "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    candidate="$(find "$PROJECT_ROOT/target" -path '*livekit_webrtc*' \
        -name 'libwebrtc.jar' 2>/dev/null | head -1)"
    if [ -n "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    return 1
}

# Keep the libs dir in a state matching the variant so a slim rebuild
# doesn't ship a stale jar.
mkdir -p "$MODULE_LIBS_DIR"
rm -f "$MODULE_LIBS_DIR/libwebrtc.jar"
if [ "${MEDIA:-0}" = "1" ]; then
    if WEBRTC_JAR="$(find_webrtc_jar)"; then
        echo "Bundling libwebrtc.jar into the module (from $WEBRTC_JAR)..."
        cp "$WEBRTC_JAR" "$MODULE_LIBS_DIR/libwebrtc.jar"
    else
        echo "❌ MEDIA=1 but libwebrtc.jar was not found in the project target"
        echo "   dir, the cargo registry, or the libwebrtc download directory."
        echo "   Look for it manually: find ~/.cargo target -name libwebrtc.jar"
        exit 1
    fi
fi

# Generate Kotlin bindings
echo "Generating Kotlin bindings..."
KOTLIN_OUT="$ANDROID_MODULE_ROOT/src/main/java"
mkdir -p "$KOTLIN_OUT"
cargo run -p uniffi-bindgen -- generate \
  --library "$PROJECT_ROOT/target/aarch64-linux-android/release/libmatrix_rtc_ffi.so" \
  --language kotlin \
  --out-dir "$KOTLIN_OUT"

# Size report (the media variant carries libwebrtc; track it per ABI).
echo ""
echo "Native library sizes:"
find "$JNI_LIBS_DIR" -name 'libmatrix_rtc_ffi.so' -exec du -h {} \;

# Check if Gradle is available in the Android module
if [ ! -f "$PROJECT_ROOT/mobile/android/gradlew" ]; then
    echo ""
    echo "⚠️  Gradle wrapper not found at $PROJECT_ROOT/mobile/android/gradlew"
    echo "Please ensure the Android Gradle project has been set up."
    echo ""
    echo "To complete the AAR build, run from the Android directory:"
    echo "  cd $PROJECT_ROOT/mobile/android"
    echo "  ./gradlew :matrixrtc:assembleRelease"
    echo ""
else
    # Build AAR using Gradle
    echo "Building AAR with Gradle..."
    cd "$PROJECT_ROOT/mobile/android"
    ./gradlew :matrixrtc:assembleRelease

    AAR_OUTPUT="$ANDROID_MODULE_ROOT/build/outputs/aar/matrixrtc-release.aar"
    if [ -f "$AAR_OUTPUT" ]; then
        echo ""
        echo "✅ Android AAR built successfully!"
        echo ""
        echo "Outputs:"
        echo "  AAR: $AAR_OUTPUT"
        echo "  Native libraries: $JNI_LIBS_DIR"
        echo "  Kotlin bindings: $KOTLIN_OUT"
        echo ""
        echo "Next steps:"
        echo "1. Copy $AAR_OUTPUT to your Maven repository or local project libs"
        echo "2. Add to your Android app's build.gradle:"
        echo "   implementation files('path/to/matrixrtc-release.aar')"
    else
        echo ""
        echo "❌ AAR build failed or output not found at $AAR_OUTPUT"
        exit 1
    fi
fi

