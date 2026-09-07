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

# Build the Android AAR from the matrix-rtc-ffi crate.
#
#   scripts/build-android-aar.sh [options]
#
# Options:
#   --target <triple>   Build this Android target only (aarch64-linux-android,
#                       armv7-linux-androideabi or x86_64-linux-android) and
#                       stop before Gradle. The release workflow builds the
#                       three ABIs as parallel jobs and assembles the AAR
#                       afterwards with --skip-build --skip-codegen.
#   --profile <name>    Cargo profile (default: release; the release workflow
#                       passes mobile-release, defined in the root Cargo.toml).
#   --split-debug       Split each .so's debug info into
#                       mobile/android/debuginfo/<abi>/libmatrix_rtc_ffi.so.debug
#                       and strip the shipped library, leaving a .gnu_debuglink
#                       to it. Needs the NDK's llvm-objcopy.
#   --skip-build        Do not run cargo; assemble from what is in jniLibs/.
#   --skip-codegen      Do not regenerate the Kotlin bindings.
#   --version <v>       Version of the AAR / Maven coordinates (default: the
#                       workspace version in Cargo.toml).
#
# Environment:
#   MEDIA=1  builds the media-enabled variant (matrix-rtc-ffi `media` feature):
#            participants + frame streams + publishing. This compiles libwebrtc
#            (needs the NDK's C++ toolchain and network access for the prebuilt
#            download on first build) and bundles libwebrtc.jar into the AAR.
#            Expect the .so to grow by roughly 8-15 MB per ABI — see
#            mobile/PACKAGING.md.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ANDROID_MODULE_ROOT="$PROJECT_ROOT/mobile/android/matrixrtc"
JNI_LIBS_DIR="$ANDROID_MODULE_ROOT/src/main/jniLibs"
MODULE_LIBS_DIR="$ANDROID_MODULE_ROOT/libs"
DEBUGINFO_DIR="$PROJECT_ROOT/mobile/android/debuginfo"
KOTLIN_OUT="$ANDROID_MODULE_ROOT/src/main/java"
LIB_NAME="libmatrix_rtc_ffi.so"

# uniffi-bindgen locates crates/matrix-rtc-ffi/uniffi.toml through `cargo
# metadata` of the current directory.
cd "$PROJECT_ROOT"

ALL_TARGETS=(aarch64-linux-android armv7-linux-androideabi x86_64-linux-android)
TARGETS=("${ALL_TARGETS[@]}")
PROFILE="release"
DO_BUILD=1
DO_CODEGEN=1
DO_GRADLE=1
SPLIT_DEBUG=0
VERSION=""

usage() {
    sed -n '/^# Build the Android AAR/,/^$/p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --target) TARGETS=("$2"); DO_GRADLE=0; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --split-debug) SPLIT_DEBUG=1; shift ;;
        --skip-build) DO_BUILD=0; shift ;;
        --skip-codegen) DO_CODEGEN=0; shift ;;
        --version) VERSION="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

# cargo stores the built-in dev profile under target/<triple>/debug; every
# other profile under its own name.
case "$PROFILE" in
    dev) PROFILE_DIR="debug" ;;
    *) PROFILE_DIR="$PROFILE" ;;
esac

abi_for_triple() {
    case "$1" in
        aarch64-linux-android) echo "arm64-v8a" ;;
        armv7-linux-androideabi) echo "armeabi-v7a" ;;
        x86_64-linux-android) echo "x86_64" ;;
        i686-linux-android) echo "x86" ;;
        *) echo "❌ Not an Android target: $1" >&2; return 1 ;;
    esac
}

for t in "${TARGETS[@]}"; do abi_for_triple "$t" > /dev/null; done

FEATURE_ARGS=()
if [ "${MEDIA:-0}" = "1" ]; then
    FEATURE_ARGS=(--features media)
    echo "Building Android AAR (MEDIA variant: frame streams + publishing)..."
else
    echo "Building Android AAR (slim signalling-only variant; MEDIA=1 for media)..."
fi
echo "Project root: $PROJECT_ROOT"
echo "Android module: $ANDROID_MODULE_ROOT"
echo "Targets: ${TARGETS[*]}  profile: $PROFILE"

if [ "$DO_BUILD" = "1" ] && ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
fi

# The NDK is needed for the media JNI-symbol check (llvm-readelf) and for the
# debug split (llvm-objcopy). Not for the Gradle-only assembly step.
NEED_NDK=0
[ "${MEDIA:-0}" = "1" ] && [ "$DO_BUILD" = "1" ] && NEED_NDK=1
[ "$SPLIT_DEBUG" = "1" ] && NEED_NDK=1

READELF=""
OBJCOPY=""
if [ "$NEED_NDK" = "1" ]; then
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
        echo "❌ This build needs the Android NDK and none was found."
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
    NDK_BIN="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/$HOST_TAG/bin"
    READELF="$NDK_BIN/llvm-readelf"
    OBJCOPY="$NDK_BIN/llvm-objcopy"
    if [ -n "$HOST_TAG" ] && [ ! -x "$READELF" ]; then
        echo "❌ llvm-readelf not found at:"
        echo "   $READELF"
        echo "   The resolved NDK looks incomplete; reinstall it or point"
        echo "   ANDROID_NDK_HOME at another NDK (versioned dir or ndk/ parent"
        echo "   both work)."
        exit 1
    fi
    if [ "$SPLIT_DEBUG" = "1" ] && [ ! -x "$OBJCOPY" ]; then
        echo "❌ --split-debug needs llvm-objcopy, not found at:"
        echo "   $OBJCOPY"
        exit 1
    fi
    echo "Using NDK: $ANDROID_NDK_HOME"
fi

# ---- Native libraries -------------------------------------------------------

if [ "$DO_BUILD" = "1" ]; then
    echo "Ensuring Rust targets are installed..."
    rustup target add "${TARGETS[@]}"

    NDK_TARGET_ARGS=()
    for t in "${TARGETS[@]}"; do NDK_TARGET_ARGS+=(-t "$t"); done

    echo "Building native libraries with cargo-ndk..."
    cargo ndk "${NDK_TARGET_ARGS[@]}" \
      build -p matrix-rtc-ffi --profile "$PROFILE" "${FEATURE_ARGS[@]}"

    # Copy only our library into jniLibs (cargo-ndk's -o would copy every .so
    # in the target dir), one ABI directory per target.
    for t in "${TARGETS[@]}"; do
        abi="$(abi_for_triple "$t")"
        mkdir -p "$JNI_LIBS_DIR/$abi"
        cp "$PROJECT_ROOT/target/$t/$PROFILE_DIR/$LIB_NAME" "$JNI_LIBS_DIR/$abi/$LIB_NAME"
    done
fi

# ---- Kotlin bindings --------------------------------------------------------
# Generated before the debug split so the metadata is read from the unstripped
# library. The bindings are identical for every ABI.

if [ "$DO_CODEGEN" = "1" ]; then
    if [ "$DO_BUILD" = "1" ]; then
        CODEGEN_LIB="$PROJECT_ROOT/target/${TARGETS[0]}/$PROFILE_DIR/$LIB_NAME"
    else
        CODEGEN_LIB="$JNI_LIBS_DIR/$(abi_for_triple "${TARGETS[0]}")/$LIB_NAME"
    fi
    echo "Generating Kotlin bindings from $CODEGEN_LIB..."
    # The whole directory is generated output (the hand-written sources are in
    # src/main/kotlin), so clear it to drop bindings from an older layout.
    rm -rf "$KOTLIN_OUT"
    mkdir -p "$KOTLIN_OUT"
    cargo run -p uniffi-bindgen -- generate \
      --library "$CODEGEN_LIB" \
      --language kotlin \
      --out-dir "$KOTLIN_OUT"
fi

# ---- Debug symbols ----------------------------------------------------------

if [ "$SPLIT_DEBUG" = "1" ]; then
    echo "Splitting debug symbols..."
    for t in "${TARGETS[@]}"; do
        abi="$(abi_for_triple "$t")"
        so="$JNI_LIBS_DIR/$abi/$LIB_NAME"
        out="$DEBUGINFO_DIR/$abi"
        mkdir -p "$out"
        "$OBJCOPY" --only-keep-debug "$so" "$out/$LIB_NAME.debug"
        "$OBJCOPY" --strip-debug --strip-unneeded "$so"
        "$OBJCOPY" --add-gnu-debuglink="$out/$LIB_NAME.debug" "$so"
        echo "  $abi: $(du -h "$so" | cut -f1) shipped, $(du -h "$out/$LIB_NAME.debug" | cut -f1) debug"
    done
fi

# Assert the Java->C++ direction survived linking (and stripping). matrix-rtc-ffi's
# build.rs re-emits libwebrtc's --undefined/--version-script link args
# (webrtc-sys emits them from an rlib, where cargo drops them), but if that ever
# regresses the .so still builds and installs perfectly — and then aborts the
# host process with SIGABRT the first time libwebrtc constructs its
# PeerConnectionFactory, because DefaultVideoEncoderFactory's native methods are
# missing. A runtime abort in someone else's app is a terrible place to discover
# this, so fail the build here instead.
if [ "${MEDIA:-0}" = "1" ] && [ "$DO_BUILD" = "1" ] && [ -x "$READELF" ]; then
    echo "Verifying libwebrtc JNI symbols are exported..."
    for t in "${TARGETS[@]}"; do
        abi="$(abi_for_triple "$t")"
        so="$JNI_LIBS_DIR/$abi/$LIB_NAME"
        jni_count="$("$READELF" --dyn-syms "$so" | grep -c 'Java_livekit_org_webrtc' || true)"
        if [ "$jni_count" -eq 0 ]; then
            echo "❌ $so exports no Java_livekit_org_webrtc* symbols."
            echo "   libwebrtc's Java classes would have no native implementation,"
            echo "   aborting the app on the first session. Check that"
            echo "   crates/matrix-rtc-ffi/build.rs ran configure_jni_symbols()"
            echo "   for this target (it is gated on the media feature)."
            exit 1
        fi
        echo "  $abi: $jni_count JNI symbols"
    done
fi

# ---- libwebrtc.jar ----------------------------------------------------------
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
    candidate="$(ls "$PROJECT_ROOT"/target/*-linux-android*/"$PROFILE_DIR"/libwebrtc.jar 2>/dev/null | head -1)"
    if [ -n "$candidate" ]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    cargo_home="${CARGO_HOME:-$HOME/.cargo}"
    candidate="$(find "$cargo_home/registry/src" -maxdepth 6 \
        -path '*/target/*-linux-android*/*/libwebrtc.jar' 2>/dev/null | head -1)"
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

mkdir -p "$MODULE_LIBS_DIR"
if [ "$DO_BUILD" = "1" ]; then
    # Keep the libs dir in a state matching the variant so a slim rebuild
    # doesn't ship a stale jar.
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
elif [ "${MEDIA:-0}" = "1" ] && [ ! -f "$MODULE_LIBS_DIR/libwebrtc.jar" ]; then
    echo "❌ MEDIA=1 --skip-build but $MODULE_LIBS_DIR/libwebrtc.jar is missing."
    echo "   Put the jar from the build that produced jniLibs/ there first."
    exit 1
fi

echo ""
echo "Native library sizes:"
find "$JNI_LIBS_DIR" -name "$LIB_NAME" -exec du -h {} \;

if [ "$DO_GRADLE" = "0" ]; then
    echo ""
    echo "✅ Native libraries ready in $JNI_LIBS_DIR (no Gradle step for --target)."
    exit 0
fi

# ---- AAR --------------------------------------------------------------------

if [ ! -f "$PROJECT_ROOT/mobile/android/gradlew" ]; then
    echo ""
    echo "❌ Gradle wrapper not found at $PROJECT_ROOT/mobile/android/gradlew"
    exit 1
fi

GRADLE_ARGS=(:matrixrtc:assembleRelease)
# Selects the callback-pinning source dir: the media bindings carry
# callback interfaces the slim ones don't generate.
[ "${MEDIA:-0}" = "1" ] && GRADLE_ARGS+=(-PmatrixRtcMedia=true)
[ -n "$VERSION" ] && GRADLE_ARGS+=("-PmatrixRtcVersion=$VERSION")

echo "Building AAR with Gradle..."
(cd "$PROJECT_ROOT/mobile/android" && ./gradlew "${GRADLE_ARGS[@]}")

AAR_OUTPUT="$ANDROID_MODULE_ROOT/build/outputs/aar/matrixrtc-release.aar"
if [ ! -f "$AAR_OUTPUT" ]; then
    echo ""
    echo "❌ AAR build failed or output not found at $AAR_OUTPUT"
    exit 1
fi

# What a consumer gets is what is inside the AAR, not what is in our source
# tree, so check the archive itself.
echo "Verifying AAR contents..."
AAR_LISTING="$(unzip -Z1 "$AAR_OUTPUT")"
for abi_dir in "$JNI_LIBS_DIR"/*/; do
    abi="$(basename "$abi_dir")"
    if ! grep -qx "jni/$abi/$LIB_NAME" <<< "$AAR_LISTING"; then
        echo "❌ $AAR_OUTPUT does not contain jni/$abi/$LIB_NAME"
        exit 1
    fi
done
if [ "${MEDIA:-0}" = "1" ] && ! grep -qx "libs/libwebrtc.jar" <<< "$AAR_LISTING"; then
    echo "❌ $AAR_OUTPUT does not contain libs/libwebrtc.jar (media build)"
    exit 1
fi

echo ""
echo "✅ Android AAR built successfully!"
echo ""
echo "Outputs:"
echo "  AAR: $AAR_OUTPUT"
echo "  Native libraries: $JNI_LIBS_DIR"
echo "  Kotlin bindings: $KOTLIN_OUT"
[ "$SPLIT_DEBUG" = "1" ] && echo "  Debug symbols: $DEBUGINFO_DIR"
echo ""
echo "Published releases install with a Gradle dependency instead — see"
echo "mobile/PACKAGING.md. To try this AAR directly:"
echo "   implementation files('$AAR_OUTPUT')"
echo "   implementation 'net.java.dev.jna:jna:5.19.1@aar'"
