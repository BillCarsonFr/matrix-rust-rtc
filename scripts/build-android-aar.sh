#!/bin/bash
# Copyright 2026 Valere Fedronic
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

set -e

# Build Android AAR from Rust FFI crate
# Supports arm64-v8a, armeabi-v7a, and x86_64 ABIs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ANDROID_MODULE_ROOT="$PROJECT_ROOT/mobile/android/matrixrtc"
JNI_LIBS_DIR="$ANDROID_MODULE_ROOT/src/main/jniLibs"

echo "Building Android AAR..."
echo "Project root: $PROJECT_ROOT"
echo "Android module: $ANDROID_MODULE_ROOT"

# Check if cargo-ndk is installed
if ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
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
  build -p matrix-rtc-ffi --release

# Generate Kotlin bindings
echo "Generating Kotlin bindings..."
KOTLIN_OUT="$ANDROID_MODULE_ROOT/src/main/java"
mkdir -p "$KOTLIN_OUT"
uniffi-bindgen generate \
  --library "$PROJECT_ROOT/target/aarch64-linux-android/release/libmatrix_rtc_ffi.so" \
  --language kotlin \
  --out-dir "$KOTLIN_OUT"

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

