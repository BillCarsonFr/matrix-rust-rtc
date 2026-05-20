#!/usr/bin/env bash

# Unified mobile build script
# Builds both Android AAR and iOS XCFramework

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Matrix RTC Mobile Build"
echo "=========================================="
echo ""

# Check dependencies
echo "Checking dependencies..."
if ! command -v rustup &> /dev/null; then
    echo "❌ Rust not found. Please install from https://rustup.rs/"
    exit 1
fi

if ! command -v uniffi-bindgen &> /dev/null; then
    echo "⚠️  uniffi-bindgen not found. Installing..."
    cargo install uniffi_bindgen
fi

# Determine platform
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "✅ Detected macOS"
    BUILD_IOS=true
    BUILD_ANDROID=false
    if command -v gradle &> /dev/null || [ -f "$PROJECT_ROOT/mobile/android/gradlew" ]; then
        BUILD_ANDROID=true
    fi
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "✅ Detected Linux"
    BUILD_IOS=false
    BUILD_ANDROID=true
else
    echo "❌ Unsupported OS: $OSTYPE"
    exit 1
fi

# Prompt for what to build
echo ""
echo "What would you like to build?"
if [ "$BUILD_IOS" = true ]; then
    echo "1) iOS XCFramework"
fi
if [ "$BUILD_ANDROID" = true ]; then
    echo "2) Android AAR"
fi
echo "3) Both (if available on this platform)"
echo ""
read -p "Select (1-3): " choice

case $choice in
    1)
        if [ "$BUILD_IOS" = true ]; then
            echo ""
            echo "Building iOS XCFramework..."
            "$PROJECT_ROOT/scripts/build-ios-xcframework.sh"
        else
            echo "❌ iOS build not available on this platform"
            exit 1
        fi
        ;;
    2)
        if [ "$BUILD_ANDROID" = true ]; then
            echo ""
            echo "Building Android AAR..."
            "$PROJECT_ROOT/scripts/build-android-aar.sh"
        else
            echo "❌ Android build not available on this platform"
            exit 1
        fi
        ;;
    3)
        if [ "$BUILD_IOS" = true ]; then
            echo ""
            echo "Building iOS XCFramework..."
            "$PROJECT_ROOT/scripts/build-ios-xcframework.sh"
        fi
        if [ "$BUILD_ANDROID" = true ]; then
            echo ""
            echo "Building Android AAR..."
            "$PROJECT_ROOT/scripts/build-android-aar.sh"
        fi
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "Build complete!"
echo "=========================================="

