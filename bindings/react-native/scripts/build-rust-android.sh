#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
SDK_KIT_DIR="$REPO_ROOT/sdk-kit"

echo "Building Turso sdk-kit for Android..."

# Check for required tools
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed. Please install Rust toolchain."
    exit 1
fi

# Check for Android NDK
if [ -z "$ANDROID_NDK_HOME" ] && [ -z "$NDK_HOME" ]; then
    # Try to find NDK in common locations
    if [ -d "$ANDROID_HOME/ndk" ]; then
        NDK_VERSION=$(ls "$ANDROID_HOME/ndk" | sort -V | tail -1)
        export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/$NDK_VERSION"
    elif [ -d "$ANDROID_HOME/ndk-bundle" ]; then
        export ANDROID_NDK_HOME="$ANDROID_HOME/ndk-bundle"
    fi
fi

NDK_PATH="${ANDROID_NDK_HOME:-$NDK_HOME}"
if [ -z "$NDK_PATH" ] || [ ! -d "$NDK_PATH" ]; then
    echo "Warning: Android NDK not found. Set ANDROID_NDK_HOME or NDK_HOME environment variable."
    echo "Attempting to build anyway (cargo-ndk will try to find NDK)..."
fi

# Android targets with their ABI names
declare -A TARGETS
TARGETS["aarch64-linux-android"]="arm64-v8a"
TARGETS["armv7-linux-androideabi"]="armeabi-v7a"
TARGETS["x86_64-linux-android"]="x86_64"
TARGETS["i686-linux-android"]="x86"

# Ensure targets are installed
for target in "${!TARGETS[@]}"; do
    if ! rustup target list --installed | grep -q "$target"; then
        echo "Installing Rust target: $target"
        rustup target add "$target"
    fi
done

# Check if cargo-ndk is installed
if ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
fi

cd "$REPO_ROOT"

# Create output directory
OUTPUT_DIR="$PROJECT_DIR/libs/android"
mkdir -p "$OUTPUT_DIR"

# Build for each target
for target in "${!TARGETS[@]}"; do
    abi="${TARGETS[$target]}"
    echo "Building sdk-kit for $target ($abi)..."

    # Create ABI-specific output directory
    ABI_OUTPUT_DIR="$OUTPUT_DIR/$abi"
    mkdir -p "$ABI_OUTPUT_DIR"

    # Build using cargo-ndk
    cargo ndk -t "$target" build -p turso_sdk_kit --release

    # Copy to ABI-named directory
    cp "$REPO_ROOT/target/$target/release/libturso_sdk_kit.a" "$ABI_OUTPUT_DIR/"
done

# Copy header file
cp "$SDK_KIT_DIR/turso.h" "$OUTPUT_DIR/"

echo "Android build complete!"
echo "Libraries built:"
for target in "${!TARGETS[@]}"; do
    abi="${TARGETS[$target]}"
    echo "  - $OUTPUT_DIR/$abi/libturso_sdk_kit.a"
done
echo "Header: $OUTPUT_DIR/turso.h"
