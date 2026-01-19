#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
SDK_KIT_DIR="$REPO_ROOT/sdk-kit"
SYNC_SDK_KIT_DIR="$REPO_ROOT/sync/sdk-kit"

echo "Building Turso sync-sdk-kit for iOS (React Native)..."

# Check for required tools
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo is not installed. Please install Rust toolchain."
    exit 1
fi

# iOS targets
TARGETS=(
    "aarch64-apple-ios"           # iOS device (arm64)
    "aarch64-apple-ios-sim"       # iOS simulator (Apple Silicon)
    "x86_64-apple-ios"            # iOS simulator (Intel)
)

# Ensure targets are installed
for target in "${TARGETS[@]}"; do
    if ! rustup target list --installed | grep -q "$target"; then
        echo "Installing Rust target: $target"
        rustup target add "$target"
    fi
done

cd "$REPO_ROOT"

# Build for each target
for target in "${TARGETS[@]}"; do
    echo "Building turso_sync_sdk_kit for $target..."
    cargo build -p turso_sync_sdk_kit --release --target "$target"
done

# Create output directory
OUTPUT_DIR="$PROJECT_DIR/libs/ios"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/device"
mkdir -p "$OUTPUT_DIR/simulator"

# For simulator: combine x86_64 and aarch64-sim into fat library
echo "Creating simulator fat library..."
lipo -create \
    "$REPO_ROOT/target/aarch64-apple-ios-sim/release/libturso_sync_sdk_kit.dylib" \
    "$REPO_ROOT/target/x86_64-apple-ios/release/libturso_sync_sdk_kit.dylib" \
    -output "$OUTPUT_DIR/simulator/libturso_sync_sdk_kit.dylib" 2>/dev/null || \
    cp "$REPO_ROOT/target/aarch64-apple-ios-sim/release/libturso_sync_sdk_kit.dylib" "$OUTPUT_DIR/simulator/libturso_sync_sdk_kit.dylib"

# For device: just copy aarch64
cp "$REPO_ROOT/target/aarch64-apple-ios/release/libturso_sync_sdk_kit.dylib" "$OUTPUT_DIR/device/libturso_sync_sdk_kit.dylib"

# Create XCFramework (the proper way to bundle device + simulator)
echo "Creating XCFramework..."
rm -rf "$OUTPUT_DIR/turso_sync_sdk_kit.xcframework"
xcodebuild -create-xcframework \
    -library "$OUTPUT_DIR/device/libturso_sync_sdk_kit.dylib" \
    -library "$OUTPUT_DIR/simulator/libturso_sync_sdk_kit.dylib" \
    -output "$OUTPUT_DIR/turso_sync_sdk_kit.xcframework"

# Copy header files
cp "$SDK_KIT_DIR/turso.h" "$OUTPUT_DIR/"
cp "$SYNC_SDK_KIT_DIR/turso_sync.h" "$OUTPUT_DIR/"

echo "iOS build complete!"
echo "XCFramework: $OUTPUT_DIR/turso_sync_sdk_kit.xcframework"
echo "Headers:"
echo "  - $OUTPUT_DIR/turso.h"
echo "  - $OUTPUT_DIR/turso_sync.h"
