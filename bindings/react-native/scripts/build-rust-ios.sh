#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$PROJECT_DIR/../.." && pwd)"
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
    echo "Building sync-sdk-kit for $target..."
    cargo build -p turso-sync-sdk-kit --release --target "$target"
done

# Create output directory for universal library
OUTPUT_DIR="$PROJECT_DIR/libs/ios"
mkdir -p "$OUTPUT_DIR"

# Create universal (fat) library using lipo
echo "Creating universal library..."

# For simulator: combine x86_64 and aarch64-sim
lipo -create \
    "$REPO_ROOT/target/aarch64-apple-ios-sim/release/libturso_sync_sdk_kit.a" \
    "$REPO_ROOT/target/x86_64-apple-ios/release/libturso_sync_sdk_kit.a" \
    -output "$OUTPUT_DIR/libturso_sync_sdk_kit_sim.a" 2>/dev/null || \
    cp "$REPO_ROOT/target/aarch64-apple-ios-sim/release/libturso_sync_sdk_kit.a" "$OUTPUT_DIR/libturso_sync_sdk_kit_sim.a"

# For device: just copy aarch64
cp "$REPO_ROOT/target/aarch64-apple-ios/release/libturso_sync_sdk_kit.a" "$OUTPUT_DIR/libturso_sync_sdk_kit_device.a"

# Create combined universal library
# Note: This may fail if the architectures conflict - in that case we'll use XCFramework instead
echo "Creating combined universal library..."
lipo -create \
    "$OUTPUT_DIR/libturso_sync_sdk_kit_device.a" \
    "$OUTPUT_DIR/libturso_sync_sdk_kit_sim.a" \
    -output "$OUTPUT_DIR/libturso_sync_sdk_kit.a" 2>/dev/null || \
    cp "$OUTPUT_DIR/libturso_sync_sdk_kit_device.a" "$OUTPUT_DIR/libturso_sync_sdk_kit.a"

# Copy header files
cp "$SYNC_SDK_KIT_DIR/turso.h" "$OUTPUT_DIR/"
cp "$SYNC_SDK_KIT_DIR/turso_sync.h" "$OUTPUT_DIR/"

echo "iOS build complete!"
echo "Universal library: $OUTPUT_DIR/libturso_sync_sdk_kit.a"
echo "Headers:"
echo "  - $OUTPUT_DIR/turso.h"
echo "  - $OUTPUT_DIR/turso_sync.h"
