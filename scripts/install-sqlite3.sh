#!/bin/bash

# Portable script to download and extract SQLite3 binary
# Works on Linux (x64), macOS (x64/arm64), and Windows (x64)

set -e

SQLITE_VERSION="${SQLITE_VERSION:-3490100}"
SQLITE_YEAR="${SQLITE_YEAR:-2025}"

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTALL_DIR="${INSTALL_DIR:-$PROJECT_ROOT/.sqlite3}"

# Detect OS and architecture
detect_platform() {
    local os arch

    case "$(uname -s)" in
        Linux*)
            os="linux"
            ;;
        Darwin*)
            os="macos"
            ;;
        CYGWIN*|MINGW*|MSYS*|Windows_NT)
            os="win32"
            ;;
        *)
            echo "Error: Unsupported operating system: $(uname -s)" >&2
            exit 1
            ;;
    esac

    case "$(uname -m)" in
        x86_64|amd64)
            arch="x64"
            ;;
        arm64|aarch64)
            arch="arm64"
            ;;
        *)
            echo "Error: Unsupported architecture: $(uname -m)" >&2
            exit 1
            ;;
    esac

    echo "${os}-${arch}"
}

# Get download URL based on platform
get_download_url() {
    local platform="$1"
    local base_url="https://sqlite.org/$SQLITE_YEAR"

    case "$platform" in
        linux-x64)
            echo "$base_url/sqlite-tools-linux-x64-$SQLITE_VERSION.zip"
            ;;
        macos-x64|macos-arm64)
            # macOS uses a universal binary (works on both x64 and arm64)
            echo "$base_url/sqlite-tools-osx-x64-$SQLITE_VERSION.zip"
            ;;
        win32-x64)
            echo "$base_url/sqlite-tools-win-x64-$SQLITE_VERSION.zip"
            ;;
        *)
            echo "Error: No download available for platform: $platform" >&2
            exit 1
            ;;
    esac
}

# Get the binary name based on OS
get_binary_name() {
    local platform="$1"

    case "$platform" in
        win32-*)
            echo "sqlite3.exe"
            ;;
        *)
            echo "sqlite3"
            ;;
    esac
}

main() {
    local platform
    platform="$(detect_platform)"

    local url
    url="$(get_download_url "$platform")"

    local binary_name
    binary_name="$(get_binary_name "$platform")"

    local target_binary="$INSTALL_DIR/$binary_name"

    echo "Platform: $platform"
    echo "SQLite version: $SQLITE_VERSION"
    echo "Download URL: $url"
    echo "Install directory: $INSTALL_DIR"

    # Check if already installed
    if [ -x "$target_binary" ]; then
        echo "SQLite3 already installed at $target_binary"
        "$target_binary" --version
        exit 0
    fi

    # Create install directory
    mkdir -p "$INSTALL_DIR"

    # Create temp directory
    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    local zip_file="$tmp_dir/sqlite.zip"

    echo "Downloading SQLite3..."
    if command -v curl &> /dev/null; then
        curl -fsSL -o "$zip_file" "$url"
    elif command -v wget &> /dev/null; then
        wget -q -O "$zip_file" "$url"
    else
        echo "Error: Neither curl nor wget found. Please install one of them." >&2
        exit 1
    fi

    echo "Extracting SQLite3..."
    if command -v unzip &> /dev/null; then
        # -j: junk paths (don't create subdirectories)
        # -o: overwrite without prompting
        unzip -j -o "$zip_file" "*$binary_name" -d "$INSTALL_DIR"
    else
        echo "Error: unzip not found. Please install it." >&2
        exit 1
    fi

    # Make binary executable (no-op on Windows, needed on Unix)
    chmod +x "$target_binary" 2>/dev/null || true

    echo "SQLite3 installed successfully!"
    "$target_binary" --version

    echo ""
    echo "Binary location: $target_binary"
    echo "Add to PATH: export PATH=\"$INSTALL_DIR:\$PATH\""
}

main "$@"
