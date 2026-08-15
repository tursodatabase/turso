#!/usr/bin/env bash
#
# Builds prebuilt Turso libraries for Apple and Android, so apps can ship this
# package without a Rust toolchain.
#
#   ./tool/build_native.sh xcframework   -> build/Turso.xcframework  (macOS + iOS)
#   ./tool/build_native.sh aar           -> build/turso.aar          (4 Android ABIs)
#   ./tool/build_native.sh all
#
# Set PROFILE=debug for a faster build when you only want to check the
# packaging. Distribution artifacts should stay on the default release profile.
set -euo pipefail

PROFILE="${PROFILE:-release}"
CRATE=turso_sqlite3
LIB=libturso_sqlite3
PACKAGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PACKAGE_DIR/../.." && pwd)"
BUILD_DIR="$PACKAGE_DIR/build"
HEADER_DIR="$REPO_ROOT/bindings/c/include"

cargo_flags=(--package "$CRATE")
[ "$PROFILE" = "release" ] && cargo_flags+=(--release)

# Builds the crate for one Rust target and echoes the resulting library path.
build_target() {
  local target="$1" filename="$2"
  rustup target add "$target" >/dev/null
  ( cd "$REPO_ROOT" && cargo build "${cargo_flags[@]}" --target "$target" >&2 )
  echo "$REPO_ROOT/target/$target/$PROFILE/$filename"
}

# Same, but through cargo-ndk so the NDK toolchain and linker get wired up.
build_android_target() {
  local target="$1" api="$2"
  rustup target add "$target" >/dev/null
  ( cd "$REPO_ROOT" && cargo ndk --target "$target" --platform "$api" build "${cargo_flags[@]}" >&2 )
  echo "$REPO_ROOT/target/$target/$PROFILE/$LIB.so"
}

build_xcframework() {
  local out="$BUILD_DIR/Turso.xcframework"
  rm -rf "$out" "$BUILD_DIR/apple"
  mkdir -p "$BUILD_DIR/apple/macos" "$BUILD_DIR/apple/ios-sim" "$BUILD_DIR/apple/headers"
  cp "$HEADER_DIR/sqlite3.h" "$BUILD_DIR/apple/headers/"

  local macos_arm64 macos_x64 ios_device ios_sim_arm64 ios_sim_x64
  macos_arm64="$(build_target aarch64-apple-darwin "$LIB.dylib")"
  macos_x64="$(build_target x86_64-apple-darwin "$LIB.dylib")"
  ios_device="$(build_target aarch64-apple-ios "$LIB.dylib")"
  ios_sim_arm64="$(build_target aarch64-apple-ios-sim "$LIB.dylib")"
  ios_sim_x64="$(build_target x86_64-apple-ios "$LIB.dylib")"

  # An xcframework holds one slice per platform, so the two macOS
  # architectures and the two simulator architectures each get merged first.
  lipo -create "$macos_arm64" "$macos_x64" -output "$BUILD_DIR/apple/macos/$LIB.dylib"
  lipo -create "$ios_sim_arm64" "$ios_sim_x64" -output "$BUILD_DIR/apple/ios-sim/$LIB.dylib"
  mkdir -p "$BUILD_DIR/apple/ios"
  cp "$ios_device" "$BUILD_DIR/apple/ios/$LIB.dylib"

  # cargo stamps an absolute build path as the install name, which resolves to
  # nothing on another machine. Rewrite it to @rpath and ad-hoc sign, or the
  # dynamic loader rejects the library at launch.
  for slice in macos ios ios-sim; do
    install_name_tool -id "@rpath/$LIB.dylib" "$BUILD_DIR/apple/$slice/$LIB.dylib"
    codesign -f -s - "$BUILD_DIR/apple/$slice/$LIB.dylib" >/dev/null 2>&1
  done

  xcodebuild -create-xcframework \
    -library "$BUILD_DIR/apple/macos/$LIB.dylib" -headers "$BUILD_DIR/apple/headers" \
    -library "$BUILD_DIR/apple/ios/$LIB.dylib" -headers "$BUILD_DIR/apple/headers" \
    -library "$BUILD_DIR/apple/ios-sim/$LIB.dylib" -headers "$BUILD_DIR/apple/headers" \
    -output "$out"
  echo "built $out"
}

build_aar() {
  local api="${ANDROID_API:-21}"
  local staging="$BUILD_DIR/aar"
  rm -rf "$staging" "$BUILD_DIR/turso.aar"
  mkdir -p "$staging/jni"

  # Rust target -> Android ABI directory name.
  local pairs=(
    "aarch64-linux-android:arm64-v8a"
    "armv7-linux-androideabi:armeabi-v7a"
    "x86_64-linux-android:x86_64"
    "i686-linux-android:x86"
  )
  for pair in "${pairs[@]}"; do
    local target="${pair%%:*}" abi="${pair##*:}" built
    built="$(build_android_target "$target" "$api")"
    mkdir -p "$staging/jni/$abi"
    cp "$built" "$staging/jni/$abi/$LIB.so"
  done

  cat > "$staging/AndroidManifest.xml" <<XML
<manifest xmlns:android="http://schemas.android.com/apk/res/android"
    package="tech.turso.dart">
    <uses-sdk android:minSdkVersion="$api" />
</manifest>
XML

  # An AAR is a zip, and Gradle expects a classes.jar entry even when the
  # library is pure native code. A jar is just a zip, so write an empty one
  # rather than dragging in a JDK for it.
  python3 -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1],'w').close()" \
    "$staging/classes.jar"

  ( cd "$staging" && zip -qr "$BUILD_DIR/turso.aar" AndroidManifest.xml classes.jar jni )
  echo "built $BUILD_DIR/turso.aar"
}

mkdir -p "$BUILD_DIR"
case "${1:-all}" in
  xcframework) build_xcframework ;;
  aar) build_aar ;;
  all) build_xcframework; build_aar ;;
  *) echo "usage: $0 [xcframework|aar|all]" >&2; exit 1 ;;
esac
