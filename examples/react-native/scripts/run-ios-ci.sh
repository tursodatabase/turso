#!/usr/bin/env bash
#
# Build and run the TursoExample app on an iOS simulator, capturing
# structured benchmark output from console logs.
#
# Usage:
#   ./scripts/run-ios-ci.sh [--timeout 120]
#
# Requirements (macOS only):
#   - Xcode with iOS simulator runtime installed
#   - Node.js >= 20
#   - CocoaPods (gem install cocoapods)
#   - Rust iOS targets built (bindings/react-native/libs/ios/ populated)
#
# The script:
#   1. Installs npm deps + pods
#   2. Bundles JS (offline bundle for Release-like determinism)
#   3. Builds the app for iOS Simulator
#   4. Boots a simulator, installs & launches the app
#   5. Tails simulator logs until benchmark results appear
#   6. Exits 0 if all tests pass, 1 otherwise

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMEOUT=${1:-180}  # seconds to wait for results

SCHEME="TursoExample"
WORKSPACE="$PROJECT_DIR/ios/TursoExample.xcworkspace"
BUNDLE_ID="org.reactjs.native.example.TursoExample"
DERIVED_DATA="$PROJECT_DIR/ios/build"
LOG_FILE="$PROJECT_DIR/ci-simulator.log"

# Pick a simulator runtime and device
DEVICE_NAME="iPhone 16"

echo "==> Project dir: $PROJECT_DIR"

# ---- 1. Install dependencies ----
echo "==> Installing npm dependencies..."
cd "$PROJECT_DIR"
npm install --prefer-offline 2>&1

echo "==> Installing CocoaPods..."
cd "$PROJECT_DIR/ios"
bundle install --quiet 2>/dev/null || true
pod install 2>&1
cd "$PROJECT_DIR"

# ---- 2. Bundle JS offline ----
echo "==> Bundling JavaScript..."
npx react-native bundle \
  --platform ios \
  --dev false \
  --entry-file index.js \
  --bundle-output ios/main.jsbundle \
  --assets-dest ios 2>&1

# ---- 3. Build for simulator ----
echo "==> Building $SCHEME for iOS Simulator..."
xcodebuild \
  -workspace "$WORKSPACE" \
  -scheme "$SCHEME" \
  -configuration Release \
  -sdk iphonesimulator \
  -derivedDataPath "$DERIVED_DATA" \
  -destination "generic/platform=iOS Simulator" \
  CODE_SIGNING_ALLOWED=NO \
  ONLY_ACTIVE_ARCH=YES \
  -quiet 2>&1

APP_PATH=$(find "$DERIVED_DATA" -name "$SCHEME.app" -type d | head -1)
if [ -z "$APP_PATH" ]; then
  echo "ERROR: Could not find built .app bundle"
  exit 1
fi
echo "==> Built app at: $APP_PATH"

# ---- 4. Boot simulator ----
echo "==> Booting simulator ($DEVICE_NAME)..."

# Create or find a simulator
DEVICE_UDID=$(xcrun simctl list devices available -j \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
for runtime, devices in data.get('devices', {}).items():
    if 'iOS' not in runtime:
        continue
    for d in devices:
        if d['name'] == '$DEVICE_NAME' and d['isAvailable']:
            print(d['udid'])
            sys.exit(0)
# No exact match found, pick first available iPhone
for runtime, devices in data.get('devices', {}).items():
    if 'iOS' not in runtime:
        continue
    for d in devices:
        if 'iPhone' in d['name'] and d['isAvailable']:
            print(d['udid'])
            sys.exit(0)
print('')
" 2>/dev/null)

if [ -z "$DEVICE_UDID" ]; then
  echo "ERROR: No available iOS simulator found"
  exit 1
fi
echo "==> Using simulator: $DEVICE_UDID"

xcrun simctl boot "$DEVICE_UDID" 2>/dev/null || true  # may already be booted
sleep 2

# ---- 5. Install and launch ----
echo "==> Installing app..."
xcrun simctl install "$DEVICE_UDID" "$APP_PATH"

echo "==> Launching app..."
xcrun simctl launch "$DEVICE_UDID" "$BUNDLE_ID"

# ---- 6. Capture logs ----
echo "==> Waiting for benchmark results (timeout: ${TIMEOUT}s)..."
rm -f "$LOG_FILE"

# Stream simulator logs, filter for our app's output
# Use a background process with timeout
xcrun simctl spawn "$DEVICE_UDID" log stream \
  --predicate "subsystem == 'com.apple.os_log' OR process == '$SCHEME'" \
  --style compact 2>/dev/null > "$LOG_FILE" &
LOG_PID=$!

# Wait for results marker or timeout
ELAPSED=0
FOUND=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  if grep -q "TURSO_BENCH_RESULTS_END" "$LOG_FILE" 2>/dev/null; then
    FOUND=1
    break
  fi
done

# Kill log streaming
kill $LOG_PID 2>/dev/null || true
wait $LOG_PID 2>/dev/null || true

# ---- 7. Parse results ----
if [ $FOUND -eq 0 ]; then
  echo "ERROR: Timed out waiting for benchmark results after ${TIMEOUT}s"
  echo "==> Last 50 lines of log:"
  tail -50 "$LOG_FILE" 2>/dev/null || true
  xcrun simctl shutdown "$DEVICE_UDID" 2>/dev/null || true
  exit 1
fi

echo ""
echo "============================================"
echo "  BENCHMARK RESULTS"
echo "============================================"

# Extract the results section
sed -n '/TURSO_BENCH_RESULTS_START/,/TURSO_BENCH_RESULTS_END/p' "$LOG_FILE" \
  | grep -v "TURSO_BENCH_RESULTS_" || true

echo "============================================"
echo ""

# Check exit code
if grep -q "TURSO_BENCH_EXIT_CODE=1" "$LOG_FILE" 2>/dev/null; then
  echo "RESULT: Some tests FAILED"
  xcrun simctl shutdown "$DEVICE_UDID" 2>/dev/null || true
  exit 1
else
  echo "RESULT: All tests PASSED"
  xcrun simctl shutdown "$DEVICE_UDID" 2>/dev/null || true
  exit 0
fi
