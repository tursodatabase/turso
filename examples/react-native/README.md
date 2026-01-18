# Turso React Native Example

A simple example app demonstrating the Turso React Native bindings.

## Prerequisites

- Node.js 18+
- React Native development environment set up ([React Native docs](https://reactnative.dev/docs/environment-setup))
- Android SDK with NDK 26+
- Rust toolchain with Android targets

## Setup

### 1. Build the Turso native libraries

First, build the Rust libraries for Android:

```bash
# From the repo root
cd bindings/react-native
./scripts/build-rust-android.sh
```

This creates the native libraries in `bindings/react-native/libs/android/`.

### 2. Install dependencies

```bash
cd examples/react-native
npm install
```

### 3. Run on Android

Connect your Android device with USB debugging enabled:

```bash
# Check device is connected
adb devices

# Run the app
npm run android
```

### 4. Build a release APK

```bash
cd android
./gradlew assembleRelease

# APK location:
# android/app/build/outputs/apk/release/app-release.apk
```

Transfer the APK to your device and install.

## What the example does

The example app demonstrates:

- Opening an in-memory database
- Creating tables with `exec()`
- Inserting data with `run()` and bound parameters
- Querying single rows with `get()`
- Querying multiple rows with `all()`
- Using prepared statements
- Running transactions
- Closing the database

## Troubleshooting

### "Native module not found"

Make sure you've built the native libraries:
```bash
cd ../../bindings/react-native
./scripts/build-rust-android.sh
```

### Build fails with NDK errors

Ensure you have NDK 26 installed:
```bash
sdkmanager "ndk;26.1.10909125"
export ANDROID_NDK_HOME=$ANDROID_HOME/ndk/26.1.10909125
```

### App crashes on startup

Check logcat for errors:
```bash
adb logcat | grep -E "(Turso|ReactNative|FATAL)"
```
