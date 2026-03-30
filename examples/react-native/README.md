# React Native Example App

Example app for `@tursodatabase/sync-react-native`. Runs functional tests,
a performance benchmark, concurrency smoke tests, and (optionally) sync/encryption tests. Results including
DB open time and query throughput are displayed in the app UI.

## Prerequisites

- [React Native environment](https://reactnative.dev/docs/set-up-your-environment) set up
- Rust toolchain (`rustup`)
- For Android: Android SDK/NDK, `cargo-ndk` (`cargo install cargo-ndk`)
- For iOS: Xcode, CocoaPods, Ruby >= 3.2 (see [Run on iOS](#run-on-ios))

## Build the native Rust library

Before building the example app, you must cross-compile the Rust core for
your target platform. From the repo root:

```sh
# Android (all ABIs)
cd bindings/react-native && make android

# iOS (device + simulator)
cd bindings/react-native && make ios
```

## Install JS dependencies

The example app links to the binding via `file:../../bindings/react-native`.
When npm installs it, it runs the binding's `prepare` script which needs
`react-native-builder-bob` (a devDep of the binding). To avoid a
`bob: command not found` error, install the binding's dependencies first:

```sh
# Step 1: install binding deps (--ignore-scripts avoids chicken-and-egg problem)
cd bindings/react-native && npm install --ignore-scripts

# Step 2: install example app (this will now find bob when prepare runs)
cd ../../examples/react-native && npm install
```

## Run on Android

```sh
# Terminal 1: start Metro
npm start

# Terminal 2: build and deploy
npm run android
```

## Run on iOS

macOS ships with Ruby 2.6 which is too old for the bundler version in
`Gemfile.lock`. Install a modern Ruby via Homebrew:

```sh
brew install ruby
# Add to your shell profile (~/.zshrc or ~/.bashrc):
export PATH="/opt/homebrew/opt/ruby/bin:$PATH"
```

Then install the required bundler:

```sh
gem install bundler:4.0.4 # might require sudo
```

Install CocoaPods deps (first time only, or after updating native deps):

```sh
cd ios && bundle install && bundle exec pod install && cd ..
```

Then run:

```sh
# Terminal 1: start Metro
npm start

# Terminal 2: build and deploy
npm run ios
```

If the default simulator target doesn't work (e.g. "application is not
supported on this Mac"), specify one explicitly:

```sh
npm run ios -- --simulator="iPhone 16 Pro"
```

## Testing with a custom database

To benchmark against an existing `.sqlite` file, push it to the device.
The app opens it via `connect({ path: 'yourfile.sqlite' })` which resolves
to the app's data directory.

If the database has an active WAL, checkpoint it first so all data is in
the main file:

```sh
sqlite3 yourfile.sqlite "PRAGMA wal_checkpoint(TRUNCATE);"
```

**Android:**

```sh
adb push yourfile.sqlite /sdcard/yourfile.sqlite
adb shell "run-as com.tursoexample mkdir -p databases"
adb shell "cat /sdcard/yourfile.sqlite | run-as com.tursoexample sh -c 'cat > databases/yourfile.sqlite'"

# Verify the file copied correctly (check size matches the original)
adb shell "run-as com.tursoexample ls -la databases/yourfile.sqlite"
```

**iOS Simulator:**

```sh
# Launch the app once first so the container exists, then:
APP_DATA="$(xcrun simctl get_app_container booted org.reactjs.native.example.TursoExample data)"
cp yourfile.sqlite "$APP_DATA/Documents/yourfile.sqlite"
```

Then open it in your app code with a relative path matching the filename:

```ts
const db = await connect({ path: 'yourfile.sqlite' });
```

Relative paths resolve to the app's data directory automatically
(Android: `databases/`, iOS: `Documents/`).

Reload the app after copying (Cmd+R on iOS, R R on Android).

## Sync and encryption tests

Set environment variables before starting Metro. You **must** use
`--reset-cache` when changing env vars (they're inlined at build time by Babel).

```sh
# Sync only
TURSO_DATABASE_URL=libsql://your-db.turso.io \
TURSO_AUTH_TOKEN=your-auth-token \
npm start -- --reset-cache

# Sync + encryption
TURSO_DATABASE_URL=libsql://your-db.turso.io \
TURSO_AUTH_TOKEN=your-auth-token \
TURSO_ENCRYPTION_KEY=your-base64-key \
TURSO_ENCRYPTION_CIPHER=aes256gcm \
npm start -- --reset-cache
```

Supported ciphers: `aes256gcm`, `aes128gcm`, `chacha20poly1305` (default: `aes256gcm`)
