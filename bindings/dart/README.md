# Turso Dart bindings

Dart FFI bindings for Turso. They call the `turso_sqlite3` C API in
[`bindings/c`](../c), so there is no second copy of the engine here.

`lib/src/bindings.g.dart` is generated from `../c/include/sqlite3.h` by
[package:ffigen](https://pub.dev/packages/ffigen). `lib/turso.dart` wraps it in
an API that speaks Dart types.

## Use

```dart
import 'package:turso/turso.dart';

void main() {
  final db = Database.memory();
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
  db.execute('INSERT INTO users (name) VALUES (?)', ['alice']);

  for (final row in db.query('SELECT id, name FROM users')) {
    print('${row['id']}: ${row['name']}');
  }
  db.close();
}
```

Values map to Dart as `null`, `int`, `double`, `String`, and `Uint8List`. Binds
also accept `bool` (stored as 0/1). `Database.prepare` returns a reusable
`Statement`; dispose it when done, though `Database.close` releases whatever is
left open.

## Native assets

`hook/build.dart` is a [build hook](https://dart.dev/tools/hooks). Dart and
Flutter run it automatically and bundle the result, so there is no shared
library to load, ship, or point an environment variable at. It compiles the
`turso_sqlite3` crate with cargo for whichever target is being built, which
means **a Rust toolchain is needed at build time**.

To use a library you already built instead, name it from the app's
`pubspec.yaml`:

```yaml
hooks:
  user_defines:
    turso:
      library: /path/to/libturso_sqlite3.dylib
```

Android builds go through [`cargo-ndk`](https://github.com/bbqsrc/cargo-ndk),
so install it (`cargo install cargo-ndk`) and have the NDK available.

## Prebuilt artifacts

For shipping without a Rust toolchain on the build machine:

```bash
./tool/build_native.sh xcframework   # build/Turso.xcframework, macOS + iOS
./tool/build_native.sh aar           # build/turso.aar, 4 Android ABIs
```

The XCFramework carries macOS (arm64 + x86_64), iOS device (arm64), and iOS
simulator (arm64 + x86_64) slices. The AAR carries `arm64-v8a`, `armeabi-v7a`,
`x86_64`, and `x86`. Feed a slice back to the build hook with the `library`
user-define above.

Set `PROFILE=debug` for a quicker build when you are only checking packaging.

## Platform support

| Platform | Supported |
|---|---|
| macOS, iOS, Android, Linux, Windows | Yes |
| Web | **No** |

Web is not a missing feature, it is a hard constraint: `dart:ffi` does not
exist when compiling to JavaScript or Wasm, so no FFI package can run in a
browser regardless of how Turso itself is compiled. For web apps, use the
JavaScript/Wasm build of Turso instead of this package.

## Example

[`example/`](example/) is a Flutter todo app that stores its todos in Turso.

```bash
cd example
flutter run -d macos
```

## Test

```bash
dart test
```

## Regenerate the bindings

Re-run this after changing the C header:

```bash
dart run ffigen --config ffigen.yaml
```
