# Development

## Pre-requisites

- [Flutter](https://docs.flutter.dev/get-started/install)
- [Rust](https://www.rust-lang.org/tools/install)
- [frb](https://cjycode.com/flutter_rust_bridge/quickstart)

## Steps

- Modify rust code under `rust` directory;
- Inside this directory, run `flutter_rust_bridge_codegen generate`
- Modify dart code under `lib` directory;

## Run test

- From this directory, run `cargo build --package turso_dart --target-dir=rust/test_build`
- Run `flutter test`
