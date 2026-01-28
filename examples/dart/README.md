# Dart Examples

## Prerequisites

Generate the bindings first:

```bash
cd bindings/dart
cargo build --package turso_dart --target-dir=rust/test_build
```

## Encryption Example

```shell
cd bindings/dart/example/encryption
dart run example_cli.dart
```
