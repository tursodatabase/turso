# Go Examples

This directory contains example applications demonstrating the Turso Go SDK.

## Prerequisites

1. **Go 1.24+** installed
2. **Build the native library** from the repository root:
   ```bash
   cargo build -p turso_sync_sdk_kit --release
   ```

## Encryption

Demonstrates local database encryption using the aegis256 cipher.

```bash
# Set the library path and run
DYLD_LIBRARY_PATH=../../target/release go run encryption.go
```

On Linux, use `LD_LIBRARY_PATH` instead:
```bash
LD_LIBRARY_PATH=../../target/release go run encryption.go
```

## More Information

See the [Go bindings documentation](../../bindings/go/README.md) for the full API reference.
