# .NET Examples

## Prerequisites

1. .NET 9.0 SDK
2. Build the native library:
   ```bash
   cd /path/to/limbo
   cargo build --release -p turso-dotnet
   ```

## Encryption

This example demonstrates how to use local database encryption with the Turso .NET SDK.

```bash
cd examples/dotnet
dotnet run
```