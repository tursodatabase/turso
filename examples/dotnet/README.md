# .NET Examples

## Local encryption

This example demonstrates local database encryption with the Turso .NET SDK.

Build the native SDK library first so the project reference can copy the runtime asset:

```powershell
cargo build -p turso_sdk_kit --target-dir bindings\dotnet\rs_compiled --target x86_64-pc-windows-msvc
```

Then run the example from the repository root:

```powershell
dotnet run --project examples\dotnet\EncryptionExample.csproj
```

## External page codecs

The managed wxSQLite3/SQLite3MC AES-128-CBC, System.Data.SQLite legacy CryptoAPI, and ordered detector reference code lives in `bindings\dotnet\src\Turso.Tests\Support`, with coverage in `bindings\dotnet\src\Turso.Tests\TursoPageCodecTests.cs`. Keeping that code test-owned avoids duplicating compatibility crypto in the sample app.
