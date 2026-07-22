# Turso NativeAOT sample

This sample publishes a NativeAOT executable that statically links `turso_sdk_kit` from the RID-specific `Turso.Data.Sqlite.NativeAot.*` package:

```bash
dotnet publish -c Release -r win-x64
```

Set `<TursoUseStaticNativeLibrary>true</TursoUseStaticNativeLibrary>` with `<PublishAot>true</PublishAot>` and reference the matching `Turso.Data.Sqlite.NativeAot.<rid>` package to enable static native assets. Supported runtime identifiers are `win-x64`, `win-arm64`, `linux-x64`, `linux-arm64`, `osx-x64`, and `osx-arm64`.

When using an unreleased local package, add the package output folder as a restore source, for example:

```bash
dotnet publish -c Release -r win-x64 -p:RestoreAdditionalProjectSources=..\..\artifacts\nuget-packages
```
