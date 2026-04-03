
# CREATE EXTENSION

> **Note**
> **Turso Extension**: CREATE EXTENSION is a Turso-specific statement not available in standard SQLite. It registers a WASM module containing multiple functions, types, and/or virtual tables as a named extension.

> **Warning**
> **Unstable Feature**: WASM extensions are an unstable feature and subject to change. The CLI requires `--unstable-wasm` to enable this functionality. SDK APIs use `with_unstable_wasm_runtime()` (Rust) or equivalent unstable-prefixed names.

The CREATE EXTENSION statement loads a WebAssembly module and registers all resources declared in its manifest. Unlike [CREATE FUNCTION](create-function.md) (which registers a single function), CREATE EXTENSION supports multi-resource modules.

## Syntax

```sql
CREATE EXTENSION [IF NOT EXISTS] extension-name LANGUAGE wasm AS X'hex-encoded-wasm';
```

## Description

The WASM module must export a `turso_ext_init` function that returns a JSON manifest (as a TEXT value) declaring the resources to register. The host calls `turso_ext_init` once at load time, parses the manifest, and registers all declared functions, types, and virtual tables.

For Rust extensions, the `turso-wasm-sdk` crate provides helpers to build the manifest and implement exports. For C extensions using the sqlite3 API, the `sqlite3_wasm_shim` bridges `sqlite3_create_function` calls to Turso's registration protocol. See the [WASM SDK](../wasm-sdk.md) guide for details.

## Parameters

| Parameter | Description |
|-----------|-------------|
| `IF NOT EXISTS` | If specified, no error is raised if the extension already exists. |
| `extension-name` | The name of the extension. Used as the identifier for DROP EXTENSION. |
| `LANGUAGE wasm` | Declares this as a WebAssembly extension. Currently the only supported language. |
| `AS X'...'` | The compiled WASM binary, hex-encoded as a blob literal. |

## Manifest Format

The JSON manifest returned by `turso_ext_init` has the following top-level fields:

| Field | Type | Description |
|-------|------|-------------|
| `functions` | array | Scalar functions to register. Each has `name`, `export`, and optional `narg` (default -1 = variadic). |
| `types` | array | Custom types. Each has `name`, `base` (TEXT/INTEGER/REAL/BLOB), optional `params`, `encode`, `decode`, `operators`. |
| `vtabs` | array | Virtual tables. Each declares `name`, `columns`, and WASM exports for `open`, `filter`, `column`, `next`, `eof`, `rowid`. |

All fields are optional — an extension can declare any combination of resource types. See the [WASM SDK](../wasm-sdk.md) guide for the full manifest specification and examples.

## Examples

### CLI with file://

```sql
CREATE EXTENSION geo LANGUAGE wasm AS file://path/to/geo_extension.wasm;

-- All functions declared in the manifest are now available:
SELECT geo_point(1.0, 2.0);
SELECT geo_distance(geo_point(0, 0), geo_point(3, 4));
```

### SDK with hex blob

```javascript
const wasm = fs.readFileSync('geo_extension.wasm');
const hex = wasm.toString('hex');
await db.exec(`CREATE EXTENSION geo LANGUAGE wasm AS X'${hex}'`);
```

### Minimal manifest example

A simple extension declaring two scalar functions:

```json
{
  "functions": [
    {"name": "geo_point", "export": "geo_point", "narg": 2},
    {"name": "geo_distance", "export": "geo_distance", "narg": 2}
  ]
}
```

## DROP EXTENSION

Remove a previously registered extension and all of its resources:

```sql
DROP EXTENSION [IF EXISTS] extension-name;
```

All functions, types, and virtual tables registered by the extension are removed. Any subsequent SQL referencing those resources will produce an error.

```sql
CREATE EXTENSION geo LANGUAGE wasm AS file://geo_extension.wasm;
SELECT geo_point(1.0, 2.0);  -- works

DROP EXTENSION geo;
SELECT geo_point(1.0, 2.0);  -- Error: no such function: geo_point
```

## See Also

- [CREATE FUNCTION](create-function.md) for registering a single WASM function
- [WASM SDK](../wasm-sdk.md) for how to write extensions in Rust or C
- [Extensions](../extensions.md) for built-in extensions shipped with Turso
