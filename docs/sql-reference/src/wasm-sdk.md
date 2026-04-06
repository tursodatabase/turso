
# WASM SDK

> **Warning**
> **Unstable Feature**: WASM UDFs and extensions are an unstable feature and subject to change. The CLI requires `--unstable-wasm` to enable WASM support. SDK APIs use unstable-prefixed names (e.g. `with_unstable_wasm_runtime()` in Rust, `unstable_wasm_runtime` in Python, `unstableWasmRuntime` in JavaScript).

Turso can be extended with custom functions, types, and virtual tables written as WebAssembly modules. There are three approaches, from simplest to most advanced:

1. **Rust with `turso-wasm-sdk`** — write natural Rust functions, the proc macro handles ABI marshalling
2. **C with the sqlite3 shim** — compile unmodified sqlite3 C extensions to WASM
3. **Manual WASM** — write raw WASM (WAT or any language) against the ABI directly

## Writing a Function in Rust

The `turso-wasm-sdk` crate provides the `#[turso_wasm]` proc macro that transforms natural Rust functions into WASM exports with automatic ABI marshalling.

### Setup

```bash
cargo init --lib my-udfs
cd my-udfs
```

Set up `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
turso-wasm-sdk = "0.6"
```

### Write functions

Write your UDFs in `src/lib.rs`:

```rust
#![no_std]

extern crate alloc;
use alloc::string::String;
use turso_wasm_sdk::turso_wasm;

#[turso_wasm]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

#[turso_wasm]
fn upper(s: &str) -> String {
    s.to_uppercase()
}

#[turso_wasm]
fn nullable_len(s: Option<&str>) -> Option<i64> {
    s.map(|s| s.len() as i64)
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
```

### Build

```bash
cargo build --target wasm32-unknown-unknown --release
```

### Load into Turso

Each function in the `.wasm` module gets its own `CREATE FUNCTION` statement. The `EXPORT` name must match the Rust function name:

```sql
CREATE FUNCTION add LANGUAGE wasm
    AS file://target/wasm32-unknown-unknown/release/my_udfs.wasm
    EXPORT 'add';

CREATE FUNCTION upper LANGUAGE wasm
    AS file://target/wasm32-unknown-unknown/release/my_udfs.wasm
    EXPORT 'upper';

SELECT add(1, 2);        -- 3
SELECT upper('hello');   -- HELLO
```

> `file://` is a CLI convenience. SDKs pass the hex blob directly:
> ```javascript
> const wasm = fs.readFileSync('my_udfs.wasm');
> const hex = wasm.toString('hex');
> await db.exec(`CREATE FUNCTION add LANGUAGE wasm AS X'${hex}' EXPORT 'add'`);
> ```

### Supported types

| Parameter | Return | Description |
|-----------|--------|-------------|
| `i64` | `i64` | 64-bit integer |
| `f64` | `f64` | 64-bit float |
| `&str` | `String` / `&str` | UTF-8 text |
| `&[u8]` | `Vec<u8>` / `&[u8]` | Binary blob |
| `Option<T>` | `Option<T>` | Nullable value (maps to/from SQL NULL) |
| — | `()` | Returns SQL NULL |

The SDK's `allocator` feature (on by default) sets a global allocator on WASM targets, so `String`, `Vec`, and other `alloc` types work out of the box. Add `extern crate alloc;` to import them.

### Manual API

For full control, use the `args` and `ret` modules directly instead of the proc macro:

```rust
use turso_wasm_sdk::{args, ret};

#[no_mangle]
pub unsafe extern "C" fn distance_sq(_argc: i32, argv: i32) -> i64 {
    let x1 = args::arg_real(argv, 0);
    let y1 = args::arg_real(argv, 1);
    let x2 = args::arg_real(argv, 2);
    let y2 = args::arg_real(argv, 3);
    let dx = x2 - x1;
    let dy = y2 - y1;
    ret::ret_real(dx * dx + dy * dy)
}
```

## Writing an Extension in Rust

An extension is a multi-resource WASM module registered with [CREATE EXTENSION](statements/create-extension.md) instead of CREATE FUNCTION. The module exports a `turso_ext_init` function that returns a JSON manifest declaring all resources: functions, types, and virtual tables.

### Setup

Same `Cargo.toml` setup as a UDF:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
turso-wasm-sdk = "0.6"
```

### Implement `turso_ext_init`

The init function returns a JSON manifest as a text value. Here is a simplified example declaring three scalar functions:

```rust
#![no_std]

extern crate alloc;
use turso_wasm_sdk::{args, ret};

#[no_mangle]
pub unsafe extern "C" fn turso_ext_init(_argc: i32, _argv: i32) -> i64 {
    let manifest = concat!(
        r#"{"functions":["#,
        r#"{"name":"geo_point","export":"geo_point","narg":2},"#,
        r#"{"name":"geo_point_x","export":"geo_point_x","narg":1},"#,
        r#"{"name":"geo_distance","export":"geo_distance","narg":2}"#,
        r#"]}"#
    );
    ret::ret_text(manifest)
}
```

### Implement the declared exports

Each function listed in the manifest must be exported from the WASM module. Use the `args` and `ret` modules from the SDK:

```rust
/// Encode two f64 coordinates into a 16-byte blob: [x as LE f64][y as LE f64]
#[no_mangle]
pub unsafe extern "C" fn geo_point(_argc: i32, argv: i32) -> i64 {
    let x = args::arg_real(argv, 0);
    let y = args::arg_real(argv, 1);
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&x.to_le_bytes());
    buf[8..].copy_from_slice(&y.to_le_bytes());
    ret::ret_blob(&buf)
}

/// Extract the X coordinate from a geo_point blob.
#[no_mangle]
pub unsafe extern "C" fn geo_point_x(_argc: i32, argv: i32) -> i64 {
    let blob = args::arg_blob(argv, 0);
    if blob.len() < 16 {
        return ret::ret_null();
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&blob[..8]);
    ret::ret_real(f64::from_le_bytes(bytes))
}

/// Euclidean distance between two geo_point blobs.
#[no_mangle]
pub unsafe extern "C" fn geo_distance(_argc: i32, argv: i32) -> i64 {
    let p1 = args::arg_blob(argv, 0);
    let p2 = args::arg_blob(argv, 1);
    if p1.len() < 16 || p2.len() < 16 {
        return ret::ret_null();
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&p1[..8]);
    let x1 = f64::from_le_bytes(b);
    b.copy_from_slice(&p1[8..16]);
    let y1 = f64::from_le_bytes(b);
    b.copy_from_slice(&p2[..8]);
    let x2 = f64::from_le_bytes(b);
    b.copy_from_slice(&p2[8..16]);
    let y2 = f64::from_le_bytes(b);
    let dx = x2 - x1;
    let dy = y2 - y1;
    ret::ret_real((dx * dx + dy * dy).sqrt())
}
```

### Build and load

```bash
cargo build --target wasm32-unknown-unknown --release
```

```sql
CREATE EXTENSION geo LANGUAGE wasm
    AS file://target/wasm32-unknown-unknown/release/geo_extension.wasm;

SELECT geo_point(1.0, 2.0);
SELECT geo_distance(geo_point(0, 0), geo_point(3, 4));  -- 5.0
```

### Manifest reference

The full manifest can declare any combination of these resource types:

```json
{
  "functions": [
    {"name": "func_name", "export": "wasm_export_name", "narg": 2}
  ],
  "types": [
    {
      "name": "point",
      "base": "BLOB",
      "params": [{"name": "x", "type": "REAL"}, {"name": "y", "type": "REAL"}],
      "encode": "geo_point",
      "decode": "geo_point_decode",
      "operators": [{"op": "<", "func": "geo_distance"}]
    }
  ],
  "vtabs": [
    {
      "name": "geo_series",
      "columns": [
        {"name": "value", "type": "REAL", "hidden": false},
        {"name": "start", "type": "REAL", "hidden": true}
      ],
      "open": "geo_series_open",
      "filter": "geo_series_filter",
      "column": "geo_series_column",
      "next": "geo_series_next",
      "eof": "geo_series_eof",
      "rowid": "geo_series_rowid"
    }
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `functions[].name` | yes | SQL function name to register |
| `functions[].export` | yes | WASM export name to call |
| `functions[].narg` | no | Number of arguments (-1 = variadic, default) |
| `types[].name` | yes | Custom type name |
| `types[].base` | yes | Base storage type (TEXT, INTEGER, REAL, BLOB) |
| `types[].params` | no | Named parameters for the type constructor |
| `types[].encode` / `decode` | no | WASM exports for encoding/decoding |
| `types[].operators` | no | Operator overloads |
| `vtabs[].columns[].hidden` | no | If true, column is a constraint parameter, not output |
| `vtabs[].close` | no | Optional cursor cleanup export |

For a complete working extension with virtual tables, see [`wasm-sdk/examples/geo-extension/`](https://github.com/nicholasgasior/turso/tree/main/wasm-sdk/examples/geo-extension).

## Compiling a C SQLite Extension

Existing C extensions that use the standard sqlite3 extension API (`sqlite3_create_function`, `sqlite3_value_*`, `sqlite3_result_*`) can be compiled to WASM using the `sqlite3_wasm_shim`. The shim bridges sqlite3 API calls to Turso's WASM ABI at compile time.

### Requirements

- [wasi-sdk](https://github.com/WebAssembly/wasi-sdk) installed (set `WASI_SDK` env var or default `/opt/wasi-sdk`)
- Extension source with a standard `sqlite3_xxx_init` entry point

### Build

```bash
$WASI_SDK/bin/clang --sysroot=$WASI_SDK/share/wasi-sysroot \
    -O2 -mexec-model=reactor \
    -I wasm-sdk/c/sqlite3_wasm/ \
    -DTURSO_SQLITE3_ENTRY=sqlite3_rot_init \
    wasm-sdk/c/sqlite3_wasm/sqlite3_wasm_shim.c \
    rot13.c \
    -o rot13.wasm \
    -Wl,--export=turso_malloc \
    -Wl,--export=turso_ext_init \
    -Wl,--export=__turso_call
```

The only per-extension customization is:
- `-DTURSO_SQLITE3_ENTRY=<init_func>` — the extension's init function name
- The source file path

A convenience script is provided at `wasm-sdk/examples/sqlite3/build.sh`:

```bash
./build.sh rot13.c sqlite3_rot_init
```

### Load

C extensions compiled through the shim are loaded as extensions (not individual functions), because the shim captures all `sqlite3_create_function` calls during init:

```sql
CREATE EXTENSION rot13 LANGUAGE wasm AS file://rot13.wasm;

SELECT rot13('hello');
```

### Supported sqlite3 APIs

The shim implements the following from the sqlite3 extension API:

| Category | Supported |
|----------|-----------|
| `sqlite3_create_function` / `_v2` | Scalar functions |
| `sqlite3_value_type`, `_text`, `_blob`, `_int`, `_int64`, `_double`, `_bytes` | All |
| `sqlite3_result_text`, `_int`, `_int64`, `_double`, `_null`, `_blob`, `_error`, `_value` | All |
| `sqlite3_user_data` | Yes |
| `sqlite3_malloc`, `_malloc64`, `_free`, `_realloc` | Yes |

**Not yet supported**: aggregate functions (`xStep`/`xFinal`), window functions, collations (stub only).

## Advanced: Writing WASM by Hand

For non-Rust/non-C languages, or for learning the ABI, you can write WASM modules directly.

### Function signature

Every WASM UDF export must have the signature:

```
(argc: i32, argv: i32) -> i64
```

- `argc` is the number of arguments
- `argv` is a pointer to an array of `argc` 8-byte values (little-endian i64) in the module's linear memory
- The return value is a signed 64-bit integer encoding the result

### Required exports

| Export | Signature | Description |
|--------|-----------|-------------|
| `memory` | — | WASM linear memory, at least 2 pages (128 KiB) |
| `turso_malloc` | `(size: i32) -> i32` | Returns a pointer to `size` bytes of free memory |
| The function | `(argc: i32, argv: i32) -> i64` | The UDF implementation |

### Memory layout

- Bytes 0–1023: argv slots (128 max arguments)
- Bytes 1024+: bump allocator region

The bump allocator never frees memory. The host resets WASM memory between calls.

### Argument encoding (host → WASM)

| Type | Encoding |
|------|----------|
| Integer | Raw i64 in slot |
| Real | f64 bits reinterpreted as i64 in slot |
| Text | Slot contains pointer to `[TAG_TEXT(0x03)][utf8 bytes][0x00]` |
| Blob | Slot contains pointer to `[TAG_BLOB(0x04)][4-byte LE size][data]` |
| NULL | Slot contains pointer to `[TAG_NULL(0x05)]` |

### Return encoding (WASM → host)

| Type | Encoding |
|------|----------|
| Integer | Return raw i64 (no allocation needed) |
| Real | Return pointer to `[TAG_REAL(0x02)][8-byte LE f64 bits]` |
| Text | Return pointer to `[TAG_TEXT(0x03)][utf8 bytes][0x00]` |
| Blob | Return pointer to `[TAG_BLOB(0x04)][4-byte LE size][data]` |
| NULL | Return pointer to `[TAG_NULL(0x05)]` |

### Minimal example (WAT)

```wasm
(module
  (memory (export "memory") 2)
  (global $bump (mut i32) (i32.const 1024))

  ;; Bump allocator
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $bump))
    (global.set $bump (i32.add (global.get $bump) (local.get $size)))
    (local.get $ptr))

  ;; add(argc, argv) -> argv[0] + argv[1]
  (func (export "add") (param $argc i32) (param $argv i32) (result i64)
    (i64.add
      (i64.load (local.get $argv))
      (i64.load (i64.add (local.get $argv) (i32.const 8))))))
```

Compile and hex-encode:

```bash
wat2wasm add.wat -o add.wasm
xxd -p add.wasm | tr -d '\n'
```

Then use the hex string in SQL:

```sql
CREATE FUNCTION add2 LANGUAGE wasm AS X'<hex>' EXPORT 'add';
SELECT add2(40, 2);  -- 42
```

## See Also

- [CREATE FUNCTION](statements/create-function.md) — SQL reference for registering single functions
- [CREATE EXTENSION](statements/create-extension.md) — SQL reference for registering multi-resource extensions
- [Extensions](extensions.md) — built-in extensions shipped with Turso
