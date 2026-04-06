# Turso WASM SDK

Write WASM user-defined functions and extensions for Turso in Rust or C with automatic ABI marshalling.

> **Full guide**: For extensions, C sqlite3 compilation, and the complete ABI reference, see the [WASM SDK Guide](../docs/sql-reference/src/wasm-sdk.md).

## Getting started (Rust)

Create a new project:

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
# For local development:
# turso-wasm-sdk = { path = "../path/to/turso/wasm-sdk/turso-wasm-sdk" }
```

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

Build:

```bash
cargo build --target wasm32-unknown-unknown --release
```

Load into Turso (CLI):

```sql
CREATE FUNCTION add LANGUAGE wasm AS file://target/wasm32-unknown-unknown/release/my_udfs.wasm EXPORT 'add';
CREATE FUNCTION upper LANGUAGE wasm AS file://target/wasm32-unknown-unknown/release/my_udfs.wasm EXPORT 'upper';

SELECT add(1, 2);        -- 3
SELECT upper('hello');    -- HELLO
```

SDKs pass the WASM binary as a hex blob directly:

```javascript
const wasm = fs.readFileSync('my_udfs.wasm');
const hex = wasm.toString('hex');
await db.exec(`CREATE FUNCTION add LANGUAGE wasm AS X'${hex}' EXPORT 'add'`);
```

A complete working example is in [`examples/rust/`](examples/rust/).

## `#[turso_wasm]`

The proc macro transforms a natural Rust function into a WASM export with automatic ABI marshalling. You write normal Rust; the macro generates the `extern "C" fn(argc: i32, argv: i32) -> i64` wrapper.

### Supported types

| Parameter | Return | Description |
|-----------|--------|-------------|
| `i64` | `i64` | 64-bit integer |
| `f64` | `f64` | 64-bit float |
| `&str` | `String` / `&str` | UTF-8 text |
| `&[u8]` | `Vec<u8>` / `&[u8]` | Binary blob |
| `Option<T>` | `Option<T>` | Nullable value |
| — | `()` | Returns SQL NULL |

### Using `String` / `Vec`

The SDK's `allocator` feature (on by default) sets a global allocator on WASM targets, so `String`, `Vec`, and other `alloc` types work out of the box. Add `extern crate alloc;` to import them.

## Manual API

For full control, use the `args` and `ret` modules directly:

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

## C

Include `turso_wasm.h` (single-file header):

```c
#include "turso_wasm.h"

__attribute__((export_name("add")))
int64_t add(int32_t argc, int32_t argv) {
    return turso_arg_int(argv, 0) + turso_arg_int(argv, 1);
}
```

Build:

```bash
clang --target=wasm32-unknown-unknown -nostdlib \
      -Wl,--export=memory -Wl,--no-entry -O2 -o add.wasm add.c
```

C examples are in [`examples/c-add/`](examples/c-add/) and [`examples/c-upper/`](examples/c-upper/).

## ABI reference

### Function signature
```
export fn <name>(argc: i32, argv: i32) -> i64
```

`argv` points to an array of `argc` i64 slots (8 bytes each).

### Argument encoding (host → WASM)
- **Integer**: raw i64 in slot
- **Real**: f64 bits reinterpreted as i64 in slot
- **Text**: slot contains pointer to `[TAG_TEXT(0x03)][utf8 bytes][0x00]`
- **Blob**: slot contains pointer to `[TAG_BLOB(0x04)][4-byte LE size][data]`
- **NULL**: slot contains pointer to `[TAG_NULL(0x05)]`

### Return encoding (WASM → host)
- **Integer**: return raw i64 (no allocation needed)
- **Real**: return pointer to `[TAG_REAL(0x02)][8-byte LE f64 bits]`
- **Text**: return pointer to `[TAG_TEXT(0x03)][utf8 bytes][0x00]`
- **Blob**: return pointer to `[TAG_BLOB(0x04)][4-byte LE size][data]`
- **NULL**: return pointer to `[TAG_NULL(0x05)]`

### Memory layout
- Bytes 0–1023: argv slots (128 max arguments)
- Bytes 1024+: bump allocator region

### Required exports
- `memory`: WASM linear memory (at least 2 pages)
- `turso_malloc(size: i32) -> i32`: allocator for marshalling

## Memory management

The bump allocator never frees memory. The host resets WASM memory between calls. For multiple allocations within a single call, all memory is reclaimed together after the function returns.

