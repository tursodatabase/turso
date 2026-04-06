
# CREATE FUNCTION

> **Note**
> **Turso Extension**: CREATE FUNCTION is a Turso-specific statement not available in standard SQLite. It allows registering WebAssembly modules as user-defined scalar functions.

> **Warning**
> **Unstable Feature**: WASM user-defined functions are an unstable feature and subject to change. The CLI requires `--unstable-wasm` to enable this functionality. SDK APIs use `with_unstable_wasm_runtime()` (Rust) or equivalent unstable-prefixed names.

The CREATE FUNCTION statement registers a WebAssembly (WASM) module as a user-defined function (UDF) that can be called from SQL queries like any built-in function.

## Syntax

```sql
CREATE FUNCTION function-name LANGUAGE wasm AS X'hex-encoded-wasm' EXPORT 'export-name';
```

## Description

WASM UDFs let you extend Turso with custom scalar functions written in any language that compiles to WebAssembly (C, C++, Rust, AssemblyScript, etc.). The WASM module is provided inline as a hex-encoded blob and must export:

- **A named function** (specified by EXPORT) with signature `(argc: i32, argv: i32) -> i64`, where `argv` is a pointer to an array of 8-byte argument slots in the module's linear memory.
- **`turso_malloc`** with signature `(size: i32) -> i32`, a bump allocator used by Turso to write arguments into the module's memory.
- **`memory`**, the module's exported linear memory.

Arguments are marshalled into the module's memory before each call, and the return value is decoded from the i64 result using the Turso WASM ABI.

## Parameters

| Parameter | Description |
|-----------|-------------|
| `function-name` | The name of the SQL function to create. Must not conflict with built-in functions. |
| `LANGUAGE wasm` | Declares this as a WebAssembly function. Currently the only supported language. |
| `AS X'...'` | The compiled WASM binary, hex-encoded as a blob literal. |
| `EXPORT 'export-name'` | The name of the exported function in the WASM module to call. |

## Architecture

Turso uses a pluggable WASM runtime architecture. The core database engine defines a runtime interface but does **not** bundle any WASM executor. Instead, each host environment provides its own:

| Environment | WASM Executor | How It Works |
|-------------|---------------|--------------|
| **CLI (`tursodb`)** | [Wasmtime](https://wasmtime.dev/) | Compiled into the CLI binary as a Rust dependency. |
| **Rust SDK** | Wasmtime (user-provided) | User adds `turso_wasm_wasmtime` crate and passes it to `Builder::with_unstable_wasm_runtime()`. |
| **Python SDK** | Wasmtime | Compiled into the native `.so`/`.dylib`. No user action needed. |
| **Go / .NET / Java SDKs** | Wasmtime | Compiled into the `libturso` shared library. No user action needed. |
| **Node.js / Bun** | Host JS engine (V8 / JSC) | Uses the runtime's built-in `WebAssembly` API via N-API. No additional dependency. |
| **Browser / Edge** | Host JS engine | Uses the browser's native `WebAssembly` API via WASM imports. No additional dependency. |

This means:

- **The core can be built without any WASM runtime.** WASM UDF support is an opt-in capability injected by the host.
- **In JavaScript environments (Node.js, Bun, browser), no runtime dependency is added.** The host engine's native WebAssembly implementation is used directly, avoiding the cost of bundling a separate WASM interpreter or JIT compiler.
- **In native environments (CLI, Python, Go), Wasmtime is compiled in** and provides AOT compilation for production-grade performance.

## Examples

### Basic Usage (SQL)

```sql
-- Register a WASM function (CLI supports file:// for convenience)
CREATE FUNCTION add2 LANGUAGE wasm
    AS file://path/to/my_udfs.wasm
    EXPORT 'add';

-- Use it like any built-in function
SELECT add2(40, 2);
-- 42

SELECT add2(add2(1, 2), add2(3, 4));
-- 10

-- Use with table data
CREATE TABLE orders (item TEXT, price INT, tax INT);
INSERT INTO orders VALUES ('Widget', 1000, 80), ('Gadget', 2500, 200);

SELECT item, add2(price, tax) AS total FROM orders ORDER BY total DESC;
-- Gadget  | 2700
-- Widget  | 1080

-- Remove when no longer needed
DROP FUNCTION add2;
```

> **Note**: `file://` is a CLI convenience — it reads the file and substitutes `X'hex'` before parsing. SDKs pass the hex blob directly (see [WASM SDK](../wasm-sdk.md) for how to build and load WASM modules).

### CLI

Wasmtime ships with the `tursodb` binary. Pass `--unstable-wasm` to enable WASM support:

```bash
tursodb --unstable-wasm database.db
turso> CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add';
turso> SELECT add2(40, 2);
42
```

### Rust SDK

Add `turso_wasm_wasmtime` as a dependency alongside `turso`:

```toml
[dependencies]
turso = "0.6"
turso_wasm_wasmtime = "0.6"
tokio = { version = "1", features = ["full"] }
```

```rust
use std::sync::Arc;
use turso::Builder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new()?;

    let db = Builder::new_local(":memory:")
        .with_unstable_wasm_runtime(Arc::new(runtime))
        .build()
        .await?;

    let conn = db.connect()?;
    conn.execute("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'", ()).await?;

    let row = conn.query("SELECT add2(40, 2) AS r", ()).await?.next().await?.unwrap();
    println!("{:?}", row.get_value(0)?); // Integer(42)

    Ok(())
}
```

The Rust SDK is the only binding where the user explicitly provides the runtime. This gives Rust users full control over their dependency tree — you can omit `turso_wasm_wasmtime` entirely if you don't need WASM UDFs, or substitute your own implementation of `WasmRuntimeApi`.

### Python SDK

Wasmtime is compiled into the `pyturso` package. No extra dependencies:

```python
import turso

conn = turso.connect(":memory:")

conn.execute("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'")

cur = conn.execute("SELECT add2(40, 2) AS r")
print(cur.fetchone()[0])  # 42

cur = conn.execute("SELECT add2(100, 200) AS r")
print(cur.fetchone()[0])  # 300
```

### Go SDK

Wasmtime is compiled into the `libturso` shared library that the Go driver loads. No extra setup:

```go
package main

import (
    "database/sql"
    "fmt"
    _ "turso.tech/database/tursogo"
)

func main() {
    db, _ := sql.Open("turso", ":memory:")

    db.Exec("CREATE FUNCTION add2 LANGUAGE wasm AS X'...' EXPORT 'add'")

    var result int
    db.QueryRow("SELECT add2(40, 2)").Scan(&result)
    fmt.Println(result) // 42
}
```

### Node.js / Bun

The host JavaScript engine's built-in `WebAssembly` API is used directly via N-API. No WASM runtime is bundled — V8 (Node.js) or JavaScriptCore (Bun) handles compilation and execution natively:

```typescript
import { connect } from "@tursodatabase/database";

const db = await connect(":memory:");

const ADD_WASM_HEX =
    "0061736d01000000010c0260017f017f60027f7f017e030302000105030100020607017f014180080b" +
    "071f03066d656d6f727902000c747572736f5f6d616c6c6f6300000361646400010a24021101017f23" +
    "002101230020006a240020010b10002001290300200141086a2903007c0b002c046e616d65021c0200" +
    "02000473697a6501037074720102000461726763010461726776070701000462756d70";

await db.exec(`CREATE FUNCTION add2 LANGUAGE wasm AS X'${ADD_WASM_HEX}' EXPORT 'add'`);

const row = await db.prepare("SELECT add2(40, 2) AS r").get();
console.log(row.r); // 42

// Works with table data
await db.exec("CREATE TABLE orders (item TEXT, price INT, tax INT)");
await db.exec("INSERT INTO orders VALUES ('Widget',1000,80),('Gadget',2500,200)");

const rows = await db
    .prepare("SELECT item, add2(price, tax) AS total FROM orders ORDER BY total DESC")
    .all();

for (const r of rows) {
    console.log(`${r.item}: ${r.total}`);
}
// Gadget: 2700
// Widget: 1080
```

### Browser

The browser runtime delegates to the host's native `WebAssembly` engine. Since Turso itself runs as WASM in the browser, UDF modules are compiled and executed by the browser's own WebAssembly implementation — no interpreter or JIT is embedded:

```typescript
import { connect } from "@tursodatabase/database-wasm";

const db = await connect(":memory:");

const ADD_WASM_HEX =
    "0061736d01000000010c0260017f017f60027f7f017e030302000105030100020607017f014180080b" +
    "071f03066d656d6f727902000c747572736f5f6d616c6c6f6300000361646400010a24021101017f23" +
    "002101230020006a240020010b10002001290300200141086a2903007c0b002c046e616d65021c0200" +
    "02000473697a6501037074720102000461726763010461726776070701000462756d70";

await db.exec(`CREATE FUNCTION add2 LANGUAGE wasm AS X'${ADD_WASM_HEX}' EXPORT 'add'`);

const rows = await db.prepare("SELECT add2(40, 2) AS r").all();
console.log(rows); // [{ r: 42 }]
```

This works in all modern browsers (Chrome, Firefox, Safari, Edge) and edge runtimes (Cloudflare Workers, Vercel Edge Functions) that support `WebAssembly.Module` and `WebAssembly.Instance`.

## DROP FUNCTION

Remove a previously registered WASM function:

```sql
DROP FUNCTION function-name;
```

After dropping, any subsequent reference to the function in SQL will produce a parse error.

## Restrictions

- Only scalar functions are supported. Aggregate and table-valued WASM functions are not yet available.
- The WASM module is stored in the database schema. It is re-compiled on each connection open.
- WASM UDFs cannot access the database or perform I/O. They are pure computational functions.
- The WASM ABI currently supports integer arguments and return values.

## See Also

- [WASM SDK](../wasm-sdk.md) for how to write WASM functions and extensions
- [CREATE EXTENSION](create-extension.md) for registering multi-resource WASM extensions
- [Scalar Functions](../functions/scalar.md) for built-in scalar functions
- [Extensions](../extensions.md) for other ways to extend Turso
- [CREATE TYPE](create-type.md) for custom type definitions with ENCODE/DECODE
