# FDW for Turso — Implementation Plan

## Goal

Add Foreign Data Wrappers to Turso as virtual table extensions.
Two FDWs: **HTTP/MCP** (REST APIs + MCP servers) and **DuckDB** (analytical queries).
No core engine changes required — built on existing vtab system.

## Reference Documents

- **`fdw_plan.md`** — Full code-level implementation plan with Rust code for every struct/trait
- **`postgres_fdw.md`** — PostgreSQL FDW architecture analysis (reference only)

## Critical Constraints (Read Before Coding)

See `fdw_plan.md` section "Critical Design Constraints Discovered" for full details.
Quick summary of the ones that will bite you:

1. **`best_index()` is static** — no `&self`, cannot access table config. Encode column indices in `idx_str`.
2. **Only 6 operators reach vtabs** — Eq, Lt, Le, Gt, Ge, Ne. NOT Like/Glob/In.
3. **`turso_ext::Value` is NOT Clone, no Display** — store rows as `serde_json::Value` or custom `CellValue` enum. Construct `turso_ext::Value` fresh on each `column()` call. Use `arg.to_text()`/`to_integer()`/`to_float()` not `.to_string()`.
4. **`argv_index` must be contiguous from 1** — no gaps allowed.
5. **Schema string must be `CREATE TABLE x (...)`** — HIDDEN columns detected by `ty_str.contains("HIDDEN")`.
6. **Cursor `rowid()` is used for UPDATE/DELETE** — engine calls `cursor.rowid()` via RowId instruction, passes it as argv[0] to VUpdate. Must return actual backing-store rowid, NOT sequential index.
7. **`duckdb-rs` has no `last_insert_rowid()`** — use `INSERT ... RETURNING rowid` with `stmt.query_row()`.
8. **`INSERT...SELECT` not supported for virtual tables** — only `INSERT INTO vtab VALUES (...)` works.
9. **Args to `create()` do NOT include module_name/table_name** — unlike SQLite, Turso passes only user-specified key=value pairs. Don't skip first args.
10. **`jsonpath_rust::JsonPath::parse()` doesn't exist** — use `JsonPath::from_str()`. `find()` returns `serde_json::Value` (array). Pin version to `0.7`.
11. **All config structs need `#[derive(Clone)]`** — `HttpConfig`, `DuckDbConfig`, `ColumnDef`, `ResponseFormat`, `DuckDbMode`. Also `DuckDbMode` needs `PartialEq`.

---

## Phase 1: HTTP Extension Scaffolding

Create the extension crate, get it compiling with a minimal vtab that returns no rows.

### Task 1.1: Create `extensions/http/` crate

Create the crate following the CSV extension template exactly.

**Files to create:**
- `extensions/http/Cargo.toml` — copy from `fdw_plan.md` section 3.3 "HTTP Client"
- `extensions/http/src/lib.rs` — minimal skeleton

**Key details:**
- Crate name: `limbo_http` (follows `limbo_*` convention from `extensions/csv/Cargo.toml`)
- `crate-type = ["cdylib", "lib"]`
- Feature: `static = ["turso_ext/static"]`
- Deps: `turso_ext`, `ureq = "2"`, `serde_json = "1"`, `jsonpath-rust = "0.7"`
- `mimalloc` under `[target.'cfg(not(target_family = "wasm"))'.dependencies]`
- `tempfile = { workspace = true }` in dev-dependencies

**Skeleton lib.rs:**
```rust
use turso_ext::*;

#[derive(Debug, VTabModuleDerive, Default)]
struct HttpVTabModule;

impl VTabModule for HttpVTabModule { ... }
impl VTable for HttpTable { ... }
impl VTabCursor for HttpCursor { ... }

register_extension! { vtabs: { HttpVTabModule } }
```

**Done when:** `cargo build -p limbo_http` compiles.

### Task 1.2: Add to workspace

Add `"extensions/http"` to `Cargo.toml` workspace members list.

**Done when:** `cargo build --workspace` includes limbo_http.

### Task 1.3: Implement `HttpConfig` parsing

Parse `key=value` args from `CREATE VIRTUAL TABLE ... USING http(...)`.

**Input:** `args: &[Value]` — each arg is a `Value::from_text("key=value")`.
Unlike SQLite, Turso does NOT prepend module_name/table_name. The args are ONLY
user-specified key=value pairs (verified: `module_args_from_sql` at `core/util.rs:237`
extracts only parenthesized content; VCreate at `execute.rs:1043-1044` stores module_name
and table_name in separate registers). Follow CSV pattern: iterate ALL args, split on `=`.

**Struct** (see `fdw_plan.md` section 3.3):
```rust
struct HttpConfig { url, method, format, json_path, headers, body, content_type, timeout_secs, cache_ttl_secs, mcp_tool, mcp_resource, column_defs: Vec<ColumnDef> }
struct ColumnDef { name, ty, hidden }
```

**Parse the `columns` parameter** to extract HIDDEN markers:
`columns='city TEXT HIDDEN, temp REAL'` → `[ColumnDef{name:"city", ty:"TEXT", hidden:true}, ColumnDef{name:"temp", ty:"REAL", hidden:false}]`

**Add `to_create_table_sql()`** that generates schema string with HIDDEN in the type:
`CREATE TABLE x (city TEXT HIDDEN, temp REAL)` — core detects HIDDEN via `ty_str.contains("HIDDEN")` at `core/schema.rs:2786`.

**Done when:** Unit tests pass for config parsing and schema generation.

### Task 1.4: Implement `HttpVTabModule::create`

Wire up config parsing → schema generation → return `HttpTable`.

```rust
fn create(args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
    let config = HttpConfig::parse(args)?;
    let schema = config.to_create_table_sql();
    Ok((schema, HttpTable { config }))
}
```

**Done when:** `CREATE VIRTUAL TABLE t USING http(url='...', columns='...')` succeeds in tursodb CLI.

### Task 1.5: Implement `HttpTable::best_index`

Static method. Accept all usable `Eq` constraints. Encode column indices in `idx_str`.

See `fdw_plan.md` section 3.3 "HttpTable" for exact code.

**idx_str format:** `"0,3,7"` — comma-separated column indices in argv order.
**`constraint_usages`:** Set `argv_index` starting from 1, `omit: true`.
**Important:** `argv_index` values must be contiguous (constraint #4).

**Done when:** Unit test passes — `best_index([col0 Eq, col2 Eq])` → `idx_str = Some("0,2")`.

### Task 1.6: Implement `HttpCursor` (filter/column/next/eof/rowid)

The main logic. `filter()` makes the HTTP request, parses response, stores rows.

**Key points (all from constraint #3):**
- Store rows as `Vec<Vec<serde_json::Value>>` NOT `Vec<Vec<turso_ext::Value>>`
- In `filter()`: extract arg values via `arg.to_text()` / `arg.to_integer()` / `arg.to_float()` — NOT `.to_string()`
- In `column()`: construct fresh `turso_ext::Value` each call from `serde_json::Value`
- Return `ResultCode::OK` if rows exist, `ResultCode::EOF` if empty

See `fdw_plan.md` section 3.3 "HttpCursor" for full implementation code.

**Done when:** `SELECT * FROM http_table` returns rows from a real HTTP endpoint.

### Task 1.7: JSON response parsing

Implement `parse_json_response(body, config) -> Vec<Vec<serde_json::Value>>`.

- Parse body as JSON
- Apply JSONPath (`config.json_path`) to extract array
- For each item, extract column values by column name from `config.column_defs`
- Return `Vec<Vec<serde_json::Value>>` (conversion to turso_ext::Value happens in `column()`)

**Done when:** Unit test passes — parse `{"users":[{"id":1,"name":"Alice"}]}` with path `$.users`.

### Task 1.8: CSV response parsing

Implement `parse_csv_response(body, config) -> Vec<Vec<serde_json::Value>>`.

- Parse body as CSV (use `csv` crate or simple split)
- Map columns by position to `config.column_defs`

**Done when:** Unit test passes for CSV parsing.

### Task 1.9: Unit tests for HTTP extension

Following CSV test pattern at `extensions/csv/src/lib.rs:370-855`.

Tests to write:
- `test_parse_http_config_basic` — url, format, columns
- `test_parse_hidden_columns` — HIDDEN detection
- `test_parse_config_with_headers` — multi-line headers
- `test_best_index_encodes_constraints` — idx_str round-trip
- `test_best_index_skips_non_eq` — non-Eq constraints get argv_index=None
- `test_json_response_parsing` — JSONPath extraction
- `test_json_nested_path` — `$.data.users` path
- `test_csv_response_parsing` — CSV parsing
- `test_column_type_conversion` — serde_json → turso_ext::Value

See `fdw_plan.md` section 7.1 for test code.

**Done when:** `cargo test -p limbo_http` passes.

### Task 1.10: Integration test with embedded HTTP server

Write a Rust integration test (not .sqltest) that:
1. Starts `tiny_http` server on a random port with canned JSON
2. Creates the virtual table pointing to localhost
3. Queries and verifies results

**Done when:** Integration test passes with real HTTP round-trip.

---

## Phase 2: MCP Integration

Add MCP (Model Context Protocol) support to the HTTP extension. MCP uses JSON-RPC over HTTP.

### Task 2.1: MCP request builder

Build JSON-RPC request bodies for `tools/call` and `resources/read`.

**tools/call:**
```json
{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"<tool>","arguments":{...}}}
```

**resources/read:**
```json
{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"<resource_uri>"}}
```

HIDDEN column constraint values become tool arguments.

**Done when:** Unit test verifies JSON-RPC body generation.

### Task 2.2: MCP response parser

Parse MCP response `result.content[]` array. Each content item with `type: "text"` has its text parsed as JSON to extract column values.

Implement `parse_mcp_response(body, config, params) -> Vec<Vec<serde_json::Value>>`.

**Done when:** Unit test passes for MCP response parsing.

### Task 2.3: Wire MCP into filter()

In `HttpCursor::filter()`, when `config.format == ResponseFormat::Mcp`:
- Build JSON-RPC body from HIDDEN column values
- POST to `config.url`
- Parse response with `parse_mcp_response`

**Done when:** End-to-end test with mock MCP server passes.

### Task 2.4: MCP integration tests

Test with mock MCP server (`tiny_http`):
- `tools/call` with arguments from HIDDEN columns
- `resources/read` for resource URIs
- Error handling (invalid tool, server error)

**Done when:** `cargo test -p limbo_http` passes all MCP tests.

---

## Phase 3: DuckDB Extension Scaffolding

### Task 3.1: Create `extensions/duckdb/` crate

**Files to create:**
- `extensions/duckdb/Cargo.toml` — see `fdw_plan.md` section 4.3 "DuckDB Dependency"
- `extensions/duckdb/src/lib.rs`

**Key details:**
- Crate name: `limbo_duckdb`
- Deps: `turso_ext`, `duckdb = { version = "1.1", features = ["bundled"] }`, `serde_json = "1"`
- `bundled` compiles DuckDB from C++ source (~20MB binary increase)

**Done when:** `cargo build -p limbo_duckdb` compiles.

### Task 3.2: Add to workspace

Add `"extensions/duckdb"` to `Cargo.toml` workspace members.

**Done when:** `cargo build --workspace` includes limbo_duckdb.

### Task 3.3: Implement `DuckDbConfig` parsing

Parse args: `database`, `table`, `query`, `columns`, `schema`.

```rust
struct DuckDbConfig { database, table: Option, query: Option, schema_name, column_defs, mode: DuckDbMode }
enum DuckDbMode { Table, Query }
```

One of `table` or `query` must be specified.

**Done when:** Unit test passes for config parsing.

### Task 3.4: Implement `CellValue` enum

Since `turso_ext::Value` is not Clone (constraint #3), define:

```rust
#[derive(Clone)]
enum CellValue { Null, Integer(i64), Float(f64), Text(String), Blob(Vec<u8>) }

impl CellValue {
    fn to_ext_value(&self) -> Value { ... }
}
```

See `fdw_plan.md` section 4.3 "DuckDbCursor" for full code.

**Done when:** Compiles and unit test converts CellValue → Value round-trip.

### Task 3.5: Implement type mapping helpers

Two helpers:

1. **`bind_turso_value_to_duckdb(stmt, idx, value)`** — bind turso_ext::Value to DuckDB prepared statement using `value.value_type()` + `value.to_text()`/`to_integer()`/`to_float()`.

2. **`fetch_duckdb_rows(stmt, column_defs) -> Vec<Vec<CellValue>>`** — fetch rows, map DuckDB types to CellValue based on column type string.

See `fdw_plan.md` section 4.4 for type mapping table and helper code.

**Done when:** Unit tests pass for all type conversions (integer, float, text, blob, null, boolean).

### Task 3.6: Implement auto-schema detection

Query DuckDB `information_schema`:
```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position
```

Map DuckDB types → Turso types:
- BIGINT/INTEGER/SMALLINT/TINYINT/BOOLEAN → INTEGER
- DOUBLE/FLOAT/DECIMAL → REAL
- VARCHAR → TEXT
- BLOB → BLOB
- DATE/TIMESTAMP → TEXT
- LIST/STRUCT/MAP → TEXT

**Done when:** Unit test creates a DuckDB table, introspects it, gets correct column defs.

---

## Phase 4: DuckDB Read Path

### Task 4.1: Implement `DuckDbVTabModule::create`

- Parse config
- Open `duckdb::Connection`
- Auto-detect schema if `columns` not specified
- Return `(schema_sql, DuckDbTable { config, conn })`

See `fdw_plan.md` section 4.3 "DuckDbVTabModule" for exact code.

**Done when:** `CREATE VIRTUAL TABLE t USING duckdb(database=':memory:', table='test')` succeeds.

### Task 4.2: Implement `DuckDbTable::best_index`

Accept Eq, Lt, Le, Gt, Ge, Ne constraints (NOT Like — constraint #2).
Encode as `column_index:op_byte` pairs in `idx_str`.

**idx_str format:** `"0:2,3:16"` — column 0 with Eq(2), column 3 with Gt(16).

See `fdw_plan.md` section 4.3 "DuckDbTable" for exact code.

**Done when:** Unit test — `best_index([col0 Eq, col3 Gt])` → `idx_str = Some("0:2,3:16")`.

### Task 4.3: Implement `DuckDbCursor::filter` (table mode)

- Decode `idx_str` into column:op pairs
- Build `SELECT rowid, cols FROM table WHERE col0 = ?1 AND col3 > ?2`
  - **MUST include `rowid` as column 0** for UPDATE/DELETE support (constraint #6)
- Bind args via `bind_turso_value_to_duckdb`
- Fetch rows via `fetch_duckdb_rows(stmt, column_defs, has_rowid=true)` — returns `(rows, row_ids)`
- Store `row_ids: Vec<i64>` on cursor

**op_byte → SQL operator mapping:**
```
2 → "=", 4 → "<", 8 → "<=", 16 → ">", 32 → ">=", 68 → "!="
```

**Done when:** `SELECT * FROM duckdb_table WHERE region = 'US'` returns filtered rows.

### Task 4.4: Implement `DuckDbCursor::filter` (query mode)

Fixed query mode — no pushdown. Just run `config.query` and fetch results.

**Done when:** `SELECT * FROM duckdb_view` returns rows from a fixed query.

### Task 4.5: Implement `DuckDbCursor::column/next/eof/rowid`

- `column()`: return `self.rows[current_row][idx].to_ext_value()` (fresh Value each call)
- `next()`: increment row, return OK or EOF
- `eof()`: `current_row >= rows.len()`
- `rowid()`: return `self.row_ids[current_row]` — actual DuckDB rowid, NOT sequential index (constraint #6)

**Done when:** Full read path works end-to-end.

### Task 4.6: DuckDB connection sharing (cursor → table)

DuckDB `Connection` is not Clone. Cursor stores raw pointer:
```rust
conn_ptr: &self.conn as *const duckdb::Connection
```
Safe because cursor lifetime <= table lifetime.
Need `unsafe impl Send for DuckDbCursor {}` and `unsafe impl Sync for DuckDbCursor {}`.

**Done when:** Multiple cursors can share the same DuckDB connection.

### Task 4.7: Read path unit tests

- `test_constraint_encoding_decoding` — idx_str round-trip
- `test_sql_generation_from_constraints` — "0:2,3:16" → `WHERE region = ?1 AND amount > ?2`
- `test_query_mode_no_pushdown` — query mode ignores constraints
- `test_auto_schema_detection` — introspect DuckDB table

**Done when:** `cargo test -p limbo_duckdb` passes.

### Task 4.8: DuckDB read integration test

Rust integration test:
1. Create DuckDB file with test data
2. Create virtual table pointing to it
3. Query with WHERE pushdown
4. Verify results

**Done when:** Integration test passes.

---

## Phase 5: DuckDB Write Path

### Task 5.1: Implement `DuckDbTable::insert`

Build `INSERT INTO <table> VALUES (?, ?, ...) RETURNING rowid`, bind args via `bind_turso_value_to_duckdb`.
Use `stmt.query_row([], |row| row.get(0))` to get the inserted rowid (constraint #7: duckdb-rs has no `last_insert_rowid()`).

Reject if query mode: `return Err("INSERT not supported in query mode")`.

**Done when:** `INSERT INTO duckdb_table VALUES (...)` works and returns correct rowid.

### Task 5.2: Implement `DuckDbTable::delete`

Build `DELETE FROM <table> WHERE rowid = ?`, bind the rowid parameter.

The `rowid` value comes from the engine calling `cursor.rowid()` → RowId instruction → VUpdate argv[0].
This works correctly because Task 4.3 includes `rowid` in SELECT queries and Task 4.5 returns
the actual DuckDB rowid from `cursor.rowid()`.

**Done when:** `DELETE FROM duckdb_table WHERE ...` works.

### Task 5.3: Implement `DuckDbTable::update`

Build `UPDATE <table> SET col0=?, col1=?, ... WHERE rowid = ?N`.

**Critical:** Bind the rowid as the LAST parameter (after all column values).
Column values are bound at positions 1..N, rowid is bound at position N+1.
Use `stmt.raw_bind_parameter()` and `stmt.raw_execute()` for explicit control.

**Done when:** `UPDATE duckdb_table SET amount = 19.99 WHERE ...` works.

### Task 5.4: Write path unit tests

- `test_insert_values` — insert and verify in DuckDB
- `test_delete_by_rowid` — delete and verify
- `test_update_columns` — update and verify
- `test_query_mode_rejects_writes` — errors on INSERT/UPDATE/DELETE in query mode

**Done when:** `cargo test -p limbo_duckdb` passes all write tests.

---

## Phase 6: Polish & CI

### Task 6.1: Error messages

Surface meaningful errors:
- HTTP: status codes, connection failures, JSON parse errors, JSONPath misses
- DuckDB: connection errors, query errors, type conversion failures
- Config: missing required params, invalid values

**Done when:** Errors are user-readable, not just `ResultCode::Error`.

### Task 6.2: HTTP response size limit

Add `max_response_bytes` parameter (default 100MB).
Enforce via `response.into_reader().take(limit)`.

**Done when:** Large responses are truncated with an error.

### Task 6.3: CI integration

New workspace members auto-build. Verify:
- `cargo build --workspace` includes both extensions
- `cargo test --workspace` runs extension tests
- `cargo clippy --workspace --all-features --all-targets -- --deny=warnings` passes

**Done when:** CI passes with both extensions.

### Task 6.4: Manual smoke test

Run in tursodb CLI:
```sql
CREATE VIRTUAL TABLE repos USING http(
    url='https://api.github.com/users/tursodatabase/repos',
    format='json', columns='name TEXT, stargazers_count INTEGER');
SELECT name, stargazers_count FROM repos ORDER BY stargazers_count DESC LIMIT 5;
```

**Done when:** Real GitHub API query returns results.

---

## Task Dependency Graph

```
Phase 1 (HTTP Read)
  1.1 → 1.2 → 1.3 → 1.4 → 1.5 → 1.6 → 1.7 → 1.8 → 1.9 → 1.10
                                          ↑
                                     1.7 feeds into 1.6

Phase 2 (MCP) — depends on Phase 1
  2.1 → 2.3
  2.2 → 2.3 → 2.4

Phase 3 (DuckDB Scaffold) — independent of Phase 1/2
  3.1 → 3.2 → 3.3
                3.4 (independent)
                3.5 (depends on 3.4)
                3.6 (depends on 3.3)

Phase 4 (DuckDB Read) — depends on Phase 3
  4.1 (depends on 3.3, 3.6) → 4.2 → 4.3 → 4.5 → 4.7 → 4.8
                                      4.4 → 4.5
                                4.6 (depends on 4.1)

Phase 5 (DuckDB Write) — depends on Phase 4
  5.1 → 5.4
  5.2 → 5.4
  5.3 → 5.4

Phase 6 (Polish) — depends on all above
  6.1, 6.2, 6.3, 6.4 (all independent of each other)
```

**Parallelism:** Phase 3 can start alongside Phase 1. Phases 1+2 (HTTP) and Phases 3+4+5 (DuckDB) are independent tracks after scaffolding.

---

## Quick Reference

```bash
# Build
cargo build -p limbo_http
cargo build -p limbo_duckdb

# Test
cargo test -p limbo_http
cargo test -p limbo_duckdb
cargo test --workspace

# Lint
cargo clippy --workspace --all-features --all-targets -- --deny=warnings
cargo fmt

# Manual
cargo run -q --bin tursodb -- -q

# Integration
make -C testing/runner run-rust
```

## Code Reference

All implementation code (structs, trait impls, helpers, Cargo.toml) is in `fdw_plan.md`:
- Section 3.3: HTTP extension code
- Section 4.3: DuckDB extension code
- Section 4.4: Type mapping
- Section 7: Test code
