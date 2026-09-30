---
name: cdc
description: Change Data Capture - architecture, entrypoints, bytecode emission, sync engine integration, tests
---
# CDC (Change Data Capture) - Internal Feature Map

## Overview

CDC tracks INSERT/UPDATE/DELETE changes on database tables by writing change records into a
dedicated CDC table (`turso_cdc` by default). It is per-connection, enabled via PRAGMA, and
operates at the bytecode generation (translate) layer. The sync engine consumes CDC records
to push local changes to the remote.

## Architecture Diagram

```
User SQL (INSERT/UPDATE/DELETE/DDL)
        |
        v
  ┌─────────────────────────────────────────────────┐
  │  Translate layer (core/translate/)              │
  │  ┌───────────────────────────────────────────┐  │
  │  │ prepare_cdc_if_necessary()                │  │
  │  │   - checks CaptureDataChangesInfo         │  │
  │  │   - opens CDC table cursor (OpenWrite)    │  │
  │  │   - skips if target == CDC table itself   │  │
  │  └───────────────────────────────────────────┘  │
  │  ┌───────────────────────────────────────────┐  │
  │  │ emit_cdc_insns()                          │  │
  │  │   - writes (change_id, change_time,       │  │
  │  │     change_type, table_name, id,          │  │
  │  │     before, after, updates) into CDC tbl  │  │
  │  └───────────────────────────────────────────┘  │
  │  + emit_cdc_full_record() / emit_cdc_patch_record() │
  └─────────────────────────────────────────────────┘
        |
        v
  CDC table (turso_cdc or custom name)
        |
        v
  ┌─────────────────────────────────────────────────┐
  │  Sync engine (sync/engine/)                     │
  │  DatabaseTape reads CDC table → DatabaseChange  │
  │  → apply/revert → push to remote                 │
  └─────────────────────────────────────────────────┘
```

## Core Data Types

### `CaptureDataChangesMode` + `CaptureDataChangesInfo` — `core/lib.rs`

CDC behavior is controlled by two types:

```rust
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
enum CdcVersion {
    V1 = 1,
    V2 = 2,
}

const CDC_VERSION_CURRENT: CdcVersion = CdcVersion::V2;

enum CaptureDataChangesMode {
    Id,          // capture only rowid
    Before,      // capture before-image
    After,       // capture after-image
    Full,        // before + after + updates
}

struct CaptureDataChangesInfo {
    mode: CaptureDataChangesMode,
    table: String,                  // CDC table name
    version: Option<CdcVersion>,    // schema version (V1 or V2)
}
```

The connection stores `Option<CaptureDataChangesInfo>` — `None` means CDC is off.

Key methods on `CdcVersion`:
- `has_commit_record()` — `self >= V2`, gates COMMIT record emission
- `Display`/`FromStr` — round-trips `"v1"` ↔ `V1`, `"v2"` ↔ `V2`

Key methods on `CaptureDataChangesInfo`:
- `parse(value: &str, version: Option<CdcVersion>)` — parses PRAGMA argument `"<mode>[,<table_name>]"`, returns `None` for "off"
- `cdc_version()` — returns `CdcVersion` (panics if version is None). Single accessor replacing old `is_v1()`/`is_v2()`/`version()` methods.
- `has_before()` / `has_after()` / `has_updates()` — mode capability checks
- `mode_name()` — returns mode as string

Convenience trait `CaptureDataChangesExt` on `Option<CaptureDataChangesInfo>` provides:
- `has_before()` / `has_after()` / `has_updates()` — delegates to inner, returns false for None
- `table()` — returns `Option<&str>`, None when CDC is off

### CDC Table Schema v1

Default table name: `turso_cdc` (constant `TURSO_CDC_DEFAULT_TABLE_NAME`)

```sql
CREATE TABLE turso_cdc (
    change_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    change_time INTEGER,        -- unixepoch()
    change_type INTEGER,        -- 1=INSERT, 0=UPDATE, -1=DELETE
    table_name  TEXT,
    id          <untyped>,      -- rowid of changed row
    before      BLOB,           -- binary record (before-image)
    after       BLOB,           -- binary record (after-image)
    updates     BLOB            -- binary record of per-column changes
);
```

### CDC Table Schema v2 (current)

```sql
CREATE TABLE turso_cdc (
    change_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    change_time  INTEGER,        -- unixepoch()
    change_type  INTEGER,        -- 1=INSERT, 0=UPDATE, -1=DELETE, 2=COMMIT
    table_name   TEXT,
    id           <untyped>,      -- rowid of changed row
    before       BLOB,           -- binary record (before-image)
    after        BLOB,           -- binary record (after-image)
    updates      BLOB,           -- binary record of per-column changes
    change_txn_id INTEGER        -- transaction ID (groups rows into transactions)
);
```

v2 adds:
- `change_txn_id` column — groups CDC rows by transaction. Assigned via `conn_txn_id(candidate)` opcode which get-or-sets a per-connection transaction ID.
- `change_type=2` (COMMIT) records — mark transaction boundaries. Emitted once per statement in autocommit mode, or on explicit `COMMIT`.

The CDC table is created at runtime by the `InitCdcVersion` opcode via `CREATE TABLE IF NOT EXISTS`.

### CDC Version Table

When CDC is first enabled, a version tracking table is created:

```sql
CREATE TABLE turso_cdc_version (
    table_name TEXT PRIMARY KEY,
    version TEXT NOT NULL
);
```

Current version: `CDC_VERSION_CURRENT = CdcVersion::V2` (defined in `core/lib.rs`, re-exported from `core/translate/pragma.rs`)

### Version Detection in InitCdcVersion

The `InitCdcVersion` opcode detects v1 vs v2 by checking whether the CDC table already exists before creating it:
- If CDC table already exists but has no version row → v1 (pre-existing table from before version tracking)
- If CDC table doesn't exist → create with current version (v2)
- If version row already exists → use that version as-is

### `DatabaseChange` — `sync/engine/src/types.rs:229-249`

Sync engine's Rust representation of a CDC row. Has `into_apply()` and `into_revert()` methods
for forward/backward replay.

### `OperationMode` — `core/translate/emitter.rs`

Used by `emit_cdc_insns()` to determine `change_type` value:
- `INSERT` → 1
- `UPDATE` / `SELECT` → 0
- `DELETE` → -1
- `COMMIT` → 2 (v2 only, emitted by `emit_cdc_commit_insns`)

## Entry Points

### 1. PRAGMA — Enable/Disable CDC

**Set:** `core/translate/pragma.rs`
- Checks MVCC is not enabled (CDC and MVCC are mutually exclusive)
- Parses mode string via `CaptureDataChangesInfo::parse()` with `CDC_VERSION_CURRENT`
- Emits a single `InitCdcVersion` opcode — all CDC setup (table creation, version tracking, state change) happens at execution time

**Get (read current mode):** `core/translate/pragma.rs`
- Returns 3 columns: `mode`, `table`, `version`
- When off: returns `("off", NULL, NULL)`
- When active: returns `(mode_name, table, version)`

**Pragma registration:** `core/pragma.rs` — `CaptureDataChangesConn` (and deprecated alias `UnstableCaptureDataChangesConn`) with columns `["mode", "table", "version"]`

### 2. Connection State

**Field:** `core/connection.rs` — `capture_data_changes: RwLock<Option<CaptureDataChangesInfo>>`
**Getter:** `get_capture_data_changes_info()` — returns read guard
**Setter:** `set_capture_data_changes_info(opts: Option<CaptureDataChangesInfo>)`
**Default:** initialized as `None` (CDC off)

### 3. ProgramBuilder Integration

**Field:** `core/vdbe/builder.rs` — `capture_data_changes_info: Option<CaptureDataChangesInfo>`
**Accessor:** `capture_data_changes_info()` — returns `&Option<CaptureDataChangesInfo>`
**Passed from:** `core/translate/mod.rs` — read from connection when creating builder

### 4. PrepareContext

**Field:** `core/vdbe/mod.rs` — `capture_data_changes: Option<CaptureDataChangesInfo>`
**Set from:** `PrepareContext::from_connection()` — clones from `connection.get_capture_data_changes_info()`

### 5. InitCdcVersion Opcode — `core/vdbe/execute.rs`

Always emitted by PRAGMA SET. Handles all CDC setup at execution time:
1. For "off": stores `None` in `state.pending_cdc_info`, returns early
2. Checks if CDC table already exists (for v1 backward compatibility)
3. Creates CDC table (`CREATE TABLE IF NOT EXISTS <cdc_table_name> ...`) — v2 schema with `change_txn_id` column
4. Creates version table (`CREATE TABLE IF NOT EXISTS turso_cdc_version ...`)
5. Inserts version row: if CDC table pre-existed → "v1", otherwise → current version ("v2"). Uses `INSERT OR IGNORE` to preserve existing version rows.
6. Reads back actual version from the table
7. Stores computed `CaptureDataChangesInfo` in `state.pending_cdc_info`

The connection's CDC state is **not applied in the opcode**. Instead, `pending_cdc_info` is applied in `halt()` only after the transaction commits successfully. This ensures atomicity: if any step fails and the transaction rolls back, the connection's CDC state remains unchanged.

All table creation is done via nested `conn.prepare()`/`run_ignore_rows()` calls rather than bytecode emission, because the PRAGMA plan can't contain DML against tables that don't exist yet in the schema.

## Bytecode Emission (core/translate/emitter.rs)

These are the core CDC code generation functions:

| Function | Purpose |
|----------|---------|
| `prepare_cdc_if_necessary()` | Opens CDC table cursor if CDC is active and target != CDC table |
| `emit_cdc_full_record()` | Reads all columns from cursor into a MakeRecord (for before/after image) |
| `emit_cdc_patch_record()` | Builds record from in-flight register values (for after-image of INSERT/UPDATE) |
| `emit_cdc_insns()` | Writes a single CDC row per changed row (INSERT/UPDATE/DELETE). Called per-row inside DML loops. |
| `emit_cdc_commit_insns()` | Writes a COMMIT record (change_type=2) into CDC table (v2 only). Raw emission, no autocommit check. |
| `emit_cdc_autocommit_commit()` | End-of-statement COMMIT emission. Checks `is_autocommit()` at runtime — only emits COMMIT if in autocommit mode. v2 only. |

### COMMIT Emission Strategy (v2)

Per-row call sites use `emit_cdc_insns()` (no COMMIT). End-of-statement sites call `emit_cdc_autocommit_commit()` which checks `is_autocommit()` at runtime:
- **Autocommit mode:** emits a COMMIT record after the statement completes
- **Explicit transaction (`BEGIN...COMMIT`):** skips per-statement COMMIT; the explicit `COMMIT` statement emits the COMMIT record via `emit_cdc_commit_insns()`

This ensures multi-row statements like `INSERT INTO t VALUES (1),(2),(3)` produce one COMMIT at the end, not one per row.

## Integration Points — Where CDC Records Are Emitted

### INSERT — `core/translate/insert.rs`
- **Per-row:** `emit_cdc_insns()` after insert, and before delete for REPLACE/conflict
- **End-of-statement:** `emit_cdc_autocommit_commit()` in `emit_epilogue()` after the insert loop

### UPDATE — `core/translate/emitter.rs`
- **Per-row:** captures before-image, after-image via patch record, emits `emit_cdc_insns()`
- **End-of-statement:** `emit_cdc_autocommit_commit()` after the update loop

### DELETE — `core/translate/emitter.rs`
- **Per-row:** captures before-image and emits `emit_cdc_insns()`
- **End-of-statement:** `emit_cdc_autocommit_commit()` after the delete loop

### UPSERT (ON CONFLICT DO UPDATE) — `core/translate/upsert.rs`
- **Per-row:** `emit_cdc_insns()` for all three cases: pure insert, update after conflict, replace
- No end-of-statement COMMIT — upsert shares INSERT's epilogue

### Schema Changes (DDL) — `core/translate/schema.rs`
- **CREATE TABLE:** `emit_cdc_insns()` (insert into `sqlite_schema`) + `emit_cdc_autocommit_commit()`
- **DROP TABLE:** `emit_cdc_insns()` per-row in metadata loop + `emit_cdc_autocommit_commit()` after loop
- **CREATE INDEX:** `emit_cdc_insns()` + `emit_cdc_autocommit_commit()` (`core/translate/schema.rs`)
- **DROP INDEX:** `emit_cdc_insns()` per-row + `emit_cdc_autocommit_commit()` after loop (`core/translate/index.rs`)

DDL in explicit transactions (`BEGIN; CREATE TABLE t(x); COMMIT`) does NOT emit per-statement COMMIT — the autocommit check prevents it.

### ALTER TABLE — `core/translate/update.rs`
- Sets `cdc_update_alter_statement` on the update plan when CDC has updates mode

### Views/Triggers — Explicitly excluded
- `core/translate/view.rs` — passes `None` for CDC cursor
- `core/translate/trigger.rs` — passes `None` for CDC cursor

### Subqueries — No CDC
- `core/translate/subquery.rs` — `cdc_cursor_id: None`

## Helper Functions (for reading CDC data)

### `table_columns_json_array(table_name)` — `core/function.rs`, `core/vdbe/execute.rs`
Returns JSON array of column names for a table. Used to interpret binary records.

### `bin_record_json_object(columns_json, blob)` — `core/function.rs`, `core/vdbe/execute.rs`
Decodes a binary record (from `before`/`after`/`updates` columns) into a JSON object using column names.

## Sync Engine Integration

The sync engine is the primary consumer of CDC data.

### DatabaseTape — `sync/engine/src/database_tape.rs`
- **CDC config:** `DEFAULT_CDC_TABLE_NAME = "turso_cdc"`, `DEFAULT_CDC_MODE = "full"`
- **PRAGMA name:** `CDC_PRAGMA_NAME = "capture_data_changes_conn"`
- **Initialization:** `connect()` sets CDC pragma and caches `cdc_version` from `turso_cdc_version` table. Must be called before `iterate_changes()`.
- **Version caching:** `cdc_version: RwLock<Option<CdcVersion>>` — set by `connect()`, read by `iterate_changes()`. Panics if not set.
- **Iterator:** `DatabaseChangesIterator` reads CDC table in batches, emits `DatabaseTapeOperation`. For v2, real COMMIT records from the table are emitted. For v1, a synthetic Commit is appended at end of batch. `ignore_schema_changes: true` (default) filters out `sqlite_schema` row changes but not COMMIT records.

### Sync Operations — `sync/engine/src/database_sync_operations.rs`
- **Change counting:** `SELECT COUNT(*) FROM turso_cdc WHERE change_id > ?`

### Sync Engine — `sync/engine/src/database_sync_engine.rs`
- **Initialization:** `open_db()` calls `main_tape.connect(coro)` to ensure CDC is set up and version is cached before any `iterate_changes()` calls.
- During `apply_changes`, checks if CDC table existed, re-creates it after sync

### Replay Generator — `sync/engine/src/database_replay_generator.rs`
- Requires `updates` column to be populated (full mode)

## Bindings CDC Surface

All bindings expose `cdc_operations` as part of sync stats:

| Binding | File |
|---------|------|
| Python | `bindings/python/src/turso_sync.rs` |
| JavaScript | `bindings/javascript/sync/src/lib.rs` |
| JS (generator) | `bindings/javascript/sync/src/generator.rs` |
| Go | `bindings/go/bindings_sync.go` |
| React Native | `bindings/react-native/src/types.ts` |
| SDK Kit (C header) | `sync/sdk-kit/turso_sync.h` |
| SDK Kit (Rust) | `sync/sdk-kit/src/bindings.rs` |

## Tests

- **Integration tests:** `tests/integration/functions/test_cdc.rs` — covers all modes, CRUD, transactions, schema changes, version table, backward compatibility. Registered in `tests/integration/functions/mod.rs`.
- **Sync engine tests:** `sync/engine/src/database_tape.rs` — CDC table reads, tape iteration, replay of schema changes.
- **JS binding tests:** `bindings/javascript/sync/packages/{wasm,native}/promise.test.ts`

Run: `cargo test -- test_cdc` (integration) or `cargo test -p turso_sync_engine -- database_tape` (sync engine).

## User-facing Documentation

- **CLI manual page:** `cli/manuals/cdc.md` — accessible via `.manual cdc` in the REPL
- **Database manual:** `docs/manual.md` — CDC section linked in TOC

## Key Design Decisions

1. **Per-connection, not per-database.** Each connection has its own CDC mode and can target different tables.
2. **Bytecode-level implementation.** CDC instructions are emitted alongside the actual DML bytecode during translation — no runtime hooks or triggers.
3. **Self-exclusion.** Changes to the CDC table and `turso_cdc_version` table are never captured (checked in `prepare_cdc_if_necessary`).
4. **Schema changes tracked.** DDL operations are recorded as changes to `sqlite_schema` table.
5. **Binary record format.** Before/after/updates columns use SQLite's MakeRecord format (same as B-tree payload).
6. **Transaction-aware.** CDC writes happen within the same transaction as the DML, so rollback naturally discards CDC entries.
7. **Version tracking.** CDC schema version is recorded in `turso_cdc_version` table and carried in `CaptureDataChangesInfo.version` for future schema evolution.
8. **Atomic PRAGMA.** Connection CDC state is deferred via `pending_cdc_info` in `ProgramState` and applied only at Halt. If the PRAGMA's disk writes fail and the transaction rolls back, the connection state stays unchanged.
9. **Per-statement COMMIT (v2).** COMMIT records are emitted once per statement (not per row), using `emit_cdc_autocommit_commit()` which checks `is_autocommit()` at runtime. In explicit transactions, only the final `COMMIT` emits a COMMIT CDC record.
10. **Backward-compatible version detection.** Pre-existing v1 CDC tables (without `turso_cdc_version`) are detected by checking table existence before creation. Existing tables get `CdcVersion::V1` inserted into the version table.
11. **Typed version enum.** `CdcVersion` enum with `#[repr(u8)]` and `Ord`/`PartialOrd` enables feature gating via integer comparison (`has_commit_record()` = `self >= V2`). `Display`/`FromStr` handles database round-trip.
12. **CDC and MVCC mutual exclusion.** Enabling CDC when MVCC is active (or vice versa) returns an error. Checked at PRAGMA set time and journal mode switch time.
