# Identifier type follow-up work

## What was done (branch: `identifier-type`, 2 commits)

1. **Added `Identifier` type** (`parser/src/identifier.rs`): newtype around `String` with ASCII-only case-insensitive `Eq`/`Hash`/`Ord`. Implements `PartialEq<str>`, `PartialEq<&str>`, `Display`, `From<String>`, `From<&str>`, serde.

2. **`Name` wraps `Identifier`** (`parser/src/ast.rs`): `Name { quote: Option<char>, value: Identifier }`. Name's `PartialEq` and `Hash` delegate to Identifier. Added `PartialEq<str>` and `PartialEq<&str>` for Name. Added `name.identifier()` and `name.into_identifier()`.

3. **Schema maps use `Identifier` keys**: `HashMap<Identifier, ...>` for tables/views/triggers/indexes/etc. `BTreeSet<Identifier>` for materialized_view_names/has_indexes/incompatible_views. Same for `AnalyzeStats`.

4. **`normalize_ident` is fully deleted** from the codebase (was ~259 call sites). `check_ident_equivalency` also deleted.

## What remains — push `Identifier` deeper

The current code still has friction points where `&str` is extracted from `Name`/`Identifier` just to pass through function boundaries, or where struct fields store `String` and comparisons use `eq_ignore_ascii_case`. The type should flow naturally instead.

### 1. Change function signatures from `&str` to `&Identifier`

Functions that accept identifier names as `&str` should take `&Identifier` instead. This eliminates the `Identifier::from(x)` allocation at every call site (the caller already has one from the `Name`). Key functions:

**Schema methods** (`core/schema.rs`):
- `get_table`, `get_btree_table`, `get_view`, `get_materialized_view`, `get_trigger`, `get_trigger_for_table`, `get_triggers_for_table`, `get_indices`, `get_index`, `add_trigger`, `remove_trigger`, `remove_table`, `remove_view`, `remove_indices_for_table`, `table_has_indexes`, `table_set_has_index`, `is_materialized_view`, `has_compatible_dbsp_state_table`, `has_incompatible_dependent_views`, `add_materialized_view_dependency`, `get_dependent_materialized_views`, `check_object_name_conflict`, `is_unique_idx_name`

**Connection methods** (`core/connection.rs`):
- `get_database_by_name`

**Resolver/emitter methods** in translate/:
- `resolve_database_id`, `get_attached_database`, helper functions in `plan.rs`

### 2. Migrate struct fields from `String` to `Identifier`

These structs store identifier names as `String` but should use `Identifier`:

- `BTreeTable.name: String` → `Identifier`
- `Index.name: String`, `Index.table_name: String` → `Identifier`
- `IndexColumn.name: String` → `Identifier`
- `Column.name: Option<String>` → `Option<Identifier>`
- `View.name: String` → `Identifier`
- `Trigger.name: String`, `Trigger.table_name: String` → `Identifier`
- `ForeignKey.parent_table: String`, `ForeignKey.child_columns: Vec<String>`, `ForeignKey.parent_columns: Vec<String>` → `Identifier` / `Vec<Identifier>`
- `JoinedTable.identifier: String` in `core/translate/plan.rs` → `Identifier`
- `VirtualTable.name: String` → `Identifier`

This will cascade to their construction sites and eliminate many remaining `eq_ignore_ascii_case` calls and `.as_str().to_owned()` patterns.

### 3. Make `ROWID_STRS` use `Identifier`

Currently `pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"]` in `core/translate/planner.rs`. Every usage does `.iter().any(|s| s.eq_ignore_ascii_case(...))`. Since `Identifier` wraps `String` (heap-allocated), it can't be `const`. Options:
- `lazy_static!` / `LazyLock` with `[Identifier; 3]`  
- Keep as `[&str; 3]` and compare via `Name`'s `PartialEq<&str>` (already works: `*name == *rowid_str`)
- A helper function `is_rowid_name(name: &Name) -> bool`

### 4. Change local collection types

Places that build `HashSet<String>` or `HashMap<String, ...>` with identifier keys should use `Identifier`:
- `updated_col_names` in `core/translate/emitter/update.rs` (~line 1516)
- Column lookup maps in `core/translate/update.rs`, `core/translate/upsert.rs`
- Various local maps in `core/util.rs` rename/rewrite functions

### 5. Remaining `eq_ignore_ascii_case` on identifiers

After struct fields become `Identifier`, most `eq_ignore_ascii_case` calls on identifier strings become simple `==` comparisons. Grep for `eq_ignore_ascii_case` and check which ones compare database identifiers (vs keywords like "STORED", "TRUE", etc. which should stay as-is).

## Approach

Start with #2 (struct fields) since it has the largest cascading effect — once `BTreeTable.name` is `Identifier`, many `.as_str()` extractions and `Identifier::from()` wrappers disappear naturally. Then #1 (function signatures) becomes mostly mechanical. #3 and #4 are small cleanups after that.

## Verification

- `cargo check` after each step
- `cargo clippy --workspace --all-features --all-targets -- --deny=warnings`
- `cargo test -p turso_core` (12 vector distance test failures are pre-existing)
- `cargo fmt`
- `grep -rn "eq_ignore_ascii_case" core/` — count should decrease significantly after struct field migration
