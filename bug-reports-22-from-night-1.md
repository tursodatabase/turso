# Fuzzer Panic Reproducers

## Summary

22 panic locations from lost fuzzer seeds. **20 confirmed, 1 confirmed from recovered seed, 1 already fixed.**

Total crashes represented: ~11,283

| # | Location | Crashes | Status | Reproducer |
|---|----------|---------|--------|------------|
| 1 | `schema.rs:2007` | 37 | CONFIRMED | `CREATE TABLE t(a, b, UNIQUE(a) ON CONFLICT IGNORE);` |
| 2 | `schema.rs:2747` | 10 | CONFIRMED | `CREATE TABLE t(x);ALTER TABLE t ADD COLUMN y COLLATE bogus;` |
| 3 | `btree.rs:2475` | 1 | CONFIRMED | UPDATE with self-join subquery + 26+ RTRIM rows |
| 4 | `expr.rs:2434` | 348 | CONFIRMED | `WITH c(v) AS (SELECT 9) UPDATE t SET a=c.v;` (keyed table) |
| 5 | `expr.rs:2594` | 1 | CONFIRMED | `WITH c AS (SELECT 1 x) SELECT (SELECT c.x) FROM c t;` |
| 6 | `insert.rs:1573` | 31 | CONFIRMED | `INSERT INTO t(x) SELECT a FROM t;` (bad column + self-ref) |
| 7 | `integrity_check.rs:101` | 21 | CONFIRMED | `CREATE INDEX i ON t(a+1);PRAGMA integrity_check;` |
| 8 | `main_loop.rs:433` | 857 | CONFIRMED | `DELETE FROM t1 WHERE a=1 OR b=2;` (multi-index) |
| 9 | `order.rs:132` | 256 | CONFIRMED | `SELECT a FROM t GROUP BY a ORDER BY a, a;` |
| 10 | `plan.rs:70` | 7,215 | CONFIRMED | `SELECT * FROM t1 WHERE x IN (SELECT x FROM t2);` |
| 11 | `planner.rs:1534` | 16 | CONFIRMED | `SELECT * FROM (SELECT 1) NATURAL JOIN (SELECT 1);` |
| 12 | `pragma.rs:379` | 208 | CONFIRMED | `PRAGMA database_list=1;` |
| 13 | `rollback.rs:13` | 167 | CONFIRMED | `ROLLBACK TO x;` |
| 14 | `types.rs:409` | 42 | CONFIRMED | `SELECT sum(x) FROM (SELECT 1e308 x UNION ALL SELECT 1e308 UNION ALL SELECT 0);` |
| 15 | `types.rs:1843` | 54 | CONFIRMED | `SELECT sum(x) OVER(PARTITION BY x,x,x) FROM t;` |
| 16 | `execute.rs:3245` | 224 | CONFIRMED | `SELECT * FROM t WHERE id < 1 OR a = 1;` (IPK + index) |
| 17 | `execute.rs:3731` | 1 | CONFIRMED | Row-value IN subquery clobbers LIMIT register |
| 18 | `execute.rs:7803` | 1,776 | CONFIRMED | `SELECT (SELECT 1, 2) = (SELECT 1, 2);` |
| 19 | `insn.rs:10` | 1 | CONFIRMED | `SELECT 1,1,...(32767 cols)... UNION SELECT ...;` |
| 20 | `vdbe/mod.rs:159` | 15 | CONFIRMED | `INSERT INTO t VALUES(NULL,4);CREATE INDEX i ON t(a) WHERE a ISNULL OR b>2;` |
| 21 | `vdbe/mod.rs:582` | 13 | CONFIRMED | LEFT JOIN autoindex + empty table + ungrouped aggregate |
| 22 | `rowset.rs:82` | 2 | CONFIRMED | Multi-index OR subquery with 2+ outer rows |

---

## Detailed Reports

### 1. `core/schema.rs:2007` - unimplemented ON CONFLICT (37 crashes)

**Panic:** `unimplemented!("ON CONFLICT not implemented")`

**Reproducer:**
```sql
CREATE TABLE t(a, b, UNIQUE(a) ON CONFLICT IGNORE);
```

**Root cause:** Table-level `UNIQUE` constraint with `ON CONFLICT` clause is parsed correctly but `create_table()` has an `unimplemented!()` stub at line 2007.

**Fix:** Implement ON CONFLICT for table-level UNIQUE constraints, or return a graceful error instead of panicking.

---

### 2. `core/schema.rs:2747` - bad collation in ALTER TABLE ADD COLUMN (10 crashes)

**Panic:** `.expect("collation should have been set correctly in create table")`

**Reproducer:**
```sql
CREATE TABLE t(x);
ALTER TABLE t ADD COLUMN y COLLATE bogus;
```

**Root cause:** `Column::from(&ColumnDefinition)` uses `.expect()` on `CollationSeq::new()` which only accepts Binary/NoCase/Rtrim/Unset. The `create_table()` path properly uses `?` to propagate the error, but `ALTER TABLE ADD COLUMN` bypasses that validation.

**Fix:** Change `From<&ColumnDefinition>` to `TryFrom`, or validate collation before calling `Column::from()` in the ALTER path.

---

### 3. `core/storage/btree.rs:2475` - rightmost_pointer unwrap (1 crash)

**Panic:** `.unwrap()` on `None` from `parent_contents.rightmost_pointer()`

**Reproducer (minimized to 26 rows):**
```sql
CREATE TABLE v0 ( c1 TEXT COLLATE RTRIM );
INSERT INTO v0 ( c1 ) VALUES ( x'68617265' ), ( 'v1' ), ( 'v1' ), ( 'v1' ),
  ( 'v0' ), ( 'v1' ), ( 'x' ), ( 'x' ), ( 'three-a' ), ( 'v0' ), ( 'x' ),
  ( 'x' ), ( 'x' ), ( 'x' ), ( 'three-c' ), ( 'v1' ), ( 'av3 c' ), ( 'x' ),
  ( 'MED PACK' ), ( 'v0' ), ( 'x' ), ( 'x' ), ( 'v0' ), ( 'v0' ), ( 'v1' ),
  ( 'v1' );
UPDATE v0 SET c1 = c1 + 18446744073709518848 WHERE ( SELECT * FROM v0 AS a4
  NATURAL JOIN v0 AS a5 JOIN v0 AS a6 USING ( c1, c1, c1, c1 ) ORDER BY c1,
  c1, c1, c1, + sum ( 18446744071562067968 ) OVER ( ORDER BY substr ( c1, 127 ),
  c1 ) ) = 0;
```

**Root cause:** The UPDATE with a complex self-join subquery in the WHERE clause causes a specific B-tree page split pattern during mass row updates. The balance_quick fast-path check at line 2475 expects the parent page to be an interior page, but the specific sequence of inserts followed by updates (which delete + re-insert with different values) creates a state where the parent page type is unexpected.

**Fix:** Replace `.unwrap()` with `.map_or(false, |ptr| ptr == cur_page.get().id as u32)` to gracefully skip `balance_quick`.

---

### 4. `core/translate/expr.rs:2434` - CTE lost in UPDATE ephemeral table transform (348 crashes)

**Panic:** `unreachable!` when `find_table_by_internal_id` returns `None`

**Reproducer:**
```sql
CREATE TABLE t(a INTEGER PRIMARY KEY, b);
INSERT INTO t VALUES(1, 2);
WITH c(v) AS (SELECT 9) UPDATE t SET a = c.v;
```

**Root cause:** `add_ephemeral_table_to_update_plan()` in `optimizer/mod.rs` replaces `table_references` but drops the original `outer_query_refs` (which contain CTEs). SET clause expressions still reference the CTE's internal ID, which no longer exists.

**Fix:** Preserve original `outer_query_refs` when constructing the new `table_references` in `add_ephemeral_table_to_update_plan`.

---

### 5. `core/translate/expr.rs:2594` - CTE alias mismatch in scalar subquery (1 crash)

**Panic:** `.expect("Outer query subquery result_columns_start_reg must be set in program")`

**Reproducer:**
```sql
WITH c AS (SELECT 1 x) SELECT (SELECT c.x) FROM c t;
```

**Root cause:** CTE `c` aliased as `t` in FROM clause. Scalar subquery `(SELECT c.x)` resolves `c.x` via `outer_query_refs` to internal_id A, but the emitter only registers internal_id B (from `joined_tables`). `get_subquery_result_reg(A)` returns `None`.

**Fix:** When a CTE is aliased, the original name should not be resolvable, or `outer_query_refs` should be cleaned up after CTE consumption.

---

### 6. `core/translate/insert.rs:1573` - unwrap on nonexistent column in INSERT-SELECT (31 crashes)

**Panic:** `.unwrap()` on `None` from `table.get_column_by_name()`

**Reproducer:**
```sql
CREATE TABLE t(a);
INSERT INTO t(x) SELECT a FROM t;
```

**Root cause:** `init_source_emission` runs before column validation in `build_column_mappings`. The ephemeral table path (triggered by self-referencing INSERT-SELECT) calls `.unwrap()` on the column lookup before validation can produce a clean error.

**Fix:** Replace `.unwrap()` with proper error handling, or move column validation before `init_source_emission`.

---

### 7. `core/translate/integrity_check.rs:101` - expression index sentinel (21 crashes)

**Panic:** Index out of bounds (`usize::MAX` used as array index)

**Reproducer:**
```sql
CREATE TABLE t(a);
CREATE INDEX i ON t(a+1);
PRAGMA integrity_check;
```

**Root cause:** Expression-based index columns use `EXPR_INDEX_SENTINEL = usize::MAX` for `pos_in_table`. The integrity check loop doesn't filter these out before indexing into `btree_table.columns[pos]`.

**Fix:** Skip expression-based index columns (where `pos == EXPR_INDEX_SENTINEL` or `expr.is_some()`) in the integrity check loop.

---

### 8. `core/translate/main_loop.rs:433` - MultiIndexScan in DELETE/UPDATE (857 crashes)

**Panic:** `unreachable!("Multi-index scans should only occur in SELECT operations")`

**Reproducer:**
```sql
CREATE TABLE t1(a INTEGER, b INTEGER);
CREATE INDEX idx_a ON t1(a);
CREATE INDEX idx_b ON t1(b);
DELETE FROM t1 WHERE a = 1 OR b = 2;
```

**Root cause:** Optimizer picks `MultiIndexScan` for DELETE/UPDATE, but `init_loop` only implements it for SELECT. The optimizer has no guard against this.

**Fix:** Either guard the optimizer to prevent MultiIndexScan for non-SELECT, or implement it in the emitter for DELETE/UPDATE.

---

### 9. `core/translate/optimizer/order.rs:132` - duplicate ORDER BY with GROUP BY (256 crashes)

**Panic:** `assert!(group_by.exprs.len() >= order_by.len())`

**Reproducer:**
```sql
CREATE TABLE t(a);
SELECT a FROM t GROUP BY a ORDER BY a, a;
```

**Root cause:** `group_by_contains_all` check uses `.all()` + `.any()` which allows the same GROUP BY expression to satisfy multiple ORDER BY duplicates, but the assertion assumes ORDER BY can't be longer than GROUP BY.

**Fix:** Deduplicate ORDER BY before the check, or remove the assertion and fall back to the non-optimized path.

---

### 10. `core/translate/plan.rs:70` - correlated IN subquery (7,215 crashes) TOP OFFENDER

**Panic:** `.unwrap()` on `None` from `find_joined_table_by_internal_id()`

**Reproducer:**
```sql
CREATE TABLE t1(x);
CREATE TABLE t2(y);
SELECT * FROM t1 WHERE x IN (SELECT x FROM t2);
```

**Root cause:** In a correlated `IN` subquery, the inner SELECT references an outer table column. `find_joined_table_by_internal_id()` only searches `joined_tables`, not `outer_query_refs`. The outer table's ID is only in `outer_query_refs`.

**Fix:** Change `.unwrap()` to `?` (return `None`) since the method already returns `Option<&str>`.

---

### 11. `core/translate/planner.rs:1534` - NATURAL JOIN with unnamed columns (16 crashes)

**Panic:** `.expect("column name is None")`

**Reproducer:**
```sql
SELECT * FROM (SELECT 1) NATURAL JOIN (SELECT 1);
```

**Root cause:** `None == None` evaluates to `true` in Rust's `Option` equality, so two unnamed columns are treated as matching in the NATURAL JOIN logic. Then `.expect()` panics on the `None` name.

**Fix:** Add `left_col.name.is_some() &&` guard before the equality check.

---

### 12. `core/translate/pragma.rs:379` - DatabaseList missing from routing (208 crashes)

**Panic:** `unreachable!("database_list cannot be set")`

**Reproducer:**
```sql
PRAGMA database_list=1;
```

**Root cause:** `PragmaName::DatabaseList` is missing from the read-only pragma routing list at lines 76-87 of `translate_pragma`. It falls through to `update_pragma` which hits `unreachable!`.

**Fix:** Add `PragmaName::DatabaseList` to the routing list alongside `TableList`, `IndexList`, etc.

---

### 13. `core/translate/rollback.rs:13` - named ROLLBACK assert (167 crashes)

**Panic:** `assert!(txn_name.is_none() && savepoint_name.is_none(), "txn_name and savepoint not supported yet")`

**Reproducer:**
```sql
ROLLBACK TO x;
```

**Root cause:** `translate_rollback` uses `assert!` instead of graceful error. The parser correctly parses `ROLLBACK TO <name>` but the translator panics.

**Fix:** Replace `assert!` with `bail_parse_error!` consistent with adjacent RELEASE/SAVEPOINT handlers.

---

### 14. `core/types.rs:409` - NaN from KBN compensation (42 crashes)

**Panic:** `panic!("as_float must be called only for Value::Numeric")`

**Reproducer:**
```sql
SELECT sum(x) FROM (SELECT 1e308 x UNION ALL SELECT 1e308 UNION ALL SELECT 0);
```

**Root cause:** KBN compensation step computes `inf - inf = NaN`. `Value::from_f64(NaN)` silently converts to `Value::Null`. Later `as_float()` panics on Null.

**Fix:** Handle NaN in KBN arithmetic, or use raw `f64` for internal aggregate state instead of `Value`.

---

### 15. `core/types.rs:1843` - duplicate PARTITION BY mismatch (54 crashes)

**Panic:** `assert!(l.len() >= column_info.len())`

**Reproducer:**
```sql
CREATE TABLE t(x);
INSERT INTO t VALUES(1);
SELECT sum(x) OVER(PARTITION BY x,x,x) FROM t;
```

**Root cause:** `compare_key_info` is built from raw `partition_by.len()` (3) but register count uses `deduplicated_partition_by_len` (1). The assertion `2 >= 3` fails.

**Fix:** Use deduplicated count for `compare_key_info` construction.

---

### 16. `core/vdbe/execute.rs:3245` - multi-index OR with upper-bound rowid (224 crashes)

**Panic:** `assert_eq!` failure: `op_seek: num_regs should be 1 for table-btree` (0 != 1)

**Reproducer:**
```sql
CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b);
CREATE INDEX i ON t(a);
INSERT INTO t VALUES(1,1,'x');
SELECT * FROM t WHERE id < 1 OR a = 1;
```

**Root cause:** `emit_multi_index_scan` doesn't handle upper-bound-only rowid constraints (`id < X`). `seek_def.size(&seek_def.start)` returns 0 for no lower bound, used as `num_regs`. The regular `emit_seek` handles this by emitting `Rewind` instead.

**Fix:** Add empty-seek-key handling in `emit_multi_index_scan`, same as `emit_seek` (emit `Rewind`/`Last` when `key_count == 0`).

---

### 17. `core/vdbe/execute.rs:3731` - DecrJumpZero on non-integer register (1 crash)

**Panic:** `unreachable!("DecrJumpZero on non-integer register")`

**Reproducer (minimized):**
```sql
CREATE TABLE v0 ( c1, c2, c3 );
INSERT INTO v0 VALUES ( NULL, NULL, NULL );
UPDATE v0 SET c1 = 1 WHERE ( SELECT c1 FROM v0 GROUP BY c2
  ORDER BY ( SELECT * FROM v0 ) IN ( SELECT c1 FROM v0 ) );
```

**Root cause:** Row-value subquery `(SELECT * FROM v0)` returns 3 registers but the IN expression LHS allocates only 1 register. The `Copy` instruction spills 3 registers into a 1-register slot, clobbering the LIMIT counter (r[29]) with NULL. When `DecrJumpZero` tries to decrement it, it finds a non-integer value.

The bug is in two locations:
1. `subquery.rs:457-460` - `lhs_column_count` for IN expression treats row-value subqueries as 1 column
2. `expr.rs:775-780` - Same issue in IN expression translation, allocates 1 register for multi-column row value

**Fix:** Detect row-value subqueries in IN expressions and either reject them (SQLite does) or allocate the correct number of registers.

---

### 18. `core/vdbe/execute.rs:7803` - multi-column subquery in binary expr (1,776 crashes)

**Panic:** Index out of bounds on `state.registers`

**Reproducer:**
```sql
SELECT (SELECT 1, 2) = (SELECT 1, 2);
```

**Root cause:** `binary_expr_shared` allocates only 2 registers (1 per side), but multi-column subqueries write `num_regs` values via `Copy` instruction, overflowing the allocated range.

**Fix:** Either reject multi-column subqueries in binary expressions, or allocate `expr_vector_size` registers per side.

---

### 19. `core/vdbe/insn.rs:10` - u16 overflow (1 crash)

**Panic:** `value exceeds u16::MAX`

**Reproducer:**
```sql
SELECT 1,1,...(32767 times)... UNION SELECT 1,1,...(32767 times)...;
```

**Root cause:** No `SQLITE_MAX_COLUMN` limit enforced. Register count exceeds `u16::MAX` with enough columns. Boundary: 32766 columns OK, 32767 panics.

**Fix:** Enforce a column limit (SQLite defaults to 2000, max 32767) during parsing or plan creation.

---

### 20. `core/vdbe/mod.rs:159` - unresolved placeholder in partial index OR (15 crashes)

**Panic:** `unreachable!("Unresolved placeholder")`

**Reproducer:**
```sql
CREATE TABLE t(a, b);
INSERT INTO t VALUES(NULL, 4);
CREATE INDEX i ON t(a) WHERE a ISNULL OR b > 2;
```

**Root cause:** `translate_create_index` creates `ConditionMetadata` with `jump_target_when_true: Placeholder`. OR handler propagates this Placeholder when recursing on LHS. `IsNull` instruction emits it as `target_pc`. `resolve_labels()` only resolves `Label` variants, not `Placeholder`. At runtime, `op_is_null` calls `as_offset_int()` on the Placeholder.

**Fix:** Use a proper `Label` instead of `Placeholder` for `jump_target_when_true` in the partial index condition, or add `is_offset()` guards in `op_is_null`.

---

### 21. `core/vdbe/mod.rs:582` - cursor id is None (13 crashes)

**Panic:** `cursor id 2 is None` - cursor allocated but never opened

**Reproducer (minimized):**
```sql
CREATE TABLE t(x);
SELECT b.x, COUNT(*) FROM t a LEFT JOIN t b ON a.x=b.x;
```

**Root cause:** LEFT JOIN with autoindex creates cursor slot for the ephemeral autoindex (cursor 2). The `OpenAutoindex` instruction is emitted inside the outer loop body (within a `Once` block). When the left table is empty, `Rewind` skips the entire loop body, so `OpenAutoindex` never executes. After the loop, ungrouped aggregation (`COUNT(*)`) always produces a result row. The post-loop "loop never ran" path in `emit_ungrouped_aggregation` tries to evaluate `b.x` via `Column 2 0 2`, but cursor 2 was never opened.

Required conditions:
1. LEFT JOIN with ON clause (triggers autoindex)
2. Empty left table (loop never executes)
3. Ungrouped aggregate (always emits result row)
4. Non-aggregate column from right table in SELECT list
5. No real index on join column (forces autoindex instead of OpenRead)

**Fix:** In `op_column`, check if the cursor is `None` before accessing it, and return `Value::Null` (matches SQLite behavior for reading from an unopened cursor).

---

### 22. `core/vdbe/rowset.rs:82` - stale RowSet in coroutine (2 crashes)

**Panic:** `turso_assert!("cannot insert after smallest() has been used")`

**Reproducer:**
```sql
CREATE TABLE t1(a INTEGER, b INTEGER);
CREATE INDEX idx_a ON t1(a);
CREATE INDEX idx_b ON t1(b);
INSERT INTO t1 VALUES(1,10);
CREATE TABLE t2(x);
INSERT INTO t2 VALUES(1);
INSERT INTO t2 VALUES(2);
SELECT t2.x, sub.a FROM t2, (SELECT a FROM t1 WHERE a=1 OR b=10) sub;
```

**Root cause:** Multi-index OR scan in a FROM-clause subquery (coroutine). First iteration uses `RowSetRead`/`smallest()` switching to `Smallest` mode. On second iteration, `Null` instruction resets the register but doesn't clear `state.rowsets`. `RowSetAdd` finds the stale RowSet still in `Smallest` mode.

**Fix:** Have `op_null` also call `state.rowsets.remove(&dest)`, or have `op_rowset_add` check for stale entries.

---

## Pre-existing Bug (found separately)

### CTE shadowing view causes stack overflow

**Reproducer:**
```sql
CREATE VIEW v AS SELECT 1;
WITH v AS (SELECT * FROM v) SELECT * FROM v;
```

**Expected (SQLite):** `Parse error: circular reference: v`
**Actual (tursodb):** Stack overflow / process abort

**Root cause:** In `planner.rs`, CTE name conflict check only calls `schema.get_table()` but never `schema.get_view()` or `schema.get_materialized_view()`. Two locations need the fix: `plan_ctes_as_outer_refs()` and `parse_from()`.
