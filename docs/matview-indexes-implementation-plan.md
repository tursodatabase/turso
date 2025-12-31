# Materialized View Indexes - Implementation Plan

## Executive Summary

This document outlines the implementation plan for adding secondary index support to Turso's materialized views. The implementation follows a test-driven approach where tests are written first in `testing/materialized_views.test`.

## Research Summary

### Differential Dataflow / DBSP Approach (Feldera, Materialize)

In Differential Dataflow (the foundation for DBSP), indexes are implemented as **arrangements** - incrementally maintained, versioned indexes shared across operators.

Key characteristics:
- **Data Structure**: `Trace` trait mapping `(index_key, time) → list of values`
- **Incremental Maintenance**: Uses differencing and Moebius inversion over partial orders
- **Version-aware**: Structure is `key → versions → list of values`
- **Sharing**: Multiple operators can reuse one arrangement

### RisingWave Approach

RisingWave uses **shared indexes** stored in a cloud-native state store:
- Indexes are maintained alongside primary view data
- Lookups check in-memory caches first, then persistent storage
- Incremental maintenance during buffer flushes

### Key Insight for Turso

Turso already has a working BTree infrastructure for regular table indexes. The most pragmatic approach is to:
1. Treat materialized views as tables (which they already are - `BTreeTable`)
2. Allow creating standard indexes on them
3. Maintain indexes incrementally alongside the view's DBSP circuit output

## Current Turso Architecture

### Materialized View Storage

```
View Definition (sqlite_schema, type='view')
    ↓
DBSP State Table: __turso_internal_dbsp_state_v<version>_<viewname>
    - Stores intermediate operator state (Z-sets)
    ↓
DBSP State Index: sqlite_autoindex___turso_internal_dbsp_state_v<version>_<viewname>_1
    - Internal index for DBSP state lookups (NOT user-facing)
    ↓
View Output Table: BTreeTable (root_page stored in view metadata)
    - The actual queryable materialized data
```

### Current Limitation

In `core/incremental/cursor.rs:319-321`:
```rust
SeekKey::IndexKey(_) => {
    return Err(LimboError::ParseError(
        "Cannot search a materialized view with an index key".to_string(),
    ));
}
```

This error occurs because the `IncrementalViewCursor` doesn't support index-based access - it only supports table rowid access.

## Proposed Architecture

### Option A: Treat as Regular Table Index (Recommended)

Since materialized views already store their output in a `BTreeTable`, we can:
1. Allow `CREATE INDEX` on materialized views (syntax already exists in SQL)
2. Store these indexes like regular table indexes
3. Incrementally maintain them during DBSP delta application

**Pros:**
- Reuses existing index infrastructure
- Minimal new code
- Compatible with query optimizer

**Cons:**
- Need to intercept index maintenance during delta writes

### Option B: DBSP Arrangements (Full Implementation)

Implement arrangements as per Differential Dataflow:
1. Create a new `Arrangement` struct that wraps indexed state
2. Support `arrange_by_key()` operation in the DBSP compiler
3. Maintain arrangements incrementally alongside Z-sets

**Pros:**
- More aligned with DBSP theory
- Could enable additional optimizations (shared arrangements for joins)

**Cons:**
- Significantly more complex
- Requires changes to the DBSP compiler

### Recommendation

Start with **Option A** as it provides immediate value with minimal complexity. Option B can be pursued later for advanced optimizations.

## Implementation Plan

### Phase 1: Test Cases First

Add the following tests to `testing/materialized_views.test`:

```tcl
# Basic index creation on materialized view
do_execsql_test_on_specific_db {:memory:} matview-create-index-basic {
    CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT);
    INSERT INTO products VALUES
        (1, 'Laptop', 1200, 'Electronics'),
        (2, 'Mouse', 25, 'Electronics'),
        (3, 'Desk', 350, 'Furniture');

    CREATE MATERIALIZED VIEW expensive_items AS
        SELECT id, name, price, category FROM products WHERE price > 100;

    -- Create an index on the materialized view
    CREATE INDEX idx_expensive_category ON expensive_items(category);

    -- Query should use the index (verify via EXPLAIN later)
    SELECT name FROM expensive_items WHERE category = 'Electronics' ORDER BY name;
} {Laptop}

# Index maintenance on INSERT
do_execsql_test_on_specific_db {:memory:} matview-index-insert-maintenance {
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
    CREATE MATERIALIZED VIEW v AS SELECT id, a, b FROM t WHERE b > 10;
    CREATE INDEX idx_v_a ON v(a);

    INSERT INTO t VALUES (1, 'alpha', 20), (2, 'beta', 5), (3, 'alpha', 30);
    SELECT id FROM v WHERE a = 'alpha' ORDER BY id;
} {1
3}

# Index maintenance on DELETE
do_execsql_test_on_specific_db {:memory:} matview-index-delete-maintenance {
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
    INSERT INTO t VALUES (1, 'x', 20), (2, 'x', 30), (3, 'y', 40);
    CREATE MATERIALIZED VIEW v AS SELECT id, a, b FROM t WHERE b > 10;
    CREATE INDEX idx_v_a ON v(a);

    SELECT COUNT(*) FROM v WHERE a = 'x';
    DELETE FROM t WHERE id = 1;
    SELECT COUNT(*) FROM v WHERE a = 'x';
} {2
1}

# Index maintenance on UPDATE
do_execsql_test_on_specific_db {:memory:} matview-index-update-maintenance {
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
    INSERT INTO t VALUES (1, 'old', 20), (2, 'old', 30);
    CREATE MATERIALIZED VIEW v AS SELECT id, a, b FROM t WHERE b > 10;
    CREATE INDEX idx_v_a ON v(a);

    SELECT COUNT(*) FROM v WHERE a = 'old';
    UPDATE t SET a = 'new' WHERE id = 1;
    SELECT COUNT(*) FROM v WHERE a = 'old';
    SELECT COUNT(*) FROM v WHERE a = 'new';
} {2
1
1}

# Multiple indexes on same materialized view
do_execsql_test_on_specific_db {:memory:} matview-multiple-indexes {
    CREATE TABLE orders(id INTEGER PRIMARY KEY, customer TEXT, amount INTEGER, status TEXT);
    INSERT INTO orders VALUES
        (1, 'Alice', 100, 'shipped'),
        (2, 'Bob', 200, 'pending'),
        (3, 'Alice', 150, 'shipped');

    CREATE MATERIALIZED VIEW shipped_orders AS
        SELECT id, customer, amount FROM orders WHERE status = 'shipped';

    CREATE INDEX idx_shipped_customer ON shipped_orders(customer);
    CREATE INDEX idx_shipped_amount ON shipped_orders(amount);

    SELECT id FROM shipped_orders WHERE customer = 'Alice' ORDER BY id;
    SELECT id FROM shipped_orders WHERE amount > 120 ORDER BY id;
} {1
3
3}

# Unique index on materialized view
do_execsql_test_on_specific_db {:memory:} matview-unique-index {
    CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT, value INTEGER);
    INSERT INTO t VALUES (1, 'A001', 100), (2, 'A002', 200);

    CREATE MATERIALIZED VIEW v AS SELECT id, code, value FROM t WHERE value > 50;
    CREATE UNIQUE INDEX idx_v_code ON v(code);

    SELECT code FROM v ORDER BY code;
} {A001
A002}

# Index with aggregation view
do_execsql_test_on_specific_db {:memory:} matview-index-on-aggregation {
    CREATE TABLE sales(id INTEGER, product TEXT, region TEXT, amount INTEGER);
    INSERT INTO sales VALUES
        (1, 'Widget', 'North', 100),
        (2, 'Widget', 'South', 150),
        (3, 'Gadget', 'North', 200),
        (4, 'Gadget', 'South', 250);

    CREATE MATERIALIZED VIEW regional_sales AS
        SELECT region, SUM(amount) as total, COUNT(*) as cnt
        FROM sales
        GROUP BY region;

    CREATE INDEX idx_regional_total ON regional_sales(total);

    SELECT region FROM regional_sales WHERE total > 200 ORDER BY region;
} {North
South}

# Index maintenance with rollback
do_execsql_test_on_specific_db {:memory:} matview-index-rollback {
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
    INSERT INTO t VALUES (1, 'x', 20);
    CREATE MATERIALIZED VIEW v AS SELECT id, a, b FROM t WHERE b > 10;
    CREATE INDEX idx_v_a ON v(a);

    SELECT COUNT(*) FROM v WHERE a = 'x';

    BEGIN;
    INSERT INTO t VALUES (2, 'x', 30);
    SELECT COUNT(*) FROM v WHERE a = 'x';
    ROLLBACK;

    SELECT COUNT(*) FROM v WHERE a = 'x';
} {1
2
1}

# Drop index on materialized view
do_execsql_test_on_specific_db {:memory:} matview-drop-index {
    CREATE TABLE t(id INTEGER PRIMARY KEY, a TEXT);
    INSERT INTO t VALUES (1, 'test');
    CREATE MATERIALIZED VIEW v AS SELECT * FROM t;
    CREATE INDEX idx_v_a ON v(a);

    -- Verify index exists
    SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = 'idx_v_a';

    DROP INDEX idx_v_a;

    -- Verify index is gone
    SELECT COUNT(*) FROM sqlite_schema WHERE type = 'index' AND name = 'idx_v_a';
} {1
0}

# Index on join materialized view
do_execsql_test_on_specific_db {:memory:} matview-index-on-join {
    CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER);

    INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
    INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150);

    CREATE MATERIALIZED VIEW user_orders AS
        SELECT u.name, o.amount
        FROM users u
        JOIN orders o ON u.id = o.user_id;

    CREATE INDEX idx_uo_name ON user_orders(name);

    SELECT amount FROM user_orders WHERE name = 'Alice' ORDER BY amount;
} {100
200}

# Composite index on materialized view
do_execsql_test_on_specific_db {:memory:} matview-composite-index {
    CREATE TABLE events(id INTEGER PRIMARY KEY, category TEXT, status TEXT, priority INTEGER);
    INSERT INTO events VALUES
        (1, 'A', 'open', 1),
        (2, 'A', 'closed', 2),
        (3, 'B', 'open', 3),
        (4, 'A', 'open', 4);

    CREATE MATERIALIZED VIEW active_events AS
        SELECT id, category, status, priority FROM events WHERE status = 'open';

    CREATE INDEX idx_active_cat_pri ON active_events(category, priority);

    SELECT id FROM active_events WHERE category = 'A' ORDER BY priority;
} {1
4}

# Error: Cannot create index on non-existent materialized view
do_execsql_test_in_memory_any_error matview-index-error-no-view {
    CREATE INDEX idx_foo ON nonexistent_view(col);
}
```

### Phase 2: Allow CREATE INDEX on Materialized Views

**Files to modify:**

1. **`core/translate/index.rs`** - `translate_create_index()`
   - Currently checks `table.btree()` - need to also check for materialized views
   - Add check: if target is a materialized view, get its underlying `BTreeTable`

2. **`core/schema.rs`** - Add method to get materialized view's table
   ```rust
   impl Schema {
       pub fn get_matview_table(&self, view_name: &str) -> Option<Arc<BTreeTable>> {
           self.materialized_views.get(view_name)
               .and_then(|v| v.get_output_table())
       }
   }
   ```

3. **`core/incremental/view.rs`** - `IncrementalView`
   - Add method to return the output table reference
   - Store indexes created on this view

### Phase 3: Index Maintenance During Delta Application

**Files to modify:**

1. **`core/incremental/compiler.rs`** - `WriteRowView` state machine
   - After writing to the view's BTreeTable, also update any indexes
   - Need access to the schema to find indexes on this view

2. **`core/vdbe/execute.rs`** - Delta commit logic
   - When committing view deltas, ensure index updates are included

**Algorithm for index maintenance:**

```rust
// Pseudocode for index maintenance during delta application
fn apply_delta_to_view(delta: &Delta, view: &IncrementalView) -> Result<()> {
    let indexes = schema.get_indices(&view.name);

    for (row, weight) in delta.changes.iter() {
        if weight > 0 {
            // INSERT: add to view table and all indexes
            view_cursor.insert(row.rowid, &row.values)?;
            for index in &indexes {
                let index_key = build_index_key(index, &row.values);
                index_cursor.insert(index_key, row.rowid)?;
            }
        } else {
            // DELETE: remove from view table and all indexes
            view_cursor.delete(row.rowid)?;
            for index in &indexes {
                let index_key = build_index_key(index, &row.values);
                index_cursor.delete(index_key)?;
            }
        }
    }
    Ok(())
}
```

### Phase 4: Query Optimizer Integration

**Files to modify:**

1. **`core/translate/optimizer/join.rs`** and **`core/translate/optimizer/cost.rs`**
   - Ensure the optimizer considers indexes on materialized views
   - Should work mostly automatically since views are BTreeTables

2. **`core/incremental/cursor.rs`**
   - Remove the error for `SeekKey::IndexKey`
   - Support index-based access to materialized view data

### Phase 5: DROP INDEX Support

**Files to modify:**

1. **`core/translate/mod.rs`** - `translate_drop_index()`
   - Verify it works correctly for indexes on materialized views
   - May need special handling to ensure DBSP state is not affected

## Implementation Order

1. **Week 1: Tests + Basic CREATE INDEX**
   - Add all test cases to `materialized_views.test`
   - Modify `translate_create_index()` to accept materialized views
   - Basic schema storage for matview indexes

2. **Week 2: Index Maintenance**
   - Implement index updates in `WriteRowView`
   - Handle INSERT/DELETE/UPDATE propagation
   - Ensure transactional consistency (rollback support)

3. **Week 3: Query Optimizer + Polish**
   - Enable optimizer to use matview indexes
   - Remove `SeekKey::IndexKey` error in cursor
   - DROP INDEX support
   - Edge cases and error handling

## Data Structures

### Existing (No Changes Required)

```rust
// core/schema.rs - Already exists
pub struct Index {
    pub name: String,
    pub table_name: String,  // Will be the matview name
    pub root_page: i64,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub ephemeral: bool,
    pub has_rowid: bool,
    pub where_clause: Option<Box<Expr>>,
    pub index_method: Option<Arc<dyn IndexMethodAttachment>>,
}
```

### New (Minimal Additions)

```rust
// core/incremental/view.rs
impl IncrementalView {
    /// Get indexes defined on this materialized view
    pub fn get_indexes(&self, schema: &Schema) -> Vec<Arc<Index>> {
        schema.get_indices(&self.name).collect()
    }
}
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Index/view consistency during crashes | Data corruption | Ensure indexes are updated in same transaction as view |
| Performance regression on write-heavy workloads | Slower inserts | Document overhead; make indexes optional |
| Complex rollback scenarios | Incorrect data | Comprehensive test coverage for rollback cases |
| Interaction with DBSP state table indexes | Confusion | Clear naming convention; validation in CREATE INDEX |

## Success Criteria

1. All test cases pass
2. `CREATE INDEX` works on materialized views
3. Indexes are maintained correctly on INSERT/UPDATE/DELETE
4. Rollback correctly reverts index changes
5. Query optimizer uses indexes (verify with EXPLAIN)
6. No regression in existing matview tests

## References

- [Differential Dataflow Arrangements](https://timelydataflow.github.io/differential-dataflow/chapter_5/chapter_5.html)
- [DBSP Paper](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
- [RisingWave Shared Indexes](https://www.risingwave.com/blog/shared-indexes-and-joins-in-streaming-databases/)
- [Materialize Arrangements](https://materialize.com/blog/differential-from-scratch/)
