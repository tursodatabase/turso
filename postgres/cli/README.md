# pgmicro

In-process PostgreSQL reimplementation backed by Turso's SQLite-compatible engine.

pgmicro translates PostgreSQL SQL (via libpg_query) directly into Turso's AST,
then executes it through the full Turso compilation and bytecode pipeline. No
SQLite text is ever generated — the entire path from PG parse tree to bytecode
is native.

## Live Materialized Views

Unlike PostgreSQL, where materialized views are static snapshots that require
`REFRESH MATERIALIZED VIEW` to update, pgmicro's materialized views are **live**.
They automatically reflect changes to the underlying tables without any manual
refresh step.

This is possible because Turso implements materialized views using DBSP
(Differential DataFlow Stream Processing) for incremental maintenance. When you
insert, update, or delete rows in a base table, the materialized view is updated
incrementally — no full recomputation needed.

```sql
CREATE TABLE sales(product TEXT, amount INT);
INSERT INTO sales VALUES ('Widget', 100);

CREATE MATERIALIZED VIEW totals AS
  SELECT product, SUM(amount) as total FROM sales GROUP BY product;

SELECT * FROM totals;
-- Widget | 100

INSERT INTO sales VALUES ('Widget', 50);

-- No REFRESH needed — the view is already up to date
SELECT * FROM totals;
-- Widget | 150
```

`REFRESH MATERIALIZED VIEW` is accepted for compatibility but is a no-op.
`DROP MATERIALIZED VIEW` works as expected.
