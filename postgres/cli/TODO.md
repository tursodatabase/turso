# pgmicro: Missing PG Features

Sorted by implementation difficulty.

## Trivial (hours, mostly wiring)

1. **COMMENT ON** — parse + ignore (no-op, just don't error)
2. **PREPARE / EXECUTE / DEALLOCATE** — wire protocol already handles this; just need the SQL syntax to not error
3. **GRANT/REVOKE** — parse + ignore (single-user system, just don't error)
4. **More PG system functions** — register existing Turso functions under PG aliases (e.g. `string_agg` → `group_concat`, `regexp_replace`, etc.)
5. **SET search_path** — intercept in try_prepare_pg, store on connection, use in table resolution

## Easy (a day or two)

6. **CREATE TABLE AS / SELECT INTO** — translate to `CREATE TABLE ... AS SELECT` which Turso already supports
7. **ALTER COLUMN SET/DROP DEFAULT, SET/DROP NOT NULL** — Turso's ALTER TABLE is limited but these map to SQLite operations
8. **CONCURRENTLY on CREATE INDEX** — just ignore the keyword (SQLite doesn't do concurrent DDL anyway)
9. **COPY ... FROM/TO with simple CSV** — implement as INSERT loop or SELECT output (not the wire protocol COPY)

## Medium (a few days)

10. **Transaction isolation levels** — parse BEGIN ISOLATION LEVEL, map to pragmas or ignore (SQLite has limited isolation)
11. **EXPLAIN** — Turso has EXPLAIN; just need to pass through the PG EXPLAIN node
12. ~~**Named windows**~~ — DONE (WINDOW clause + OVER name references + window inheritance)
13. ~~**CREATE MATERIALIZED VIEW**~~ — DONE (uses Turso's live incremental materialized views)

## Hard (a week+)

16. **User-defined functions (CREATE FUNCTION)** — needs a function registry, PL/pgSQL is out of scope but SQL-body functions are feasible
17. **INTERVAL type with arithmetic** — needs a custom type in Turso core with operator overloading
18. **Full COPY protocol** — wire-level CopyData/CopyDone/CopyFail message handling in `postgres/server/lib.rs`
19. **Triggers** — Turso has experimental trigger support but wiring PG syntax is non-trivial
20. **Deferred constraints** — fundamental architectural change; SQLite checks constraints immediately

## Very Hard (weeks+, architectural)

21. **Stored procedures / CALL** — needs a procedural execution layer
22. **LISTEN/NOTIFY** — needs async pub/sub infrastructure
23. **Replication / logical decoding** — entire subsystem
24. **Full-text search (tsvector/tsquery/@@)** — needs a new index type and matching engine
