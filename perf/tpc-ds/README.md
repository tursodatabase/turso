# TPC-DS query set

The 99 TPC-DS benchmark queries (qgen output with default substitutions)
and the standard TPC-DS schema (24 tables), vendored from DuckDB's TPC-DS
extension (`extension/tpcds/dsdgen` in <https://github.com/duckdb/duckdb>,
MIT licensed). TPC-DS is a trademark of the Transaction Processing
Performance Council.

- `schema.sql` - all 24 table definitions, concatenated
- `queries/01.sql` .. `queries/99.sql` - one query per file

The queries are currently used by `core/benches/prepare_benchmark.rs` to
track statement preparation (parse + plan + codegen) cost; no data is
needed for that. Queries that Turso cannot compile yet (ROLLUP,
stddev_samp, custom window frames, parenthesized compound selects,
non-equality FULL OUTER JOIN) are listed and skipped in the benchmark
itself, so newly supported features should be removed from that skip list.
