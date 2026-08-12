# Machine-readable EXPLAIN QUERY PLAN (FORMAT=JSON)

`EXPLAIN QUERY PLAN` prints human-readable detail strings ("SCAN users USING
COVERING INDEX ..."), and parsing those is brittle. Turso can instead return
the whole plan as one JSON document:

```console
tursodb> EXPLAIN QUERY PLAN FORMAT=JSON SELECT * FROM users WHERE age > 21;
{"version":1,"sql":"EXPLAIN QUERY PLAN FORMAT=JSON SELECT * FROM users WHERE age > 21;","result_columns":["id","name","age"],"nodes":[...]}
```

The statement is only prepared, never executed — the plan is fully known after
preparation, so exploring plans for `INSERT`/`UPDATE`/`DELETE` cannot change
the database. `FORMAT=TEXT` (or omitting the clause) gives the classic
four-column rows. The clause is case-insensitive, works from any binding or
driver because it is plain SQL, and returns one row with one TEXT column named
`plan_json`.

To *see* a plan instead of reading JSON, paste the output into the
[Turso plan visualizer](https://github.com/LeMikaelF/turso-explain-temp), which
draws it as an interactive dataflow graph.

Also exposed as a Rust API: prepare a statement as `EXPLAIN QUERY PLAN <stmt>`
(either format) and call [`Statement::query_plan_json`]. Returns `None` for
statements not prepared in EXPLAIN QUERY PLAN mode.

## The JSON format

```json
{
  "version": 1,
  "sql": "EXPLAIN QUERY PLAN SELECT ...",
  "result_columns": ["name", "amount"],
  "nodes": [
    {
      "id": 2,
      "parent": null,
      "detail": "SEARCH o USING INDEX idx_orders_user (user_id=?) LEFT-JOIN",
      "op": {
        "type": "search",
        "table": "orders",
        "alias": "o",
        "join": "left",
        "search_kind": "seek",
        "index": {"name": "idx_orders_user", "covering": false, "ephemeral": false},
        "constraints": ["user_id=?"]
      }
    }
  ],
  "cte_materializations": [{"cte_id": 1, "name": "spenders", "nodes": [4, 7]}]
}
```

`nodes` lists the same rows `EXPLAIN QUERY PLAN` prints, in the same order:
`id`/`parent` encode the tree (`parent: null` marks a root) and `detail` is
the exact display string. `op` adds the structured fields, discriminated by
`op.type`:

| `op.type` | Meaning | Extra fields |
|---|---|---|
| `scan` | iterate a row source | `table`, `alias?`, `source` (`table`/`virtual_table`/`subquery`/`recursive_cte_input`), `index?` , `backwards?`, `join?`, `subquery?` |
| `search` | seek by key | `table`, `alias?`, `search_kind` (`rowid_eq`/`seek`/`in_seek`), `index?` or `integer_primary_key`, `constraints`, `backwards?`, `join?`, `subquery?` |
| `multi_index` | combine rowid sets from several indexes | `set_op` (`or`/`and`), `indexes` |
| `index_method` | pluggable index method access | `method` |
| `hash_join` | hash side of a hash join | `table`, `alias?`, `join?`, `subquery?` |
| `hash_build` | materialize a hash join's build input | `table`, `alias?` |
| `distinct` / `distinct_aggregate` | hash-table de-duplication | `function` (aggregate only) |
| `order_by` / `group_by` | sorting stage | `method` (`sorter`/`temp_btree`) |
| `compound` / `compound_arm` | compound select and its arms | `op` (`union_all`/`union`/`intersect`/`except`/`left_most`), `temp_btree` |
| `list_subquery` / `scalar_subquery` | `IN (SELECT ...)` / scalar subquery | `subquery_id`, `correlated` |
| `recursive_setup` / `recursive_step` | recursive CTE phases | |
| `constant_row` | query with no FROM clause | |

Fields that are absent are simply omitted (e.g. `join` on the first table,
`index` on a rowid search). Notable structured-only information that the text
output does not show:

- `join`: how the table participates in the join — `inner`, `cross`, `left`,
  `full`, and crucially `semi` / `anti` for `EXISTS` / `NOT EXISTS` rewrites,
  which are indistinguishable from inner joins in the text output.
- `subquery` on scans/searches of FROM-clause subqueries:
  `{"execution": "coroutine" | "materialized" | "indexed_materialized" |
  "materialized_reuse", "cte_id": n, "recursive": true}`. `cte_id` links every
  reader of a shared CTE to the same materialization.
- `cte_materializations`: shared CTEs that are materialized before the main
  query runs. Their plan nodes appear at the root level of the tree; this
  side channel groups them and names the CTE so tools can draw them as one
  unit.
- `backwards` scans, `covering`/`ephemeral` index flags, and the seek
  constraint list as an array instead of a parenthesized string.

`version` is bumped whenever the format changes in a way consumers can
observe; additions of new optional fields or new `op.type` values are not
considered breaking, so consumers should ignore fields and types they do not
know.
