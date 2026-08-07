---
display_name: "a graphical query plan viewer"
---

# Query Plans - Machine-Readable Export and a Graphical Viewer

## Overview

`EXPLAIN QUERY PLAN` prints one line of English per plan step:

```
QUERY PLAN
|--SEARCH u USING INDEX idx_users_age (age>?)
|--SEARCH o USING INDEX idx_orders_user (user_id=?)
`--USE SORTER FOR ORDER BY
```

That is fine to read and painful to build on: any tool that wants the index
name, the seek key, or whether a scan is covering has to parse the sentence
back apart, and the wording changes under it.

Turso exports the same plan as JSON, and ships a local viewer that draws it as
a diagram.

## `.plan` - print the plan as JSON

```
turso> .plan SELECT * FROM users WHERE age > 30
```

```json
{
  "sql": "SELECT * FROM users WHERE age > 30",
  "nodes": [
    {
      "id": 1,
      "parent_id": null,
      "detail": "SEARCH users USING INDEX idx_users_age (age>?)",
      "op": "Search",
      "table": { "name": "users", "identifier": "users", "kind": "table" },
      "estimated_rows": 1600,
      "index": { "name": "idx_users_age", "covering": false },
      "constraints": ["age>?"],
      "left_join": false
    }
  ]
}
```

Do not prefix the statement with `EXPLAIN QUERY PLAN`; `.plan` compiles it in
that mode for you. Nothing is executed.

### The node list

`nodes` is flat, and every node names its parent through `parent_id`. Nodes
come in the order the compiler emitted them, so a parent always appears before
its children and one pass is enough to build the tree.

Every node carries:

| Field | Meaning |
|-------|---------|
| `id` | The step's address. Same number `EXPLAIN QUERY PLAN` reports. |
| `parent_id` | The step this one runs under, or `null` at the top level. |
| `op` | What kind of step it is. Switch on this, not on `detail`. |
| `detail` | The `EXPLAIN QUERY PLAN` line, for display. |

`op` is one of `Scan`, `Search`, `MultiIndexScan`, `HashJoin`,
`IndexMethodQuery`, `ConstantRow`, `CompoundQuery`, `CompoundLeftMost`,
`CompoundOperator`, `Sort`, `Distinct`, `MaterializeHashBuildInput`,
`Subquery`, or `RecursiveCte`. Treat an unknown `op` as a plain step and fall
back to `detail`: new kinds get added as the planner grows.

The rest of the keys depend on `op` and are absent when they do not apply:

| Field | On | Meaning |
|-------|----|---------|
| `table` | table access | `name` in the schema, `identifier` as the query refers to it, and `kind` (`table`, `virtual_table`, `subquery`, `recursive_cte_input`) |
| `estimated_rows` | table access | Rows the optimizer expects per row of the tables before it in the join order |
| `index` | `Scan`, `Search` | Index name, and whether it covers the query so the table is never read |
| `constraints` | `Search` | The key parts the seek pins down, e.g. `["city=?", "age>?"]` |
| `left_join` | `Scan`, `Search` | True when the table is the right side of a LEFT JOIN |
| `set_op` | `MultiIndexScan`, `CompoundOperator` | `OR`/`AND`, or `UNION`/`UNION ALL`/`INTERSECT`/`EXCEPT` |
| `indexes` | `MultiIndexScan` | One index per branch |
| `purpose`, `strategy` | `Sort` | `ORDER BY`/`GROUP BY`, and `SORTER`/`TEMP B-TREE` |
| `aggregate` | `Distinct` | The aggregate the DISTINCT belongs to, when it has one |
| `subquery_kind`, `subquery_id`, `correlated` | `Subquery` | Whether it runs once or once per outer row |
| `phase` | `RecursiveCte` | `SETUP` or `RECURSIVE STEP` |
| `method` | `IndexMethodQuery` | The index method that answered the query |

The text says slightly less than the fields do: a `SCAN` line never prints
`LEFT-JOIN` and a `SEARCH` line never prints `COVERING INDEX`, because those
lines have always read that way. The fields still record both, so read the
fields when you want the whole picture.

## `--explain-server` - the graphical viewer

```bash
tursodb mydb.db --explain-server
```

The viewer comes up on `http://127.0.0.1:8375/`. Pass an address to change it:

```bash
tursodb mydb.db --explain-server 127.0.0.1:9000
```

Write a query, press Explain (or Ctrl+Enter), and the plan is drawn bottom-up:
the steps that read rows sit at the bottom, arrows follow the rows into the
steps that consume them, and the query result is on top. Click a node for its
full detail, drag to pan, scroll to zoom, and switch to Table view for the
whole plan as text.

Nodes are colored by what they do — a scan that reads every row, a seek that
jumps straight to matching rows, or a step that shapes the query — and each
node also spells out its kind, so the color never carries the meaning alone.
The bar next to a node's row estimate compares it against the largest estimate
in the same plan.

The server binds to loopback by default, serves one request at a time, and only
ever compiles the SQL it is given. It is a development tool; do not put it on a
public address.

## Programmatic access

From Rust:

```rust
let plan = conn.query_plan("SELECT * FROM users WHERE age > 30")?;
for node in &plan.nodes {
    println!("{} {}", node.id, node.op);
}
println!("{}", plan.to_json());
```

`Statement::query_plan()` returns the same thing for a statement already
prepared as `EXPLAIN QUERY PLAN`.
