# Semantic HIR extraction

This directory is an isolated review copy. Nothing in `core/translate/mod.rs`,
`core/lib.rs`, or the workspace manifests imports it, so it does not change the
compiled database.

The Rust files under this directory and `core/schema_expr/` were copied without
code changes from revision `xqxxrxww` (`use hir`). They capture the proposed
semantic boundary without mixing it into the current binding and translation
paths.

## Included

- `hir/`: the owned semantic document, typed identities, expressions, query
  graph, statement roots, and bound schema-program references.
- `context.rs`: the read-only catalog and connection facts visible during
  analysis.
- `scope.rs`, `expr.rs`, `query.rs`, and `cte.rs`: name resolution and query
  binding.
- `dml.rs`, `trigger.rs`, and `sequence.rs`: statement-specific semantic roots.
- `schema_expr.rs` and `schema_program.rs`: the bridge from stored schema
  expressions into document-owned HIR.
- `core/schema_expr/`: the stored-expression representation required by that
  bridge.

## Deliberately excluded

- changes to the existing `bind.rs` path;
- `PlanExpr`, planner, optimizer, VDBE, and DBSP integration;
- module declarations, feature flags, or Cargo changes; and
- tests or behavior changes.

The live pre-HIR binding code remains in `core/translate/bind.rs` for direct
comparison. A useful reading order is `hir/mod.rs`, `mod.rs`, `context.rs`,
`scope.rs`, `expr.rs`, `query.rs`, and then the statement-specific modules.

This extraction is expected to remain dead code. Making it compile would be a
separate integration decision and would require changing existing modules.
