# HIR preparation checkpoints 1–4

Implementation: https://ampcode.com/threads/T-01a1a9ff-96d8-713c-bdfa-f060c9c5f71b

Base: `65154dad6519df8dd70e8a46ee58f6960fea48a1`, also fetched `origin/main`.
Git branch: `hir-preparation-1-4`. Implementation and same-model subagent reviews
use this orb checkout. No analyzer or production binding replacement is authorized.
The attached design was copied verbatim before updating its completion status.

## 1. Generic seek data

Commit: the commit adding this section (`core/translate: make seek data expression-generic`).
Review: independent subagent approved; no algorithm or AST NULL-classification changes.
Generic operations impose no Clone/Copy bounds on expression values. AST-specific
`IS` versus `=` classification remains in `impl SeekDef<ast::Expr>`.

Validation:

- `cargo build -p turso_cli -p sqltest --bin tursodb --bin sqltest`: passed.
- `cargo fmt --all`, `cargo fmt --all --check`, `git diff --check`: passed after formatting.
- From `sqlite/conformance`, the built `sqltest run` with `--backend rust --snapshot-mode no`
  on `index-seek-null-key.sqltest`, `index-seek-null-computed-bound.sqltest`,
  `index-seek-subquery-affinity.sqltest`, `index_seek_comparison_affinity.sqltest`,
  `composite-index-sort-elim.sqltest`, `is-operator-index-seek-order-by.sqltest`,
  and `snapshot_tests/indexes`: **167 passed, 1 failed**.
- The failure is `join-with-covering-index-bytecode`. An independently built untouched
  baseline reproduces it: `sqltest run sqlite-sqltests/snapshot_tests/indexes --backend rust
  --snapshot-filter join-with-covering-index-bytecode --snapshot-mode no`: 1 passed, 1 failed.
  No snapshots were updated. This remains a baseline validation limitation.
- A temporary CLI comparison script executed 24 queries in result, EQP and EXPLAIN
  modes against separately built baseline and checkpoint binaries: **72 identical comparisons**,
  including NULL seeks, mixed-direction composite indexes, expression indexes, MIN/MAX,
  RIGHT/FULL joins, hints, correlation, repeated CTEs and preparation errors. No normalization.

This is a pure refactor, so existing tests were retained unchanged. A separate
baseline worktree and saved executables prevent comparisons against accidentally
rebuilt code. Preparation benchmarks use `--profile dev` consistently; these are
diagnostic comparisons, not release-performance claims.
