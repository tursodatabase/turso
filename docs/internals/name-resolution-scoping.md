# Name Resolution And Scoping

This note describes a path out of the current `TableReferences + outer_query_refs + BindingBehavior`
model used by the translator.

The immediate trigger is alias and correlated-subquery resolution. The current implementation can
choose only between:

- result aliases first
- canonical/source columns first
- result aliases disallowed

That is too coarse for SQLite-compatible resolution.

## Problem Statement

Today the binder resolves identifiers against:

- `TableReferences::joined_tables`
- `TableReferences::outer_query_refs`
- optionally `ResultSetColumn.alias`

This is sufficient for many cases, but it flattens multiple distinct namespaces and scopes into a
single lookup procedure:

- local source names (tables in the current `FROM`)
- outer source names (correlated references)
- local result aliases
- outer result aliases
- CTE names available for `FROM` lookup but not column lookup

SQLite does not treat these as one flat namespace.

The most important examples:

- `WHERE` allows an unambiguous select alias, but prefers a source column on conflict.
- direct `ORDER BY` prefers the select alias over a source column on conflict.
- correlated subqueries inside outer `ORDER BY` can see outer select aliases.
- correlated subqueries inside outer `HAVING` can see aggregate aliases.
- the above rules are not the same as window-clause resolution rules.

This is why "phase-aware binding" by itself is not enough. The binding rules are not determined
only by clause order; they depend on nested name contexts and clause-specific precedence.

## Existing Constraints

Any replacement needs to fit these existing translator responsibilities:

- `JoinedTable` and `OuterQueryReference` carry table identity and dependency tracking.
- `ResultSetColumn` currently stores only `{ expr, alias, contains_aggregates }`.
- correlated-subquery planning uses outer references to determine evaluation depth.
- CTE handling uses outer refs both for deferred planning and for visibility.

We should preserve:

- existing `TableInternalId` identity
- existing column-usage tracking for covering-index logic
- existing `OuterQueryReference` dependency tracking used by `EvalAt`

We should not preserve:

- expression-wide alias lookup mode as the main abstraction
- cloning alias expressions into the AST as the only representation of alias binding

## Proposed Model

Introduce a new binder-facing structure:

```rust
pub struct NameContext<'a> {
    pub parent: Option<&'a NameContext<'a>>,
    pub source_scope: &'a SourceScope,
    pub result_scope: Option<&'a ResultAliasScope>,
    pub policy: NameResolutionPolicy,
}

pub struct SourceScope {
    pub local_tables: Vec<SourceTableRef>,
    pub outer_refs: Vec<OuterSourceRef>,
    pub from_visible_ctes: Vec<CteNameRef>,
}

pub struct ResultAliasScope {
    pub aliases: Vec<ResultAliasBinding>,
}

pub struct ResultAliasBinding {
    pub alias: String,
    pub result_column_index: usize,
    pub contains_aggregates: bool,
}
```

The binder resolves an identifier against a `NameContext`, not directly against `TableReferences`.

### NameResolutionPolicy

Each clause gets an explicit policy:

```rust
pub struct NameResolutionPolicy {
    pub local_result_aliases: AliasVisibility,
    pub outer_result_aliases: AliasVisibility,
    pub source_precedence: SourcePrecedence,
    pub allow_aggregates: AggregateVisibility,
}

pub enum AliasVisibility {
    Hidden,
    Fallback,
    Preferred,
}
```

This must be clause-specific, not statement-wide.

Examples:

- select-list expressions: local aliases hidden
- `WHERE`: local aliases fallback, outer aliases fallback
- `GROUP BY`: local aliases fallback, outer aliases fallback
- `HAVING`: local aliases fallback, outer aliases fallback
- top-level `ORDER BY`: local aliases preferred, outer aliases fallback
- window `PARTITION BY` / `ORDER BY`: local aliases hidden

The exact policy values should be verified against SQLite with tests.

## Bound Name Representation

Do not immediately rewrite every alias reference into a cloned expression.

Instead, introduce an intermediate bound-name form:

```rust
pub enum BoundName {
    SourceColumn {
        table: TableInternalId,
        column: usize,
        is_outer: bool,
        is_rowid_alias: bool,
    },
    ResultAlias {
        result_column_index: usize,
        is_outer: bool,
    },
}
```

Then binding can produce:

```rust
pub enum BoundExpr {
    Expr(ast::Expr),
    Name(BoundName),
}
```

Or, if we want smaller changes, keep `ast::Expr` but add explicit alias marker nodes:

```rust
ast::Expr::ResultAliasRef { index, is_outer }
```

That lets later phases decide when alias references should be expanded to the underlying expression
and when they should remain as stable references.

This matters for:

- aggregate misuse checks
- nested correlated subqueries
- conflict resolution between source columns and aliases

## Relationship To Existing Structures

This does not require deleting `TableReferences` immediately.

Short term:

- `SourceScope` can be built from `TableReferences`
- `OuterSourceRef` can wrap `OuterQueryReference`
- `ResultAliasScope` can be built from `ResultSetColumn`

Medium term:

- `TableReferences` becomes planning/emission metadata
- `NameContext` becomes the sole binding abstraction

This separation is important:

- planning cares about loops, dependencies, and covering indexes
- binding cares about nested name visibility and precedence

Trying to make one structure do both is what produced the current accidental complexity.

## Resolution Algorithm

For an unqualified identifier in a given `NameContext`:

1. Search local source names.
2. Search local result aliases if the policy allows them.
3. If both matched, apply the clause precedence rule.
4. If unresolved, recurse to the parent context:
   - parent source names are always eligible for correlation
   - parent result aliases are eligible only if the current policy allows outer result aliases
5. If still unresolved, report `no such column`.

For qualified identifiers:

1. Resolve table name in local source scope, then parent source scopes.
2. Never resolve `tbl.col` against result aliases.
3. Keep current table-alias shadowing rules: alias hides the base table name in the same scope.

For duplicate result aliases:

- direct `ORDER BY` should follow SQLite behavior
- tests suggest SQLite picks the first matching result alias instead of erroring

That behavior should be codified in tests before implementation.

## Clause-Specific Notes

### `WHERE`

SQLite allows direct references to select aliases when they are unambiguous:

```sql
SELECT -a AS x FROM t WHERE x < 0;
```

But source columns win on conflict:

```sql
SELECT -a AS b, t.b FROM t WHERE b > 15;
```

So `WHERE` is not "aliases forbidden". It is "source first, alias fallback".

### `ORDER BY`

Direct `ORDER BY` prefers result aliases over source columns:

```sql
SELECT -a AS b, t.b FROM t ORDER BY b;
```

But correlated subqueries inside `ORDER BY` can still see outer source names and outer result aliases:

```sql
SELECT -a AS x FROM t ORDER BY (SELECT x);
```

So "ORDER BY aliases work" is not enough. The alias namespace must be inherited by nested contexts.

### `HAVING`

Correlated subqueries inside `HAVING` can see aggregate aliases:

```sql
SELECT a % 2 AS g, SUM(b) AS s FROM t GROUP BY g HAVING (SELECT s) > 15;
```

This means aggregate alias visibility is also a name-context property.

### Window Clauses

Window `ORDER BY` and `PARTITION BY` should remain a separate policy surface.
Do not reuse top-level `ORDER BY` behavior automatically.

## Migration Plan

### Phase 1: Introduce `NameContext` Without Behavior Changes

- Add `NameContext`, `SourceScope`, `ResultAliasScope`, and `NameResolutionPolicy`
- Build contexts from existing `TableReferences` and `ResultSetColumn`
- Keep current behavior by mapping old `BindingBehavior` onto policy presets

This phase should be mechanical and low-risk.

### Phase 2: Move Binding To `NameContext`

- Replace direct `joined_tables` / `outer_query_refs` lookup in `bind_and_rewrite_expr`
- Centralize unqualified and qualified name resolution behind helpers
- Preserve current `Expr::Column` rewrite for source-column bindings
- add explicit alias-reference representation for result alias bindings

At the end of this phase, binding logic should no longer know about `TableReferences` internals.

### Phase 3: Implement Clause Policies Verified By SQLite Tests

Start with the cases already codified in:

- `testing/runner/tests/alias-scoping.sqltest`
- `testing/runner/tests/alias-scoping-rules.sqltest`
- `testing/runner/tests/table-alias-shadowing.sqltest`
- `testing/runner/tests/subquery/correlated-group-by-scope.sqltest`
- `testing/runner/tests/alias-scoping-sqlite-compat.sqltest`

The current known gaps are:

- correlated subquery in `ORDER BY` seeing outer result aliases
- correlated subquery in `ORDER BY` resolving source-vs-alias conflicts correctly
- correlated subquery in `HAVING` seeing aggregate aliases
- nested correlated subquery in `WHERE` seeing outer result aliases

### Phase 4: Decouple Correlation Tracking From Name Lookup

Once `NameContext` is authoritative for binding:

- keep `OuterQueryReference` only for dependency tracking and `EvalAt`
- mark dependency usage from bound outer references directly
- stop using `outer_query_refs` as a lookup namespace

This is the point where the old flat model can be retired.

## Recommended Immediate Work

The next implementation step should be:

1. add `NameContext` and policy types
2. thread a `NameContext` into `bind_and_rewrite_expr`
3. preserve old behavior via compatibility policy presets
4. switch one clause at a time to the new path
5. use the SQLite-compat tests to drive policy corrections

Do not start by rewriting all planner code around scopes. The binding abstraction can move first,
while planning still uses `TableReferences`.

That keeps the blast radius contained and lets us prove behavior on tests before reworking the rest
of translator internals.
