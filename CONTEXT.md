# Turso Database

Turso compiles SQLite-compatible SQL into executable database programs. This
context records the project-specific language used for the SQL compilation
rewrite.

## SQL compilation language

**Syntax AST**:
The parser-owned representation of SQL as written, before names, scopes,
functions, types, or row sources have been resolved.
_Avoid_: Bound AST, compiler AST, parser IR

**Semantic analysis**:
The compiler phase that applies SQLite name, scope, alias, CTE, function, and
type rules to produce Semantic HIR.
_Avoid_: Binding, bind phase, resolver pass

**Semantic HIR**:
An owned, fully resolved, execution-independent SQL root between the Syntax AST
and either planner. It contains no parser names that still need lookup and no
VDBE registers, cursors, or labels.
_Avoid_: Bound AST, binding sidecar, annotated AST

**Source identity**:
A typed identity, local to one Semantic HIR document, for a base table, CTE,
derived query, table function, or semantic pseudo-source.
_Avoid_: TableInternalId, table ID

**Stored schema expression**:
An expression persisted as part of schema state, such as a CHECK constraint,
generated column, partial-index predicate, or expression-index key. Its valid
form refers to its table by column position; its explicit unresolved form keeps
syntax needed for lenient schema loading and repair.
_Avoid_: Self-table expression, bound schema AST
