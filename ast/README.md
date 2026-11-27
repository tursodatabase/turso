# Turso SQL abstract syntax tree

This crate has the syntax tree that the SQLite parser and the PostgreSQL
translator produce, and that `turso_core` compiles.

## Types of expressions: stored or derived

The tree does not store the type of every expression. Core derives the type
of an expression from the expression and the schema each time it needs it. A
frontend stores a type in the tree only where the expression and the schema
do not tell it.

### Where a type has to be stored

`Type::ty_annotation` is the one place where the tree stores what a frontend
knows about a type. A `Type` is the type of a cast or of a column definition.

A frontend has to fill it in when it writes a type under another name than
the statement has. The PostgreSQL frontend writes `x::json` as
`CAST(x AS TEXT)`, because TEXT is how core stores and converts the value.
After that the name says TEXT, and nothing in the tree or in the schema says
json. The annotation holds the identifier of json, so the type that the
statement names is not lost.

A stored annotation follows these rules:

- It does not change the value of the expression. Core carries it to the
  type information of a result column and does not compute with it.
- SQL text has none. The printer does not write it, and two types that
  differ only in their annotation are equal.
- So it does not outlive the statement. An expression that is stored as text
  and parsed again, such as the expression of an index, has lost it.

### What is derived

Everything else is derived. In `core/translate/expr/expr_type.rs`, `type_of`
derives the type that core computes with, and `ty_annotation_of` derives the
frontend's identifier of that type for a result column.

| Expression | Where its annotation comes from |
| --- | --- |
| A column, `t.x` | The declared type in the schema. Core read it from stored SQL, so it asks `Dialect::type_annotation` for the identifier. |
| A literal, `true` | `Dialect::literal_type_annotation`. |
| `NULL` and a string literal | The values it is used with: `coalesce(x, NULL)` has the annotation of `x`. |
| `coalesce(a, b)`, `nullif(a, b)`, `iif(c, a, b)`, `CASE` | The annotation that its values all have. Values with different annotations have none in common. |
| `(x)`, `x COLLATE c`, `+x` | The annotation of `x`. |
| `CAST(x AS t)`, where `t` has no stored annotation | `Dialect::type_annotation` for `t`. |
| A value of a type made with `CREATE TYPE` | `Dialect::type_annotation` for the name of the type. |

Some expressions have no annotation yet: the result of an operator
(`a + 1`), a function that computes its result (`avg(a)`), and a column that
is read through a subquery. They need a rule for deriving it, not a place to
store it.

### Which one to use

Derive the type when the expression and the schema tell it. A derived type
cannot be lost when SQL is stored as text, and it cannot disagree with the
tree after core rewrites an expression. Store an annotation only for what
the tree would otherwise not have.
