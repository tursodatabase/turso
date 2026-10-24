# Schema namespaces

This document describes how Turso keeps the objects of a database apart by
namespace: how the catalog is keyed, how a written name resolves to a
namespace, and how the namespace travels through the planner and the
bytecode. It is for people working on `core/` and `postgres/`.

## What a namespace is

A namespace is a set of object names. Two tables can have the same name if
they are in different namespaces. Every database has one namespace to begin
with, the root namespace, whose id is `NamespaceId::ROOT` (zero). It has the
objects of the database itself: for a SQLite database, everything in
`sqlite_schema`. A database can have more namespaces, each with its own
tables, views, indexes, triggers and sequences. The PostgreSQL frontend uses
them for schemas.

This is the catalog shape PostgreSQL has: `pg_class` is keyed by
`(relname, relnamespace)`, and name lookup walks a list of namespace ids,
probing each. Core has the same split. `Schema` is the catalog keyed by
namespace, the resolver is the policy that picks the namespace, and the
connection owns the names it knows.

The types live in `core/namespace.rs`:

- `NamespaceId` is the id of a namespace within one database's schema.
- `NamespaceRef` is a database id together with a namespace id. It is the
  place a name resolves to. `NamespaceRef::root_of(database_id)` is the root
  namespace of a database.
- `Namespaces` is the connection's table of names, such as `main`, `aux` or
  `public`, and what each refers to.

An object is always looked up by `(NamespaceId, bare name)`. The bare name is
the name the user wrote, `t`, never an encoded form. Nothing above the schema
loader and the DDL writers knows how a namespace other than the root is
written to disk.

## The catalog

`Schema` in `core/schema.rs` is the catalog of one database. It keeps one
`NamespaceSchema` per namespace, and every accessor takes the namespace id
first: `get_table(namespace, name)`, `get_indices(namespace, table)`,
`get_view`, `get_sequence`, `triggers(namespace)`, and so on. The maps
inside a `NamespaceSchema` are private to `schema.rs`; code outside it goes
through these named methods.

Each method treats its name argument exactly as the map access it replaced
did. `get_indices` normalizes the name and leaves out backing B-tree
indexes, `indices_including_backing` takes the stored name and returns every
index, and `views` yields the lookup name next to each view so error
messages print the name the user wrote.

## Resolving names

The connection owns the names. `Namespaces` maps each name to a database.
`main` and `temp` always exist and refer to the main and temp databases.
ATTACH adds a name for the attached database. A frontend can add more names
for a database: the PostgreSQL frontend adds `public` as a second name for
main.

The resolver (`core/translate/emitter/mod.rs`) turns a written name into a
`NamespaceRef`:

- A qualified name, `aux.t`, is looked up in the namespace called `aux`.
- An unqualified name, `t`, is looked up through the search order, a list
  of namespaces. With no search path the order is main, then the attached
  databases by id. With a search path it is the namespaces of the names
  that exist, each once. The first namespace of the order is where CREATE
  puts an object whose name has no namespace. Temp is not in the list: it
  comes before the list when the connection has a temp database.
- The body of a view or trigger resolves names written without a namespace in
  the namespace of the view or trigger, not in the search path of the
  session that runs it.

Once resolved, the namespace travels with the object. Places that cannot
know a namespace yet, because only a database id reaches them from a plan or
an opcode, or because they address an internal table like `sqlite_schema`,
name `NamespaceId::ROOT` explicitly.

## The SQLite frontend

Only root namespaces exist, so every `NamespaceRef` is the root of some
database, the search order is main, then the attached databases, and every
lookup finds what it found when the catalog was keyed by database alone.
