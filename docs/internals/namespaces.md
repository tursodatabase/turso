# Schema namespaces

This document describes how Turso keeps the objects of a database apart by
namespace: how the catalog is keyed, how a written name resolves to a
namespace, how the namespace travels through the planner and the bytecode,
and how the PostgreSQL frontend stores schemas as namespaces of the main
database. It is for people working on `core/` and `postgres/`.

## What a namespace is

A namespace is a set of object names. Two tables can have the same name if
they are in different namespaces. Every database has one namespace to begin
with, the root namespace, whose id is `NamespaceId::ROOT` (zero). It has the
objects of the database itself: for a SQLite database, everything in
`sqlite_schema`. A database can have more namespaces, each with its own
tables, views, indexes, triggers and sequences. They are stored namespaces,
and the PostgreSQL frontend uses them for schemas.

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
- `Namespace` is a name the connection knows, such as `main`, `aux`, `public`
  or `app`, together with the `NamespaceRef` it refers to.
- `Namespaces` is the connection's table of those names.

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
through these named methods. Whole-schema scans that must see every object,
such as MVCC recovery and checkpointing, use `all_tables`, `all_indexes` and
`all_sequences`.

Each method treats its name argument exactly as the map access it replaced
did. `get_indices` normalizes the name and leaves out backing B-tree
indexes, `indices_including_backing` takes the stored name and returns every
index, and `views` yields the lookup name next to each view so error
messages print the name the user wrote.

A stored namespace has a name once the schema has read its marker table (see
below). `stored_namespaces()` lists the named ones, `namespace_named(name)`
finds one by name, and `namespace_name(id)` is the reverse. Objects the loader
meets before their marker table create the namespace without a name; the
marker table names it.

## Resolving names

The connection owns the names. `Namespaces` maps each name to a
`NamespaceRef`. `main` and `temp` always exist and refer to the roots of the
main and temp databases. ATTACH adds a name for the root of the attached
database. A frontend can add more names for a database: the PostgreSQL
frontend adds `public` as a second name for the root of main, and
`Connection::namespaces()` adds every stored namespace of the main database
under its name.

The resolver (`core/translate/emitter/mod.rs`) turns a written name into a
`NamespaceRef`:

- A qualified name, `aux.t`, is looked up in the namespace called `aux`.
- An unqualified name, `t`, is looked up through the search order, a list
  of namespaces. With no search path the order is main, then the attached
  databases by id. With a search path it is the namespaces of the names
  that exist, each once, so `public` and `app` can both be in the order
  although both are in the main database. The first namespace of the order
  is where CREATE puts an object whose name has no namespace. Temp is not
  in the list: it comes before the list when the connection has a temp
  database.
- The body of a view or trigger resolves names written without a namespace in
  the namespace of the view or trigger, not in the search path of the
  session that runs it.

Once resolved, the namespace travels with the object through the planner
and into the bytecode:

- `JoinedTable` and `OuterQueryReference` in `core/translate/plan.rs` have a
  `namespace: NamespaceRef`, so two tables named `t` in two schemas are
  different tables in a plan.
- The opcodes that change the schema in memory (`DropTable`, `DropView`,
  `DropIndex`, `DropTrigger`, `DropSequence`, `RenameTable`, `DropColumn`,
  `AddColumnData`, `AlterColumnData`) and the sequence opcodes have a
  `namespace: NamespaceId` next to the database id.
- A foreign key records the namespace of its parent table
  (`ForeignKey::parent_namespace`), so a table of one schema can reference a
  table of another, and the checks that find the foreign keys referencing a
  table scan every namespace.
- Analyze statistics are keyed by `(NamespaceId, table name)`.

Places that address an internal table like `sqlite_schema` name
`NamespaceId::ROOT` explicitly.

## How a stored namespace is stored

The main database has one `sqlite_schema`, so the rows of every namespace
share one table with one name column. The storage encodes the namespace into
the name, and only there.

An object `t` of the stored namespace with id 17 is stored under the name
`__turso_ns17_t`. The `name` and `tbl_name` columns of its `sqlite_schema`
row use that name, and so does the SQL text of the row: the loader reads
`CREATE TABLE __turso_ns17_t (...)` and decodes it to table `t` of namespace
17. Views, indexes, triggers and sequences are named the same way. Encoding
the id rather than the name means the stored names of two schemas can never
collide, whatever the schemas are called: `app."sub.t"` and `"app.sub".t`
are different tables, and `public."app.x"` is not a table of schema `app`.

A stored namespace exists while its marker table exists. The marker table of
namespace 17 called `app` is `__turso_internal_namespace_17_app`, an empty
table in the root namespace. Because it is an ordinary row of
`sqlite_schema`, CREATE SCHEMA and DROP SCHEMA are DDL of the main database:
they roll back with their transaction, other connections see them when the
schema cookie changes, and the schema knows its namespaces whenever it is
loaded, with no extra scan. A schema can be empty.

The objects the engine derives from a table also use the stored name and
live in the root namespace: the autoincrement sequence of `app.t` is
`__turso_internal_autoincrement___turso_ns17_t`, the backing table of a
sequence is named after the stored sequence name, and the rows of
`sqlite_sequence` and `sqlite_stat1` name the stored table.

The encoding is confined to two places:

- The loader decodes. `handle_schema_row` and `populate_indices` in
  `core/schema.rs` call `parse_stored_name` and `parse_marker_table_name`,
  and `load_sqlite_stat1_row` decodes the `tbl` column.
- The DDL writers encode. `Resolver::stored_name(namespace, name)` is the
  name to write into a schema row, and `Resolver::name_in_sql` is the name to
  write into stored SQL text. CREATE TABLE, CREATE TABLE AS, CREATE INDEX,
  CREATE VIEW, CREATE TRIGGER, CREATE SEQUENCE and ALTER TABLE RENAME use
  them.

The names are reserved. `is_reserved_object_name` refuses any user object
named `__turso_ns...` in any namespace, and no user statement may create or
drop a marker table. The engine bypasses the check for its own DDL through
`Connection::run_engine_ddl`.

## Creating and dropping a stored namespace

`Connection::create_stored_namespace(name)` picks the next id, one more than
the largest id of the stored namespaces that exist, and creates the marker
table through engine DDL. The schema change makes the namespace visible to
every connection of the database. `Connection::drop_stored_namespace(name)`
drops the marker table; the frontend drops the objects of the namespace
first.

The frontend (`postgres/frontend/session.rs`) maps the statements:

- `CREATE SCHEMA app` creates the stored namespace `app`. Its elements, if
  any, are created in it.
- `DROP SCHEMA app [CASCADE]` lists the objects of the namespace through the
  catalog, drops them, then drops the namespace. Without CASCADE a namespace
  that still has objects is refused.
- A foreign key that names a table with a schema is rewritten to the table's
  stored name before the statement is translated, because the stored text
  names the table as it is stored. A foreign key to another database is
  refused.
- A missing namespace is reported as `schema "x" does not exist`.

## What the catalogs show

`pg_namespace` lists the schemas. `public` has OID 2200, and the OID of a
stored namespace is 16384 plus its id, so it stays the same while other
schemas come and go. `pg_class`, `pg_tables`, `pg_indexes`, `pg_sequences`
and the other catalogs iterate every namespace of the main database and
report each object under its schema. Attached databases are not schemas and
are not listed.

## The SQLite frontend

Only root namespaces exist, so every `NamespaceRef` is the root of some
database, the search order is main, then the attached databases, and every
lookup finds what it found when the catalog was keyed by database alone.

## Limits

- Materialized views cannot be created in a stored namespace.
- `ALTER TABLE RENAME` rewrites the table's own rows but does not rewrite
  references written without a schema inside the view and trigger bodies of
  the same schema on disk.
- A table in a stored namespace whose column default calls a sequence stores
  the translated statement text rather than the original, so the sequence
  name is not derived from the stored table name when the schema loads.
- Databases with schemas in `turso-postgres-schema-*.db` files from earlier
  versions are not read and are not migrated.
