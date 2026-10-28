# Schema namespaces

This document describes how Turso manages schema namespaces: what a namespace is, how it is stored, how a written name resolves to one, how namespaces are created and dropped, and what the SQLite and Postgres frontends do with them. It is for people working on `core/` and `postgres/`.

## What is a namespace?

A namespace is a set of object names. For example, two tables can have the same table name if they are in different namespaces. In SQLite, two tables have the same name if a database is attached to the main database. In Postgres, you have the same thing if the tables belong to different Postgres schemas. In other words, a namespace is a unified view of the object names that are visible to a connection.

Every database has one namespace to begin with, the root namespace, whose id is `NamespaceId::ROOT`. For SQLite it holds the objects of the database itself, and an attached database is the root namespace of another database, so SQLite's "same name in two databases" is this mechanism. The main database can also have namespaces, each with its own tables, views, indexes, triggers and sequences. Postgres schemas are namespaces, `public` included, so "same name in two schemas" is the same mechanism again. In a Postgres database the root namespace holds only the engine's own tables and the catalog tables.

The types live in `core/namespace.rs`:

- `NamespaceId` is the id of a namespace within one database's schema.
- `NamespaceRef` is a database id together with a namespace id. It is the place a name resolves to. `NamespaceRef::root_of(database_id)` is the root namespace of a database.
- `Namespace` is a name the connection knows, such as `main`, `aux`, `public` or `app`, together with the `NamespaceRef` it refers to.
- `Qualifiers` is the connection's table of those names.

`Schema` in `core/schema.rs` is the catalog of one database. It keeps the objects of each namespace apart, and every accessor takes the namespace id first: `get_table(namespace, name)`, `get_indices(namespace, table)`, `get_view`, `get_sequence`, `triggers(namespace)`, and so on. An object is always looked up by `(NamespaceId, bare name)`. The bare name is the name the user wrote, `users`, never an encoded form. Nothing above the schema loader and the DDL writers knows how a namespace is written to disk.

## Namespace representation

The on-disk representation of namespaces extends the `sqlite_schema` table adding a `schema` type, which holds Postgres `CREATE SCHEMA` statements, similar to `table` holds `CREATE TABLE` statements, shown in the figure below.

![On-disk format](namespaces/ondisk.svg)

A `schema` row records a namespace: its name in `name` and `tbl_name`, its id in `rootpage`, and `CREATE SCHEMA app` as its `sql`. No B-tree has that root: `rootpage` is an integer column that means nothing for a row without a B-tree, the way views and triggers store 0 there. A namespace exists while its row exists, so a schema can be empty. The Postgres frontend creates the `public` row when it first connects to a database, as initdb does, so `public` is usually namespace 1.

The objects of a namespace share the one `sqlite_schema` with the engine's tables of the root namespace, so their names encode the namespace. An object `users` of the namespace with id 2 is stored under the name `__turso_ns2_users`. The `name` and `tbl_name` columns of its row use that name, and so does the SQL text of the row: the loader reads `CREATE TABLE __turso_ns2_users (...)` and decodes it to table `users` of namespace 2. Views, indexes, triggers and sequences are named the same way. Encoding the id rather than the name means the mangled names of two schemas can never collide, whatever the schemas are called: `app."sub.t"` and `"app.sub".t` are different tables, and `public."app.x"` is not a table of schema `app`.

The objects the engine derives from a table also use the mangled name and live in the root namespace: the autoincrement sequence of `app.users` is `__turso_internal_autoincrement___turso_ns2_users`, the backing table of a sequence is named after the mangled sequence name, the hidden state tables of a materialized view are named after the view's mangled name, and the rows of `sqlite_sequence` and `sqlite_stat1` name the mangled table. The maintenance of a materialized view knows the view by that mangled name, its view key.

The encoding is confined to two places:

- The loader decodes. `handle_schema_row` and `populate_indices` in `core/schema.rs` call `demangle_name`, a `schema` row names its namespace, and `load_sqlite_stat1_row` decodes the `tbl` column.
- The DDL writers encode. `Resolver::mangle_name(namespace, name)` is the name to write into a schema row, and `Resolver::name_in_sql` is the name to write into stored SQL text. CREATE TABLE, CREATE TABLE AS, CREATE INDEX, CREATE VIEW, CREATE TRIGGER, CREATE SEQUENCE and ALTER TABLE use them.

Only a database of a dialect that supports namespaces decodes these names, reads `schema` rows, or refuses `__turso_ns...` as a user name. `Dialect::supports_namespaces` says so; it is true for the Postgres dialect and false for SQLite, and the schema keeps the answer for its loader. SQLite allows a table called `__turso_ns1_t`, and a database made by SQLite may hold one, so under the SQLite dialect such a name is the name of an ordinary object of the root namespace and nothing is decoded. A database is always opened with the dialect that made it, so a Postgres database never reads its mangled names as plain ones.

VACUUM INTO recreates each namespace in the copy under its own id, before the objects stored under that id, so the copy reads the same.

Limits of the format:

- The MVCC logical log carries a `schema` row like any other row of `sqlite_schema`, and recovery replays it. The sync engine knows table, index, view and trigger rows only and rejects a `schema` row as an unsupported object type, so a database that has a schema cannot be synced.
- Databases with schemas in `turso-postgres-schema-*.db` files from earlier versions are not read and are not migrated.

## Object name resolution

Object name resolution happens through a connection, which has a set of namespaces represented by the `Namespaces` in-memory data structure.

![Object name resolution](namespaces/resolution.svg)

The figure shows `public.users` shadowed by `app.users`: with `app` first on the search path, the bare name `users` resolves to namespace 2.

`Qualifiers` maps each name to a `NamespaceRef`. `main` and `temp` always exist and refer to the roots of the main and temp databases. ATTACH adds a name for the root of the attached database. `Connection::namespaces()` adds every namespace of the main database under its name; for a Postgres database that includes `public`.

The resolver (`core/translate/emitter/mod.rs`) turns a written name into a `NamespaceRef`:

- A qualified name, `app.users`, is looked up in the namespace called `app`.
- An unqualified name, `users`, is looked up through the search order, a list of namespaces. With no search path the order is main, then the attached databases by id. With a search path it is the namespaces of the names that exist, each once, so `public` and `app` can both be in the order although both are in the main database. The first namespace of the order is where CREATE puts an object whose name has no namespace. Temp is not in the list: it comes before the list when the connection has a temp database.
- The body of a view or trigger resolves names written without a namespace in the namespace of the view or trigger, not in the search path of the session that runs it. A trigger created without a namespace goes into the namespace of its table.
- The engine's virtual tables, such as table-valued functions and a frontend's catalog tables, exist only in the root namespace and are found from every namespace when no namespace of the search order has an object of that name, the way PostgreSQL's `pg_catalog` is on every search path.

Once resolved, the namespace travels with the object through the planner and into the bytecode:

- `JoinedTable` and `OuterQueryReference` in `core/translate/plan.rs` have a `namespace: NamespaceRef`, so two tables named `users` in two schemas are different tables in a plan.
- The opcodes that change the schema in memory (`DropTable`, `DropView`, `DropIndex`, `DropTrigger`, `DropSequence`, `DropNamespace`, `RenameTable`, `DropColumn`, `AddColumnData`, `AlterColumnData`) and the sequence opcodes have a `namespace: NamespaceId` next to the database id.
- A foreign key records the namespace of its parent table (`ForeignKey::parent_namespace`), so a table of one schema can reference a table of another, and the checks that find the foreign keys referencing a table scan every namespace.
- Analyze statistics are keyed by `(NamespaceId, table name)`.

Places that address an internal table like `sqlite_schema` name `NamespaceId::ROOT` explicitly.

## Creating and dropping namespaces

`CREATE SCHEMA app` and `DROP SCHEMA app` are statements of core (`translate_create_schema` and `translate_drop_schema` in `core/translate/schema.rs`), accepted only when the dialect supports namespaces. They exist only in the AST: the SQLite parser has no such syntax, and a frontend runs them through `Connection::create_namespace` and `drop_namespace`, which prepare the statement without parsing it.

CREATE SCHEMA picks the next id, one more than the largest id that a namespace or any object stored under a namespace has, so an id whose objects are still stored is never given out again. It inserts the `schema` row, reparses it so the schema in memory names the namespace, and bumps the schema cookie. The cookie change makes the namespace visible to every connection of the database, and the row is deleted again if the transaction rolls back.

DROP SCHEMA deletes the row and forgets the name with the `DropNamespace` opcode. The objects stored under the id stay until they are dropped, so the caller drops them first; the id is not reused while any of them remain.

`Connection::create_namespace(name)` and `Connection::drop_namespace(name)` run these statements for a frontend that creates schemas from its own syntax.

## SQLite frontend

Only root namespaces exist, so every `NamespaceRef` is the root of some database, the search order is main, then the attached databases, and every lookup finds what it found when the catalog was keyed by database alone. `CREATE SCHEMA` is refused, `schema` rows are skipped, and a name that looks like a mangled name is an ordinary name.

## Postgres frontend

The frontend (`postgres/frontend/session.rs`) maps the statements:

- Opening a new database, a file that is empty or an in-memory database, creates the namespace `public`, as CREATE DATABASE does in PostgreSQL. It is created once: a dropped `public` stays dropped for later connections until CREATE SCHEMA public. A database whose tables are in the root namespace and in no schema, as an earlier version of tursopg or SQLite made it, is refused when a session connects, since every table would be hidden otherwise. `search_path` starts as `"$user", public`. An object qualified with `main` or `temp` is refused with `schema "main" does not exist`, as PostgreSQL has no such schemas, so the root namespace of a PostgreSQL database holds only the engine's own tables.
- `CREATE SCHEMA app` creates the namespace `app`. Its elements, if any, are created in it.
- `DROP SCHEMA app [CASCADE]` lists the objects of the namespace through the catalog, drops them, then drops the namespace. Without CASCADE a namespace that still has objects is refused. `public` is dropped like any other schema and can be recreated.
- Every table a foreign key names is rewritten to the table's mangled name before the statement is translated, because the stored text names the table as it is stored. A table named with a schema is in that schema; a table named without one is in the first schema of the search path that has it, or else in the schema the statement creates its table in. A foreign key to another database is refused.
- A missing namespace is reported as `schema "x" does not exist`.

`pg_namespace` lists the schemas. `public` has OID 2200, and the OID of any other namespace is 16384 plus its id, so it stays the same while other schemas come and go. `pg_class`, `pg_tables`, `pg_indexes`, `pg_sequences` and the other catalogs iterate every namespace of the main database and report each object under its schema. The root namespace and attached databases are not schemas and are not listed.

Limits of the frontend:

- `ALTER TABLE RENAME` rewrites the table's own rows but does not rewrite references written without a schema inside the view and trigger bodies of the same schema on disk.
- A table in a schema whose column default calls a sequence stores the translated statement text rather than the original, so the sequence name is not derived from the mangled table name when the schema loads.
