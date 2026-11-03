# Turso Security Model

This document describes how the Turso database decides what a SQL statement is
allowed to do: the role model and privilege checks in core, and how the
frontends expose them.

Turso database trusts the host program that opens a connection but not the
executed SQL queries. The host program chooses the session role a connection
initially runs as. SQL statements are checked against that role and can only do
what the role is allowed to do. Turso has no authentication; instead, it assumes
the host program handles it.

> [!NOTE]
> Roles, ownership, privileges, `GRANT` and `REVOKE` are implemented: a role
> owns what it creates, every object has an access control list, the checks
> consult them, and the owner changes them. Row-level security is in scope
> and not implemented. Authentication is out of scope for Core: it belongs
> to the host program or the frontend.

## Glossary

| Term                   | Definition                                                                                   |
|------------------------|----------------------------------------------------------------------------------------------|
| **Authentication**     | Deciding who a client is and which role its connection runs as. The host program's job.      |
| **Catalog**            | The tables that describe the database itself: its schema, namespaces, types, and roles.      |
| **Client**             | Whoever sends SQL over a connection. Not trusted.                                            |
| **Core**               | The database engine: it compiles and runs SQL, stores data, and enforces the security model. |
| **Current role**       | The role a connection's statements are checked against; the session role until `SET ROLE`.   |
| **Frontend**           | The SQL dialect layer on top of Core, such as the SQLite or Postgres frontend.               |
| **Host program**       | The program that embeds Turso Database and opens connections. Trusted.                       |
| **Access control list**| The privileges every role holds on one object, and who granted them. ACL for short.          |
| **Owner**              | The role that created an object. It holds every privilege on it and may grant them.          |
| **Privilege**          | Permission for a role to do one action on one object, such as reading a table.               |
| **Privilege check**    | The check that denies a statement whose current role lacks a privilege it needs.             |
| **Role**               | An identity that statements are checked against.                                             |
| **Role catalog**       | The in-memory list of all roles of a database, kept with the schema.                         |
| **Row-level security** | Policies that filter which rows of a table a role may read or change.                        |
| **Schema**             | A namespace of a database that holds objects, such as `public`. Called a namespace in Core.  |
| **Search path**        | The ordered list of schemas a connection looks a name up in when the name has no schema.     |
| **Session role**       | The role the host program gave a connection when it opened it.                               |
| **Superuser**          | A role that passes every privilege check.                                                    |

## Threat model

The attacker is a SQL client holding a connection whose current role is not a
superuser. For example, a network client of the Postgres wire server that
switched to another role with `SET ROLE` after connecting is such a client. A
connection whose current role is a superuser is not an attacker: it is the
host program, and it can do anything the host program can do. A client whose
session role is a superuser can switch back with `RESET ROLE`, so its role
switch holds only as long as the host program does not let it run `RESET ROLE`
or `SET ROLE`.

The attacker may try to:

- read or change stored data that its role has no privilege on;
- switch to a role it may not switch to;
- reach the process or the filesystem through SQL, for example by loading an
  extension or copying from a file;
- read stored data through a side door, such as a virtual table, a function,
  or a catalog;
- make a name mean another object, for example by changing the search path or
  by creating an object that shadows one.

Turso defends against all of these. A statement cannot reach stored data,
another role, or the host program without passing a check against the current
role, and the check is made on the object a name resolved to, not on the name.

Turso does not defend against:

- denial of service by a client, for example a slow query or a large result;
- side channels and timing;
- a host program that hands a superuser connection to the wrong party;
- a tampered database file;
- the network between a client and the wire server, which has no
  authentication or transport security yet;
- bugs in code the host program runs with the connection, such as native
  extensions.

| Area               | Status                                                  |
|--------------------|---------------------------------------------------------|
| Roles              | In scope, implemented                                   |
| Privileges         | In scope, implemented                                   |
| Ownership          | In scope, implemented                                   |
| Row-level security | In scope, not implemented                               |
| Authentication     | Out of scope for Core                                   |

## Authentication

Core does not authenticate anyone. Authentication is the job of the host
program: it decides who the client is and which role the connection runs as,
and passes that role when it opens the connection. From then on Core enforces
what the role may do, and nothing else. There is no login, no password, and no
statement that changes the session role.

For example, a server program that embeds Turso and accepts network clients
authenticates each client itself, with whatever mechanism it chooses, and then
opens a connection with the role that client is entitled to. The Postgres wire
server does not do this yet: every client gets a superuser connection.

## Roles

A role is an identity with a name and a flag that says whether it is a
superuser. A superuser passes every privilege check.
Roles belong to the database file, not to the process, so every connection to
the same file sees the same roles.

Two role ids are built in and not stored: the superuser, which every
connection starts as, and `public`, which every role is a member of. Core
gives them no names; a frontend names them as its users expect. No connection
can run as `public`.

Other roles are created with `CREATE ROLE` and stored in the
`__turso_internal_roles` table, which is created together with the first role.
A role's id is its rowid in that table. A dropped role's row stays in the
table, marked dropped, so that its id is never given to another role while a
connection may still run as the dropped role.
Creating a role is DDL: it runs in the caller's transaction, is undone by a
rollback, and bumps the schema cookie so that other connections and other
processes reload the roles. Under MVCC that change of the cookie does not
abort concurrent transactions, as a change of the schema does: their
statements keep the privileges they were checked against.

Each schema holds a role catalog, an in-memory copy of the stored roles. It is loaded
when the database is opened, after MVCC bootstrap, and again whenever the
schema is reloaded, for example after `VACUUM` or after another process changed
the schema. Looking up a role never reads the database. A roles table that
cannot be read fails the open or the reload; a connection never continues
without its roles.

A connection has a session role and a current role. The host program chooses
the session role when it opens the connection: the superuser, or a stored role
by name. A name that does not exist fails the connection. The current role
starts out as the session role, and privilege checks run against the current
role.

A frontend changes the current role, for example with `SET ROLE`. A superuser
session may switch to any role. Any other session may only switch to its own
session role, because role membership does not exist yet. A prepared statement
records the role it was checked against and is prepared again when it runs as
another role, so switching to another role and back prepares nothing again.
A query that is already returning rows
is not checked again, as in PostgreSQL. The checks that run while a statement
runs, such as those of `currval()`, use the role that is current at that time,
and `current_user` returns it.

## Schemas and the search path

A schema is a namespace of a database that holds tables, views, indexes,
triggers, sequences, and types. Core calls it a namespace. Every database has
a root namespace, and a dialect that supports schemas, such as PostgreSQL,
stores more of them in the `__turso_internal_namespaces` table. The PostgreSQL
frontend gives every database the schema `public` and keeps every table in a
schema. Creating and dropping a schema is DDL: it runs in the caller's
transaction and bumps the schema cookie, so other connections see it when
they reload the schema. On disk an object of a schema is stored under a name
that starts with `__turso_ns<id>_`, but every lookup uses the schema's id and
the object's name, so a role cannot reach an object of one schema by naming it
in another.

A name in a statement either has a schema, as in `app.t`, or not. A name with
a schema is looked up in that schema only. A name without a schema is looked
up through the connection's search path: an ordered list of schema names that
the frontend sets, for example with `SET search_path`. Core looks the name up
in each schema of the search path in order and takes the first match, skips
names of schemas that do not exist, and creates an object whose name has no
schema in the first schema of the search path. Without a search path, Core
looks in the main database and then in the attached databases, as SQLite does.
Any role may change the search path, and changing it makes prepared
statements prepare again.

The search path changes which object a name means, never what a role may do
with it. Privileges are checked after a name has resolved, on the table, view,
or virtual table itself, so a role that changes its search path, or that
names a schema it may not create in, is checked exactly as if it had named
the object directly. A denial for creating an object names the schema the
object would be created in. The PostgreSQL frontend qualifies the name of a
catalog table with the catalog before Core resolves it, so the search path
does not apply to catalog names.

`CREATE SCHEMA` needs `CREATE` on the database and `DROP SCHEMA` the owner of
the schema. Creating an object in a schema needs `CREATE` on it. Every role
may resolve names in every schema, where PostgreSQL requires the `USAGE`
privilege on the schema.

## Privilege checks

Privileges are checked when a statement is prepared, against the current role
at that time. A superuser passes every check. Because changing the role makes
prepared statements prepare again, a statement is always checked against the
role that is current when it starts. Only statements from the client are
checked. Statements that the engine prepares for itself run SQL that the engine
wrote, such as the reads of sequence state and table statistics that load the
schema. The one such statement whose rows reach the client, the PRAGMA that a
PRAGMA table function runs, is checked when the function is compiled.

A denial is data, not a message: the action that was denied, the object it was
denied on, and whether the role lacked a privilege or was not the owner. Each
frontend turns that into the wording its users expect.

### Privileges and access control lists

A privilege is a right to do one kind of action on one object. Which
privileges an object can have depends on its kind, as in PostgreSQL: a table
or view has `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`
and `TRIGGER`; a sequence `USAGE`, `SELECT` and `UPDATE`; a function
`EXECUTE`; a schema `USAGE` and `CREATE`; the database `CREATE` and
`TEMPORARY`; a type `USAGE`; a setting `SELECT` and `SET`.

Every object has an owner and an access control list: the privileges each
role holds on the object, with the grant option, and the role that granted
them. A role owns what it creates: `CREATE` writes the owner's list for the
new table, view, sequence, type or schema when the creating role is not the
superuser. The list is stored in the `__turso_internal_acl` table, one row
per object, and loaded with the schema like the roles. An object without a
row is owned by the superuser and has the default list of its kind: its owner
holds every privilege and may grant them, and `public` holds what every role
may do with such an object by default, which is `EXECUTE` on a function, `USAGE` on a type, `TEMPORARY` on
the database, and nothing on stored data, schemas or settings. Objects that
no statement created, such as the schema table, have no owner's list, so the
dialect registers theirs when it registers the objects: Core makes its
catalog tables readable by every role and `load_extension()`,
`sqlite_dbpage()` and `btree_dump()` usable by the superuser only, and the
PostgreSQL dialect adds its `pg_catalog` tables and `USAGE` on the `public`
schema. Every other built-in function is executable by every role, as a
function is by default, so neither `json_each()` nor `generate_series()`
needs an entry, and neither does a table-valued function of an extension.

A check asks whether the current role holds one of the privileges the
statement needs on the object, or owns it. A superuser passes every check. A
role holds a privilege if it was granted to the role or to `public`.

`GRANT` adds privileges to an object's list and `REVOKE` removes them, for
named roles or for `public`, with `WITH GRANT OPTION` and `GRANT OPTION FOR`
as in PostgreSQL. The grantor is the current role, which must own the object
or hold the grant option for every privilege named; a superuser grants as the
owner. A privilege the kind of object cannot have, such as `EXECUTE` on a
table, is an error. The statement is DDL: it writes the object's row in the
access control table, replacing it if there is one, in the caller's
transaction, bumps the schema cookie so other connections reload the lists,
and changes the in-memory catalog when it completes. A built-in object can
be granted or revoked on too, so `REVOKE SELECT ON pg_class FROM public`
hides that catalog table from every role but the superuser.

### Statements

The first check looks only at the statement, before it is compiled. A
superuser may run every statement. For any other role, every kind of statement
has a rule, so a new kind of statement must choose its rule before the code
compiles.

Reading and changing data:

| Statement | Rule                                                |
|-----------|-----------------------------------------------------|
| `SELECT`  | Allowed, then checked as described in [Data](#data) |
| `INSERT`  | Allowed, then checked as described in [Data](#data) |
| `UPDATE`  | Allowed, then checked as described in [Data](#data) |
| `DELETE`  | Allowed, then checked as described in [Data](#data) |

Transactions:

| Statement   | Rule    |
|-------------|---------|
| `BEGIN`     | Allowed |
| `COMMIT`    | Allowed |
| `ROLLBACK`  | Allowed |
| `SAVEPOINT` | Allowed |
| `RELEASE`   | Allowed |

Roles:

| Statement     | Rule                                                                                        |
|---------------|---------------------------------------------------------------------------------------------|
| `CREATE ROLE` | Superuser only                                                                              |
| `GRANT`       | Allowed, then checked when it is translated: the grantor must own the object or hold the grant option |
| `REVOKE`      | Allowed, then checked like `GRANT`                                                          |

Creating objects:

| Statement                  | Rule                                                            |
|----------------------------|-----------------------------------------------------------------|
| `CREATE TABLE`             | Needs the privilege to create in the schema                     |
| `CREATE VIEW`              | Needs the privilege to create in the schema                     |
| `CREATE MATERIALIZED VIEW` | Needs the privilege to create in the schema                     |
| `CREATE VIRTUAL TABLE`     | Needs the privilege to create in the schema                     |
| `CREATE SEQUENCE`          | Needs the privilege to create in the schema                     |
| `CREATE TYPE`              | Needs the privilege to create in the schema                     |
| `CREATE DOMAIN`            | Needs the privilege to create in the schema                     |
| `CREATE TEMP TABLE`        | Needs `TEMPORARY` on the database, which every role holds       |
| `CREATE TEMP VIEW`         | Needs `TEMPORARY` on the database, which every role holds       |
| `CREATE TEMP TRIGGER`      | Needs `TEMPORARY` on the database, which every role holds       |
| `CREATE TRIGGER`           | Needs `TRIGGER` on the table                                    |
| `CREATE INDEX`             | Only the owner of the table                                     |
| `CREATE SCHEMA`            | Needs `CREATE` on the database                                  |

Changing and dropping objects:

| Statement       | Rule                           |
|-----------------|--------------------------------|
| `ALTER TABLE`   | Only the owner of the table    |
| `DROP TABLE`    | Only the owner of the table    |
| `DROP INDEX`    | Only the owner of the index    |
| `DROP VIEW`     | Only the owner of the view     |
| `DROP TRIGGER`  | Only the owner of the trigger  |
| `DROP TYPE`     | Only the owner of the type     |
| `DROP DOMAIN`   | Only the owner of the domain   |
| `DROP SEQUENCE` | Only the owner of the sequence |
| `DROP SCHEMA`   | Only the owner of the schema   |

Settings:

| Statement             | Rule                                      |
|-----------------------|-------------------------------------------|
| `PRAGMA name`         | Needs `SELECT` on the setting             |
| `PRAGMA name = value` | Needs `SET` on the setting                |

Maintenance and attached databases:

| Statement  | Rule           |
|------------|----------------|
| `ANALYZE`  | Superuser only |
| `VACUUM`   | Superuser only |
| `REINDEX`  | Superuser only |
| `OPTIMIZE` | Superuser only |
| `ATTACH`   | Superuser only |
| `DETACH`   | Superuser only |

### Data

The second check runs after the statement is compiled, on the bytecode. Every
access to stored data goes through a small set of instructions: opening a table
or index for reading or writing, or clearing it. For each one the check finds
the object that owns the root page, the table, the sequence, or the
materialized view, or the table that an index belongs to, and asks for
`SELECT` on a read, and on a write for the privilege the statement needs:
`INSERT`, `UPDATE` or `DELETE`. A sequence needs `USAGE` or `SELECT` to be
read and `USAGE` or `UPDATE` to be advanced. The denial names the object. The
engine's own bookkeeping, the schema table on a `CREATE`, `sqlite_sequence`
on an insert into an `AUTOINCREMENT` table, and the state of a materialized
view, is written on behalf of a statement that passed its check and is not
checked again. Because the check is
on the compiled program and not on the SQL, a statement cannot reach stored
data through a subquery, a view, or any other rewrite of the query without
being checked. The bodies of triggers and the actions of foreign keys are
compiled into subprograms of the statement and checked the same way; their
denial is reported after the statement's own accesses pass, so a role that
may not change a table is told about the table before anything its triggers
would touch, as in PostgreSQL.

Three kinds of access do not open stored data of their own and are checked
separately:

- **Views** are replaced by their query when a statement is compiled. Every
  view a statement reads is recorded with its schema and needs `SELECT` on
  the view, so a view over a table is reported as the view, and a view that
  reads no table is still checked.
- **Virtual tables** read through their own code. Every virtual table a
  statement opens is recorded. A `pragma_*()` function is the setting it
  shows and needs `SELECT` on it, like `PRAGMA`. A table-valued function is
  a function and needs `EXECUTE`, which every role holds by default, unless
  the dialect registered the privileges of a table under its name, as the
  PostgreSQL dialect does for `pg_class`; then it is that table and needs
  `SELECT`. Any other virtual table is a table. So `json_each()` and
  `generate_series()` work for every role, `sqlite_dbpage()` and
  `btree_dump()` only for the superuser, and a virtual table made with
  `CREATE VIRTUAL TABLE` only for roles granted `SELECT` on it. Writes to a
  virtual table need the statement's write privilege on it.
- **Functions** that return stored state without reading storage, such as
  `currval()` and `load_extension()`, check the role when they run, because
  their argument may be a parameter that is not known when the statement is
  prepared.

## Frontends

Core defines the model but adds no SQL syntax for it: the SQLite parser
never produces `CREATE ROLE` or `GRANT`. Each frontend decides what to
expose, how to name the built-in roles, and how to word a denial.

The SQLite frontend exposes nothing. Every connection it opens runs as the
superuser, `CREATE ROLE` is a syntax error, and the privilege checks never
deny anything. SQLite databases behave as they did before roles existed.

The Postgres frontend exposes roles the way PostgreSQL does:

- The built-in superuser is named `postgres`, like the PostgreSQL bootstrap
  superuser. `pg_roles` lists it with OID 10 and stored roles with OIDs from
  16385 up. `public` is not a role, only a name for everyone in access control
  lists, where it has OID 0. Only the superuser can log in.
- `CREATE ROLE name` is translated to the core statement. Role options,
  `CREATE USER` and `CREATE GROUP` are rejected as not supported, names
  starting with `pg_` and the name `public` are reserved, and `CREATE ROLE
  postgres` fails because the superuser exists.
- `SET ROLE name`, `SET ROLE NONE` and `RESET ROLE` change the `role` setting,
  which follows transactions and savepoints like every setting: it is undone
  by a rollback, `SET LOCAL ROLE` lasts until the end of the transaction, and
  `SHOW role`, `current_setting('role')` and `set_config('role', ...)` work.
- `current_user`, `current_role` and `user` return the name of the current
  role, and `session_user` the name of the session role. `pg_get_userbyid()`
  and `acldefault()` look roles up by OID.
- A denial is worded as PostgreSQL words it: `permission denied for table t`,
  `must be owner of table t`, `permission denied for schema public`, and
  `permission denied to create role`, and is reported with SQLSTATE `42501`,
  `insufficient_privilege`. The main database is reported as the `public`
  schema.
- `SET search_path` sets the connection's search path, and `$user`, `main`
  and `temp` are dropped from it, because there is no schema named after the
  user and the core database names are not schemas.
- Every `pg_catalog` table is readable by every role. The one value in them
  that is not schema, `last_value` in `pg_sequences`, is NULL for a role
  without privileges on the sequence, as in PostgreSQL.
- The wire server opens one connection per client, so the current role of one
  client is never visible to another.

Statements that the Postgres frontend runs itself, outside the core statement
path, are not covered by the core checks and must check the role themselves.
Today these are:

- `CREATE SCHEMA`, which needs `CREATE` on the database, `DROP SCHEMA`,
  which needs the owner of the schema, and `COPY ... FROM` a file, which is
  allowed only for a superuser;
- `COPY ... TO STDOUT`, which prepares a core `SELECT` of the table and is
  checked like one;
- `SET`, `SHOW`, `SET TRANSACTION` and `RESET`, which change or show the
  session's settings and are allowed for every role, as in PostgreSQL;
- `DISCARD ALL`, which resets the settings and the current role to the
  session role, as `RESET ROLE` does, and is allowed for every role;
- `REFRESH MATERIALIZED VIEW` and `COMMENT ON`, which do nothing and are
  allowed for every role, where PostgreSQL requires the owner.

Roles differ from PostgreSQL in two ways that clients may notice. Roles belong
to a database file, where PostgreSQL shares them across a cluster. And the wire
server does not authenticate clients yet, so every client gets a superuser
connection.

## Future work

In the order the pieces depend on each other:

- **`ALTER ... OWNER TO`.** Ownership is recorded at `CREATE` and cannot be
  transferred yet.
- **Column privileges, default privileges and `GRANT ... ON ALL TABLES IN
  SCHEMA`.** `GRANT` and `REVOKE` take one or more named objects today.
- **Schema privileges.** Record the owner of a schema, and the `USAGE` and
  `CREATE` privileges on it. `CREATE SCHEMA` and `DROP SCHEMA` then stop being
  superuser-only, and resolving a name in a schema needs `USAGE` on it, as in
  PostgreSQL.
- **Role membership.** Granting a role to another role, and `SET ROLE` for
  members, which removes the superuser-only rule for switching roles.
- **Row-level security.** Policies attached to a table that filter the rows a
  role may read or change. The role flag that says whether row policies apply
  already exists, and the superuser already ignores them. PostgreSQL applies
  policies by adding their predicates to the query when it is planned, and the
  same fits Turso, since the data check already runs on the compiled
  program. Policies on a table are stored with the schema, like indexes and
  triggers.
- **Role management.** `ALTER ROLE`, `DROP ROLE`, and role attributes such as
  `SUPERUSER` and `CREATEROLE`. `DROP ROLE` marks the role's row dropped
  instead of deleting it, so that the id is not given to a new role.
- **Privilege inquiry.** `has_table_privilege()` and the other access
  privilege inquiry functions, and the `relacl` columns of the catalogs.

## References

- [Chapter 21, Database Roles](https://www.postgresql.org/docs/current/user-manag.html):
  role attributes, membership, and function security.
- [Section 5.8, Privileges](https://www.postgresql.org/docs/current/ddl-priv.html):
  the privilege names and the owner rule.
- [Section 5.9, Row Security Policies](https://www.postgresql.org/docs/current/ddl-rowsecurity.html).
- [`CREATE ROLE`](https://www.postgresql.org/docs/current/sql-createrole.html),
  [`SET ROLE`](https://www.postgresql.org/docs/current/sql-set-role.html),
  [`GRANT`](https://www.postgresql.org/docs/current/sql-grant.html).
- [Section 9.27, System Information Functions](https://www.postgresql.org/docs/current/functions-info.html):
  `current_user`, `session_user`, `pg_get_userbyid()`, `acldefault()`, and the
  access privilege inquiry functions.
- [`pg_roles`](https://www.postgresql.org/docs/current/view-pg-roles.html) and
  [`pg_authid`](https://www.postgresql.org/docs/current/catalog-pg-authid.html).
- [Appendix A, Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html).
