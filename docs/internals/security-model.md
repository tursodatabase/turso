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
> Roles are implemented. Privileges, including ownership, are in scope: the
> checks that enforce them exist, but nothing can grant them yet, so a role
> that is not a superuser has none. Row-level security is in scope and not
> implemented. Authentication is out of scope for Core: it belongs to the host
> program or the frontend.

## Glossary

| Term                   | Definition                                                                                   |
|------------------------|----------------------------------------------------------------------------------------------|
| **Authentication**     | Deciding who a client is and which role its connection runs as. The host program's job.      |
| **Client**             | Whoever sends SQL over a connection. Not trusted.                                            |
| **Core**               | The database engine: it compiles and runs SQL, stores data, and enforces the security model. |
| **Current role**       | The role a connection's statements are checked against; the session role until `SET ROLE`.   |
| **Frontend**           | The SQL dialect layer on top of Core, such as the SQLite or Postgres frontend.               |
| **Host program**       | The program that embeds Turso Database and opens connections. Trusted.                       |
| **Owner**              | The role that created an object.                                                             |
| **Privilege**          | Permission for a role to do one action on one object, such as reading a table.               |
| **Privilege check**    | The check that denies a statement whose current role lacks a privilege it needs.             |
| **Role**               | An identity that statements are checked against.                                             |
| **Role catalog**       | The in-memory list of all roles of a database, kept with the schema.                         |
| **Row-level security** | Policies that filter which rows of a table a role may read or change.                        |
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
  or a catalog.

Turso defends against all of these. A statement cannot reach stored data,
another role, or the host program without passing a check against the current
role.

Turso does not defend against:

- denial of service by a client, for example a slow query or a large result;
- side channels and timing;
- a host program that hands a superuser connection to the wrong party;
- a tampered database file;
- the network between a client and the wire server, which has no
  authentication or transport security yet;
- bugs in code the host program runs with the connection, such as native
  extensions.

| Area               | Status                                        |
|--------------------|-----------------------------------------------|
| Roles              | In scope, implemented                         |
| Privileges         | In scope, checks implemented, granting is not |
| Ownership          | In scope, not implemented                     |
| Row-level security | In scope, not implemented                     |
| Authentication     | Out of scope for Core                         |

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

A role is an identity with a name and two flags: whether it is a superuser,
and whether row policies apply to it. A superuser passes every privilege check.
Roles belong to the database file, not to the process, so every connection to
the same file sees the same roles.

Every database has two built-in roles that are not stored: the superuser, and
`public`, which every role is a member of. The dialect names them, so the
Postgres frontend sees `postgres` and `public`. No connection can run as
`public`, and no stored role can have the name of a built-in role.

Other roles are created with `CREATE ROLE` and stored in the
`__turso_internal_roles` table, which is created together with the first role.
Creating a role is DDL: it runs in the caller's transaction, is undone by a
rollback, and bumps the schema cookie so that other connections and other
processes reload the roles.

Each schema holds a role catalog, an in-memory copy of all roles. It is loaded
when the database is opened, after MVCC bootstrap, and again whenever the
schema is reloaded, for example after `VACUUM` or after another process changed
the schema. Looking up a role never reads the database. A roles table that
cannot be read fails the open or the reload; a connection never continues with
only the built-in roles.

A connection has a session role and a current role. The host program chooses
the session role when it opens the connection: the superuser, or a stored role
by name. A name that does not exist fails the connection. The current role
starts out as the session role, and privilege checks run against the current
role.

`SET ROLE` changes the current role and `RESET ROLE` sets it back to the
session role. A superuser session may switch to any role. Any other session may
only switch to its own session role, because role membership does not exist
yet. The switch happens when the statement completes, so an interrupted `SET
ROLE` changes nothing. `SET ROLE` is refused inside a transaction block,
because rolling back would not restore the previous role. Changing the role
makes prepared statements prepare again. A query that is already returning rows
finishes under the role it started with, as in PostgreSQL.

## Privilege checks

Privileges are checked when a statement is prepared, against the current role
at that time. A superuser passes every check. Because changing the role makes
prepared statements prepare again, a statement always runs with the privileges
of the role that is current when it starts. Statements that the engine prepares
for itself, for example to reload the schema, are checked the same way as
statements from the client.

A denial is data, not a message: the action that was denied, the object it was
denied on, and whether the role lacked a privilege or was not the owner. Each
frontend turns that into the wording its users expect.

Privileges cannot be granted yet and objects have no owner, so a role that is
not a superuser has no privileges on any database object. It may run statements
that touch no object, such as `SELECT 1`, switch roles, and read the catalogs.
The checks below are written so that adding granted privileges and ownership
later changes what is allowed, not where the checks run.

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
| `SET ROLE`    | Allowed, then checked when it runs: the session role must be a superuser or the role itself |
| `RESET ROLE`  | Allowed                                                                                     |
| `CREATE ROLE` | Needs the privilege to create roles                                                         |

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
| `CREATE TEMP TABLE`        | Needs the privilege to create temporary objects in the database |
| `CREATE TEMP VIEW`         | Needs the privilege to create temporary objects in the database |
| `CREATE TEMP TRIGGER`      | Needs the privilege to create temporary objects in the database |
| `CREATE TRIGGER`           | Needs the privilege to create triggers on the table             |
| `CREATE INDEX`             | Only the owner of the table                                     |

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

Settings:

| Statement             | Rule                                      |
|-----------------------|-------------------------------------------|
| `PRAGMA name`         | Needs the privilege to read the setting   |
| `PRAGMA name = value` | Needs the privilege to change the setting |

Maintenance and attached databases:

| Statement  | Rule                                            |
|------------|-------------------------------------------------|
| `ANALYZE`  | Needs the privilege to analyze the database     |
| `VACUUM`   | Needs the privilege to vacuum the database      |
| `REINDEX`  | Needs the privilege to reindex the database     |
| `OPTIMIZE` | Needs the privilege to optimize the database    |
| `ATTACH`   | Needs the privilege to attach to the database   |
| `DETACH`   | Needs the privilege to detach from the database |

### Data

The second check runs after the statement is compiled, on the bytecode. Every
access to stored data goes through a small set of instructions: opening a table
or index for reading or writing, clearing it, creating it, or destroying it. If
the program contains any of these, the statement is denied, and the denial
names the object that the instruction touches: the table, the sequence, or the
materialized view, or the table that an index belongs to. Because the check is
on the compiled program and not on the SQL, a statement cannot reach stored
data through a subquery, a trigger, a foreign key, or any other indirection
without being checked.

Three kinds of access do not open stored data of their own and are checked
separately:

- **Views** are replaced by their query when a statement is compiled. Every
  view a statement reads is recorded by name and denied as reading the view,
  so a view over a table is reported as the view, and a view that reads no
  table is still denied.
- **Virtual tables** read through their own code. Every virtual table a
  statement opens is recorded. An internal or native virtual table is denied
  unless it declares that a role without privileges may read it, which the
  catalogs and `json_each()` do and which anything that reads pages or records
  directly, such as `sqlite_dbpage` and `btree_dump()`, does not. `pragma_*()`
  functions are denied like `PRAGMA`. Writes to a virtual table are always
  denied.
- **Functions** that return stored state without reading storage, such as
  `currval()` and `load_extension()`, check the role when they run, because
  their argument may be a parameter that is not known when the statement is
  prepared.

Every role may read the catalogs: the schema table, the types table, and the
roles table. They hold the schema and the role names, which PostgreSQL also
lets every role see, and a connection reads them to reload its own schema.

## Frontends

Core defines the model but adds no SQL syntax for it: the SQLite parser
never produces `CREATE ROLE` or `SET ROLE`. Each frontend decides what to
expose, how to name the built-in roles, and how to word a denial.

The SQLite frontend exposes nothing. Every connection it opens runs as the
superuser, `CREATE ROLE` is a syntax error, and the privilege checks never
deny anything. SQLite databases behave as they did before roles existed.

The Postgres frontend exposes roles the way PostgreSQL does:

- The built-in roles are named `postgres` and `public`. `pg_roles` lists the
  superuser with OID 10, the OID of the PostgreSQL bootstrap superuser, and
  stored roles with OIDs from 16384 up. `public` has OID 0, as in PostgreSQL's
  access control lists, and is not listed. Only the superuser can log in.
- `CREATE ROLE name` is translated to the core statement. Role options,
  `CREATE USER` and `CREATE GROUP` are rejected as not supported, and names
  starting with `pg_` are reserved.
- `SET ROLE name`, `SET ROLE NONE` and `RESET ROLE` are translated to the core
  statement. `SET LOCAL ROLE` is rejected.
- `current_user`, `current_role` and `user` return the name of the current
  role, and `session_user` the name of the session role. `pg_get_userbyid()`
  and `acldefault()` look roles up by OID.
- A denial is worded as PostgreSQL words it: `permission denied for table t`,
  `must be owner of table t`, `permission denied for schema public`, and
  `permission denied to create role`. The main database is reported as the
  `public` schema.
- Every `pg_catalog` table is readable by every role. The one value in them
  that is not schema, `last_value` in `pg_sequences`, is NULL for a role
  without privileges on the sequence, as in PostgreSQL.
- The wire server opens one connection per client, so the current role of one
  client is never visible to another.

Statements that the Postgres frontend runs itself, outside the core statement
path, are not covered by the core checks and must check the role themselves.
Today these are `CREATE SCHEMA`, `DROP SCHEMA` and `COPY ... FROM` a file, and
all three are allowed only for a superuser.

Roles differ from PostgreSQL in two ways that clients may notice. Roles belong
to a database file, where PostgreSQL shares them across a cluster. And the wire
server does not authenticate clients yet, so every client gets a superuser
connection.

## Future work

In the order the pieces depend on each other:

- **Ownership.** Record the role that created each table, view, index,
  trigger, sequence and type. The denials that today say "not the owner" then
  pass for the owner, and `DROP`, `ALTER` and `CREATE INDEX` become usable by
  roles other than the superuser.
- **Granted privileges.** Store privileges per object and per role, assigned
  with `GRANT` and removed with `REVOKE`, with `public` as a grantee. The
  data check then consults the privileges of the current role instead
  of denying every access, and `CREATE` on the schema decides who may create
  objects.
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
  `SUPERUSER` and `CREATEROLE`.
- **Privilege inquiry.** `has_table_privilege()` and the other access
  privilege inquiry functions, and the `relacl` columns of the catalogs.
- **Error codes.** The wire server reports denials with SQLSTATE `42501`,
  `insufficient_privilege`, instead of a generic error.

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
