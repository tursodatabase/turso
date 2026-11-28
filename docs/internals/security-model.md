# Turso Security Model

This document specifies the Turso security model:

- what a SQL statement is allowed to do;
- which roles exist;
- who owns each object;
- which privileges a role has;
- which rows of a table row-level security lets a role read and change.

The security model follows PostgreSQL 18, which is the reference for every
rule in this document.

> [!NOTE]
> One rule covers the PostgreSQL features that Turso does not have: a
> statement that Turso cannot handle as this document says must fail with
> SQLSTATE `0A000` (`feature_not_supported`). It must never be accepted and
> then checked by a weaker rule.

A statement passes three layers of checks before it can touch a row:

1. **Name resolution.** The names in the statement resolve to objects,
   through the schema written in the name or through the search path. The
   role needs `USAGE` on the schema of every object it uses.
2. **Privileges.** The role must own the object, or hold the privilege the
   statement needs on it, such as `SELECT` on a table.
3. **Row-level security.** If the table has row-level security, the table's
   policies decide which of its rows the role may read, and which rows it
   may write.

A superuser passes every check of layers 1 and 2, and layer 3 does not limit
it.

## Glossary

| Term                    | Definition                                                                                          |
|-------------------------|-----------------------------------------------------------------------------------------------------|
| **Access control list** | The privileges that roles hold on one object, and who granted them. ACL for short.                  |
| **Authentication**      | Deciding who a client is, and so which role its connection starts as.                               |
| **Client**              | Whoever sends SQL over a connection. Not trusted.                                                   |
| **Core**                | The database engine: it compiles and runs SQL, stores data, and enforces the security model.        |
| **Current role**        | The role a connection's statements are checked against; the session role until `SET ROLE`.          |
| **Frontend**            | The SQL dialect layer on top of core: SQLite or PostgreSQL.                                          |
| **Host program**        | The program that embeds Turso and opens connections. Trusted.                                       |
| **Member**              | A role that was granted another role. It can use that role's privileges, or switch to it.            |
| **Owner**               | The role an object belongs to. It may alter, drop, and grant on it.                                  |
| **Policy**              | A rule of a table with row-level security: which roles and commands it is for, and its conditions.  |
| **Privilege**           | Permission for a role to do one kind of action on one object, such as `SELECT` on a table.          |
| **Relation**            | A table, view, materialized view, sequence, or index.                                               |
| **Role**                | An identity that owns objects and holds privileges. Users and groups are both roles.                |
| **Row-level security**  | RLS for short. Policies that limit which rows of a table a role may read and write.                 |
| **Schema**              | A set of object names inside a database, such as `public`. Core calls it a namespace.               |
| **Search path**         | The ordered list of schemas that an unqualified name is looked up in.                               |
| **Session role**        | The role a connection was opened as. PostgreSQL's `session_user`.                                    |
| **Superuser**           | A role that passes every privilege check and that row-level security does not limit.               |

## Trust model

Turso trusts the host program that opens a connection, and does not trust the
SQL that the connection runs. Every statement from the client is checked
against the connection's current role and can only do what that role may do.

### Authentication

PostgreSQL manual: [Client Authentication](https://www.postgresql.org/docs/18/client-authentication.html).

Authentication decides the session role of a connection. SQL never does.

- **Wire server.** The PostgreSQL wire server authenticates each client
  before it runs any of its SQL. The session role is the role the client
  authenticated as, and that role must have the `LOGIN` attribute. A client
  that fails authentication gets no connection.
- **Embedded.** A host program that opens a connection through the API
  names the session role itself. The host program is trusted, so naming the
  role is the authentication. A role name that does not exist fails the
  connection.

### Threat model

The attacker is a SQL client whose session role is not a superuser. A
connection whose session role is a superuser is the host program: it can do
anything the host program can.

The attacker may try to:

- read or change rows of a table that its role has no privilege on;
- read or change rows that the table's policies hide from its role;
- switch to a role it may not switch to, or give itself privileges;
- learn the values of hidden rows indirectly, for example from a constraint
  error, from an aggregate, or from a function that runs on hidden rows;
- reach the process or the filesystem through SQL, for example by loading an
  extension or copying from a file;
- read stored data through a side door, such as a virtual table, a PRAGMA, a
  catalog table, or an engine table;
- make a name mean another object, for example by changing the search path
  or by creating an object that shadows another.

Turso defends against all of these. A statement cannot reach stored data,
another role, or the host program without passing a check against its role.
The check is made on the object that a name resolved to, not on the name.

Turso does not defend against:

- denial of service, such as a slow query or a large result;
- timing and other side channels: a role can learn how many rows a table
  has, or how they are distributed, from how long a statement takes or from
  `EXPLAIN`, as in PostgreSQL. It cannot learn the values of rows it may not
  read;
- learning that a hidden row exists from a unique or foreign key violation,
  which PostgreSQL does not prevent either;
- a host program that gives a connection to the wrong client, or that lets a
  client run its own SQL on a connection whose session role is a superuser;
- a host program that lets a client set a setting that a policy reads (see
  [Policies for application users](#policies-for-application-users));
- a tampered database file;
- bugs in native code that the host program loads, such as extensions.

## Roles

PostgreSQL manual: [Database Roles](https://www.postgresql.org/docs/18/user-manag.html).

A role is an identity that owns objects and holds privileges. There are no
separate users and groups: a user is a role that can log in, and a group is a
role that other roles are members of.

Roles belong to a database file. Every connection to the file sees the same
roles. This is the one place where the model differs from PostgreSQL on
purpose: PostgreSQL shares roles across the databases of a cluster, and Turso
has no cluster.

### Built-in roles

PostgreSQL manual: [Predefined Roles](https://www.postgresql.org/docs/18/predefined-roles.html). Source: [`src/include/catalog/pg_authid.dat`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/catalog/pg_authid.dat).

- **`postgres`** is the bootstrap superuser, with OID 10. It always exists,
  cannot be dropped, and owns the database and every built-in object.
- **`PUBLIC`** is not a role. It is a name for every role in `GRANT`,
  `REVOKE`, and policies, with OID 0 in ACLs. No connection can run as it,
  and it cannot be created, dropped, or switched to.
- **Predefined roles** have names that start with `pg_`. They hold fixed
  rights that cannot be granted otherwise, and a role gets those rights by
  becoming a member:

  | Role                      | Its members may                                                        |
  |---------------------------|------------------------------------------------------------------------|
  | `pg_database_owner`       | Nothing by membership. Its one implicit member is the owner of the database. It owns `public`. |
  | `pg_read_all_data`        | Read every table, view, and sequence, as if they had `SELECT` on them and `USAGE` on every schema. Row-level security still applies. |
  | `pg_write_all_data`       | Write every table, view, and sequence, as if they had `INSERT`, `UPDATE`, and `DELETE` on them and `USAGE` on every schema. |
  | `pg_maintain`             | Do what `MAINTAIN` allows, on every relation.                          |
  | `pg_read_server_files`    | `COPY ... FROM` a file on the host.                                    |
  | `pg_write_server_files`   | `COPY ... TO` a file on the host.                                      |

Role names that start with `pg_` are reserved (SQLSTATE `42939`), and so are
`public` and `none`. Created roles have OIDs from 16384 up, and an OID is
never used again for another role.

### Attributes

PostgreSQL manual: [Role Attributes](https://www.postgresql.org/docs/18/role-attributes.html), [`CREATE ROLE`](https://www.postgresql.org/docs/18/sql-createrole.html), [`ALTER ROLE`](https://www.postgresql.org/docs/18/sql-alterrole.html), [`DROP ROLE`](https://www.postgresql.org/docs/18/sql-droprole.html), [Dropping Roles](https://www.postgresql.org/docs/18/role-removal.html). Source: [`src/backend/commands/user.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/user.c).

`CREATE ROLE name [options]` creates a role. `CREATE USER` is the same with
`LOGIN` as the default.

| Attribute          | Default          | Meaning                                                                    |
|--------------------|------------------|----------------------------------------------------------------------------|
| `SUPERUSER`        | `NOSUPERUSER`    | Passes every privilege check; row-level security does not limit it.        |
| `LOGIN`            | `NOLOGIN`        | May be the session role of a wire connection.                              |
| `CREATEROLE`       | `NOCREATEROLE`   | May create roles, and alter and drop the roles it has `ADMIN OPTION` on.    |
| `CREATEDB`         | `NOCREATEDB`     | May create databases.                                                      |
| `BYPASSRLS`        | `NOBYPASSRLS`    | Row-level security does not limit it. It still needs privileges.           |
| `INHERIT`          | `INHERIT`        | The default for the `INHERIT` option of the memberships it is given.       |
| `REPLICATION`      | `NOREPLICATION`  | May start replication.                                                     |
| `PASSWORD`         | none             | The password the wire server checks.                                       |
| `CONNECTION LIMIT` | no limit         | How many connections the role may have open.                               |
| `VALID UNTIL`      | forever          | When the password stops working.                                           |

Attributes belong to the role itself. A member never gets them through
membership: a member of a superuser role is not a superuser until it runs
`SET ROLE`.

Who may manage roles:

- `CREATE ROLE` needs `CREATEROLE`. Otherwise: `permission denied to create
  role` (`42501`).
- Only a superuser may create a `SUPERUSER` role. A role may give `CREATEDB`,
  `REPLICATION`, and `BYPASSRLS` only if it has that attribute itself.
- A role that is not a superuser becomes a member of the role it creates,
  with `ADMIN OPTION`.
- `ALTER ROLE` and `DROP ROLE` need `CREATEROLE` and `ADMIN OPTION` on the
  role. Only a superuser may alter or drop a superuser role, or change the
  `SUPERUSER` attribute. Any role may change its own password.
- `DROP ROLE` fails with `2BP01` while the role owns an object, holds a
  privilege on one, or is named in a policy. `REASSIGN OWNED` and
  `DROP OWNED` remove those first. The current role and the session role
  cannot be dropped.
- An existing name fails with `42710`.

### Membership

PostgreSQL manual: [Role Membership](https://www.postgresql.org/docs/18/role-membership.html), [`GRANT`](https://www.postgresql.org/docs/18/sql-grant.html). Source: [`src/backend/utils/adt/acl.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/acl.c).

`GRANT role TO member` makes `member` a member of `role`. `REVOKE role FROM
member` ends it. Granting needs `ADMIN OPTION` on `role`, or a superuser.
Membership cannot form a loop.

A membership has three options:

| Option    | Default                          | Meaning                                                       |
|-----------|----------------------------------|---------------------------------------------------------------|
| `INHERIT` | The member's `INHERIT` attribute | The member uses the role's privileges without switching to it. |
| `SET`     | true                             | The member may `SET ROLE` to the role.                         |
| `ADMIN`   | false                            | The member may grant and revoke membership in the role.        |

A role **has the privileges of** itself, of `PUBLIC`, and of every role it
reaches through a chain of memberships that all have `INHERIT`. Wherever this
document says a role holds a privilege, owns an object, or is named by a
policy, it means that some role it has the privileges of does.

### Session role and current role

PostgreSQL manual: [`SET ROLE`](https://www.postgresql.org/docs/18/sql-set-role.html), [`SET SESSION AUTHORIZATION`](https://www.postgresql.org/docs/18/sql-set-session-authorization.html). Source: [`src/backend/commands/variable.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/variable.c).

A connection has a session role and a current role. Authentication sets the
session role. The current role starts as the session role, and privilege
checks and policies use the current role.

- `SET ROLE role` changes the current role. It is allowed when the
  **session** role reaches `role` through a chain of memberships that all
  have `SET`, when `role` is the session role itself, or when the session
  role is a superuser. Otherwise: `permission denied to set role "r"`
  (`42501`). A role that does not exist fails with `42704`.
- After `SET ROLE`, the connection has the privileges of the new role only.
  It loses the session role's privileges and attributes, and gains the new
  role's.
- `SET ROLE NONE` and `RESET ROLE` return to the session role.
- The role is a setting named `role`, and follows transactions like every
  setting: a rollback or `ROLLBACK TO SAVEPOINT` undoes a change made since,
  and `SET LOCAL ROLE` lasts until the end of the transaction. `RESET ALL`
  does not change it. `DISCARD ALL` resets it.
- `SET SESSION AUTHORIZATION role` changes the session role and the current
  role. Only a connection that authenticated as a superuser may run it.
- `current_user`, `current_role`, and `user` return the name of the current
  role. `session_user` returns the name of the session role. They are
  evaluated when the statement runs.

## Schemas and the search path

A database contains schemas, and a schema contains tables, views, indexes,
sequences, types, and functions. Two objects of the same kind can have the
same name in two schemas. [namespaces.md](namespaces.md) describes how schemas
are stored and how names resolve; this section specifies what matters for
security.

### Schemas

PostgreSQL manual: [Schemas](https://www.postgresql.org/docs/18/ddl-schemas.html), [`CREATE SCHEMA`](https://www.postgresql.org/docs/18/sql-createschema.html).

- Every database has the schema `public`, owned by `pg_database_owner`.
  Every role has `USAGE` on it. Only its owner has `CREATE` on it.
- `pg_catalog` holds the catalog tables and the built-in functions and
  types. `information_schema` holds the standard views of the same. Every
  role has `USAGE` on both, and no role but a superuser can create in them.
- `CREATE SCHEMA` needs `CREATE` on the database. The new schema belongs to
  the role that created it, and no other role has any privilege on it.
  Schema names that start with `pg_` are reserved (`42939`).
- `DROP SCHEMA` needs the owner of the schema. `DROP SCHEMA ... CASCADE`
  drops the objects in it, whoever owns them.

A schema has two privileges:

- **`USAGE`** lets a role look up the objects in the schema. Without it the
  role cannot use any object of the schema, whatever privileges it has on
  the object: `permission denied for schema s` (`42501`).
- **`CREATE`** lets a role create objects in the schema, and rename or move
  into it the objects the role owns.

### Name resolution

PostgreSQL manual: [The Schema Search Path](https://www.postgresql.org/docs/18/ddl-schemas.html#DDL-SCHEMAS-PATH), [Usage Patterns](https://www.postgresql.org/docs/18/ddl-schemas.html#DDL-SCHEMAS-PATTERNS), [`search_path`](https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-SEARCH-PATH). Source: [`src/backend/catalog/namespace.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/namespace.c).

- A qualified name, `app.t`, is looked up only in `app`.
- An unqualified name is looked up in each schema of the search path in
  order, and the first match wins. Schemas on the path that do not exist,
  and schemas the role has no `USAGE` on, are skipped.
- `pg_catalog` is searched before the search path, unless the path names
  it. So `pg_class` always means the catalog table, and a user object
  cannot shadow a catalog object.
- The connection's temporary schema is searched before `pg_catalog` for
  tables, views, sequences, and types, unless the path names `pg_temp`. It
  is never searched for functions.
- An unqualified `CREATE` puts the object in the first schema of the search
  path that exists. If there is none: `no schema has been selected to create
  in` (`3F000`).
- The default search path is `"$user", public`. `$user` stands for a schema
  with the name of the current role, and is skipped when there is none.

Any role may change its search path. **The search path changes which object
a name means, never what a role may do with it**: privileges are checked on
the object that the name resolved to. A role that changes its search path is
checked as if it had written the qualified name of each object.

**The names in a view do not depend on who reads it.** They resolve when the
view is created, and the view keeps meaning those objects. The search path of
the session that reads the view has no effect, so a role cannot make a view
read another table by changing its search path.

**Stored names cannot be used to reach an object.** Core stores an object of
a schema under an encoded name, such as `__turso_ns2_t`. That name does not
resolve to the object, in any schema, and no object can be created with a
name that starts with `__turso_ns`, `__turso_internal_`, or `sqlite_`.

A schema on the search path is trusted: whoever may create objects in it can
make an unqualified name mean their object. This is why `public` gives
`CREATE` only to its owner.

## Ownership

PostgreSQL manual: [Privileges](https://www.postgresql.org/docs/18/ddl-priv.html), [`ALTER TABLE`](https://www.postgresql.org/docs/18/sql-altertable.html). Source: [`src/backend/commands/tablecmds.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/tablecmds.c).

Every object has an owner: the role that created it. An index belongs to
the owner of its table. The owner is stored with the object, stays with it
when it is renamed, and is removed when it is dropped.

The owner's rights come with ownership and cannot be granted or revoked:

- alter and drop the object;
- grant and revoke privileges on it;
- for a table: enable, disable, and force row-level security, and create,
  alter, and drop its policies.

Further rules:

- The owner of a schema may also drop the objects in it.
- `ALTER ... OWNER TO role` needs the owner. The owner must be able to
  `SET ROLE` to `role`, and `role` must have `CREATE` on the object's
  schema. A superuser may give any object to any role. The indexes of a
  table, and the sequences that its columns own, move with the table. In the
  ACL, the privileges of the old owner and the grants it made become the new
  owner's.
- A sequence may be owned by a column (`ALTER SEQUENCE ... OWNED BY`, or a
  `serial` column). It must have the same owner and the same schema as the
  column's table, and it is dropped with the column.
- The engine's own tables, such as `sqlite_schema`, `sqlite_sequence`, and
  the tables named `__turso_internal_*`, have no owner. Only a superuser may
  read or change them, and no privilege on them can be granted.

## Privileges

### Kinds of privileges

PostgreSQL manual: [Privileges](https://www.postgresql.org/docs/18/ddl-priv.html).

A privilege is a right to do one kind of action on one object. Which
privileges an object can have depends on its kind:

| Privilege    | Letter | Objects                                    | Allows                                                    |
|--------------|--------|--------------------------------------------|-----------------------------------------------------------|
| `SELECT`     | `r`    | Table, view, column, sequence              | Read rows or columns; `currval()` on a sequence           |
| `INSERT`     | `a`    | Table, view, column                        | Insert rows; with columns, only those columns may be set   |
| `UPDATE`     | `w`    | Table, view, column, sequence              | Update rows or columns; `nextval()` and `setval()`        |
| `DELETE`     | `d`    | Table, view                                | Delete rows                                               |
| `TRUNCATE`   | `D`    | Table                                      | `TRUNCATE`                                                |
| `REFERENCES` | `x`    | Table, column                              | Create a foreign key that refers to the table or columns   |
| `TRIGGER`    | `t`    | Table, view                                | Create a trigger on it                                    |
| `MAINTAIN`   | `m`    | Table, materialized view                   | `VACUUM`, `ANALYZE`, `REINDEX`, `CLUSTER`, `REFRESH MATERIALIZED VIEW` |
| `USAGE`      | `U`    | Schema, sequence, type, domain             | Look up objects in a schema; `nextval()` and `currval()`; use a type |
| `CREATE`     | `C`    | Database, schema                           | Create schemas in a database; create objects in a schema   |
| `CONNECT`    | `c`    | Database                                   | Connect to the database                                   |
| `TEMPORARY`  | `T`    | Database                                   | Create temporary tables                                   |
| `EXECUTE`    | `X`    | Function                                   | Call the function                                         |
| `SET`        | `s`    | Setting                                    | Set a setting that otherwise needs a superuser            |

### Access control lists

PostgreSQL manual: [Privileges](https://www.postgresql.org/docs/18/ddl-priv.html). Source: [`src/backend/utils/adt/acl.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/acl.c).

The ACL of an object lists who holds which privileges on it, and who granted
them, in PostgreSQL's format: `{alice=arwd/postgres,=r/postgres}` gives
`alice` `SELECT`, `INSERT`, `UPDATE`, and `DELETE`, and `PUBLIC` `SELECT`,
both granted by `postgres`. A `*` after a letter means the grantee may grant
that privilege on.

- An object that was never granted on has the default ACL of its kind. The
  owner holds every privilege, with the right to grant each. `PUBLIC` holds:

  | Object                                      | `PUBLIC` holds by default  |
  |---------------------------------------------|----------------------------|
  | Table, view, materialized view, sequence, schema, setting | Nothing      |
  | Database                                    | `CONNECT`, `TEMPORARY`     |
  | Function                                    | `EXECUTE`                  |
  | Type, domain                                | `USAGE`                    |

- A role holds a privilege on an object when the ACL gives it to a role it
  has the privileges of.
- The owner's privileges are ordinary entries of the ACL. The owner may
  revoke them from itself, and then does not hold them, but it may always
  grant them back.
- The catalogs show the ACL with role names, and NULL for a default ACL:
  `pg_class.relacl`, `pg_namespace.nspacl`, and so on.

**Column privileges.** `SELECT`, `INSERT`, `UPDATE`, and `REFERENCES` can be
granted on single columns. A statement passes when the role holds the
privilege on the table, or on every column the statement uses that way. A
privilege on the table covers all its columns, also the ones added later, and
revoking a column privilege does not take away what a table privilege gives.

### GRANT and REVOKE

PostgreSQL manual: [`GRANT`](https://www.postgresql.org/docs/18/sql-grant.html), [`REVOKE`](https://www.postgresql.org/docs/18/sql-revoke.html), [`ALTER DEFAULT PRIVILEGES`](https://www.postgresql.org/docs/18/sql-alterdefaultprivileges.html). Source: [`src/backend/catalog/aclchk.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/aclchk.c).

`GRANT privileges ON object TO roles [WITH GRANT OPTION]` adds privileges
to the ACL, and `REVOKE [GRANT OPTION FOR] privileges ON object FROM roles
[CASCADE | RESTRICT]` removes them. The roles are role names, `PUBLIC`,
`CURRENT_ROLE`, `CURRENT_USER`, or `SESSION_USER`. `ALL [PRIVILEGES]` means
every privilege the object can have. `ON ALL TABLES IN SCHEMA s` and its
likes name every such object of the schema; `ALL TABLES` includes views.

- **Who may grant.** The owner may grant every privilege. Another role may
  grant the privileges it holds with the grant option. A superuser grants as
  if it was the owner.
- **The grantor** recorded in the ACL is the role that had the right to
  grant: the owner when the owner or a superuser grants, and otherwise the
  role that holds the grant option, which can be a role that the current
  role has the privileges of.
- **A role without the right** to grant any of the privileges it names: if
  it holds no privilege at all on the object, the statement fails with
  `42501`. If it holds some, the statement grants what it can and warns
  about the rest (`no privileges were granted` or `not all privileges were
  granted`), without an error.
- **`WITH GRANT OPTION`** lets the grantee grant the privilege on. It cannot
  be given to `PUBLIC`.
- **`REVOKE`** removes only the grants that the revoking role's grantor
  made. A privilege granted to a role by two grantors stays until both are
  revoked, and a privilege that a role has through `PUBLIC` or through
  membership is not affected by revoking it from the role itself.
- **Dependent grants.** If the grantee granted the privilege on, revoking
  it or its grant option fails with `2BP01` unless `CASCADE` is given, which
  revokes those grants too.
- **A privilege the object cannot have**, such as `USAGE` on a table, fails
  with `0LP01` when it is named. `ALL` leaves it out.
- `ALTER DEFAULT PRIVILEGES` sets the privileges that objects created later
  by a role start with.
- `GRANT` and `REVOKE` are transactional, like the other statements that
  change the schema.

### What statements need

PostgreSQL manual: [SQL Commands](https://www.postgresql.org/docs/18/sql-commands.html), where the page of each command says what it needs. Source: [`src/backend/executor/execMain.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execMain.c), [`src/backend/commands/sequence.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/sequence.c).

A superuser passes all of these. Every use of an object also needs `USAGE`
on its schema.

**Reading and writing data:**

| Statement                         | Needs                                                                              |
|-----------------------------------|------------------------------------------------------------------------------------|
| `SELECT`                          | `SELECT` on every column it reads, of every table and view it names                |
| `SELECT ... FOR UPDATE`/`SHARE`   | `SELECT`, and `UPDATE` on the locked tables                                        |
| `INSERT`                          | `INSERT` on the columns it sets; `SELECT` on the columns `RETURNING` reads         |
| `INSERT ... ON CONFLICT`          | Also `SELECT` on the columns of the conflict target                                |
| `... DO UPDATE`                   | Also `UPDATE` on the columns it sets, and `SELECT` on the columns its `SET` and `WHERE` read |
| `UPDATE`                          | `UPDATE` on the columns it sets; `SELECT` on the columns that `WHERE`, `SET`, or `RETURNING` read |
| `DELETE`                          | `DELETE`; `SELECT` on the columns that `WHERE` or `RETURNING` read                 |
| `TRUNCATE`                        | `TRUNCATE`                                                                         |
| `COPY table TO`                   | `SELECT` on the columns copied                                                     |
| `COPY table FROM`                 | `INSERT` on the columns copied                                                     |
| `COPY ... FROM` or `TO` a file    | Also a member of `pg_read_server_files` or `pg_write_server_files`                 |
| `nextval()`                       | `USAGE` or `UPDATE` on the sequence                                                |
| `currval()`, `lastval()`          | `USAGE` or `SELECT` on the sequence                                                |
| `setval()`                        | `UPDATE` on the sequence                                                           |
| Calling a function                | `EXECUTE` on the function                                                          |

`UPDATE t SET a = 1` needs no `SELECT`. `UPDATE t SET a = a + 1` and
`UPDATE t SET a = 1 WHERE b = 2` do.

**Changing the schema:**

| Statement                                                     | Needs                                                              |
|---------------------------------------------------------------|--------------------------------------------------------------------|
| `CREATE TABLE`, `VIEW`, `MATERIALIZED VIEW`, `SEQUENCE`, `TYPE`, `DOMAIN` | `CREATE` on the schema; `USAGE` on the types of its columns |
| `CREATE TEMP TABLE`                                           | `TEMPORARY` on the database                                        |
| `CREATE INDEX`                                                | Owner of the table, and `CREATE` on the schema                     |
| `CREATE TRIGGER`                                              | `TRIGGER` on the table                                             |
| A foreign key                                                 | `REFERENCES` on the columns of both tables                         |
| `ALTER` of a relation, type, or sequence                      | Owner                                                              |
| `ALTER ... RENAME TO`                                         | Owner, and `CREATE` on the schema                                  |
| `ALTER ... SET SCHEMA`                                        | Owner, and `CREATE` on the new schema                              |
| `ALTER ... OWNER TO`                                          | See [Ownership](#ownership)                                        |
| `DROP` of a relation, type, or domain                         | Owner of the object, or of its schema                              |
| `DROP TRIGGER`                                                | Owner of the table                                                 |
| `ALTER TABLE ... ENABLE`, `DISABLE`, `FORCE`, `NO FORCE ROW LEVEL SECURITY` | Owner of the table                                   |
| `CREATE POLICY`, `ALTER POLICY`, `DROP POLICY`                | Owner of the table                                                 |
| `CREATE SCHEMA`                                               | `CREATE` on the database                                           |
| `ALTER SCHEMA ... RENAME TO`                                  | Owner of the schema, and `CREATE` on the database                  |
| `DROP SCHEMA`                                                 | Owner of the schema                                                |
| `COMMENT ON`                                                  | Owner of the object                                                |
| `GRANT`, `REVOKE`                                             | See [GRANT and REVOKE](#grant-and-revoke)                          |
| `CREATE ROLE`, `ALTER ROLE`, `DROP ROLE`, `GRANT role`        | See [Roles](#roles)                                                |

A statement that names a relation that does not exist fails with the
"does not exist" error, not with a permission error.

**Maintenance:**

| Statement                                                     | Needs                          |
|---------------------------------------------------------------|--------------------------------|
| `ANALYZE`, `VACUUM`, `REINDEX`, `CLUSTER`, `REFRESH MATERIALIZED VIEW` | `MAINTAIN` on the table |

**Sessions:**

| Statement                                                | Needs                                                |
|----------------------------------------------------------|------------------------------------------------------|
| `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`    | Nothing                                              |
| `SET`, `SHOW`, `RESET`, `SET TRANSACTION`, `DISCARD`     | Nothing, for a setting that every role may set       |
| `SET` of a setting whose name has a dot, such as `app.user_id` | Nothing                                        |
| `SET ROLE`, `SET SESSION AUTHORIZATION`                  | See [Session role and current role](#session-role-and-current-role) |

**The engine.** Core has statements and functions that PostgreSQL does not.
They reach the database file, the process, or the host below the level
where privileges exist, so only a superuser may use them, and no privilege
on them can be granted:

- `PRAGMA`, and the `pragma_*()` table functions;
- `ATTACH`, `DETACH`, `OPTIMIZE`, and `CREATE VIRTUAL TABLE`;
- `load_extension()`, `sqlite_dbpage()`, and `btree_dump()`;
- the engine's own tables (see [Ownership](#ownership));
- writing through a blob handle.

### Views

PostgreSQL manual: [`CREATE VIEW`](https://www.postgresql.org/docs/18/sql-createview.html), [Rules and Privileges](https://www.postgresql.org/docs/18/rules-privileges.html). Source: [`src/backend/rewrite/rewriteHandler.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/rewrite/rewriteHandler.c).

A view has an owner and privileges of its own.

- Reading a view needs `SELECT` on the view, and `USAGE` on the view's
  schema. It needs nothing on the tables the view reads.
- The tables inside the view are checked as the **owner of the view**: the
  owner must hold `SELECT` on them. Their schemas are not checked again,
  because the names resolved when the view was created. A view shows its
  readers what its owner may see. A view inside a view is checked
  as the owner of the view that names it.
- Row-level security of those tables is applied as the owner of the view
  too: the policies that apply to the owner filter the rows, and a view
  whose owner also owns the table, or is a superuser, shows every row.
- Functions in the view, `nextval()` included, are called as the role that
  reads the view. `current_user` in a view, or in a policy that a view
  triggers, is the role that reads the view.
- A view created `WITH (security_invoker = true)` checks its tables, and
  applies their policies, as the role that reads the view.
- A view created `WITH (security_barrier = true)` runs its own `WHERE`
  before any condition of the query that is not `LEAKPROOF`, as a policy
  does (see [Reads](#reads-the-row-filter)).
- Writing through an updatable view needs the write privilege on the view
  for the writer, and on the table for the owner of the view.
- Reading a materialized view needs `SELECT` on it. Its rows were computed
  as its owner when it was last refreshed, and row-level security does not
  apply to reading it.

### Functions, triggers, and foreign keys

PostgreSQL manual: [`CREATE FUNCTION`](https://www.postgresql.org/docs/18/sql-createfunction.html), [`CREATE TRIGGER`](https://www.postgresql.org/docs/18/sql-createtrigger.html). Source: [`src/backend/utils/adt/ri_triggers.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/adt/ri_triggers.c).

- Calling a function needs `EXECUTE`, which `PUBLIC` holds by default. Every
  built-in function is callable by every role, except the engine functions
  listed above.
- A function runs as the role that calls it. A function created `SECURITY
  DEFINER` runs as its owner, and must set its own `search_path` to be safe.
- A trigger runs its function as the role that runs the statement that
  fires it, unless the function is `SECURITY DEFINER`.
- Foreign key checks and actions, such as `ON DELETE CASCADE`, do not depend
  on the role that runs the statement. They run as the owner of the table
  they read or change, and row-level security never applies to them.

### Catalogs

PostgreSQL manual: [System Catalogs](https://www.postgresql.org/docs/18/catalogs.html), [Access Privilege Inquiry Functions](https://www.postgresql.org/docs/18/functions-info.html#FUNCTIONS-INFO-ACCESS). Source: [`src/backend/catalog/system_views.sql`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/system_views.sql).

- Every role may read the catalog tables of `pg_catalog` and the views of
  `information_schema`. No role but a superuser may change them, and
  row-level security never applies to them.
- Secrets are not readable: `pg_authid` with its passwords is readable only
  by a superuser, and `pg_roles` shows `rolpassword` as `********`.
  `pg_sequences.last_value` is NULL for a role that has neither `USAGE` nor
  `SELECT` on the sequence.
- The catalogs show the access control state: `pg_roles` and
  `pg_auth_members`; `relowner`, `relacl`, `relrowsecurity`, and
  `relforcerowsecurity` in `pg_class`; `nspowner` and `nspacl` in
  `pg_namespace`; `pg_policy` and `pg_policies`.
- `has_table_privilege()` and the other privilege inquiry functions,
  `pg_has_role()`, and `row_security_active()` answer by the rules of this
  document.

### What an error may show

Source: [`src/backend/executor/execMain.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execMain.c), [`src/backend/access/index/genam.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/index/genam.c).

A constraint error can show values of a row: `Key (id)=(3) already exists`
for a unique key, or `Failing row contains (...)` for a `CHECK` or `NOT NULL`
constraint. A role must not learn values from an error that it could not read
with a query. The error leaves these details out when:

- the role holds `SELECT` neither on the table nor on every column the
  detail would show, or
- row-level security applies to the role on the table, whatever the row.

## Row-level security

PostgreSQL manual: [Row Security Policies](https://www.postgresql.org/docs/18/ddl-rowsecurity.html).

Privileges decide which tables a role may use. Row-level security decides
which rows of a table the role may use. It is how many end users can share
one table while each sees only their own rows.

### Turning it on

PostgreSQL manual: [`ALTER TABLE`](https://www.postgresql.org/docs/18/sql-altertable.html), [`row_security`](https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-ROW-SECURITY), [`COPY`](https://www.postgresql.org/docs/18/sql-copy.html). Source: [`src/backend/utils/misc/rls.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/misc/rls.c), [`src/backend/commands/copy.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/copy.c).

`ALTER TABLE t ENABLE ROW LEVEL SECURITY` turns it on for table `t`, and
`DISABLE` turns it off. Once it is on, a role reads and writes only the rows
that a policy lets it. **A table with row-level security and no policies
gives no rows to any role it applies to.**

Row-level security does not apply to:

- a superuser, and a role with `BYPASSRLS`, ever;
- the owner of the table, unless the table has `FORCE ROW LEVEL SECURITY`.
  Do not let the roles that end users run as own the tables their policies
  protect.
- `TRUNCATE` and `REFERENCES`, which act on the whole table and need their
  privilege instead;
- foreign key checks and actions, and unique constraints, which see every
  row, also under `FORCE ROW LEVEL SECURITY`;
- views, materialized views, and catalog tables. Only tables have it.

The `row_security` setting is on by default. When a role turns it off, a
statement that policies would filter fails instead (`query would be affected
by row-level security policy for table "t"`, `42501`). Turning it off never
shows more rows. Tools such as `pg_dump` use it to make sure they do not read
a filtered table without noticing.

`COPY table TO` is filtered like a `SELECT`. `COPY table FROM` fails with
`0A000` when row-level security applies to the role on the table.

### Policies

PostgreSQL manual: [`CREATE POLICY`](https://www.postgresql.org/docs/18/sql-createpolicy.html), [`ALTER POLICY`](https://www.postgresql.org/docs/18/sql-alterpolicy.html), [`DROP POLICY`](https://www.postgresql.org/docs/18/sql-droppolicy.html). Source: [`src/backend/commands/policy.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/policy.c).

```sql
CREATE POLICY name ON table
    [ AS { PERMISSIVE | RESTRICTIVE } ]
    [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    [ TO { role | PUBLIC | CURRENT_ROLE | CURRENT_USER | SESSION_USER } [, ...] ]
    [ USING ( condition ) ]
    [ WITH CHECK ( condition ) ];

ALTER POLICY name ON table [ RENAME TO new_name ]
    [ TO roles ] [ USING ( condition ) ] [ WITH CHECK ( condition ) ];

DROP POLICY [ IF EXISTS ] name ON table;
```

A policy has a name, unique among the policies of its table (`42710`
otherwise). It is `PERMISSIVE` unless it says `RESTRICTIVE`, for `ALL`
commands unless it names one, and for `PUBLIC` unless it names roles. The
roles are stored as roles, not as names, so renaming a role keeps its
policies, and a role named in a policy cannot be dropped. `ALTER POLICY`
cannot change the command or whether the policy is restrictive.

A policy has up to two conditions:

- **`USING`**: which existing rows the policy lets the role see, update, or
  delete.
- **`WITH CHECK`**: which new rows the policy lets the role insert, or
  which rows an update may produce. A policy without `WITH CHECK` uses its
  `USING` condition instead.

`WITH CHECK` on a `SELECT` or `DELETE` policy, and `USING` on an `INSERT`
policy, are errors.

The conditions are boolean expressions over the columns of the row. They may
call functions, such as `current_user` and `current_setting()`, and may
contain subqueries that read other tables. They may not contain aggregate
functions, window functions, or functions that return sets.

- A subquery in a policy reads its tables as the role the policy is applied
  for, with that role's privileges and with the row-level security of those
  tables. Policies that read each other's tables in a loop fail with
  `infinite recursion detected in policy for relation "t"` (`42P17`).
- A policy follows its table and its columns when they are renamed. A
  column that a policy uses cannot be dropped without `CASCADE` (`2BP01`).

### How policies combine

PostgreSQL manual: [`CREATE POLICY`](https://www.postgresql.org/docs/18/sql-createpolicy.html), with its table of the policies each command uses. Source: [`src/backend/rewrite/rowsecurity.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/rewrite/rowsecurity.c).

For a command and a role, the policies that apply are those for that command
or for `ALL`, and for `PUBLIC` or for a role that the role has the privileges
of. A row passes when it passes **at least one** permissive policy **and
every** restrictive policy:

```
(permissive_1 OR permissive_2 OR ...) AND restrictive_1 AND restrictive_2 AND ...
```

Without a permissive policy no row passes, whatever the restrictive policies
say. A condition that is NULL fails.

Which policies each command uses. "Reads columns" means that the statement
needs `SELECT` on the table (see [What statements need](#what-statements-need)).

| Command  | Existing rows are filtered by                                              | New rows are checked against                                                    |
|----------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `SELECT` | `SELECT` policies' `USING`                                                 | -                                                                               |
| `SELECT ... FOR UPDATE`/`SHARE` | `SELECT` and `UPDATE` policies' `USING`             | -                                                                               |
| `INSERT` | -                                                                          | `INSERT` policies' `WITH CHECK`; `SELECT` policies' `USING` if it reads columns |
| `UPDATE` | `UPDATE` policies' `USING`; `SELECT` policies' `USING` if it reads columns | `UPDATE` policies' `WITH CHECK`; `SELECT` policies' `USING` if it reads columns |
| `DELETE` | `DELETE` policies' `USING`; `SELECT` policies' `USING` if it reads columns | -                                                                               |

A table that a statement only reads, such as the `FROM` table of an
`UPDATE ... FROM`, uses its `SELECT` policies.

**A row that fails a filter is skipped without an error. A row that fails a
check fails the statement.** The `SELECT` policies are checked on new rows so
that a statement cannot write a row and return it when the role could not
read it, and filter the existing rows so that a role cannot find out about
rows it cannot read from which rows its writes change.

`INSERT ... ON CONFLICT DO UPDATE` checks in three steps. Every failure is an
error, because skipping the row would lose the write without telling the
client:

1. The row to insert is checked like the new row of an `INSERT`, whether or
   not it turns out to conflict.
2. If it conflicts, the existing row is checked against the `UPDATE`
   policies' `USING`, and the `SELECT` policies' `USING` if the statement
   reads columns.
3. The updated row is checked like the new row of an `UPDATE`.

`MERGE` filters the existing rows of its target by the `SELECT` policies'
`USING` if it reads columns.
Then, for each action it takes, it checks the existing row against the
`UPDATE` or `DELETE` policies' `USING` as an error, and the new row like an
`UPDATE` or `INSERT`.

### Reads: the row filter

PostgreSQL manual: [Rules and Privileges](https://www.postgresql.org/docs/18/rules-privileges.html), on `LEAKPROOF` and the order of conditions. Source: [`src/backend/rewrite/rowsecurity.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/rewrite/rowsecurity.c).

The conditions for existing rows act as a filter on the table. The rest of
the statement sees only the rows that pass. In particular:

- **The filter runs first.** No condition or function of the query may be
  evaluated on a row before the filter has passed it, because a function can
  leak its arguments, for example through an error message. The one
  exception is a `LEAKPROOF` function, which only a superuser can mark as
  such; every other expression waits.
- **Every way of reading the table is filtered**: scans, index searches,
  joins, subqueries, CTEs, and `RETURNING`. A plan that answers without
  visiting rows, such as `count(*)` from a row count or `min()` from the
  first index entry, must give the answer the filtered rows give.
- **In an outer join**, a hidden row acts as a row that is not there. A
  `LEFT JOIN` whose only matches are hidden gives the left row with NULLs,
  and a hidden row never comes out as an unmatched row of a `RIGHT` or
  `FULL JOIN`.
- **`UPDATE` and `DELETE` act only on the rows they may see.** `UPDATE t
  SET ...` without `WHERE` changes only the role's rows, and deleting
  another role's row deletes nothing. Neither is an error.
- **Indexes may be used** for a policy's conditions, such as
  `user_id = (SELECT current_setting('app.user_id'))`.

### Writes: the row checks

Source: [`src/backend/executor/nodeModifyTable.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/nodeModifyTable.c), [`src/backend/executor/execMain.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execMain.c).

The conditions for new rows are checked on each row the statement writes:

- after the row's defaults and generated columns are computed and its
  `BEFORE ROW` triggers have run;
- before any of its constraints, `NOT NULL` included, so a row that fails
  both a policy and a constraint is reported as failing the policy;
- for `INSERT`, before looking for a conflict.

The permissive policies are checked together first, and then each
restrictive policy in the order of their names. The first failure ends the
statement with SQLSTATE `42501`:

| Failure                                             | Message                                                                           |
|-----------------------------------------------------|-----------------------------------------------------------------------------------|
| New row fails the permissive policies               | `new row violates row-level security policy for table "t"`                        |
| New row fails restrictive policy `p`                | `new row violates row-level security policy "p" for table "t"`                    |
| Conflicting row of an upsert fails `USING`          | `new row violates row-level security policy (USING expression) for table "t"`     |
| The same, for restrictive policy `p`                | `new row violates row-level security policy "p" (USING expression) for table "t"` |
| Target row of a `MERGE` action fails `USING`        | `target row violates row-level security policy (USING expression) for table "t"`  |

The error for the permissive policies names no policy, because no single
policy was violated.

Unique and foreign key checks see every row of the table, hidden ones
included. A role can learn that a hidden row with a given key exists by
trying to insert the same key. The error does not show the values of the key
or the row, because row-level security applies to the role.

### Example

As `postgres`:

```sql
CREATE ROLE alice;
CREATE ROLE bob;
CREATE TABLE notes (id INT PRIMARY KEY, owner TEXT, body TEXT, archived BOOLEAN DEFAULT false);
GRANT SELECT, INSERT, UPDATE, DELETE ON notes TO alice, bob;
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;

-- Each role sees and writes only its own notes.
CREATE POLICY own_notes ON notes USING (owner = current_user);
-- No role sees archived notes.
CREATE POLICY not_archived ON notes AS RESTRICTIVE USING (NOT archived);
```

As `alice`, after `SET ROLE alice`:

```sql
SELECT * FROM notes;                         -- only alice's notes that are not archived
SELECT count(*) FROM notes;                  -- counts the same rows
UPDATE notes SET body = '';                  -- changes only those rows
DELETE FROM notes WHERE owner = 'bob';       -- deletes nothing
INSERT INTO notes (id, owner, body) VALUES (9, 'bob', 'hi');
-- ERROR:  new row violates row-level security policy for table "notes"
```

`postgres`, the table's owner and a superuser, still sees every row.

### Policies for application users

Many applications connect as one role and know the end user only per
request. The application runs each request in a transaction with
`SET LOCAL ROLE app_user` and a setting that names the user, and writes
policies that read the setting:

```sql
CREATE POLICY own_rows ON items TO app_user
    USING (user_id = (SELECT current_setting('app.user_id')::int));
```

The scalar subquery reads no column of the row, so it is computed once per
statement, and its value can be used to search an index on `user_id`. The
PostgreSQL frontend also has the functions `auth.uid()`, `auth.email()`, and
`auth.jwt()`, which read the claims of a JWT from the `request.jwt.claims`
setting, as Supabase does.

Any role may set a setting whose name has a dot in it. A policy that reads a
setting therefore trusts whoever may run `SET`. Only the host program, which
has checked who the user is, may set it, and the client must not be able to
run SQL of its own on that connection.

## When the rules apply

Source: [`src/backend/utils/cache/plancache.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/cache/plancache.c).

- **Each execution is checked against the role that is current when it
  starts.** A prepared statement that is run again after `SET ROLE`, or
  after its search path changed, is checked again and gets the policies of
  the new role. It never runs with the checks or the policies of another
  role.
- **A statement that is already running keeps its role.** A cursor that was
  opened before `SET ROLE` goes on returning the rows it was allowed.
- **Functions that take the name of an object when they run**, such as
  `currval('s')`, check the role that is current at that time.
- **Changes are transactional.** `CREATE ROLE`, `GRANT`, `REVOKE`,
  `ALTER ... OWNER TO`, `ENABLE ROW LEVEL SECURITY`, `CREATE POLICY`, and the
  like take effect for other connections when they commit, and are undone by
  a rollback. A statement that starts after the commit is checked by the new
  rules, on every connection and in every process that has the database
  open.
- **Only the client's statements are checked.** The statements that the
  engine runs for itself, such as the reads that load the schema and the
  writes to its own tables, are not subject to roles. They never run SQL
  that the client wrote, and never return rows to the client that it could
  not have read itself.

## Errors

PostgreSQL manual: [PostgreSQL Error Codes](https://www.postgresql.org/docs/18/errcodes-appendix.html). Source: [`src/backend/catalog/aclchk.c`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/aclchk.c).

A denial has SQLSTATE `42501` (`insufficient_privilege`) and PostgreSQL's
message:

| Denial                                    | Message                                                         |
|-------------------------------------------|-----------------------------------------------------------------|
| Missing privilege on a relation           | `permission denied for table t` (or `view`, `sequence`, `materialized view`) |
| Missing `USAGE` or `CREATE` on a schema   | `permission denied for schema s`                                |
| Missing `EXECUTE` on a function           | `permission denied for function f`                              |
| Needs the owner                           | `must be owner of table t` (or the object's kind)               |
| Needs to be able to switch to a role      | `must be able to SET ROLE "r"`                                  |
| `CREATE ROLE`                             | `permission denied to create role`                              |
| `SET ROLE`                                | `permission denied to set role "r"`                             |
| A setting that needs a superuser          | `permission denied to set parameter "p"`                        |
| `COPY` with a file                        | `permission denied to COPY from a file` (or `to a file`)        |
| Changing a catalog table                  | `permission denied: "t" is a system catalog`                    |
| Row-level security, `row_security` off    | `query would be affected by row-level security policy for table "t"` |
| A row fails a policy                      | See [Writes](#writes-the-row-checks)                            |

The engine's own statements report `permission denied to` followed by what
was tried, such as `permission denied to attach a database`.

Errors that are not denials keep their own SQLSTATEs:

| SQLSTATE | When                                                                  |
|----------|-----------------------------------------------------------------------|
| `42704`  | A role or policy that does not exist                                  |
| `42710`  | A role or policy that already exists                                  |
| `42939`  | A reserved name                                                       |
| `0LP01`  | A privilege the object cannot have                                    |
| `2BP01`  | Dropping or revoking something that other objects or grants depend on |
| `3F000`  | No schema to create in                                                |
| `0A000`  | A statement that Turso does not support                               |

## Architecture

Core enforces the model, and the frontend stores it.

- **Core** resolves names, checks privileges, and compiles the row filters
  and row checks of row-level security into each statement. It knows a role
  only by an id. It reports a denial as a value, `PermissionDenied` in
  `core/security/`, that holds the action, the object, and the reason, and
  never words the error.
- **The frontend** stores the roles, memberships, owners, ACLs, and
  policies, in tables of the database file, and answers core's questions
  about them through the `Dialect` trait in `core/dialect/mod.rs`. It keeps
  an in-memory copy that is loaded with the schema, so a check never reads
  the database. It words each denial as its users expect.
- **Stored state follows its object.** The owner, ACL, row-level security
  flag, and policies of a table are written in the same transaction as the
  table, stay with it when it is renamed, and are removed when it is
  dropped. A database whose access control state cannot be read fails to
  open; it never opens without it.

### The SQLite frontend

SQLite has no roles, and neither does the SQLite frontend. Every connection
runs as the superuser, there are no statements for roles, privileges, or
policies, and no check ever denies anything. A SQLite database behaves
exactly as it would without this model.

### The PostgreSQL frontend

The PostgreSQL frontend implements everything in this document. Its code is
in `postgres/frontend/`, and its conformance tests in
`postgres/conformance/pg-sqltests/` (`roles/`, `privileges/`, and
`row-security/`) run every case against both Turso and a real PostgreSQL
server, which must agree.

## References

- [PostgreSQL 18 manual](https://www.postgresql.org/docs/18/).
- [PostgreSQL 18 source code](https://github.com/postgres/postgres/tree/REL_18_STABLE).
