# Turso Postgres Manual

## The SQL language

### `CREATE ROLE` — create a role

**Synopsis:**

```sql
CREATE ROLE name
```

**Description:**

`CREATE ROLE` adds a new role to the database. A new role is not a superuser
and cannot log in. Roles are listed in the `pg_roles` catalog.

**Parameters:**

- `name`: The name of the new role. Unquoted names are converted to lowercase,
  and quoted names keep their case. Names that start with `pg_` are reserved
  and cannot be used.

**Notes:**

The database always has one superuser role named `postgres`.

`CREATE ROLE` works inside a transaction. If the transaction is rolled back,
the role is not created.

Options such as `LOGIN`, `PASSWORD`, or `SUPERUSER` are not supported yet.

**Examples:**

Create a role:

```sql
CREATE ROLE jonathan;
```

List roles:

```sql
SELECT rolname FROM pg_roles;
```

```
 rolname
----------
 postgres
 jonathan
(2 rows)
```

### `SET ROLE` — set the current role

**Synopsis:**

```sql
SET [ SESSION | LOCAL ] ROLE role_name
SET [ SESSION | LOCAL ] ROLE NONE
RESET ROLE
```

**Description:**

`SET ROLE` makes `role_name` the current role of the session. Statements are
then checked against that role. `current_user` and `current_role` return it,
and `session_user` keeps returning the role that the session logged in as.

`SET ROLE NONE` and `RESET ROLE` make the session's login role the current
role again.

The current role is the `role` setting, so it follows the same rules as other
settings. `SET LOCAL ROLE` lasts until the end of the transaction. A role set
in a transaction that rolls back, or after a savepoint that is rolled back to,
is undone. `SHOW role` and `current_setting('role')` return the role, or
`none` when no role was set, and `set_config('role', ...)` sets it. `RESET ALL`
keeps the role, and `DISCARD ALL` resets it.

**Parameters:**

- `role_name`: The name of an existing role. A superuser session may switch to
  any role, including roles that cannot log in. Other sessions may only switch
  to their own login role.

**Notes:**

`GRANT` and table privileges are not supported yet. A role that is not a
superuser may read the rows of every table, and it may alter and drop the
relations it owns (see `ALTER TABLE ... OWNER TO`), except that renaming a
relation or creating an index on it also needs CREATE on the schema, which
only a superuser has; it cannot insert,
update, delete or truncate rows, call `nextval` or `setval`, create objects
or roles, or `COPY FROM` a file. Creating objects stays superuser-only
because PostgreSQL gates it on the schema's CREATE privilege, which does not
exist yet.

**Examples:**

Switch to a role and back:

```sql
CREATE TABLE accounts (id int);
CREATE ROLE jonathan;
SET ROLE jonathan;
SELECT current_user, session_user;
```

```
 current_user | session_user
--------------+--------------
 jonathan     | postgres
(1 row)
```

```sql
DROP TABLE accounts;
```

```
ERROR:  must be owner of table accounts
```

```sql
RESET ROLE;
SELECT current_user;
```

```
 current_user
--------------
 postgres
(1 row)
```

### `ALTER TABLE ... OWNER TO` — change a relation's owner

**Synopsis:**

```sql
ALTER TABLE [ IF EXISTS ] name OWNER TO { role_name | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
ALTER VIEW name OWNER TO role_name
ALTER MATERIALIZED VIEW name OWNER TO role_name
ALTER SEQUENCE name OWNER TO role_name
ALTER INDEX name OWNER TO role_name
```

**Description:**

`OWNER TO` makes `role_name` the owner of the relation. The owner may alter
and drop the relation; every other role that is not a superuser gets
`must be owner of table name` (SQLSTATE `42501`). `pg_class.relowner` stores
the owner, and `pg_get_userbyid(relowner)` returns its name.

A table's indexes and the sequences of its `serial` columns belong to the
table, also after the table or the column is renamed, so they change owner
with it and not on their own. `ALTER SEQUENCE name OWNED BY table.column`
makes any sequence belong to a table the same way, and `OWNED BY NONE` makes
it its own again. Dropping a table leaves the sequences it owned as their
own; PostgreSQL drops them with the table. Otherwise: `ALTER INDEX ...
OWNER TO` changes nothing, as in PostgreSQL, though without the WARNING
PostgreSQL prints, and `ALTER SEQUENCE ... OWNER TO` on a `serial` column's
sequence fails with `cannot change owner of sequence` (SQLSTATE `0A000`)
unless it names the owner the sequence has, which changes nothing.
`ALTER TABLE` accepts every relation kind; the other forms refuse a relation
of another kind, for example `"t" is not a sequence` (SQLSTATE `42809`).

**Parameters:**

- `name`: An existing table, view, materialized view, sequence or index. A
  missing relation is an error (`42P01`), unless `IF EXISTS` was given.
- `role_name`: An existing role (`42704` when there is none), or
  `CURRENT_ROLE`/`CURRENT_USER` for the role the session runs as now, or
  `SESSION_USER` for the role the session logged in as.

**Notes:**

A superuser may give a relation to any role. The relation's owner may give
it to itself only, which changes nothing; PostgreSQL also lets the owner give
it to a role it may `SET ROLE` to, which takes role membership, which Turso
does not support yet.

**Examples:**

```sql
CREATE TABLE accounts (id int);
CREATE ROLE jonathan;
ALTER TABLE accounts OWNER TO jonathan;
SET ROLE jonathan;
DROP TABLE accounts;
```
