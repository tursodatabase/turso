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

`SET ROLE` makes `role_name` the current role of the session. Privileges are
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

A role that is not a superuser holds only the privileges granted to it or to
`PUBLIC` with `GRANT`. By default it can run statements that use no database
objects, such as `SELECT 1`, read the `pg_catalog` tables, execute functions,
change settings with `SET` and show them with `SHOW`, and switch roles. A role
owns the objects it creates and may read, change, alter, drop and grant them;
creating an object needs `CREATE` on its schema.


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
SELECT * FROM accounts;
```

```
ERROR:  permission denied for table accounts
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

### `GRANT` — give privileges on an object

**Synopsis:**

```sql
GRANT { privilege [, ...] | ALL [ PRIVILEGES ] }
    ON { [ TABLE ] name [, ...] | SEQUENCE name [, ...] | FUNCTION name [, ...]
         | SCHEMA name [, ...] | DATABASE name | TYPE name [, ...] | PARAMETER name [, ...] }
    TO { role_name | PUBLIC } [, ...] [ WITH GRANT OPTION ]

REVOKE [ GRANT OPTION FOR ] { privilege [, ...] | ALL [ PRIVILEGES ] }
    ON { same as GRANT }
    FROM { role_name | PUBLIC } [, ...]
```

**Description:**

`GRANT` gives privileges on an object to roles, and `REVOKE` takes them back.
The grantor is the current role, which must own the object or hold the grant
option for every privilege named. A superuser grants as the object's owner.
`PUBLIC` stands for every role.

The privileges an object can have depend on its kind: `SELECT`, `INSERT`,
`UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES` and `TRIGGER` on a table or
view; `USAGE`, `SELECT` and `UPDATE` on a sequence; `EXECUTE` on a function;
`USAGE` and `CREATE` on a schema; `CREATE` and `TEMPORARY` on the database;
`USAGE` on a type; `SET` on a parameter.

**Notes:**

Column privileges, `ALL TABLES IN SCHEMA`, `GRANTED BY` and default
privileges are not supported. `GRANT` of a role to another role is not
supported.

Every role may execute every function and read every `pg_catalog` table by
default. `REVOKE EXECUTE ON FUNCTION name FROM PUBLIC` and `REVOKE SELECT ON
pg_class FROM PUBLIC` take that away.

**Examples:**

```sql
CREATE TABLE accounts (id int);
CREATE ROLE jonathan;
GRANT SELECT ON accounts TO jonathan;
SET ROLE jonathan;
SELECT * FROM accounts;
```

```
 id
----
(0 rows)
```

```sql
INSERT INTO accounts VALUES (1);
```

```
ERROR:  permission denied for table accounts
```
