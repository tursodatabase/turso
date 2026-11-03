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

Ownership, `GRANT` and table privileges are not supported yet. A role that is
not a superuser may read the rows of every table, but it cannot insert,
update, delete or truncate rows, call `nextval` or `setval`, create, alter or
drop objects, create roles, or `COPY FROM` a file.

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
