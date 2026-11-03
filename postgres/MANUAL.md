# Turso Postgres Manual

## The SQL language

### `ALTER TABLE ... ROW LEVEL SECURITY` — hide rows from roles

**Synopsis:**

```sql
ALTER TABLE name ENABLE ROW LEVEL SECURITY
ALTER TABLE name DISABLE ROW LEVEL SECURITY
```

**Description:**

`ENABLE ROW LEVEL SECURITY` makes the table show a role that is not a
superuser only the rows that one of the table's policies shows, as created
with `CREATE POLICY`. Without policies, such a role sees no rows. The
superuser always sees every row.

`DISABLE ROW LEVEL SECURITY` shows every row to every role again. The table
keeps its policies.

**Parameters:**

- `name`: The name of a table in the database, optionally schema-qualified.

**Notes:**

Only a superuser can change a table's row-level security.

Rows are hidden in every query that reads the table, including joins,
subqueries, CTEs and views. Views are checked as the role running the query,
not as the view's owner. On the right side of a `LEFT JOIN`, a hidden row
reads as NULLs like a row that does not exist. `FULL JOIN` with such a table
is not supported.

A role cannot insert, update or delete rows of a table with row-level
security yet. Renaming such a table is not supported. `DROP TABLE` removes
the table's row-level security and policies.

**Examples:**

```sql
CREATE TABLE documents (id int PRIMARY KEY, owner text, title text);
INSERT INTO documents VALUES (1, 'alice', 'Plan'), (2, 'bob', 'Budget');
CREATE ROLE alice;
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
SET ROLE alice;
SELECT count(*) FROM documents;
```

```
 count
-------
     0
(1 row)
```

### `CREATE POLICY` — show rows of a table to roles

**Synopsis:**

```sql
CREATE POLICY name ON table_name
    [ AS PERMISSIVE ]
    [ FOR { ALL | SELECT } ]
    [ TO { role_name | PUBLIC } [, ...] ]
    USING ( condition )
```

**Description:**

`CREATE POLICY` adds a policy to a table with row-level security. A role that
is not a superuser sees the rows that any of the table's policies for that
role show.
Policies have no effect while the table's row-level security is disabled.

**Parameters:**

- `name`: The name of the policy, unique among the table's policies.
- `table_name`: The name of the table, optionally schema-qualified.
- `role_name`: A role the policy applies to. `PUBLIC`, the default, means
  every role.
- `condition`: Which rows the policy shows: the rows for which the condition
  is true. It can use the columns of the table, written as `column` or
  `table_name.column`, and call functions such as `current_user` or
  `auth.uid()`.

**Notes:**

Only a superuser can create policies.

The condition cannot contain subqueries, except `(SELECT expression)`
without `FROM`, which is the same as `(expression)`. Supabase recommends that
form, as in `(SELECT auth.uid())`. It cannot use generated columns that are
not stored.

The condition is checked before the query's own conditions, so the query never
evaluates its expressions on a row the role cannot see. An index on a column
the condition compares can be used to find the rows.

A role named in `TO` must have been created with `CREATE ROLE`. A policy
applies only to the roles it names, not to other roles that are members of
them.

`AS RESTRICTIVE`, `FOR INSERT`, `FOR UPDATE`, `FOR DELETE` and `WITH CHECK`
are not supported. The columns a policy uses cannot be renamed,
dropped or altered. Policies are not listed in the `pg_policy` catalog.

**Examples:**

Show each role its own documents:

```sql
CREATE POLICY own_documents ON documents
    FOR SELECT
    USING (owner = (SELECT current_user));
SET ROLE alice;
SELECT id, title FROM documents;
```

```
 id | title
----+-------
  1 | Plan
(1 row)
```

Show each Supabase user their own rows. The user comes from the request's JWT
claims:

```sql
CREATE ROLE authenticated;
CREATE TABLE todos (id int PRIMARY KEY, user_id uuid, title text);
ALTER TABLE todos ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_todos ON todos
    FOR SELECT TO authenticated
    USING ((SELECT auth.uid()) = user_id);
BEGIN;
SET LOCAL ROLE authenticated;
SELECT set_config('request.jwt.claims', '{"sub": "11111111-1111-1111-1111-111111111111"}', true);
SELECT id, title FROM todos;
COMMIT;
```

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

### `DROP POLICY` — remove a policy

**Synopsis:**

```sql
DROP POLICY [ IF EXISTS ] name ON table_name
```

**Description:**

`DROP POLICY` removes a policy from a table. If it was the table's last
policy and the table has row-level security, a role that is not a superuser
then sees no rows of the table.

**Parameters:**

- `IF EXISTS`: Do nothing instead of failing if the policy does not exist.
- `name`: The name of the policy.
- `table_name`: The name of the table, optionally schema-qualified.

**Notes:**

Only a superuser can drop policies.

**Examples:**

```sql
DROP POLICY own_documents ON documents;
```

### `GRANT` and `REVOKE` — accepted but ignored

**Synopsis:**

```sql
GRANT privileges ON object TO role
REVOKE privileges ON object FROM role
```

**Description:**

`GRANT` and `REVOKE` of privileges on tables and other objects are accepted
and do nothing, so that scripts written for PostgreSQL run unchanged.
Privileges are not checked yet: a role that is not a superuser may read and
write the rows of every table that row-level security does not hide.

**Notes:**

Granting a role to another role is not supported.

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

Ownership and table privileges are not supported yet, and `GRANT` is ignored.
A role that is not a superuser may read the rows of every table, except the
rows that row-level security hides, but it cannot insert, update, delete or
truncate rows, call `nextval` or `setval`, create, alter or drop objects,
create roles, or `COPY FROM` a file.

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
