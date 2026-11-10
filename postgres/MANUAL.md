# Turso Postgres Manual

## The SQL language

### `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` — turn on row-level security

**Synopsis:**

```sql
ALTER TABLE name ENABLE ROW LEVEL SECURITY
ALTER TABLE name DISABLE ROW LEVEL SECURITY
```

**Description:**

`ENABLE ROW LEVEL SECURITY` turns on row-level security for a table, and
`DISABLE ROW LEVEL SECURITY` turns it off. The `relrowsecurity` column of
`pg_class` shows which tables have it.

A table with row-level security shows a role that is neither a superuser nor
the table's owner only the rows that the table's policies allow it, which
`CREATE POLICY` defines. Without policies, the role sees no rows, as in
PostgreSQL. The policies apply wherever the table appears in a query: in
joins, subqueries, common table expressions, views and `COPY ... TO STDOUT`.
In a `LEFT JOIN`, the columns of a row the role may not see read as NULL.
Superusers and the table's owner see every row.

**Parameters:**

- `name`: The name of an existing table, optionally qualified with its schema.

**Notes:**

A view reads its tables as the view's owner, as in PostgreSQL, so it shows its
readers the rows its owner sees. A view that a superuser or the table's owner
owns shows every row; a view that another role owns shows the rows the
policies allow that role. Unlike
PostgreSQL, a superuser reading a view sees every row, whoever owns the
view.

Virtual tables that show the database file itself can only be read by a
superuser.

`SET ROLE` applies to the statements that start after it. A statement that
is already running, such as a query a superuser started and is still reading
rows from, keeps the role it started with until it finishes.

Enabling row-level security for a table that already has it, or disabling it
for a table that does not, does nothing. Renaming a table keeps its row-level
security, and a new table created with the name of a dropped table starts
without it.

Only the owner of a table or a superuser may enable or disable its row-level
security.

`FORCE ROW LEVEL SECURITY` and `NO FORCE ROW LEVEL SECURITY` are not
supported.

**Examples:**

```sql
CREATE TABLE accounts (id int, owner text);
ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
SELECT relname, relrowsecurity FROM pg_class WHERE relname = 'accounts';
```

```
 relname  | relrowsecurity
----------+----------------
 accounts | t
(1 row)
```

### `CREATE POLICY` — define a row-level security policy for a table

**Synopsis:**

```sql
CREATE POLICY name ON table_name
    [ AS { PERMISSIVE | RESTRICTIVE } ]
    [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    [ TO { role_name | PUBLIC | CURRENT_ROLE | CURRENT_USER | SESSION_USER } [, ...] ]
    [ USING ( using_expression ) ]
    [ WITH CHECK ( check_expression ) ]
```

**Description:**

`CREATE POLICY` defines which rows of a table the roles it names may read.
It matters once `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` is on for the
table.

A role reads the rows for which the `USING` expression of at least one
permissive policy is true and the `USING` expressions of all restrictive
policies are true, counting only the policies `FOR ALL` and `FOR SELECT` that
name the role or `PUBLIC`. Without a permissive policy, it reads no rows.

The policies are checked on each row before the other conditions of a query,
so a condition of the query, or a function it calls, never sees a row the
role may not read.

**Parameters:**

- `name`: The name of the policy, unique among the policies of the table.
- `table_name`: The name of an existing table, optionally qualified with its
  schema. Views and materialized views cannot have policies.
- `PERMISSIVE`, `RESTRICTIVE`: How the policy combines with the others, as
  described above. The default is `PERMISSIVE`.
- `FOR`: The command the policy applies to. The default is `ALL`.
- `TO`: The roles the policy applies to. The default is `PUBLIC`, every role.
  `CURRENT_ROLE`, `CURRENT_USER` and `SESSION_USER` name the role at the time
  the policy is created.
- `using_expression`: Which existing rows the policy shows. It can use the
  columns of the table, written alone, with the table's name, or with the
  schema and table names, and call scalar functions such as `current_user`.
  It can also use a subquery without `FROM`, such as `(SELECT auth.uid())`.
- `check_expression`: Which new rows the policy lets a role write. It is
  stored but has no effect yet, because roles cannot write to tables.

**Notes:**

Only the owner of the table or a superuser may create its policies.

Roles that are not superusers cannot write to tables yet, so the policies
`FOR INSERT`, `FOR UPDATE` and `FOR DELETE` and the `WITH CHECK` expressions
are stored but do not change what any role can do.

Policies cannot use subqueries with `FROM`, parameters, aggregate or window
functions, or generated columns that are computed when they are read. A
column that a policy uses cannot be renamed, dropped or changed, unlike in
PostgreSQL, which renames it in the policy too. Renaming the table keeps its
policies, and dropping the table drops them.

A policy whose expression is nested too deeply, such as a hundred conditions
joined with `OR`, is rejected; write it with `IN` instead.

As in PostgreSQL, a function that a policy calls, such as `auth.uid()`,
runs for each row that the policy checks, and a subquery such as
`(SELECT auth.uid())` that reads no column of the row runs once per query,
so writing the call as a subquery is cheaper on large tables.

`pg_policy` and `pg_policies` do not show the policies yet.

Materialized views store the rows of their query when they are refreshed, so
a role that reads a materialized view built on a table with row-level
security sees the rows that were stored, as in PostgreSQL.

**Examples:**

```sql
CREATE TABLE docs (id int, owner text, title text);
ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_docs ON docs FOR SELECT USING (owner = (SELECT current_user));
GRANT SELECT ON docs TO alice;
```

### `DROP POLICY` — remove a row-level security policy from a table

**Synopsis:**

```sql
DROP POLICY [ IF EXISTS ] name ON table_name
```

**Description:**

`DROP POLICY` removes the policy `name` from the table. With `IF EXISTS`, a
policy or table that does not exist is not an error.

Only the owner of the table or a superuser may drop its policies.

`CASCADE` and `RESTRICT` are not supported.

**Examples:**

```sql
DROP POLICY own_docs ON docs;
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

### `GRANT` — give roles privileges on tables

**Synopsis:**

```sql
GRANT { { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | REFERENCES | TRIGGER | MAINTAIN }
        [, ...] | ALL [ PRIVILEGES ] }
    ON { [ TABLE ] table_name [, ...] | ALL TABLES IN SCHEMA schema_name [, ...] }
    TO role_specification [, ...]

GRANT { { USAGE | SELECT | UPDATE } [, ...] | ALL [ PRIVILEGES ] }
    ON { SEQUENCE sequence_name [, ...] | ALL SEQUENCES IN SCHEMA schema_name [, ...] }
    TO role_specification [, ...]

where role_specification is:
    role_name | PUBLIC | CURRENT_ROLE | CURRENT_USER | SESSION_USER
```

**Description:**

`GRANT` gives roles privileges on tables and sequences. `GRANT SELECT`
lets roles read tables. A role that is not a superuser reads
a table, view or materialized view only when it owns it or `SELECT` on it
was granted to it or to `PUBLIC`; otherwise reading it, directly or through
a join, subquery or CTE, fails with `permission denied for table` (SQLSTATE
42501). As in PostgreSQL, a view's tables are read as the view's owner, so
`SELECT` on a view is enough when its owner may read its tables. The
engine's own tables, which have no row in `pg_class`, can only be read and
written by a superuser: the `__turso_internal_*` tables, which hold catalogs
such as the roles and the privileges of each table, and `sqlite_sequence`.
Roles read the catalogs through `pg_class`, `pg_roles` and the other
`pg_catalog` views.

The privileges are stored in the database file, in the table's access
control list, which `pg_class.relacl` shows in PostgreSQL's form: after
`GRANT SELECT ON accounts TO reader` by the owner `postgres`, it reads
`{postgres=arwdDxtm/postgres,reader=r/postgres}`. The list is `NULL` until
the first `GRANT`, when the owner has every privilege and nobody else has
any, as in PostgreSQL. `pg_class` does not list views yet, so a view's list
is stored but not shown.

**Parameters:**

- `table_name`: The name of an existing table, view, materialized view or
  sequence, optionally qualified with its schema. Catalog tables such as
  `pg_catalog.pg_class` can be named too.
- `sequence_name`: The name of an existing sequence, optionally qualified
  with its schema. A relation that is not a sequence fails with `is not a
  sequence` (SQLSTATE 42809).
- `schema_name`: The name of an existing schema. `ALL TABLES IN SCHEMA` names
  every table in it, and `ALL SEQUENCES IN SCHEMA` every sequence.
- `ALL [ PRIVILEGES ]`: Every privilege of the relation's kind: `arwdDxtm`
  for a table, and `rwU` (`SELECT`, `UPDATE`, `USAGE`) for a sequence. As in
  PostgreSQL, a privilege that a sequence does not have, such as `INSERT`, is
  left out when it is granted on one.
- `role_name`: The name of an existing role.

**Notes:**

`GRANT` fails if a table, schema or role does not exist, if a name is an
index, or if a name refers to another database, with the same errors as
PostgreSQL. The tables are checked before the roles. A privilege that tables
do not have, `USAGE`, fails with `invalid privilege type` (SQLSTATE 0LP01)
on a table, though `GRANT USAGE ON s`, without `SEQUENCE`, works for a
sequence `s`, as in PostgreSQL. A privilege that sequences do not have, such
as `INSERT`, fails the same way with `SEQUENCE`.

As in PostgreSQL, the table's owner may grant, and a superuser grants as the
owner, so the owner is the grantor in the list either way. Any other role
gets `permission denied for table` (SQLSTATE 42501), unless the list gives
it a privilege on the table, when the statement succeeds and stores
nothing, as in PostgreSQL, which warns. `ALL TABLES IN SCHEMA` grants
on the tables, views and materialized views of the schema, not on its
sequences, and `ALL SEQUENCES IN SCHEMA` only on its sequences. A catalog view such as `pg_catalog.pg_class` can be named, but
nothing is stored for it, since every role reads it already. `GRANT` on one
of the engine's own tables, such as `pg_catalog.__turso_internal_roles`,
fails as not supported (SQLSTATE 0A000). `ALTER ... OWNER TO` puts the new owner in the
new owner's place in the list, and a rename keeps it.

`SELECT`, `INSERT`, `UPDATE` and `DELETE` let a role read and write the
table, as in PostgreSQL. A statement that reads the table's columns also
needs `SELECT`: an `UPDATE` or `DELETE` with a `WHERE` that reads them or a
`SET` that reads them, `RETURNING` that returns them, and `INSERT ... ON
CONFLICT` with a conflict target or `DO UPDATE`. `INSERT ... ON CONFLICT DO
UPDATE` also needs `UPDATE`. A subquery in these places needs `SELECT` only
when it reads a column of the table that is written, as in PostgreSQL.
Foreign-key checks and
actions need no privileges on the other table. `TRUNCATE` needs the
`TRUNCATE` privilege, not `DELETE`, as in PostgreSQL. The `REFERENCES`,
`TRIGGER` and `MAINTAIN` privileges are stored but let a role do nothing
yet. On a sequence, `nextval` needs `USAGE`
or `UPDATE`, `setval` needs `UPDATE`, and `currval` needs `SELECT` or
`USAGE`, as in PostgreSQL, so a role that inserts into a table with a
`serial` column needs `USAGE` on the column's sequence too. A `serial
PRIMARY KEY` column is the exception: its value comes from the row's rowid,
not from the sequence, so the insert needs no privilege on the sequence.
`pg_sequences.last_value` is null for a role with neither `SELECT` nor
`USAGE` on the sequence. A table with row-level security still
refuses writes by a role that does not own it: `INSERT` fails, and
`UPDATE` and `DELETE` change no rows. As in PostgreSQL, row-level security
does not apply to `TRUNCATE`, which removes every row. A constraint error
shows the values of the row or key (`Failing row contains ...`, `Key ...
already exists`) only to a role with `SELECT` on the table; PostgreSQL also
shows the values that the statement itself gave. Column
privileges, `WITH GRANT OPTION`, `GRANTED BY`, `GRANT` on other kinds of
objects, and `REVOKE` fail as not supported, so that a script never believes
it changed what a role may do when it did not. `GRANT role_name TO
role_name` is not supported either.

**Examples:**

```sql
CREATE ROLE reader;
CREATE TABLE accounts (id int);
GRANT SELECT ON accounts TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO PUBLIC;
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

Ownership is supported (see `ALTER TABLE ... OWNER TO`), and `GRANT` on
tables (see `GRANT`). A role that is not a superuser may read and write the
tables it owns or was granted privileges on, and it may alter and drop the
relations it owns, except that renaming a relation or creating an index on
it also needs CREATE on the schema, which only a superuser has; it may call
`nextval` and `setval` on the sequences it owns or was granted privileges
on; it cannot create objects or roles, or `COPY FROM` a file.
Creating objects stays superuser-only because PostgreSQL gates it on the
schema's CREATE privilege, which does not exist yet. Catalog tables and
sequences read as tables are not checked.

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
