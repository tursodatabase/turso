# Shared SQL tests

The `.sqltest` files in this directory run on every sqltest backend, through
both the SQLite and the PostgreSQL frontends. Put a test here when the same
SQL must work through both frontends. Put a test that only one frontend runs
in `sqlite/conformance/sqlite-sqltests/`, `sqlite/conformance/turso-sqltests/`
or `postgres/conformance/pg-sqltests/`.

## Where the tests run

The targets of the two conformance Makefiles that run all tests also run this
directory:

| Target | Backend | Frontend |
|--------|---------|----------|
| `make -C sqlite/conformance run-rust` | `rust` | SQLite |
| `make -C sqlite/conformance run-cli` | `cli` with `tursodb` | SQLite |
| `make -C sqlite/conformance run-js` | `js` | SQLite |
| `make -C sqlite/conformance test-sqlite` | `cli` with `sqlite3` | SQLite itself |
| `make -C postgres/conformance run` | `tursopg` | PostgreSQL |
| `make -C postgres/conformance run-postgres` | `postgres` | PostgreSQL itself |

The `check` targets of the two Makefiles also check the syntax of these files.

## Writing a shared test

- Use `@database :memory:` or `@database :temp:`. The `postgres` backend can
  create only these databases.
- Write SQL that both dialects accept. For example, give each column a type
  and write `ON true`.
- If SQLite does not support a feature, skip the file or the test on SQLite
  with `@skip-file-if sqlite "reason"` or `@skip-if sqlite "reason"`.

## Results that differ between backends

The default `expect` block applies to each backend that has no block of its
own. If a backend gives a different result, add an `expect @<backend>` block
for that backend. Error tests often need these blocks: the two frontends give
different messages, and only the `tursopg` and `postgres` backends report a
SQLSTATE. For this reason, the default block of an error test cannot have
`@sqlstate`.

```
@setup data
test lateral-cannot-read-a-later-lateral {
    SELECT * FROM t1, LATERAL (SELECT u.v AS w) AS s, LATERAL (SELECT 1 AS v) AS u;
}
expect error {
    no such table: u
}
@sqlstate 42P01
expect @tursopg error {
    no such table: u
}
@sqlstate 42P01
expect @postgres error {
    missing FROM-clause entry for table "u"
}
```
