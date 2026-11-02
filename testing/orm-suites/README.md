# ORM test suites

Runs popular ORMs' and query builders' **own** test suites against Turso and
compares them with the same suite on SQLite. The score is:

    tests that pass on both / tests that pass on SQLite

Tests that fail on SQLite too (or need another database) don't count.

```bash
testing/orm-suites/run.sh go/gorm            # run one suite, score it
testing/orm-suites/run.sh go/gorm --update   # also rewrite known-failures.txt
```

`known-failures.txt` lists the tests that pass on SQLite and fail on Turso in
CI (Linux). `run.sh` exits 1 when a test fails that is not listed, and also
when a listed test now passes on both: remove it (or rerun with `--update`) so
that a later regression of the fixed test is caught. Listed tests that SQLite
itself doesn't pass on your machine (another platform or SQLite version) are
not judged, and `--update` keeps them. The scorer groups failures by error
message, so each group is usually one bug.

The [ORM test suites](../../.github/workflows/orm-suites.yml) workflow fails
a job when a test fails on Turso that passes on SQLite and is not a known
failure, or when a known failure now passes. It runs every suite nightly, on
demand, and on pull requests that change this harness. Each job's summary
shows the score. When a fix makes known failures pass, the nightly run fails
until the shorter `known-failures.txt` is committed (`run.sh <suite> --update`).

## Suites

| Suite | Driver swap | Needs |
|---|---|---|
| `python/sqlalchemy` | `sqlite://` vs `sqlite+turso://` (pyturso's dialect) | uv |
| `python/django` | backend in `django/turso_backend`, `sqlite3` replaced by `shim/turso_sqlite3.py` | uv |
| `python/peewee` | `SqliteDatabase._connect` patched to use the shim | uv |
| `go/gorm`, `go/sqlx`, `go/bun` | `database/sql` driver name `turso` | Go |
| `js/kysely`, `js/knex`, `js/drizzle`, `js/typeorm` | `node_modules/better-sqlite3` symlinked to `js/shim/better-sqlite3` | Node (version per suite, `fnm` if installed), yarn |

Each `run.sh` clones the suite at a pinned tag into `<ecosystem>/.cache/`,
builds the Turso binding from this checkout, runs the suite on both drivers,
and writes `results/<ecosystem>-<suite>/{sqlite,turso}.xml` (JUnit).

The shims add only what is needed to open a connection. Where Turso lacks a
driver API, the shim stubs it so the tests that need it fail one by one
instead of every connection failing. ATTACH is enabled because some suites'
test setup needs it; failures caused by experimental features are not bugs to
file.

## Adding a suite

1. Copy the closest `<ecosystem>/<suite>/run.sh`. Pin a tag, swap the driver
   with the smallest change possible, and write the two JUnit files.
2. Check that both runs report the same number of tests. Upstream configs
   sometimes stop early after N failures (SQLAlchemy's `--maxfail=250`).
3. Run `testing/orm-suites/run.sh <ecosystem>/<suite> --update` and commit
   `known-failures.txt`.
4. Add the suite to the matrix in `.github/workflows/orm-suites.yml`.

## Triage

Open issues found this way have the `orm-suite-sweep` label plus an
`orm/<name>` label. Before filing, check for an existing issue and confirm the
engine bug with `scripts/diff.sh "SQL"`.
