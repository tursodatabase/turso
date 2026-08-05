# Upstream PostgreSQL regression test corpus

A curated subset of PostgreSQL's regression tests (`src/test/regress/`),
run against `tursopg` by the `pgregress` runner (`postgres/regress/`).
The pass rate of this corpus is the headline measure of how PostgreSQL
compatible tursopg is.

## Provenance

- Source: https://github.com/postgres/postgres
- Release: PostgreSQL 18, tag `REL_18_4`
  (commit `f5cc81719e6da4cbdb1f797c48b693e91018153a`)
- Imported: 2026-08-05

The `.sql` scripts, `.out` expected transcripts (including `_1.out`-style
alternates), and `data/*.data` load files are copied verbatim from that tag.
Do not edit them to make tests pass — they are the yardstick, not ours.
Only `README.md` and `schedule` are authored in this repository.

## What is in the corpus and why

103 of upstream's 233 tests: the SQL-semantics subset — types, DML, DDL,
core queries, JSON, transactions, constraints. Excluded categories, and
examples of tests they pull out:

- Physical access methods and plan shapes: `btree_index`, `gin`, `gist`,
  `brin*`, `hash_index`, `explain`, `memoize`, `equivclass`, `join_hash`,
  `partition_*`, `incremental_sort`, `tuplesort`, `predicate`
- Server internals: vacuum, stats, replication, tablespaces, TOAST,
  compression, parallel workers, `pg_lsn`, `xid`/`txid`, `combocid`, `mvcc`
- Tests needing C functions from `regress.so`: `misc`, `create_type`,
  `create_function_c`, `create_operator`, `create_aggregate`, `create_cast`
- Catalog self-consistency: `opr_sanity`, `oidjoins`, `type_sanity`,
  `sanity_check`, `misc_sanity`
- Platform- and locale-dependent: `collate*`, encoding conversions
- Feature areas deliberately deferred: plpgsql, `triggers`,
  `rules`, `privileges`/RLS, text search (`tsearch`, `tsdicts`, `tstypes`),
  geometry (`point`, `box`, `path`, ...), `xml`, `largeobject`,
  `without_overlaps`, `generated_virtual`, `inherit`, partitioning
- SQL-language function definitions (candidates to add once CREATE
  FUNCTION is on the roadmap): `create_function_sql`, `rangefuncs`,
  `polymorphism`, `plancache`

The selection principle: the corpus is the core PostgreSQL feature set
applications actually depend on. Tests are excluded when they exercise
PostgreSQL's physical implementation (access methods, plan shapes,
server internals) rather than its SQL behavior, or when their feature
area has not been started yet. Exclusion is sequencing, not a ceiling:
the long-term aim is full compatibility, and deferred tests join the
corpus as their feature areas land. When adding or removing a test,
judge it against that principle and update this list.

## Layout

- `<name>.sql` / `<name>.out` — test script and expected psql transcript;
  `<name>_1.out` etc. are upstream's accepted alternate outputs
- `data/` — files the scripts load with `COPY ... FROM` (referenced via
  `\getenv abs_srcdir`); only the files the corpus uses are imported
- `schedule` — run order, upstream's `parallel_schedule` filtered to the
  corpus. Later groups use fixtures from earlier ones (`test_setup`
  especially), so order matters even for serial runs.

## Running

```
postgres/conformance/run.py            # whole corpus
postgres/conformance/run.py boolean    # one test
make -C postgres/conformance run-upstream
```

## Blessed and known-bad tests

`run.py` runs every corpus test on every full invocation. Each test is
listed in the `STATUS` table at the top of `run.py` with one of three
statuses:

- `pass` — blessed: output must match byte-exact. Any diff or crash is a
  regression and fails the run.
- `fail` — known-bad: the test runs and its diff is reported, but does
  not fail the run. If it becomes byte-exact the run fails with a
  request to bless it, so the known-bad list only ever shrinks.
- `skip` — not run at all; reserved for tests that cannot run, with the
  reason noted next to the entry.

To bless a test after fixing its remaining diffs, change its status from
`fail` to `pass`. A test that crashes or wedges the server fails on a
timeout and the server is restarted automatically, so one bad test
cannot poison the rest of the run.

## Re-syncing to a newer PostgreSQL release

Deliberate, whole-corpus operation — never mix releases:

1. `git -C <postgres-checkout> archive <new-tag> src/test/regress | tar -x`
2. Re-copy `.sql`, `.out` (+ alternates), and referenced `data/` files for
   every test listed in `schedule`; check upstream's release notes and
   `parallel_schedule` diff for tests added, removed, or renamed.
3. Regenerate `schedule` from the new `parallel_schedule`, filtered to the
   corpus.
4. Update the provenance block above and re-baseline the pass count in CI.
