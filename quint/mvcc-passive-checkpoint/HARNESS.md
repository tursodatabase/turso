# Trace replay test

This file describes a test that replays Quint traces on the engine. A trace is
a list of spec actions with the state after each one. The test runs the same
actions on a real database and compares the state after each step.
`make traces` and `make bug-traces` write the traces (see README.md).

The test can show two things:

- For a trace of a main configuration, the engine follows the design. Every
  check must hold (`Expect::Conforms`).
- For a trace of a bug variant, the engine has the bug that the model
  predicts. The engine must fail at the same step, with the same kind of
  failure (`Expect::Violation`). After the fix, the test fails, and the fix
  changes it to `Expect::Conforms`.

## Where the test lives

The test is a `#[cfg(test)]` module in `core/mvcc/database/mbt_tests/`. Only
a module inside `turso_core` can read the private `MvStore` fields, the
`pub(crate)` yield point enums and `CheckpointStateMachine::state_for_test()`.
The `cargo nextest run --workspace --all-features` job runs the module, so CI
needs no new job. Quint runs only offline, from this Makefile.

| File | Content |
|---|---|
| `mbt_tests/mod.rs` | The replay loop, the yield injector, the checkpoint driver and the crash probe |
| `mbt_tests/itf.rs` | The ITF parser, with `serde_json` |
| `mbt_tests/rows.rs` | The actions of `MvccPassiveCheckpoint` |
| `mbt_tests/schema.rs` | The actions of `MvccPassiveSchema` that the bug traces use |
| `mbt_tests/traces/*.itf.json` | The committed traces |

To run it:

```bash
cargo test -p turso_core --lib mbt_tests
```

To replay a folder of new row traces from `make traces`:

```bash
MBT_TRACES=$PWD/quint/mvcc-passive-checkpoint/traces \
  cargo test -p turso_core --lib mbt_tests::rows::row_traces_from_folder -- --ignored --nocapture
```

## How a step maps to the engine

Each state of an ITF trace records the action (`mbt::actionTaken`) and the
values that the action picked (`mbt::nondetPicks`). The test driver reads both
and runs one engine operation.

- A writer uses its own connection with `BEGIN CONCURRENT`, one write and
  `COMMIT`. The test driver steps the `COMMIT` statement to a named
  `CommitYieldPoint` and stops there: `CommitValidation` for `WritePrepare`,
  `LogRecordMarkedWritten` for `WriteLogAppend`.
- A reader uses `BEGIN` and a `SELECT`, and ends with `ROLLBACK`. A read-only
  `COMMIT` waits for commit dependencies, so it cannot end a reader that
  read a Preparing writer.
- The checkpoint is a `CheckpointStateMachine` on its own connection. The test
  driver steps it until `state_for_test()` reaches the state that ends the
  spec action. `PRAGMA wal_checkpoint` does not work for this, because it
  runs the whole checkpoint inside one opcode.
- A test-only collect threshold of 1 makes the checkpoint yield after each
  key. A spec key that is not dirty in the engine is a no-op.
- Each connection has one `StopAt` yield injector. `StopAt` yields once at
  one yield point and one selection key.

Engine timestamps and WAL frame numbers differ from the spec values. The test
driver keeps a map from each spec commit to its engine timestamp, and from each
spec frame to the engine WAL frame. It compares order through these maps,
never raw numbers. A writer writes its spec commit timestamp as the row value,
so a read compares directly with the spec oracle. A passive WAL restarts at
frame 1 after a full backfill, so the driver counts frames across checkpoint
sequences.

## Checks after each step

| Spec value | Engine value |
|---|---|
| `ReadsCorrect` | The `SELECT` result in the reader transaction equals the spec oracle |
| `RecoveryCorrect` | The crash probe recovers the rows that the spec logged |
| `ck.snap`, `durable` | The checkpoint snapshot and `MvStore::durable_txid_max`, through the timestamp map |
| `wal`, `nBackfill`, `backfillFloor` | The WAL max frame, the backfilled frame and `MvStore::backfill_floor`, through the frame map |
| `llog = {}` | The logical log offset is 0 |
| `CookieNotBehind` | The global header cookie is at least the shared schema version (schema driver) |

The crash probe copies the database file, the `-wal` file and the `.db-log`
file to a new folder. Then it opens the copy with a new `Database` inside
`catch_unwind`. The running database does not change, so the probe can run
after any step. The probe models a process crash, not a power loss, which
matches `RecoveryCorrect`.

## Trace replay constants

Random traces of the row spec take steps that the engine never takes. Four
constants of `MvccPassiveCheckpoint.qnt` make the spec take only engine steps.
They are false in every model-checked module, which then has the same states
as before. The `mbt` module sets them, and `make traces` uses it.

| Constant | Engine behavior |
|---|---|
| `BackfillToLimit` | The passive backfill copies every frame up to the lowest read mark |
| `LogAppendOneAtATime` | A writer holds `pager_commit_lock` from its log write to its commit end |
| `WriterBeginTicks` | A writer takes a begin timestamp before its commit timestamp, so the snapshot below a Preparing writer is above the previous commit |
| `WritersPinMarks` | A `BEGIN CONCURRENT` writer holds a WAL read mark until its commit ends, and the backfill stops at it |

## Refused traces

Some engine steps contain several spec actions. The test driver refuses a
trace that splits such a step, and the test asserts a minimum number of
replayed traces. Of 200 traces of the `mbt` module, 146 replay and 54 are
refused, for these reasons:

- A reader pins its WAL mark and begins in one engine step
  (`begin_fresh_mvcc_tx`). The driver pins at `ReaderBegin`, so it refuses a
  trace with a frame commit or a backfill between `ReaderPin` and
  `ReaderBegin`.

The schema driver runs the whole collection at the last `CheckpointObj` of a
pass. A commit between two `CheckpointObj` steps is above the snapshot, so it
changes nothing that the pass collects.

## Bug traces

| Variant | Trace | Engine result |
|---|---|---|
| `issue11502` | 8 steps | Fails at step 8: after a crash, `t2` exists without its row |
| `issue11499` | 11 steps | Fails at step 11: the publish writes the header cookie 1 while the shared schema has version 2 |

On a fresh database, the engine gives a new table the temporary id -P and
then the page P. Recovery then finds the table by chance. The schema driver
setup frees two pages first. This keeps ids and pages apart, as the
`FixDisjointIds` rule of the spec does.

The shortest `issue11499` trace of `make bug-traces` has an empty checkpoint
pass. In the spec, `CheckpointPublish` writes the staged header cookie for
every pass. In the engine, a pass with an empty write set skips the header
and the publish. So that trace conforms in the engine, and the model
over-approximates the empty pass. The committed trace has a pass that writes
a frame. To find such a trace, check `CookieNotBehind or ck.frame == 0`
instead of `CookieNotBehind`.

## Limits

- The schema driver supports DDL CREATE and DROP, row writes and the
  checkpoint. It does not map readers, ALTER and GC yet. So it does not
  replay the main configuration of the schema spec, or the bug traces that
  need readers (`issue11501`, `issue11500`).
- The sequence spec has no driver yet (`setvalRace`).
- The engine picks page numbers and table ids itself. The schema driver
  compares what recovery sees, not the numbers.
- The lost INSERT of #11499 comes from the statement retry loop, which the
  spec does not model. Its Rust test in the issue stays.
