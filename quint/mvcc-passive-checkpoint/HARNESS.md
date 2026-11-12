# Trace replay harness (design, not built)

This file describes a test that replays Quint traces on the engine. A trace is
a list of spec actions with the state after each one. The test runs the same
actions on a real database and compares the state after each step. The code
for this test does not exist yet. The traces exist: `make traces` and
`make bug-traces` write them (see README.md).

The test can show two things:

- For a trace of a main configuration, the engine follows the design. Every
  check must hold.
- For a trace of a bug variant, the engine has the bug that the model
  predicts. The engine must fail at the same step, with the same kind of
  error. After the fix, the test changes to expect no error.

## Where the test lives

The test is a `#[cfg(test)]` module under `core/mvcc/database/mbt_tests/`.
Only a module inside `turso_core` can read the private `MvStore` fields, the
`pub(crate)` yield point enums and `CheckpointStateMachine::state_for_test()`.
A crate under `testing/` needs these as public API. The existing
`cargo nextest run --workspace --all-features` job runs the module, so CI needs
no new job. Quint runs only offline, from this Makefile.

| File | Content |
|---|---|
| `mbt_tests/mod.rs` | The test driver, the replay loop and the crash probe |
| `mbt_tests/itf.rs` | The ITF parser, with `serde_json` |
| `mbt_tests/rows.rs` | The actions of `MvccPassiveCheckpoint` |
| `mbt_tests/schema.rs` | The actions of `MvccPassiveSchema` |
| `mbt_tests/traces/*.itf.json` | The committed traces |

## How a step maps to the engine

Each state of an ITF trace records the action (`mbt::actionTaken`) and the
values that the action picked (`mbt::nondetPicks`). The test driver reads both
and runs one engine operation.

- A writer uses its own connection with `BEGIN CONCURRENT`, one write and
  `COMMIT`. The test driver steps the `COMMIT` statement to a named
  `CommitYieldPoint` and stops there.
- A reader uses `BEGIN` and a `SELECT`. The first statement stops at
  `TransactionYieldPoint::BeforeMvccBegin`. At that point, the reader holds
  the WAL read lock but has no MVCC transaction. This is the spec action
  `ReaderPin`.
- The checkpoint is a `CheckpointStateMachine` on its own connection. The test
  driver steps it until `state_for_test()` reaches the state that ends the
  spec action. `PRAGMA wal_checkpoint` does not work for this, because it
  runs the whole checkpoint inside one opcode.
- At open, each connection gets one `StopAt` yield injector. `StopAt`
  yields once at one yield point and one selection key, and then never again.

Engine timestamps and WAL frame numbers differ from the spec values. The test
driver keeps a map from each spec commit to its engine timestamp, and from each
spec frame to the engine WAL position. It compares order through these maps,
never raw numbers. A writer writes its spec commit timestamp as the row value,
so a read compares directly with the spec oracle.

## Checks after each step

| Spec value | Engine value |
|---|---|
| `ReadsCorrect` | The `SELECT` result in the reader transaction equals the spec oracle |
| `RecoveryCorrect` | The crash probe recovers the rows that the spec expects |
| `durable` | `MvStore::durable_txid_max`, through the timestamp map |
| `backfillFloor`, `nBackfill` | `MvStore::backfill_floor` and the WAL backfill frame, through the frame map |
| `bind`, `pub`, `cookie` | `table_id_to_rootpage`, the `sqlite_schema` rows and the header schema cookie |

The crash probe copies the database file, the `-wal` file and the `.db-log`
file to a new folder. Then it opens the copy with a new `Database` inside
`catch_unwind`. The running database does not change, so the probe can run
after any step. The probe models a process crash, not a power loss, which
matches `RecoveryCorrect`. All I/O must be complete before the copy.

## Engine changes for the test

All changes compile out of release builds.

1. A `#[cfg(test)]` collect threshold for each `CheckpointStateMachine`. The
   spec action `CheckpointCollect` visits one key, but the engine yields only
   after `COLLECT_PREEMPTION_THRESHOLD` keys. A threshold of 1 makes each
   step visit one key. A new `CheckpointYieldPoint` is not a good choice,
   because it changes the point count, and every existing simulator seed then
   gives a different schedule.
2. Optional: a `CommitYieldPoint::DirtyKeysMarked` between the dirty mark and
   the log write. Only the dirty-key variants need it. It changes the commit
   yield point count, so it goes in its own commit.

## Limits

- Some engine steps contain several spec actions. Examples: the two looks of a
  read, all prune candidates, the GC inside `GcTableRows`, and an empty
  checkpoint that skips the commit and the publish. The test driver refuses a
  trace that splits such a step. The test counts refused traces and must
  assert a minimum number of replayed traces.
- The spec picks any key to collect and any backfill target. The engine
  collects in rowid order and backfills as far as it can. The traces for the
  test need constants that make the spec do the same.
- The engine picks page numbers from the freelist. A schema trace replays only
  if the engine picks the same page as the spec. The test driver reports a
  different page as "not replayable", not as a failure.
- The lost INSERT of #11499 comes from the statement retry loop, which the spec
  does not model. Its Rust test in the issue stays.

## Order of work

1. The ITF parser and the test driver, with one row trace written by hand.
2. The collect threshold and the collect of one key per step.
3. Row traces from `make traces`.
4. The crash probe and `RecoveryCorrect`.
5. The schema driver and the bug traces of #11499, #11501 and #11502.
