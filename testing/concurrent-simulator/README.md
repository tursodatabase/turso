# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## FTS in WAL and MVCC

These profiles test full-text search (FTS) in WAL and MVCC.
FTS finds documents that contain a search word.
The FTS profiles use WAL by default.
Add `--enable-mvcc` to use MVCC.

WAL allows one writer at a time.
WAL writers use `BEGIN IMMEDIATE`, and WAL readers use `BEGIN DEFERRED`.
MVCC uses `BEGIN CONCURRENT` for write transactions that overlap.
A checkpoint copies committed changes to the database file.
A passive checkpoint does not wait for active transactions.
The MVCC profiles also enable passive checkpoints.

The profiles need at least two connections in one process.
They do not support encryption, Elle workloads, or `--multiprocess`.
A shared FTS cache stores index data between statements.
It keeps that data in memory.
The multiprocess driver does not open recovery copies or disable shared FTS caches during simulation steps.

Build Whopper and select a profile:

```bash
cargo build -p turso_whopper
SEED=8940 target/debug/turso_whopper --mode fts-merge --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-snapshots --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-recovery --max-steps 100000
```

Run the same profiles in MVCC:

```bash
SEED=8940 target/debug/turso_whopper --mode fts-merge --enable-mvcc --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-snapshots --enable-mvcc --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-recovery --enable-mvcc --max-steps 100000
```

An index merge combines smaller index parts.
A savepoint marks where to undo later transaction changes.
The profiles run these workloads:

- `fts-merge`: Several connections change rows that use the same small range of IDs.
  They update documents, request index merges, undo changes after savepoints, commit, and roll back.
  The index also merges automatically when it reaches the merge threshold.
- `fts-snapshots`: One reader keeps seeing the rows from its first read while other connections write and merge.
  The simulator disables shared FTS caches during simulation steps, so repeated statements load index data from storage.
- `fts-recovery`: The merge workload also cancels unfinished statements and opens copies of the current database files.
  The simulator copies the database and WAL before the original statement finishes or rolls back.
  If MVCC is enabled, it also copies the MVCC log.

A database view is data visible to a transaction.
Each profile compares FTS results with a table scan in the same view.
The comparison counts how often each row ID appears.
If FTS returns a row twice, the comparison fails.
Savepoint tests also make sure that rollback keeps changes made before the savepoint.
Every run reopens the database and compares results for every test word.

The output counts completed result comparisons, recovery copies, and canceled statements.
It also counts OPTIMIZE statements, commits, savepoint rollbacks, reads from an old view, and checkpoints.
An OPTIMIZE count does not prove that every statement merged index parts.
Another connection can already be doing the merge.

CI runs each profile in WAL and MVCC on stable and nightly Rust.
Each job runs ten random seeds at 100,000 steps per seed.
Each job sets the allocation failure probability to `0.001` per eligible allocation.
Only nightly jobs with `--cfg nightly` inject allocation failures.

During unfinished writes, commits, and checkpoints, `fts-recovery` opens file copies with a 2% probability per simulation step.
It cancels the statement with a 1% probability per step.
The copies contain complete writes that the simulator already issued.
They test recovery before the simulator finishes or cancels the original statement.
They do not test partly written pages, writes completed out of order, or unsaved writes lost after a power failure.

The simulator can switch connections when the engine pauses execution or waits for I/O.
Two connections can read that an index key is missing and then both insert it.
If the engine never pauses between those steps, the simulator cannot switch connections to test that sequence.
That case still needs a separate test with a fixed execution order.

## Coverage

To collect line coverage from actual Whopper execution:

```bash
make whopper-coverage WHOPPER_RUNS=10
```

Pass regular Whopper CLI flags through `WHOPPER_ARGS`:

```bash
make whopper-coverage WHOPPER_RUNS=10 \
  WHOPPER_ARGS="--mode fast --max-steps 10000 --multiprocess --processes 2 --connections-per-process 2"
```

Reports are written to:

- `.coverage/whopper/report.txt`
- `.coverage/whopper/html/index.html`

## Running Elle Locally (macOS)

### One-time setup

```bash
brew install leiningen openjdk graphviz

sudo ln -sfn $(brew --prefix openjdk)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

# Build elle-cli
git clone --depth 1 https://github.com/ligurio/elle-cli.git /tmp/elle-cli
cd /tmp/elle-cli && lein uberjar
```

### Running sim and elle analysis

Get the seed from CI logs and run the sim:

```shell
cargo build -p turso_whopper
SEED=14201626211019268779 ./target/debug/turso_whopper \
    --elle list-append \
    --elle-output elle-history.edn \
    --max-steps 100000 \
    --enable-mvcc
```

and then elle:

```shell
java -jar /tmp/elle-cli/target/elle-cli-0.1.9-standalone.jar \
    --model list-append \
    --consistency-models snapshot-isolation \
    --verbose \
    --directory elle-results \
    elle-history.edn
```
