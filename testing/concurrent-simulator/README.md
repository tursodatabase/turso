# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## FTS in WAL and MVCC

The FTS profiles use WAL by default.
Add `--enable-mvcc` to use MVCC and enable its passive checkpoints.
WAL allows one writer at a time; MVCC uses `BEGIN CONCURRENT` for overlapping write transactions.
They require at least two connections and run in one process.
They do not support encryption, Elle workloads, or `--multiprocess`.
The multiprocess driver does not run the profiles' recovery-copy checks or shared-cache controls.

Build Whopper, then select a profile:

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

The profiles use these workloads:

- `fts-merge`: Several connections reuse a small range of row IDs and run explicit and automatic merges.
  Transactions also update documents, roll back savepoints, commit, and roll back.
- `fts-snapshots`: One connection repeatedly reads an old transaction while the other connections write and merge.
  Shared segment and searcher caches are disabled during simulation steps, so repeated statements reload their snapshots from storage.
- `fts-recovery`: The merge workload also abandons suspended statements and opens copies of the current database files.
  Copies include the database and WAL, plus the MVCC log when enabled, before the original statements finish or roll back.

Each profile compares FTS results with a table scan in the same database view.
The comparison counts how often each row ID appears, so duplicate matches fail.
Savepoint tests also make sure that earlier statements survive rollback.
Every run ends with a reopen and comparisons for every test word.
The output reports completed comparisons, OPTIMIZE statements, commits, savepoint rollbacks, snapshot reads, checkpoints, recovery copies, and canceled statements.
An OPTIMIZE count does not prove that every statement merged index parts, because another connection can already be doing the work.

CI runs each profile in WAL and MVCC on stable and nightly Rust, using ten random seeds at 100,000 steps each.
The allocation failure probability is `0.001` per eligible allocation; actual allocator injection requires nightly with `--cfg nightly`.

The recovery profile samples file copies after 2% of eligible suspended-statement steps and abandons statements after 1%.
These copies test recovery without statement cleanup, but they contain complete writes issued by the simulated I/O layer.
They do not simulate torn writes, write reordering, or loss of unsynced writes after power failure.
Scheduling uses the existing engine yield points and I/O boundaries.
An uninterrupted check-then-insert race still needs its separate deterministic regression.

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
