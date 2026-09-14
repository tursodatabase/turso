# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## FTS with MVCC

The FTS profiles enable MVCC and passive checkpoints automatically.
They require at least two connections and run in one process.
They do not support encryption, Elle workloads, or `--multiprocess`.

Build Whopper, then select a profile:

```bash
cargo build -p turso_whopper
SEED=8940 target/debug/turso_whopper --mode fts-merge --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-snapshots --max-steps 100000
SEED=8940 target/debug/turso_whopper --mode fts-recovery --max-steps 100000
```

The profiles use these workloads:

- `fts-merge`: Concurrent writes reuse a small rowid range and run explicit and automatic merges.
  Transactions also update documents, roll back savepoints, commit, and roll back.
- `fts-snapshots`: One connection repeatedly reads an old transaction while the other connections write and merge.
  Shared segment and searcher caches are disabled during simulation steps, so repeated statements reload their snapshots from storage.
- `fts-recovery`: The merge workload also abandons suspended statements and opens copies of the current database files.
  Copies include the database, WAL, and MVCC log before the original statements finish or roll back.

Each profile compares FTS results with a table scan in the same snapshot.
The comparison includes the number of occurrences of each rowid, so duplicate matches fail.
Savepoint tests also make sure that earlier statements survive rollback.
Every run ends with a reopen and comparisons for every token in the workload vocabulary.
The output reports completed comparisons, OPTIMIZE statements, commits, savepoint rollbacks, snapshot reads, checkpoints, crash snapshots, and abandoned statements.
An OPTIMIZE count does not prove that every statement merged segments, because contended claims can produce no work.

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
