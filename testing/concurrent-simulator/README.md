# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

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

### Elle models

`--elle` selects the model. `list-append` and `rw-register` read and write
rows of one table by primary key. `fts-list-append` and `fts-rw-register`
run the same transaction mix, but the table also has a `body` column with a
full-text index on it, every write rewrites `body`, and every read is a
`fts_match` query on the key token instead of a primary-key lookup:

| Model             | Table            | Write                                      | Read                                                       |
|-------------------|------------------|--------------------------------------------|------------------------------------------------------------|
| `list-append`     | `elle_lists`     | append value to `vals`                     | `SELECT vals ... WHERE key = 'k1'`                         |
| `rw-register`     | `elle_rw`        | set `val`                                  | `SELECT val ... WHERE key = 'k1'`                          |
| `fts-list-append` | `elle_fts_lists` | append to `vals`, append `v<value>` to `body` | `SELECT vals ... WHERE fts_match(body, 'k1')`           |
| `fts-rw-register` | `elle_fts_rw`    | set `val`, set `body` to `k1 v<value>`     | `SELECT val ... WHERE fts_match(body, 'k1')`               |

So for the `fts-` models the full-text index is the register Elle checks:
which row a read sees is decided by the index's view of the transaction's
snapshot, including the writer's own uncommitted documents and tombstones.
The history has the same shape as the plain model, so elle-cli still runs
with `--model list-append` or `--model rw-register`. The `fts-` models also
run a low-weight `OPTIMIZE INDEX` outside transactions so segment merges
happen while the history is recorded, and a read that returns more than one
row for a key token fails the run on the spot.

```shell
SEED=1 ./target/debug/turso_whopper \
    --elle fts-rw-register \
    --elle-output elle-history.edn \
    --max-steps 100000 \
    --enable-mvcc

java -jar /tmp/elle-cli/target/elle-cli-0.1.9-standalone.jar \
    --model rw-register \
    --consistency-models snapshot-isolation \
    --verbose \
    --directory elle-results \
    elle-history.edn
```

The `fts-` models fail more transactions than the plain ones under the
default allocation-fault injection, because an FTS write allocates far more
than a primary-key upsert and so is hit by more injected `OutOfMemory`
errors. Those show up as `:fail` events, which Elle handles; pass
`--allocation-fault-probability 0` for a history with fewer of them.
