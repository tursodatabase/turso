# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## MVCC full-text search

`fts-mvcc` enables and asserts MVCC, then runs only FTS insert/replace,
update, delete, MATCH, OPTIMIZE and transaction begin/commit/rollback workloads.
It uses a reproducible eight-word corpus with 400 competing row IDs and 1–4
distinct words per document. Thirty percent of probes are adjacent two-word
phrases. Each probe compares the entire matching ID set and the ordered top
five against a base-table oracle **in the same SQL statement/snapshot**.

This simulator runs against the FTS implementation in this checkout. It does
not require the engine changes from the FTS development stack. Ranking checks
remain enabled by default. Use `--fts-match-only` to diagnose matching and
snapshot behavior independently: it compares complete ID lists ordered by ID,
without evaluating FTS scores or limiting the results. This is not a ranking
validation. `--elle` and `--multiprocess` cannot be combined with this mode.

The ranking oracle does not predict exact BM25 scores. With this corpus each
queried word occurs once, matching phrase frequency is one, fieldnorms for
lengths 1–4 are exact, and there are no field boosts. Positive shared BM25
constants therefore make shorter matching documents rank first. Equal-length
ties use explicit `id ASC`. Do not add repeated words, long documents, boosts
or alternate tokenizers without changing the oracle.

The focused database scenarios are manual-only tests in `fts_mvcc_scenarios`.
They are marked `#[ignore]`, so ordinary test/CI runs do not execute them.
No CI job selects either FTS mode. Run them explicitly, including the known
ranking failure:

```sh
cargo test -p turso_whopper --test fts_mvcc_scenarios -- --ignored --nocapture --test-threads=1
SEED=12491499896317785285 cargo run -p turso_whopper -- \
  --mode fts-mvcc --max-connections 6 --max-steps 100000 \
  --reopen-probability 0.001

SEED=3957 cargo run -p turso_whopper -- \
  --mode fts-mvcc --fts-match-only --max-connections 6 --max-steps 12000 \
  --allocation-fault-probability 0 --reopen-probability 0
```

Increase `--max-steps` for longer stress. Keep the seed, connection count,
flags and revision to replay a failure. The bounded regression runs three
12,000-step match-only seeds with six connections and replays the first byte-for-byte;
it asserts completed token/phrase checks. Separate fixed interleavings keep
a reader snapshot across a writer's delete/update/insert/OPTIMIZE commit,
then verify savepoint rollback, full rollback and logical-log reopen, with
and without ranking checks.

On main revision [554ae1b41](https://github.com/tursodatabase/turso/commit/554ae1b41),
the ranking regression fails: `alpha bravo` returns IDs `1,2` instead of `2,1`
even before concurrent writes. The shorter document is ID 2. The match-only
snapshot/recovery test and all three match-only seeds pass. The ranking test
still expects the correct order and fails when run explicitly. Like the other
focused database scenarios, it is excluded from ordinary CI runs. Use the same
default ranking checks when evaluating later FTS changes.

SimulatorIO delays Completions until `step`; operation scheduling also uses
the existing yield injector. CLI allocation-failure injection and clean
reopen are supported. CLI allocation-failure probability defaults to 0.05;
the bounded library regressions use 0. Set it explicitly for comparisons.
Cosmic-ray corruption is **not** a recoverable I/O error, and these MVCC runs
do not use multiprocess or kill modes.
There is no total-memory bound claim: logical FTS payloads and MVCC versions
can remain resident. Temporary sorter-file retention in the current simulator
can also limit long runs; this test-only port does not import the temporary-file
ownership changes from the FTS stack.

With seed 3957, six connections, allocation-failure probability 0.05 and
reopen probability 0.001, a 12,000-step match-only run completed 107 checks
(29 phrases), injected 28 allocation failures and rejected 105 checkpoint
probes against suspended statements. A 30,000-step run with the same settings
failed after 248 checks while opening a sorter file at the 1,024-descriptor
limit. That longer run did not pass; use bounded runs for this baseline.

## Rollback-heavy FTS scenario

`fts-mvcc-rollback` uses one writer (connection 0) and snapshot readers on the
other connections. The original `fts-mvcc` mode remains the competing-writer
scenario. Separating the roles lets the rollback sequence complete instead
of repeatedly aborting on write/merge contention.

Each writer sequence commits an initial document and ensures the second row
is absent, then starts another concurrent transaction with nested savepoints.
It updates, inserts and deletes documents, runs `OPTIMIZE` inside both savepoints, and
checks results after mutations and rollback/release boundaries. Each inner
rollback, outer rollback and full transaction rollback decision has probability
0.75. An outer rollback discards the still-open inner savepoint; the writer
then writes and optimizes again and rolls back to the same outer savepoint
a second time. A final `OPTIMIZE` is followed by either commit or full rollback.

Two independent checks run at these boundaries: FTS versus a base-text scan,
and the exact expected IDs/bodies (including absent rows) versus actual table
contents. Both checks run immediately before and after every `OPTIMIZE`.
The expected rows catch rollback errors even if FTS and the base table are
wrong together. Readers repeat one phrase query 2–6 times in a transaction
and require unchanged results, including when the original result was empty.
The corpus still satisfies the length-based ranking oracle's assumptions.

The CLI reports successful `OPTIMIZE` statements, row checks, `ROLLBACK TO`,
`RELEASE`, transaction commits/rollbacks and completed writer scenarios. A
successful optimize count means the statement finished, not that its enclosing
transaction committed. A rollback-mode run with no completed writer scenario
is an error. Unexpected SQL execution errors fail this mode; contention and
injected OOM abandon the sequence through Whopper's existing transaction error handling.

```sh
SEED=3957 cargo run -p turso_whopper -- \
  --mode fts-mvcc-rollback --fts-match-only --max-connections 6 --max-steps 18000 \
  --allocation-fault-probability 0 --reopen-probability 0

cargo test -p turso_whopper --test fts_mvcc_scenarios fts_rollback -- \
  --ignored --nocapture --test-threads=1
```

The manual tests execute all eight inner/outer/full-rollback combinations
against the database and require completed scenarios, successful optimizes,
row checks and rollback/release operations in the 18,000-step six-connection run.
The command above completed two writer scenarios, 212 phrase checks, 9 optimizes
and 22 row checks, including savepoint and full transaction rollbacks. Add
`--allocation-fault-probability 0.05 --reopen-probability 0.0001` for fault and
clean-reopen testing, but do not assume that budget is enough for completion:
with those flags the run injected 14 allocation failures and completed 205
phrase checks and 5 optimizes, but no complete writer scenario, so it returned
an error. Faults and reopen can interrupt every long sequence. These scenarios
do not simulate process crashes.

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
