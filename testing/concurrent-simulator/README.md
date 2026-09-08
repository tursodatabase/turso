# Turso Whopper - Concurrent Simulator

Deterministic concurrent simulator for Turso.

## MVCC full-text search

`fts-mvcc` enables and asserts MVCC, then runs only FTS insert/replace,
update, delete, MATCH, OPTIMIZE and transaction begin/commit/rollback workloads.
It uses a reproducible eight-word corpus with 400 competing row IDs and 1–4
distinct words per document. Thirty percent of probes are adjacent two-word
phrases. Each probe compares the entire matching ID set and the ordered top
five against a base-table oracle **in the same SQL statement/snapshot**.

The ranking oracle does not predict exact BM25 scores. With this corpus each
queried word occurs once, matching phrase frequency is one, fieldnorms for
lengths 1–4 are exact, and there are no field boosts. Positive shared BM25
constants therefore make shorter matching documents rank first. Equal-length
ties use explicit `id ASC`. Do not add repeated words, long documents, boosts
or alternate tokenizers without changing the oracle.

```sh
cargo test -p turso_whopper --lib --test regression_tests_cross_platform
SEED=12491499896317785285 cargo run -p turso_whopper -- \
  --mode fts-mvcc --max-connections 6 --max-steps 100000 \
  --reopen-probability 0.001
```

Increase `--max-steps` for longer stress. Keep the seed, connection count,
flags and revision to replay a failure. The bounded regression runs three
12,000-step seeds with six connections and replays the first byte-for-byte;
it asserts completed token/phrase checks. A separate fixed interleaving keeps
a reader snapshot across a writer's delete/update/insert/OPTIMIZE commit,
then verifies savepoint rollback, full rollback and logical-log reopen.

SimulatorIO delays Completions until `step`; operation scheduling also uses
the existing yield injector. CLI allocation-failure injection and clean
reopen are supported. Cosmic-ray corruption is **not** a recoverable I/O
error, and these MVCC runs do not use multiprocess or kill modes. Production
output I/O-error/cancellation tests remain in the core index-method suite.
There is no total-memory bound claim: logical FTS payloads and MVCC versions
can remain resident. See the FTS section in [PERF.md](../../PERF.md) for the
separate benchmark matrix.

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
