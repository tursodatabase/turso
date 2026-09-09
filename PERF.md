# Performance Testing

## MVCC FTS

`core/benches/fts_benchmark.rs` includes a deterministic, download-free MVCC
matrix. MVCC is enabled before schema creation and asserted through the actual
MVCC store, not inferred from a benchmark name. Every benchmark uses the
repository's CodSpeed naming macro.

| Axis | Cases |
| --- | --- |
| Query corpus | 1,000 short documents / 1,000 documents with 16 repeated sentences / 5,000 short documents |
| Commits | 500 or 100 documents per transaction, before/after explicit OPTIMIZE |
| Query | common term (all rows), selective term (1%), adjacent phrase (50%) |
| Retrieval | all matching rowids / native score-descending top 10 |
| Reader state | warm connection; fresh connection for selective queries; separate engine-cold reopen+query |
| Mutation | insert/update/delete 100 rows and commit; OPTIMIZE and commit |

The query corpus uses row ID arithmetic, not random input: each document
contains `common`, every hundredth contains `needle`, and even IDs contain
`quick brown` while odd IDs contain `brown quick`. Long bodies repeat the
same six-word sentence sixteen times. Names record rows/repetitions/commit
batch/OPTIMIZE state. Batch counts are **not** asserted segment counts: normal
foreground merging still runs. Mutation/reopen cases use the existing varied
seven-topic/five-category corpus (1,000 documents).

Setup is outside the timed query loop. Mutation cases use a fresh database
per iteration through `iter_batched(PerIteration)`; timing covers BEGIN,
mutation/OPTIMIZE and COMMIT, not setup or destruction. A separate untimed
preflight checks base row counts and FTS-vs-base matching IDs after each
mutation. Query loops check expected cardinality; ranking correctness and
snapshot isolation are checked independently by the Whopper suite.

"Fresh connection" retains the shared database/index cache and OS page cache;
it is **not cold storage**. "Engine cold" drops the setup database and opens
it with a new IO instance; opening/recovery, query preparation and the first
query are timed. It does not flush the OS page cache. Warm queries include
preparation and snapshot acquisition, not just Tantivy scoring.

```sh
# Debug correctness pass, all MVCC cases once:
cargo test -p turso_core --bench fts_benchmark --features fts -- 'FTS MVCC' --test
# Debug-only experimental timings, NOT representative production performance:
cargo test -p turso_core --bench fts_benchmark --features fts -- \
  --bench 'FTS MVCC' --quick --noplot
```

Record revision, profile, hardware and flags alongside numbers. No performance
gain or total-memory cap follows from these benchmarks. This harness does not
measure peak live allocations; cache sizes do not bound retained logical
payloads or MVCC versions. Existing benchmark CI includes this target; the
[Whopper FTS workload](testing/concurrent-simulator/README.md#mvcc-full-text-search)
provides deterministic correctness/stress coverage, not throughput timings.

### Focused profiles

Fixtures and preflight checks are initialized only after Criterion selects a
benchmark, outside its timed loop. `--list` and filters therefore do not populate
unselected databases. Warm MVCC queries have a non-inlined `run_mvcc_query`
boundary covering prepare, execution, result validation and statement cleanup.
The fresh-connection case retains its separate timing boundary.

```sh
BENCH=$(cargo test -p turso_core --bench fts_benchmark --features fts \
  --no-run --message-format=json | jq -r 'select(.reason == "compiler-artifact" and .target.name == "fts_benchmark") | .executable // empty')
CASE='FTS MVCC/selective/rankedfalse/warm/rows1000/repeat1/batch500/optfalse'
valgrind --tool=callgrind --collect-atstart=no \
  --toggle-collect=fts_benchmark::run_mvcc_query \
  --callgrind-out-file=callgrind.fts "$BENCH" --bench "$CASE" --test
callgrind_annotate --inclusive=yes --auto=no callgrind.fts
valgrind --tool=cachegrind --branch-sim=yes \
  --cachegrind-out-file=cachegrind.fts "$BENCH" --bench "$CASE" --test
cg_annotate --auto=no cachegrind.fts
"$BENCH" --bench "$CASE" --profile-time 5
```

These commands build **debug** binaries, not production-speed measurements.
Callgrind collects one warm query without setup/preflight. Cachegrind 3.19's
whole-process totals include the selected fixture and preflight as well as the
query; do not label those totals query-only. Both tools simulate events, not
hardware counters. Pin/cache-report their simulated cache geometry for paired
comparisons. Criterion's profiling mode writes a CPU-sampling flamegraph under
`target/criterion`; sampling can include initial lazy fixture creation, and
mutation profiles can include per-iteration fixture creation, even though
Criterion's wall-clock measurement excludes it. Record that scope.

## Mobibench

1. Clone the source repository of Mobibench fork for Turso:

```console
git clone git@github.com:penberg/Mobibench.git
```

2. Build Mobibench:

```console
cd Mobibench/shell
LIBS="../../target/release/libturso_sqlite3.a -lm" make
mv mobibench mobibench-turso
```

3. Run Mobibench:

(easiest way is to `cd` into `target/release`)

```console
# with strace, from target/release

strace -f -c ../../Mobibench/shell/mobibench-turso -f 1024 -r 4 -a 0 -y 0 -t 1 -d 0 -n 10000 -j 3 -s 2 -T 3 -D 1


./mobibench -p <benchmark-directory> -n 1000 -d 0 -j 4
```


## Clickbench

We have a modified version of the Clickbench benchmark script that can be run with:

```shell
make clickbench
```

This will build Turso in release mode, create a database, and run the benchmarks with a small subset of the Clickbench dataset.
It will run the queries for both Turso and SQLite, and print the results.


## Comparing VFS's/IO Back-ends (io_uring | syscall)

```shell
make bench-vfs SQL="select * from users;" N=500
```

The naive script will build and run limbo in release mode and execute the given SQL (against a copy of the `testing/testing.db` file)
`N` times with each `vfs`. This is not meant to be a definitive or thorough performance benchmark but serves to compare the two.


## TPC-H

on linux if you are using `tlp` to manage power settings, you may want to disable it while running the TPC-H benchmark as it can affect performance. consider changing swap to `swapoff -a`

Run the benchmark script:

```shell
./perf/tpc-h/benchmark.sh
```
