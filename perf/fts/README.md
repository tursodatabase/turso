# FTS search latency and throughput

Two separate benchmarks compare SQLite FTS5/WAL, Turso/WAL, and Turso/MVCC.
Both use the same deterministic document generator from `perf/memory/src/fts.rs`.
Both are read-only during measurement, not ingestion or mixed read/write benchmarks.

## Search latency

The search benchmark times individual queries with one OS worker thread per connection.
By default, it measures 10,000 total queries per case and reports p50, p95, and p99 in milliseconds.
It divides that fixed sample budget evenly across workers, then pools their individual durations within each run.
Workers start together and execute independent query loops without a barrier between queries.
These are nearest-rank percentiles of individual query durations, not total wall time divided by completed queries.
Each query includes SQL preparation, execution, result draining, and the workload adapter's overhead.

Run from the repository root with Rust and `uv` installed, using a new output directory:

```sh
bash perf/fts/scripts/run.sh /tmp/fts-search --benchmark search \
  --documents 10000 --queries 10000 --runs 5
bash perf/fts/scripts/run.sh /tmp/fts-first --benchmark search \
  --documents 10000 --state first --runs 5
BENCHMARK=search CONNECTIONS="1 2 4 8 16 32" QUERIES=10000 RUNS=3 \
  bash perf/fts/scripts/sweep.sh /tmp/fts-latency-sweep
```

Warm search runs execute one unmeasured query per connection before the measured queries.
First-query runs measure exactly one query per connection against each freshly reopened fixture without warm-up.
They do not represent a cold OS cache, because setup just wrote the database.
Search rejects duration arguments and sample counts smaller than the connection count.
The search sweep produces separate p50, p95, and p99 graphs in PNG, PDF, and SVG.
These are closed-loop clients: each worker waits for its query to finish before starting another.
Their latency excludes waiting for an externally scheduled request to be submitted.

## Search throughput

The throughput benchmark measures completed searches per second over a shared wall-time interval.
Each sample runs for at least five seconds by default, finishing its last batch before stopping.
Query counts differ between engines and connection counts, and the CSV records the actual count and elapsed time.
Throughput does not collect per-query latency samples.

```sh
bash perf/fts/scripts/run.sh /tmp/fts-throughput --benchmark throughput \
  --documents 10000 --connections 8 --seconds 5 --runs 3
bash perf/fts/scripts/sweep.sh /tmp/fts-throughput-sweep
CONNECTIONS="1 4 16 32" SECONDS_PER_RUN=10 DOCUMENTS=10000 RUNS=6 \
  bash perf/fts/scripts/sweep.sh /tmp/fts-longer-sweep
```

The sweep defaults to 1, 2, 4, 8, 16, and 32 connections, three repetitions, and five seconds per sample.
That is at least 27 minutes of measurement plus fixture creation, compilation, and plotting.
Throughput requires warm queries and rejects a fixed query count.
Each batch executes one query per connection and waits for all connections before the next batch.
The measurement includes this scheduling and synchronization, so it is not a measurement of independent clients without a batch barrier.
There are no explicit transactions or concurrent writes in either benchmark.

## Engines, fixtures, and build configuration

Every invocation runs `sqlite-wal,turso-wal,turso-mvcc` unless `--targets` selects a subset.
SQLite has no Turso MVCC journal mode, so there is no `sqlite-mvcc` target.
The target order rotates after each repetition to reduce systematic order effects.
The six cases are `rare`, `common`, `and`, `or`, `phrase`, and BM25-ranked top ten.

Each case, target, and repetition gets a fresh temporary on-disk database with 10,000 documents by default.
Common terms occur in every document, rare terms in 1%, alpha in half, and beta in roughly one-third.
The phrase case distinguishes ordered words from reversed words.
The corpus contains simple ASCII terms, not representative natural-language text.
Fixture files are deleted after each measurement.
See [the shared workload documentation](../memory/README.md) for the corpus details.

SQLite uses bundled SQLite through `rusqlite`, external-content FTS5, and the `unicode61` tokenizer.
Both engines load documents in 500-row transactions, then build the full-text index outside measurement.
SQLite returns matching FTS5 `rowid` values, equivalent to Turso document IDs.
Ranked queries use each engine's BM25 implementation, ascending SQLite scores and descending Turso scores.
Score magnitudes and tied result order can differ.

Turso connections share a reopened database handle.
SQLite opens a separate connection to the same file for each worker, with default private page caches.
For search latency, each connection has a dedicated OS thread and Turso uses a current-thread Tokio runtime there.
For throughput, Turso uses one Tokio worker per connection and SQLite uses Tokio's blocking pool,
limited to one blocking worker per connection; one-connection runs execute directly on the calling task.
Each engine uses its default page cache, and no command clears the OS cache or requires privileged access.

The scripts use optimized `bench-profile`, which inherits release settings but disables LTO and retains debug information.
LTO means link-time optimization.
No command uses `--release`, in accordance with repository guidance.
Use `PROFILE=dev` only for correctness tests, not performance claims.
The output directory contains CSV results, a build log with the SQLite version, machine details, Rust version, source revision, and arguments.

## Plot and interpret results

The plotter keeps search and throughput results separate and rejects mixed configurations or incomplete runs.
Search plots show the median of per-run p50 latency by default, with whiskers spanning the run-level values.
Use `--percentile p95` or `--percentile p99` to plot those latency statistics instead.
Throughput plots show the median completed searches per second and the observed range across repetitions.
Neither type of whisker is a confidence interval, and run-level latency percentiles are not pooled across runs.

```sh
uv run perf/fts/plot/plot-fts.py /tmp/fts-search/results.csv \
  --percentile p95 -o /tmp/fts-search/p95.png -o /tmp/fts-search/p95.pdf
uv run perf/fts/plot/plot-fts.py --sweep /tmp/fts-throughput-sweep/c*/results.csv \
  -o /tmp/fts-throughput-sweep/fts-throughput.png
uv run perf/fts/plot/plot-fts.py --sweep --relative /tmp/fts-latency-sweep/c*/results.csv \
  --percentile p99 -o /tmp/fts-latency-sweep/fts-p99-speedup.png
```

Figures use the existing SciencePlots serif style and Okabe-Ito palette, with PNG, PDF, and SVG output.
SQLite is orange, Turso WAL is blue, and Turso MVCC uses stripes or hollow dashed markers.
The grouped-bar layout follows TPC-H.
Connection sweeps use six panels, linear measurement axes starting at zero, and logarithmic connection axes.
Each panel has a table of rounded values, with thousands separators for large numbers; CSV files retain full precision.
Timing sweeps also produce `*-speedup` figures: 1× equals SQLite, 2× means faster, and 0.5× means slower.
For latency, the ratio is SQLite's median run-level percentile divided by Turso's; for throughput, it is Turso's median divided by SQLite's.
Relative whiskers transform the observed Turso range using the fixed SQLite median, not paired runs or confidence intervals.
Older CSV files containing only amortized wall time are not search-latency results and are rejected by this plotter.

## Peak query heap

Run the memory sweep separately from timing and CPU profiling:

```sh
python3 perf/fts/scripts/memory.py /tmp/fts-heap \
  --documents 10000 --connections 1 2 4 8 16 32 --queries 3 --runs 3
python3 perf/fts/scripts/memory.py /tmp/fts-first-heap \
  --documents 10000 --connections 1 4 --state first --queries 1 --runs 3
python3 perf/memory/analyze-dhat.py /tmp/fts-heap/c4/wal-common-run1.dhat.json --modules --top 15
```

This reuses `perf/memory`'s `fts-memory` binary with optimized `bench-profile` builds.
It measures all six query cases under Turso WAL and MVCC, using a fresh process and fixture for each sample.
The default is three queries per connection and three repetitions at each connection count.
Queries execute in synchronized batches, one query per connection, using one Tokio worker per connection.
This does not include the latency runner's sample buffers or independent client loops.
Use `--profile dev` only for correctness checks.

The plotted value is **peak simultaneous live bytes allocated during the query phase**, summed across all threads.
It is not divided by query count and is not cumulative allocated bytes, process RSS, or the complete database heap.
Setup, opening, warm-up, and their existing caches are outside tracking, as is cleanup.
The `first` state skips warm-up, so allocations needed for the first search are included.
The Rust allocator tracker does not measure SQLite's C allocations, so these graphs deliberately exclude SQLite.
Both modes remain read-only during measurement; this is not a test of concurrent MVCC updates.

Outputs include per-connection CSVs, per-run JSON reports and dhat allocation stacks, build/phase logs,
an environment record, and `fts-peak-heap` PNG/PDF/SVG figures in MiB (1 MiB = 1,048,576 bytes).
Tables show median per-run peaks; whiskers span the observed range.
The CSV also records retained bytes, cumulative allocated bytes, and allocation counts for diagnosis.
Allocation tracking changes scheduling and adds overhead; never use its elapsed times as latency or throughput results.

## CPU flamegraphs

Set `FLAMEGRAPH=1` to build with the optional `flamegraph` feature and collect CPU stack samples at 99 Hz.
This uses the repository's `pprof` dependency on Linux or macOS, without `perf`, root access, or cache clearing.
The default build remains optimized `bench-profile` with debug information for function names.
Select the journal modes, query cases, and connection count you want to investigate:

```sh
FLAMEGRAPH=1 bash perf/fts/scripts/run.sh /tmp/fts-cpu \
  --benchmark throughput --targets turso-wal,turso-mvcc --cases common,ranked \
  --documents 10000 --connections 4 --seconds 30 --runs 1
FLAMEGRAPH=1 bash perf/fts/scripts/run.sh /tmp/fts-search-cpu \
  --benchmark search --targets turso-wal --cases common \
  --documents 10000 --connections 8 --queries 10000 --runs 1
```

Each case, engine/mode, and repetition produces an interactive `.svg` flamegraph and a `.folded` file
of stack paths and sample counts under `profiles/`.
Open the SVG in a browser; click a frame to zoom and use Search to find a function.
Frame width shows the fraction of collected CPU samples containing that frame, not elapsed latency.
The horizontal position is not a timeline.
Use the folded counts for aggregate analysis, counting a function only once per stack for inclusive totals.

Sampling starts after fixture creation, index building, reopening, and warm-up.
It stops before fixture cleanup, result validation, and percentile calculation.
Worker startup, query preparation, query execution, result draining, and scheduling during the measured phase can appear.
The sampler covers the process, including worker threads; 99 Hz is not a separate guarantee for each thread.
No-sample runs fail with a request to increase the query count or duration.

Profiled timings go to `profiled-results.csv` with `profiled=true`, and comparison plots reject them.
Run again without `FLAMEGRAPH` to measure an optimization's effect on latency or throughput.
`FLAMEGRAPH=1` also works with `sweep.sh`; it collects profiles at each connection count and skips comparison plots.
Normal comparison plots still require all six cases, so use `--cases` with profiling or direct binary runs.

These profiles show sampled on-CPU work, not time waiting on locks, I/O, or an external request queue.
The profiler excludes libc, libgcc, pthread, and vdso frames on supported architectures to reduce unsafe-unwinding risk;
samples can also be dropped, so percentages describe collected samples rather than complete CPU accounting.
Very short first-query runs rarely provide enough samples.
Compare repeated profiles with the same fixture, case, connections, and build before choosing an optimization.
Windows profiling is not supported; ordinary benchmark builds do not enable the profiler dependency.

## Real-text extensions and limitations

These benchmarks remain synthetic read-only tests, not a production workload or a test of concurrent MVCC updates.
Shared-host noise, cache differences, scheduling overhead, and fixed connection-count order can affect results.
Collect repeated optimized runs on a documented, otherwise idle machine before making release-performance claims.
Keep the CSV, environment file, and build log with the figure, and record storage and compiler overrides separately.

For a real-text suite, reuse the existing FiQA harness rather than inventing another mutation scheduler.
The following are recommendations, not workloads implemented by this binary:

- [BEIR FiQA](https://ir-datasets.com/beir.html#beir/fiqa/test): 57,638 documents and 648 test queries with relevance judgments.
- [BEIR SciFact](https://ir-datasets.com/beir.html#beir/scifact/test): 5,183 documents and 300 test queries for quick real-text regressions.
- [Rally Wikipedia](https://github.com/elastic/rally-tracks/tree/master/wikipedia): existing-document replacements alongside searches, using clickstream-derived article-title queries.
- [Rally PMC](https://github.com/elastic/rally-tracks/tree/master/pmc): 574,199 articles with a half-load followed by concurrent ingestion and search.

Label ports or corpus subsets as adaptations, not original Rally results.
Keep query IDs, dataset hashes, tokenizer choices, and expected document states with real-text results.
For mixed writes, use the same offered operation sequence and rate across engines when making direct comparisons.
Verify visibility and final index contents, and report retrieval quality separately from execution speed.

## Tests

These commands cover both measurement methods, cross-engine query results, and plot input validation.
Smoke runs use small fixtures and counts only to test correctness.
Inspect generated figures before publication.

```sh
cargo test -p fts-benchmark
cargo test -p memory-benchmark --features fts --lib
uv run --with numpy python -m unittest discover -s perf/fts/plot -v
PROFILE=dev bash perf/fts/scripts/run.sh /tmp/fts-search-smoke \
  --benchmark search --documents 203 --connections 3 --queries 17 --runs 1
PROFILE=dev CONNECTIONS="1 4" DOCUMENTS=203 RUNS=1 SECONDS_PER_RUN=0.05 \
  bash perf/fts/scripts/sweep.sh /tmp/fts-throughput-smoke
```
