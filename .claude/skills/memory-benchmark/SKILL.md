---
name: memory-benchmark
description: How to benchmark and analyze memory usage in Turso using the memory-benchmark crate and dhat heap profiler. Use this skill whenever the user mentions memory usage, memory profiling, allocation tracking, heap analysis, memory regression, memory benchmarking, dhat, or wants to understand where memory is being allocated during SQL workloads. Also use when investigating memory growth in WAL or MVCC mode. Update this skill when you change the perf/memory crate's profiles or flags.
---

# Memory Benchmarking & Analysis

`perf/memory` benchmarks memory use of SQL workloads in WAL and MVCC journal modes. It uses `dhat` as the global allocator (every heap allocation) and `memory-stats` for RSS. Each run writes `dhat-heap.json` to the current directory.

- Crate: `perf/memory/` (docs: `perf/memory/README.md`)
- CodSpeed bench crate: `perf/memory/codspeed/` (allocation tracking in CI)
- dhat analysis: `perf/memory/analyze-dhat.py` (`--help` lists sort metrics and flags)

Use `--release` for byte and peak comparisons. Debug builds are fine for checking that a workload runs. Randomized profiles (`read-heavy`, `mixed`) use a fixed RNG seed, so workloads repeat across runs.

## Running a Workload

```bash
cargo run --release -p memory-benchmark -- --mode wal --workload insert-heavy -i 100 -b 100
cargo run --release -p memory-benchmark -- --mode mvcc --workload mixed -i 100 -b 100 --connections 4
```

Run `--help` for all flags (`--checkpoint`, `--cache-size`, `--format`, and others). Workloads: `insert-heavy`, `read-heavy`, `mixed`, `scan-heavy`, `recursive-cte`, `series-blob`, `update-churn`. `update-churn` creates superseded row versions, which is the MVCC GC case.

MVCC flags `--mvcc-checkpoint-threshold` and `--mvcc-gc-threshold` set the matching PRAGMAs; `-1` disables. To test GC alone, run `update-churn` with `--mvcc-checkpoint-threshold=-1` and compare GC threshold values. Small workloads write less than the default ~4 MB log threshold, so lower it (for example `16384`) to get automatic checkpoints during the run.

## Analyzing dhat Output

```bash
python3 perf/memory/analyze-dhat.py dhat-heap.json --modules --top 20          # which crates dominate
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter mvcc --stacks      # one subsystem with call stacks
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by eb --top 10       # still live at exit
```

Sort keys: `gb` live at global peak (default), `eb` live at exit, `tb` total bytes allocated, `mb` max live per site, `tbk` allocation count.

Workflow for a suspected regression: run the workload, look at `--modules`, drill into the top module with `--filter ... --stacks`, check `--sort-by eb` for leaks, then run the same workload in the other journal mode to see the cost of MVCC versioning.

## MVCC Memory Per Row

`mvcc-row-memory` measures heap that one MVCC transaction keeps per written row (one `INSERT`, `UPDATE` or `DELETE` over every row of `t(id INTEGER PRIMARY KEY, v BLOB)`). Use it for changes to version chains, write sets and tombstones.

```bash
cargo run -p memory-benchmark --bin mvcc-row-memory -- \
  --statement insert|update|delete --rows 100000 --payload-bytes 100 \
  --mode mvcc|wal --dhat-file /tmp/rows.json
python3 perf/memory/analyze-dhat.py /tmp/rows.json --sort-by eb --top 10
```

The byte values are requested bytes, so debug and release builds give the same numbers.

## FTS Query Allocations

`fts-memory` measures allocations of FTS queries. Commands, corpus options and limits are in `perf/memory/README.md`.

```bash
cargo run -p memory-benchmark --features fts --bin fts-memory -- \
  --query common --state warm --documents 10000 --queries 100 --dhat-file /tmp/fts-warm.json
```

## Stack Reports

`stack-report` runs a SQL file with the `stacker` feature and reports stack use per statement and per tracing span. Use `--release`; debug builds overstate stack use.

```bash
cargo run --release -q -p memory-benchmark --features stacker --bin stack-report -- --sql payload.sql --top 40
```

Options: `--sql FILE|-`, `--format human|json|csv`, `--top N`, `--statement N[,N...]`, `--sql-contains TEXT`. Filters only change what is reported; the whole payload still runs in order. The database is a fixed in-memory one.

## CodSpeed Allocation Tracking in CI

`.github/workflows/codspeed-memory.yml` runs every profile in both journal modes under CodSpeed's memory instrument. Benchmarks are in `perf/memory/codspeed/` and named `<mode>/<workload>/<total-ops>`, at 1x/2x/4x scale plus an 8x `-checkpoint` variant that lowers `mvcc_checkpoint_threshold` to 16 KiB and ends with `PRAGMA wal_checkpoint(TRUNCATE)`. WAL auto-checkpoints at 1000 frames.

The bench crate has no `[[bin]]` targets. Cargo builds bins (panic=abort in release) next to benches (panic=unwind), and the duplicated `turso_sdk_kit` units then collide on output filenames.

```bash
cargo bench -p memory-benchmark-codspeed --bench memory_profiles -- --test   # one pass of each benchmark
cargo codspeed build -m memory -p memory-benchmark-codspeed --features codspeed   # what CI runs
```

Plain `cargo bench` samples each full workload, which is slow; add `-- --test` for one pass.

## Adding a Profile

1. Add a profile in `perf/memory/src/profile/` implementing the `Profile` trait (see existing profiles) and register it in that module.
2. Add it to the list of workload profiles and to the function that creates a profile from a workload name, in `perf/memory/src/`.
3. Add it to the workload list and base size in the CodSpeed bench in `perf/memory/codspeed/`, and to the `workload` matrix in `.github/workflows/codspeed-memory.yml`.
