---
name: memory-benchmark
description: How to benchmark and analyze memory usage in Turso using the memory-benchmark crate and dhat heap profiler. Use this skill whenever the user mentions memory usage, memory profiling, allocation tracking, heap analysis, memory regression, memory benchmarking, dhat, or wants to understand where memory is being allocated during SQL workloads. Also use when investigating memory growth in WAL or MVCC mode. IMPORTANT - If you modify the perf/memory crate (add profiles, change CLI flags, change output format, etc.), update this skill document to reflect those changes so it stays accurate for future agents.
---

# Memory Benchmarking & Analysis

The `perf/memory` crate benchmarks memory usage of SQL workloads under WAL and MVCC journal modes. It uses `dhat` as the global allocator to track every heap allocation, and `memory-stats` for process-level RSS snapshots.

It also contains a `stack-report` helper binary for stack-usage investigations.
That binary runs a SQL payload with the `stacker` feature enabled and captures
`turso_stack` tracing events in-process, aggregating structured tracing fields
instead of parsing stderr log text.

## Location

- Benchmark crate: `perf/memory/`
- Analysis script: `perf/memory/analyze-dhat.py`
- dhat output: `dhat-heap.json` (written to CWD after each run)

## Running Stack Reports

Use this when investigating stack usage from SQL translation/execution probes.
Run stack reports in release mode with `--features stacker` when comparing
against server logs or CI stack-size output. Debug builds can materially
overstate stack deltas and should only be used for quick local iteration.

```bash
cargo run --release -q -p memory-benchmark --features stacker --bin stack-report -- \
  --sql path/to/payload.sql \
  --top 40
```

Useful options:

```bash
--sql FILE|-             # SQL payload, or stdin with -
--format human|json|csv  # output format
--top N                  # aggregate/span rows per statement in human output
```

The report is statement-oriented. For each SQL statement, it records the
remaining stack before execution, the minimum remaining stack sampled while that
statement ran, and `stack_used = baseline_remaining_stack - min_remaining_stack`.
Statements are sorted by `stack_used` descending so the worst SQL statements are
first. The human report also prints global and per-statement span aggregates
sorted by `total_inclusive_stack_used` descending. These aggregate rows group by
`label` plus `detail` and include call count, total/max self stack, total/max
inclusive stack, max cumulative stack at span entry, and `peak_path_hits` for
spans that were active at the statement's minimum remaining-stack sample.

Within each statement, raw span rows are still sorted by `stack_used`
descending, with the original tracing emission sequence kept in the
`trace_sequence` field (`seq` in human output). Raw span rows include
`inclusive_stack_used`, which is measured from the span's parent stack level down
to the deepest sampled remaining stack while the span was active. This is an
inclusive profiler-style metric, so nested spans intentionally overlap; use it
for ranking likely contributors, not for summing to statement total stack.

JSON and CSV formats are deterministic and intended for comparing runs. CSV
uses a `row_type` column with `global_aggregate`, `statement_aggregate`, `span`,
and `statement` rows.

`stack-report` splits payloads with `turso_parser::parser::Parser::next_cmd()`.
It then executes statements with no result columns, and queries and drains
row-producing statements. Do not change binding `execute_batch` semantics for
stack reports.

The runner currently uses a fixed in-memory database and enables generated
columns, custom types, and materialized views internally. There are no stack
report CLI flags for selecting the database path or toggling those experimental
features.

## Running Benchmarks

Always run in release mode — debug builds have wildly different allocation patterns and the results are not representative of real-world usage.

```bash
# Basic: single connection, WAL mode, insert-heavy workload
cargo run --release -p memory-benchmark -- --mode wal --workload insert-heavy -i 100 -b 100

# MVCC with concurrent connections
cargo run --release -p memory-benchmark -- --mode mvcc --workload mixed -i 100 -b 100 --connections 4

# Run a final checkpoint after the workload
cargo run --release -p memory-benchmark -- --mode wal --workload read-heavy --checkpoint

# All CLI options
cargo run --release -p memory-benchmark -- \
  --mode wal|mvcc \
  --workload insert-heavy|read-heavy|mixed|scan-heavy|series-blob \
  -i <iterations> \
  -b <batch-size> \
  --connections <N> \
  --checkpoint \
  --timeout <ms> \
  --cache-size <pages> \
  --format human|json|csv
```

Every run produces a `dhat-heap.json` in the current directory. This file contains per-allocation-site data for the entire run.

## Built-in Workload Profiles

| Profile | Description | Setup |
|---------|-------------|-------|
| `insert-heavy` | 100% INSERT statements | Creates table |
| `read-heavy` | 90% SELECT by id / 10% INSERT | Seeds 10k rows |
| `mixed` | 50% SELECT / 50% INSERT | Seeds 10k rows |
| `scan-heavy` | Full table scans with LIKE | Seeds 10k rows |
| `series-blob` | `INSERT INTO bench(data) SELECT zeroblob(2048) FROM generate_series(1, ?)` | Creates `bench`; `batch-size` is the series length |

Profiles implement the `Profile` trait in `perf/memory/src/profile/`. To add a new workload, create a new file implementing the trait and wire it into the `WorkloadProfile` enum in `main.rs`.

## Understanding the Output

The benchmark reports three categories of metrics:

### RSS (process-level)
Measured via `memory-stats` crate. Includes everything: heap, mmap'd files (WAL, DB pages pulled into OS page cache), tokio runtime, etc. Snapshots are taken at phase transitions (setup -> run) and after each batch.

- **Baseline**: RSS before any DB work (runtime overhead)
- **Peak**: Highest RSS observed during the run
- **Net growth**: Final RSS minus baseline — the memory attributable to the workload

### Heap (dhat)
Precise allocation tracking via the `dhat` global allocator. Only counts explicit heap allocations (malloc/alloc), not mmap.

- **Current**: Bytes still allocated at measurement time
- **Peak**: Highest simultaneous live allocation during the entire run
- **Total allocs**: Number of individual allocation calls
- **Total bytes**: Cumulative bytes allocated (includes freed memory) — measures allocation pressure

### Disk
File sizes after the benchmark completes:
- **DB file**: The `.db` file
- **WAL file**: The `.db-wal` file (WAL mode only)
- **Log file**: The `.db-log` file (MVCC logical log only)

## Analyzing dhat Output

After running a benchmark, use the analysis script to produce a readable report from `dhat-heap.json`:

```bash
# Overview: top allocation sites by bytes live at global peak
python3 perf/memory/analyze-dhat.py dhat-heap.json --top 15 --modules

# Focus on a specific subsystem
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter mvcc --stacks
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter btree --stacks
python3 perf/memory/analyze-dhat.py dhat-heap.json --filter page_cache --stacks

# Sort by different metrics
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by eb  # bytes at exit (leaks)
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by tb  # total bytes (pressure)
python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by mb  # max live bytes per site

# JSON output for programmatic use
python3 perf/memory/analyze-dhat.py dhat-heap.json --json
```

### Sort Metrics

| Flag | Metric | Use when |
|------|--------|----------|
| `gb` | Bytes live at global peak (default) | Finding what dominates memory at the high-water mark |
| `eb` | Bytes live at exit | Finding memory leaks or things that never get freed |
| `tb` | Total bytes allocated | Finding allocation pressure hotspots (GC churn) |
| `mb` | Max bytes live per site | Finding per-site high-water marks |
| `tbk` | Total allocation count | Finding chatty allocators (many small allocs) |

### Analysis Flags

- `--top N` — Show top N sites (default 15)
- `--filter PATTERN` — Filter to sites/stacks containing substring (e.g. `mvcc`, `btree`, `wal`, `pager`)
- `--stacks` — Show full callstacks for top allocation sites
- `--modules` — Aggregate by crate/module for a high-level breakdown
- `--json` — Machine-readable aggregated output

## Typical Workflow

When investigating memory usage or a suspected regression:

1. **Run the benchmark** with parameters matching the scenario:
   ```bash
   cargo run -p memory-benchmark -- --mode mvcc --workload mixed -i 500 -b 100 --connections 4
   ```

2. **Get the high-level picture** — which modules use the most memory:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --modules --top 20
   ```

3. **Drill into the hot module** — e.g. if `turso_core` dominates:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --filter turso_core --stacks --top 10
   ```

4. **Check for leaks** — anything still alive at exit that shouldn't be:
   ```bash
   python3 perf/memory/analyze-dhat.py dhat-heap.json --sort-by eb --top 10
   ```

5. **Compare modes** — run the same workload under WAL and MVCC and compare the reports to see the memory cost of MVCC versioning.

## Concurrency Details

When `--connections > 1`:
- Setup phase (schema creation, seeding) always runs on a single connection sequentially
- Run phase spawns one tokio task per connection, each executing its batch concurrently
- `--checkpoint` adds a final single-connection `PRAGMA wal_checkpoint(TRUNCATE)` phase after the run phase
- Each connection gets `busy_timeout` set (default 30s, configurable via `--timeout`)
- WAL mode uses `BEGIN`, MVCC uses `BEGIN CONCURRENT`
- The `Profile` trait's `next_batch(connections)` returns one batch per connection with non-overlapping row IDs

## Adding a New Profile

1. Create `perf/memory/src/profile/your_profile.rs` implementing the `Profile` trait
2. Add `pub mod your_profile;` to `perf/memory/src/profile/mod.rs`
3. Add a variant to `WorkloadProfile` enum in `main.rs`
4. Wire it into `create_profile()` in `main.rs`

The `Profile` trait:
```rust
pub trait Profile {
    fn name(&self) -> &str;
    fn next_batch(&mut self, connections: usize) -> (Phase, Vec<Vec<WorkItem>>);
}
```

Return `Phase::Setup` for schema/seeding (single batch), `Phase::Run` for measured work (one batch per connection), `Phase::Done` when finished.

## Keeping This Skill Up to Date

This skill document is the source of truth for how agents use the memory benchmark tooling. If you modify the `perf/memory` crate — adding profiles, changing CLI flags, altering output format, updating the analysis script, changing the `Profile` trait, etc. — update this SKILL.md to match. Specifically:

- New CLI flags: add to the "Running Benchmarks" section
- New profiles: add to the "Built-in Workload Profiles" table
- Changed output metrics: update the "Understanding the Output" section
- New analyze-dhat.py flags or sort metrics: update the "Analyzing dhat Output" section
- Changed `Profile` trait signature: update "Adding a New Profile"

Future agents rely on this document being accurate. Stale instructions cause wasted work.
