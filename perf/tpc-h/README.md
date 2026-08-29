# TPC-H Benchmarks

Performance comparison of Turso vs SQLite using the [TPC-H](http://www.tpc.org/tpch/) benchmark queries against a 1.2 GB database.

## Prerequisites

- Rust toolchain (for building `tursodb`)
- `sudo` access (for clearing system caches between runs)
- `wget` or `curl` (for downloading the TPC-H database)
- [uv](https://docs.astral.sh/uv/) (for plotting)

## Step 1: Run the benchmarks

From the repo root:

```bash
./perf/tpc-h/benchmark.sh
```

This will:
1. Build `tursodb` in release mode
2. Download the TPC-H database (1.2 GB) if not already present
3. Install a local SQLite3 binary
4. Run all supported TPC-H queries on both Turso and SQLite, clearing system caches between each run

It produces a timestamped results file, e.g. `perf/tpc-h/results_20260216_143000.txt`.

## Step 2: Plot the results

Convert the results file to CSV and generate the figures:

```bash
cd perf/tpc-h/plot
./results2csv.sh ../results_20260216_143000.txt > results.csv
uv run plot-tpch.py results.csv
```

This draws a grouped bar chart of per-query runtime for Limbo and SQLite on
a log scale, in the same style as the `perf/latency` and `perf/throughput`
plots. It always writes `tpch.png`, `tpch.pdf` and `tpch.tikz`; the `.tikz`
is a pgfplots picture to `\input` into a LaTeX document that loads pgfplots
with `\pgfplotsset{compat=1.18}`. A query an engine did not run is marked
`n/a` in the table under the bars.
Use `--name limbo=Turso` to change an engine's legend name.

By default, `results2csv.sh` uses the "WITHOUT ANALYZE" results. To use the "WITH ANALYZE" results instead:

```bash
./results2csv.sh ../results_20260216_143000.txt analyze > results.csv
```
