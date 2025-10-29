# Turso throughput benchmark

This directory contains Turso throughput benchmark.

First, run the benchmarks:

```console
cd rusqlite
./scripts/bench.sh > ../plot/sqlite.csv

cd turso
./scripts/bench.sh > ../plot/turso.csv
```

Then, generate the plots:

```console
cd plot
uv run plot-thread-scaling.py turso.csv sqlite.csv
uv run plot-compute-impact.py turso.csv sqlite.csv
```

This will generate:
- `thread-scaling.pdf`: Write throughput vs. number of threads (scalability test)
- `compute-impact.pdf`: How CPU-bound work affects write throughput
