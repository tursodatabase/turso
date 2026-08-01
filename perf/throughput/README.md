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

### MVCC checkpoint modes

`write-throughput` supports comparing blocking TRUNCATE vs passive auto-checkpoint under concurrent MVCC writes:

```console
cargo run -p write-throughput -- --mode mvcc-truncate --threads 4 --batch-size 100 -i 100
cargo run -p write-throughput -- --mode mvcc-passive --threads 4 --batch-size 100 -i 100
```

Both modes use `journal_mode=mvcc` and `BEGIN CONCURRENT`. The only difference is `experimental_mvcc_passive_checkpoint` on the database builder.

This will generate:
- `thread-scaling.pdf`: Write throughput vs. number of threads (scalability test)
- `compute-impact.pdf`: How CPU-bound work affects write throughput
