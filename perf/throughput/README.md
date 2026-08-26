# Turso throughput benchmark

A single `write-throughput` binary that runs the same write workload against
either engine, picked with `--engine`:

```console
cargo run --release -p write-throughput -- --engine turso --mode concurrent -t 4 -i 1000
cargo run --release -p write-throughput -- --engine sqlite -t 4 -i 1000
```

Each thread commits `--batch-size` rows per transaction, `--iterations` times.
Both engines write with `journal_mode=WAL`/`mvcc`, `synchronous=FULL` and, on
macOS, `fullfsync=true`, so a commit costs the same on both sides.

The binary prints one CSV row per run:

```text
system,mode,threads,batch_size,compute,throughput
```

`throughput` is inserted rows per second.

## Running a sweep

`scripts/bench.sh` sweeps both engines and writes one CSV:

```console
./scripts/bench.sh > plot/throughput.csv
```

The sweep is controlled by environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `ENGINES` | `turso sqlite` | engines to run |
| `THREADS` | `1 2 3 4` | thread counts to sweep |
| `COMPUTE` | `0 100 500 1000` | per-transaction compute time (us) |
| `REPEATS` | `1` | how many times to repeat the whole sweep |
| `BATCH_SIZE` | `100` | rows per transaction |
| `ITERATIONS` | `1000` | transactions per thread |
| `MODE` | `concurrent` | Turso transaction mode |

## Plotting

```console
cd plot
uv run plot-throughput.py throughput.csv
uv run plot-thread-scaling.py throughput.csv
uv run plot-compute-impact.py throughput.csv
```

This will generate:
- `throughput.png`: Write throughput vs. number of threads, as a line plot
- `thread-scaling.pdf`: Write throughput vs. number of threads, as a bar chart
- `compute-impact.pdf`: How CPU-bound work affects write throughput

A thread-scaling plot only needs `compute=0`, so the sweep can be cut down and
repeated instead to get a stable median:

```console
COMPUTE=0 REPEATS=5 ./scripts/bench.sh > plot/throughput.csv
cd plot && uv run plot-throughput.py throughput.csv
```

`plot-throughput.py` takes the median of repeated runs of the same
configuration and prints the run configuration under the plot. Use
`--max-threads`, `--compute`, `-o/--output`, and `--x-label` to narrow or
relabel it. The plot scripts still accept several CSVs, so separately collected
runs can be passed together.

## MVCC checkpoint modes

`--mode` also selects between blocking TRUNCATE and passive auto-checkpoint
under concurrent MVCC writes:

```console
cargo run --release -p write-throughput -- --mode mvcc-truncate --threads 4 --batch-size 100 -i 100
cargo run --release -p write-throughput -- --mode mvcc-passive --threads 4 --batch-size 100 -i 100
```

Both modes use `journal_mode=mvcc` and `BEGIN CONCURRENT`. The only difference is `experimental_mvcc_passive_checkpoint` on the database builder.
