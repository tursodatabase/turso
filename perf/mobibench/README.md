# Mobibench: SQLite vs Turso

Experimental evaluation comparing SQLite and Turso using
[Mobibench](https://github.com/penberg/Mobibench), inspired by
Purohith et al., "The Dangers and Complexities of SQLite Benchmarking"
(APSys'17).

## Build

Build Turso from the repo root:

```
cargo build --release
```

Clone and build the benchmark binaries:

```
cd perf/mobibench
git clone git@github.com:penberg/Mobibench.git
cd Mobibench/shell
LIBS=sqlite3.c make && mv mobibench mobibench-sqlite3
make clean
LIBS="../../../../target/release/libturso_sqlite3.a -lm" make && mv mobibench mobibench-turso
```

## Run evaluation

From `perf/mobibench/`:

```
./run-eval.sh
```

Runs Insert, Update, and Delete workloads (5 iterations each) under
WAL journal mode + FULL synchronous for both SQLite and Turso.

Results are written to `plot/results.csv`.

Override the number of runs with `RUNS=10 ./run-eval.sh`.

## Generate plot

```
cd plot
uv run python plot.py
```

Produces `plot/mobibench.pdf` — a grouped bar chart comparing
SQLite and Turso across Insert/Update/Delete operations.

## Mobibench parameters

| Flag | Description | Value |
|---|---|---|
| `-d` | DB mode | 0=insert, 1=update, 2=delete |
| `-j` | Journal mode | 3 (WAL) |
| `-s` | Sync mode | 2 (FULL) |
| `-y` | File sync | 2 (fsync) |
| `-n` | Transactions | 1000 |
| `-t` | Threads | 1 |
| `-f` | File size (KB) | 1024 |
| `-r` | Record size (KB) | 4 |

## Manual runs

```
./Mobibench/shell/mobibench-sqlite3 -p mobibench-data -f 1024 -r 4 -a 0 -y 2 -t 1 -d 0 -n 1000 -j 3 -s 2
./Mobibench/shell/mobibench-turso   -p mobibench-data -f 1024 -r 4 -a 0 -y 2 -t 1 -d 0 -n 1000 -j 3 -s 2
```
