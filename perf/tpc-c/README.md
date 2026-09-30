# TPC-C Benchmark for SQLite and Turso

A TPC-C benchmark harness adapted for SQLite-compatible databases. Supports both
system SQLite3 and [Turso](https://github.com/tursodatabase/turso) (a SQLite
rewrite in Rust), with a comparison script that runs both back-to-back and
generates a report.

## Quick Start

Run the full comparison benchmark:

```bash
./run_bench.sh
```

The script auto-detects how to find turso:

- **Inside the turso repo** (e.g. at `perf/tpc-c/`): uses the parent turso tree directly.
- **Standalone**: clones turso into `tmp-turso/` on first run.

You can also override the turso root explicitly via the Makefile:
```bash
make -C src all TURSO_ROOT=/path/to/turso
```

The script will:
1. Build turso's sqlite3 library (release mode)
2. Build both turso and sqlite3 versions of the benchmark
3. Load TPC-C data and run the benchmark for each
4. Print a side-by-side comparison report

### Options

```
./run_bench.sh [warehouses] [connections] [warmup] [measure] [interval]
```

| Argument    | Default | Description                          |
|-------------|---------|--------------------------------------|
| warehouses  | 1       | Number of TPC-C warehouses           |
| connections | 1       | Number of concurrent threads         |
| warmup      | 5       | Warmup period in seconds             |
| measure     | 30      | Measurement period in seconds        |
| interval    | 1       | Reporting interval in seconds        |

Examples:

```bash
./run_bench.sh 1 1 5 30 1     # 1 warehouse, 1 thread, 30s measurement
./run_bench.sh 4 4 10 60 5    # 4 warehouses, 4 threads, 60s measurement
```

Reports and logs are saved to `results/`.

## Building Individually

Build for turso (default, expects turso at `../tmp-turso` or override with `TURSO_ROOT`):

```bash
cd src && make all
cd src && make all TURSO_ROOT=/path/to/turso   # explicit path
```

Build for system sqlite3:

```bash
cd src && make all BACKEND=sqlite
```

This produces separate binaries: `tpcc_load`/`tpcc_start` (turso) and
`tpcc_load-sqlite`/`tpcc_start-sqlite` (system sqlite3). Each uses its own
database file (`tpcc.db` vs `tpcc-sqlite.db`).

## Running Manually

```bash
# Create schema
sqlite3 tpcc.db < create_table_sqlite.sql
sqlite3 tpcc.db < add_fkey_idx_sqlite.sql

# Load data (1 warehouse)
./tpcc_load -w 1

# Run benchmark
./tpcc_start -w 1 -c 1 -r 5 -l 30 -i 1
```

### tpcc_start options

| Flag | Description                      |
|------|----------------------------------|
| -w   | Number of warehouses             |
| -c   | Number of database connections   |
| -r   | Ramp-up (warmup) time in seconds |
| -l   | Measurement time in seconds      |
| -i   | Report interval in seconds       |
| -t   | Max transactions (optional)      |

## Output

Per-interval lines during measurement:

```
  10, trx: 12920, 95%: 9.483, 99%: 18.738, max_rt: 213.169, 12919|98.778, 1292|101.096, 1293|443.955, 1293|670.842
```

| Field          | Meaning                                              |
|----------------|------------------------------------------------------|
| 10             | Seconds elapsed since measurement start              |
| trx: 12920     | New-Order transactions in this interval              |
| 95%: 9.483     | 95th percentile response time (ms) for New-Order     |
| 99%: 18.738    | 99th percentile response time (ms) for New-Order     |
| max_rt: 213.169| Max response time (ms) for New-Order                 |
| remaining      | Throughput and max RT for Payment, Order-Status, Delivery, Stock-Level |

## Prerequisites

- C compiler (gcc/clang)
- System sqlite3 library and CLI (`libsqlite3-dev`, `sqlite3`)
- Rust toolchain (for building turso)
- Git (for cloning turso on first run)
