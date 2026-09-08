# TPC-C benchmark

Runs the TPC-C transaction mix against SQLite and Turso through the SQLite
C API, the same harness built twice, and draws the figures a TPC-C report
has: New-Order throughput against connections and against elapsed time,
and the response time distribution of every transaction type. The harness
is [tpcc-mysql](https://github.com/Percona-Lab/tpcc-mysql) ported to
SQLite.

## Quickstart

```console
./scripts/run.sh
```

That builds Turso's `sqlite3` library and both harness binaries, loads
four warehouses once per engine, runs both engines at 1, 2, 4, 8 and 16
connections, three runs each, and draws the figures. It takes about 40
minutes, most of it the 30 s the drive is left idle before each run, and
asks for sudo before every run to trim the drive and drop the page cache.
You need Rust, a C compiler, the system `sqlite3` library and command, and
`uv` for the plots.

You get:

- `plot/tpmc.png`, `.pdf` and `.tikz`: New-Order transactions per minute
  (tpmC) against connections on the left axis and, on the right axis from
  0 to 100%, the CPU the whole process used as a share of every hardware
  thread, one line per engine for each, every point the mean of the runs
  with an error bar of one standard deviation. The same numbers, with each
  transaction type's 90th percentile response time, are printed as a
  table.
- `plot/timeline-c<connections>.png`, `.pdf` and `.tikz`: the graph TPC-C
  asks for in clause 5.6.5, tpmC in every one-second interval from the
  moment the connections start, ramp-up shaded, the mean of the runs
  inside a band from the slowest run to the fastest, and a dashed line at
  each engine's tpmC over the measured window.
- `plot/response-time-c<connections>.png`, `.pdf` and `.tikz`: the graph
  TPC-C asks for in clause 5.6.1, a panel per transaction type with the
  share of an engine's transactions against their response time, from
  zero to four times the slowest engine's 90th percentile, and a dashed
  line at each engine's 90th percentile.
- `plot/<engine>-c<connections>-r<run>-result.csv`: one row per run with
  everything the summary says: tpmC, wall time, CPU time, and for every
  transaction type its count, how many missed TPC-C's response time
  limit, retries, failures, and the mean, 90th percentile and maximum
  response time.
- `plot/<engine>-c<connections>-r<run>-timeline.csv`: one row per
  interval, ramp-up included: transactions of each type finished in it,
  New-Order's 95th and 99th percentile response time, and each type's
  slowest transaction.
- `plot/<engine>-c<connections>-r<run>-hist.csv`: the response time
  histogram of the measured transactions, 10 µs buckets, one row per
  non-empty bucket.
- `plot/bench.log`, and the terminal: the machine, the settings, and every
  run's output.
- `db/<timestamp>/`: each engine's loaded database and every run's copy
  of it. Nothing is ever deleted.

Every setting is an environment variable: `WAREHOUSES`, `CONNS`, `REPEATS`,
`WARMUP`, `MEASURE`, `INTERVAL`, `IDLE`, and `OUT` and `DB_DIR` for where
the files go. `CONNS="1 8" REPEATS=1 IDLE=0 ./scripts/run.sh` is a quick
look. `scripts/bench.sh` runs the benchmark without drawing, and
`plot/plot-tpcc.py` draws any set of result files; its docstring says how.

## Methodology

**Workload.** TPC-C's nine tables and five transactions, New-Order,
Payment, Order-Status, Delivery and Stock-Level, in the standard 45:43:4:4:4
mix, on a database of `WAREHOUSES` warehouses (4). Every connection picks a
home warehouse at random for each transaction. The New-Order, Payment and
Delivery transactions write; Order-Status and Stock-Level only read.

**Load.** Closed loop with no keying or think time: each connection
starts its next transaction the moment the previous one finishes, so the
number of connections is the number of transactions in flight, and the
engine is asked for as much as it can do. TPC-C's tpmC is the number of
New-Order transactions completed per minute over the measured window,
which starts after `WARMUP` seconds (5) and lasts `MEASURE` seconds (30).
Response time is measured from before `BEGIN` to after `COMMIT`, and a
transaction that fails is rolled back and retried, the retry counted in its
response time.

**Durability.** Both engines run in WAL mode with `PRAGMA synchronous =
NORMAL`, so a commit is durable against a process crash but not against
power loss until the next checkpoint.

**Transactions.** A transaction that writes begins with `BEGIN IMMEDIATE`,
so it takes the write lock up front and a connection that finds the lock
held waits for it, up to 60 s, with the busy handler. A deferred `BEGIN`
would take the lock at the first write and, when another connection had
committed in the meantime, fail at once without waiting. Read-only
transactions begin with a plain `BEGIN`. Every statement is prepared once
per connection.

**Between runs.** Each engine's database is loaded once and every run
starts from a copy of it. Before each run the drive is told which blocks
are free and left idle for `IDLE` seconds (30) so a consumer SSD can drain
its write cache, and the page cache is dropped so every run starts cold.
Odd runs walk the connection counts upwards, even ones downwards, so an
effect of the order shows up as a difference between them.

## Building and running by hand

```console
make -C src all BACKEND=turso     # tpcc_load-turso and tpcc_start-turso
make -C src all BACKEND=sqlite    # tpcc_load-sqlite and tpcc_start-sqlite
```

The Turso build links `libturso_sqlite3` from `target/release`; build it
with `cargo build -p turso_sqlite3 --release`, or pass `TURSO_LIB` if it
is somewhere else. Then create the schema with the `sqlite3` command, load
the data, and run:

```console
sqlite3 tpcc-turso.db < create_table_sqlite.sql
sqlite3 tpcc-turso.db < add_fkey_idx_sqlite.sql
./tpcc_load-turso -w 1 -d tpcc-turso.db
./tpcc_start-turso -w 1 -c 1 -r 5 -l 30 -i 1 -d tpcc-turso.db -o results/turso-c1-r1
```

| Flag | Description                                                 |
|------|-------------------------------------------------------------|
| -w   | Number of warehouses                                        |
| -c   | Number of connections                                       |
| -r   | Ramp-up time in seconds                                     |
| -l   | Measurement time in seconds                                 |
| -i   | Report interval in seconds                                  |
| -d   | Database file (default `tpcc-<engine>.db`)                  |
| -o   | Prefix of the `-result.csv`, `-timeline.csv` and `-hist.csv` files; without it nothing is written |
| -t   | Stop after this many transactions per connection            |

During the run one line per interval says how many transactions of each
type finished in it and the slowest of them, and the summary at the end
says whether the mix and the 90th percentile response times meet TPC-C's
requirements, then the tpmC.
