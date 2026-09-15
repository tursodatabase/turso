# Concurrent write throughput benchmark

Measures how many small write transactions per second SQLite and Turso
commit as the number of connections writing at once grows, and what each
transaction costs in CPU. Every connection runs its transactions back to
back, so the number of connections is the number of transactions in
flight, and the sweep goes well past the core count, which is where one
writer at a time shows.

## Quickstart

```console
./scripts/run.sh
```

That builds the harness, runs both engines at 1, 2, 4, 8, 16, 32 and 64
connections, three runs each, and draws the figure. It takes about an hour,
half of it the 30 s the drive is left idle before each run, and asks for
sudo before every run to trim the drive and drop the page cache. You need Rust, `uv`
for the plots, and Linux for the io_uring backend.

You get:

- `plot/throughput.png`, `.pdf` and `.tikz`: transactions per second
  against connections on the left axis and, on the right axis from 0 to
  100%, the CPU the whole process used as a share of every hardware
  thread, one line per engine for each, every point the mean of the runs
  with an error bar of one standard deviation. Throughput is never shown
  without its cost.
- `plot/<engine>-c<connections>-r<run>-result.csv`: one row per run
  with everything the summary says: transactions and rows per second,
  service time percentiles, CPU, disk and checkpoint figures.
- `plot/<engine>-c<connections>-r<run>.csv`: every transaction of
  the run, with when it started, whether it was warmup, how many times it
  restarted, and its begin, work and commit time.
- `plot/<engine>-c<connections>-r<run>-timeline.csv`: one row per second of
  the run: transactions committed and CPU used in that second.
- `plot/<engine>-c<connections>-r<run>-checkpoints.csv`: when each
  checkpoint started and how long it took.
- `plot/bench.log`, and the terminal: the machine, the settings, and every
  run's summary.
- `db/<timestamp>/`: every run's database file. Nothing is ever deleted.

## Methodology

**Workload.** One table, `test_table(id INTEGER PRIMARY KEY, data TEXT)`.
Every transaction inserts `BATCH_SIZE` rows (100) with ids drawn from one
counter shared by all connections, so transactions never touch the same row
and the only thing they contend for is the engine's own write path. The
data carries the run number and the id, so no two runs write the same
bytes. `BEGIN`, the `INSERT` and `COMMIT` are prepared once per connection.

**Load.** Closed loop: each connection starts its next transaction the
moment the previous one commits, so the number of connections is exactly
the number of transactions in flight, and the engine is asked for as much
as it can do. Throughput is the number of transactions that started inside the
measured window, divided by the window: the same wall time for every
engine and setting, steady state, checkpoints included. Each transaction's
service time is recorded too.

**Durability.** Both engines run with `PRAGMA synchronous = FULL`, so every
commit ends in an fsync.

**SQLite.** WAL mode, every connection on its own thread through
`rusqlite`, and `BEGIN IMMEDIATE`. A deferred `BEGIN` would take the write
lock at the first `INSERT` and, when another connection had committed in the
meantime, fail at once with `SQLITE_BUSY_SNAPSHOT`; taking the lock up
front makes the wait SQLite's busy handler's business, which is what
SQLite documents for applications with several writers. This is the best
configuration SQLite offers for several writers; that they still take
turns is the architecture, not the setup.

**Turso.** MVCC (`PRAGMA journal_mode = mvcc`), every connection on its
own thread with its own tokio runtime, `BEGIN CONCURRENT` and the
io_uring backend on Linux. Concurrent transactions do not block each
other; they serialize only at commit. A transaction that finds its
snapshot stale restarts, and restarts count towards its service time;
with disjoint rows there are none, and the results say how many there
were.

**Checkpointing.** Both engines checkpoint the way a server does: the
writers' auto-checkpoint is off and a separate connection runs
`PRAGMA wal_checkpoint(PASSIVE)` every `CHECKPOINTER` milliseconds (1000).
Turso uses its passive checkpoint mode. Every checkpoint's duration is
recorded.

**Cost.** Once a second through every run, the process's own user and
system CPU time, Turso's io_uring polling thread included, is sampled
and recorded as a share of every hardware thread on the machine; the
summary also gives it per transaction, along with what the disk under
the database did. A throughput number without its CPU cost is not a
result, so the figure carries the CPU utilization next to the throughput.

**Runs.** Every run starts from a new, empty database file of its own and
measures 30 s after a 3 s warmup. Before each run the drive is told which
blocks are free (`fstrim`) and left idle for `IDLE` seconds (30), then the
page cache is dropped. Each configuration is run `REPEATS` times (3); odd
repeats walk the connection counts upwards and even ones downwards, so an
effect of the order shows up as a difference between them. When a run is
done, the table is counted through a fresh connection and the count has
to match what was committed.

## Configuring the benchmark

Every setting is an environment variable read by `scripts/bench.sh`, which
`run.sh` calls, so both take them:

| Variable | Default | Meaning |
|---|---|---|
| `CONNS` | `"1 2 4 8 16 32 64"` | Connection counts to run |
| `REPEATS` | `3` | Runs per configuration; every run writes its own files |
| `IDLE` | `30` | Seconds the drive is left idle before each run, after `fstrim` |
| `DURATION` | `30` | Seconds measured per run |
| `WARMUP` | `3` | Seconds run before measuring starts |
| `BATCH_SIZE` | `100` | Rows inserted per transaction |
| `CHECKPOINTER` | `1000` | Milliseconds between checkpoints from a separate connection; `0` lets each writer checkpoint itself |
| `OUT` | `plot/` | Where the results and figures go; a run refuses to overwrite files that exist |
| `DB_DIR` | `db/<timestamp>/` | Where the database files go; every run gets its own file there |

A quick look, two runs of 10 seconds at three connection counts:

```console
CONNS="1 8 64" REPEATS=2 DURATION=10 IDLE=5 ./scripts/run.sh
```

`txn-throughput --help` lists the harness's own options, including
`--mode immediate` to run Turso with WAL and `BEGIN IMMEDIATE` like SQLite,
and `--io syscall`.
