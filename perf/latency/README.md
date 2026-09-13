# Transaction latency benchmark

Measures how long a small write transaction takes in SQLite and in Turso,
from the moment it was due to the moment it commits, and plots the whole
distribution. Transactions arrive on a fixed schedule that does not slow
down when the database does, so time spent queued behind a stalled writer
counts. More connections means more transactions in flight at the same
offered rate, which is where SQLite's one-writer-at-a-time shows.

## Quickstart

```console
./scripts/run.sh
```

That builds the harness, runs both engines at 1, 8, 16 and 32 connections,
three runs each at 1000 transactions/s, and draws the figure. It takes
about an hour, most of it the minute the drive is left idle before each
run, and asks for sudo before every run to trim the drive and drop the
page cache. You need Rust, `uv` for the plot, and Linux for the io_uring
backend.

You get:

- `plot/latency-ecdf.png`, `.pdf` and `.tikz`: one eCDF panel per
  connection count, SQLite against Turso, with markers at p50 and p90 and a
  line at p99.9. The `.tikz` is a pgfplots picture to `\input` into a paper.
- `plot/<engine>-c<N>-r<i>.csv`: one file per run, one row per
  transaction: when it was due, whether it was warmup, how many times it
  restarted, and its queue, begin, work and commit time.
- `plot/<engine>-c<N>-r<i>-checkpoints.csv`: when each checkpoint of that
  run started and how long it took.
- `plot/bench.log`, and the terminal: every run's summary: percentiles, the
  run's own CPU use, what the disk under the database did, and checkpoint
  timings.
- `db/<timestamp>/`: every run's database file. Nothing is ever deleted.

## Methodology

**Workload.** One table, `test_table(id INTEGER PRIMARY KEY, data TEXT)`.
Every transaction inserts `BATCH_SIZE` rows (10) with ids drawn from one
counter shared by all connections, so transactions never touch the same
row and the only thing they contend for is the engine's own write path.
`BEGIN`, the `INSERT` and `COMMIT` are prepared once per connection, so
parsing is not charged to the transaction.

**Load.** Open loop: the arrival times are fixed before the run starts,
a transaction is due at its arrival time whether or not a connection is
free, and whichever connection is free takes the next due one. Latency
is measured from the due time, so time queued behind a stalled writer
lands in the sample instead of being omitted. Arrivals are a Poisson
process at `RATE` by default (exponential gaps from a seeded generator,
so every engine and repeat sees the same schedule); `ARRIVALS=fixed`
spaces them exactly `1/RATE` apart instead. The rate is the total over
all connections; more connections means more transactions in flight,
not more load. The first `WARMUP` seconds are recorded but flagged, and
left out of every summary and figure. Each sample carries its due time,
how many times it restarted, and its queue (waiting for a connection),
begin, work (the inserts) and commit time.

**Durability.** Both engines run with `PRAGMA synchronous = FULL`, so every
commit ends in an fsync and the comparison is between two durable commit
paths, not between two durability settings.

**SQLite.** WAL mode, one connection per OS thread through `rusqlite`, and
`BEGIN IMMEDIATE`. A deferred `BEGIN` would take the write lock at the
first `INSERT` instead, and when another connection has committed in the
meantime that upgrade fails at once with `SQLITE_BUSY_SNAPSHOT` and the
transaction has to be rolled back and retried by the application. Taking
the lock up front puts the wait in the `begin` phase, where SQLite's busy
handler waits it out, and it is what SQLite documents for applications
with several writers. The busy timeout is 60 s with the default busy
handler.

**Turso.** MVCC (`PRAGMA journal_mode = mvcc`), one connection per OS
thread with its own tokio runtime, `BEGIN CONCURRENT` and the io_uring
backend on Linux. Concurrent transactions do not block each other; they
serialize only at commit, where the log record is appended and fsynced.
A transaction that finds its snapshot stale restarts, and restarts count
towards its latency; with disjoint rows there are none, and the summary
reports how many happened.

**Checkpointing.** Both engines checkpoint the way a server does: the
writer's own auto-checkpoint is off (`wal_autocheckpoint = 0` in SQLite,
`mvcc_checkpoint_threshold = -1` in Turso) and a separate connection runs
`PRAGMA wal_checkpoint(PASSIVE)` every `CHECKPOINTER` milliseconds
(1000). Turso uses its passive checkpoint mode, which drains committed
versions into the B-tree without blocking writers. Every checkpoint's
duration is reported, so a writer stall can be matched against the
checkpoint that caused it.

**Runs.** Every run starts from a new, empty database file of its own
and measures 60 s after a 5 s warmup. Before each run the drive is told
which blocks are free (`fstrim`) and left idle for `IDLE` seconds (60),
because a consumer SSD drains its write cache and collects garbage while
idle and would otherwise carry the previous runs' backlog into this one;
then the page cache is dropped. Each engine and connection count is run
`REPEATS` times (3). Every run writes its own samples file, and its
summary, including the process's own CPU use and what the disk under
the database did, goes to stderr and to `plot/bench.log`.

## Configuring the benchmark

Every setting is an environment variable read by `scripts/bench.sh`, which
`run.sh` calls, so both take them:

| Variable | Default | Meaning |
|---|---|---|
| `RATE` | `1000` | Transactions offered per second, the total across all connections |
| `CONNECTIONS` | `"1 8 16 32"` | Connection counts to run, one panel each |
| `REPEATS` | `3` | Runs per engine and connection count; every run writes its own CSV |
| `IDLE` | `60` | Seconds the drive is left idle before each run, after `fstrim`, so a run does not inherit the write backlog of the runs before it |
| `DURATION` | `60` | Seconds measured per run |
| `WARMUP` | `5` | Seconds run before measuring starts |
| `BATCH_SIZE` | `10` | Rows inserted per transaction |
| `CHECKPOINTER` | `1000` | Milliseconds between checkpoints from a separate connection; `0` lets each writer checkpoint itself |
| `ARRIVALS` | `poisson` | `poisson` spaces arrivals with exponential gaps around `1/RATE`; `fixed` puts them exactly `1/RATE` apart |
| `SEED` | `1` | Seed of the poisson schedule; the same seed gives every run the same arrivals |
| `OUT` | `plot/` | Where the CSVs and figures go; a run refuses to overwrite a CSV that exists |
| `DB_DIR` | `db/<timestamp>/` | Where the database files go; every run gets its own file there |

A quick look, one run of 20 seconds at two connection counts:

```console
CONNECTIONS="1 8" REPEATS=1 DURATION=20 ./scripts/run.sh
```

`turso-txn-latency --help` lists the harness's own options, including `--mode
immediate` to run Turso with WAL and `BEGIN IMMEDIATE` like SQLite, and
`--io syscall`. The summary warns when a run could not keep up with the
offered rate; a curve from such a run shows how far behind it fell, not
its latency.
