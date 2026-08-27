# Transaction latency benchmark

Measures how long a write transaction takes, from the moment it should have
started to the moment it commits, for SQLite and Turso under the same load.

A pool of connections runs small write transactions against one database.
Transactions arrive on a fixed schedule set by `--rate`, and that schedule does
not slow down when the database does.

## Running

Pick a rate both engines can sustain, otherwise you are measuring queue growth
rather than transaction latency:

```console
./scripts/saturation.sh
```

That prints the closed-loop throughput of each engine. Set `--rate` comfortably
below the lower of the two, then collect samples:

```console
RATE=200 ./scripts/bench.sh
```

Then plot:

```console
cd plot
uv run plot-latency-ecdf.py sqlite.csv turso.csv
```

The benchmark warns on stderr when an engine could not keep up with the offered
rate. Believe that warning: a curve from a saturated engine says how far behind
it fell during that particular run, not what its latency is.

## Coordinated omission

Timing a transaction from the moment a connection got around to starting it
hides the interesting part. A database that blocks writers is not asked for work
while it is busy, so the transactions it stalled never appear in the numbers,
and the tail looks clean.

Instead, transaction `k` is due at `start + k / rate` whether or not a connection
is free. The connection that picks it up records latency from that due time, so
the wait a caller experienced behind an earlier writer is part of the sample.
`--closed-loop` turns this off and measures service time only, which is what the
throughput benchmark wants and what a latency benchmark should not report.

## Options

| Flag | Meaning |
|---|---|
| `--engine` | `sqlite` or `turso` |
| `--mode` | `immediate` (WAL, `BEGIN IMMEDIATE`) or `concurrent` (MVCC, `BEGIN CONCURRENT`, passive checkpointing). Turso defaults to `concurrent`, SQLite only has `immediate` |
| `--rate` | Transactions offered per second |
| `--connections` | Connections serving the arrivals |
| `--batch-size` | Rows inserted per transaction |
| `--duration`, `--warmup` | Seconds measured and seconds discarded |
| `--closed-loop` | Offer the next transaction only when the previous one finishes |
| `--max-overrun` | Give up once the run takes this many times longer than planned |
| `--io` | Turso IO backend, `syscall` (default) or `io_uring` (Linux only). SQLite ignores it |

Both engines run with `synchronous = FULL`, so every commit pays for an fsync
and the comparison is between the durability paths rather than between two
different durability settings.

## Output

One CSV row per transaction on stdout, and a summary on stderr:

```
engine,mode,connections,thread_id,queue_ns,begin_ns,work_ns,commit_ns,total_ns
```

`total_ns` is the latency a caller saw, from due time to commit. It splits into
`queue_ns` (waiting for a free connection), `begin_ns` (waiting for the
transaction to start, which for a blocking writer is the wait for the write
lock), `work_ns` (the inserts), and `commit_ns` (the commit itself, mostly
fsync). Transactions that had to restart on a stale snapshot carry every attempt
in `total_ns`, while the phase columns describe the attempt that succeeded, so
`total_ns` can exceed the sum of the parts.
