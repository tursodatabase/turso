# TATP benchmark

The Telecom Application Transaction Processing (TATP) benchmark, run
against SQLite and Turso. TATP models the Home Location Register database
of a mobile network: four tables of subscribers and their services, and
seven short transactions that read and change them, 80% reads and 20%
writes. Every client runs them back to back as fast as the database lets
it, and the result is the Mean Qualified Throughput (MQTh): transactions
completed per second. TATP was formerly known as TM1 and Network Database
Benchmark.

## Quickstart

```console
./scripts/run.sh
```

That builds the harness, runs both engines against 10,000, 100,000 and
1,000,000 subscribers at 1, 4 and 16 connections, three runs each, and
draws the figures. It takes about two hours, most of it the 70 s each run
warms up and measures and the 30 s the drive is left idle before it. It
asks for sudo once, at the start, to trim the drive and drop the page
cache before every run. You need Rust, `uv` for the plots, and Linux for
the io_uring backend.

You get:

- `plot/mqth.png`, `.pdf` and `.tikz`: MQTh against the number of
  subscribers, one panel per connection count and one bar per engine,
  every bar the mean of the runs with an error bar of one standard
  deviation and labelled with its value. The panel for one connection is
  the figure the SQLite VLDB paper draws for TATP (Gaffney et al., "SQLite:
  Past, Present, and Future", PVLDB 15, Figure 2).
- `plot/response-times.png` and `.pdf`: one panel per transaction, its
  median and 99th percentile response time against connections, one line
  per engine, for the middle database size.
- `plot/<engine>-s<subscribers>-c<connections>-r<run>-result.csv`: one row
  per run with everything the summary says: MQTh, response time
  percentiles, CPU, disk and checkpoint figures.
- `plot/<engine>-s<subscribers>-c<connections>-r<run>-transactions.csv`:
  one row per transaction kind: how many completed, found their rows, were
  rejected or restarted, and their response time percentiles.
- `plot/<engine>-s<subscribers>-c<connections>-r<run>.csv`: every
  transaction of the run, with when it started, its kind, whether it was
  warmup, how it ended, how many times it restarted and its response time.
- `plot/<engine>-s<subscribers>-c<connections>-r<run>-timeline.csv`: one
  row per second of the run: transactions completed and CPU used in that
  second.
- `plot/<engine>-s<subscribers>-c<connections>-r<run>-checkpoints.csv`:
  when each checkpoint started and how long it took.
- `plot/bench.log`, and the terminal: the machine, the settings, every
  population and every run's summary.
- `db/<timestamp>/<engine>-s<subscribers>-populated.db`: each engine's
  populated database of each size. A run works on a copy, which is
  deleted when the run is done.

## Methodology

The workload follows the TATP specification,
`upstream/doc/TATP_Description.pdf`, and where the specification leaves
something open, the reference implementation in `upstream/source/src/`.

**Schema.** `subscriber`, `access_info`, `special_facility` and
`call_forwarding`, exactly as `upstream/bin/targetDBSchema.sql` defines
them, with `PRAGMA foreign_keys = ON` in both engines so the foreign keys
are checked.

**Population.** Every run starts from a fresh database, as the
specification requires. Each engine's database of each size is populated
once, and every run gets its own copy of that file; the rows come from a
seeded generator, so a copy holds exactly what populating again would.
Populating runs with `PRAGMA synchronous = OFF`, because `bench.sh` syncs
the copy to disk before every run anyway. Subscribers are inserted in
shuffled order,
2000 per transaction, each with 1 to 4 `access_info` rows, 1 to 4
`special_facility` rows and 0 to 3 `call_forwarding` rows under each of
those, every value drawn the way the reference's `populateDatabase` and
`rnd` draw it, and both engines get the same data. A
`wal_checkpoint(TRUNCATE)` after the last insert moves everything into the
database file, so copying that file copies the whole database;
`scripts/bench.sh` checks that the files next to it are empty.

**Transactions.** The seven transactions of
`upstream/bin/tr_mix_generic.sql`, in the standard mix:

| Transaction | Share | Rows found, spec | Rows found, measured |
|---|---|---|---|
| GET_SUBSCRIBER_DATA | 35% | 100% | 100% |
| GET_NEW_DESTINATION | 10% | 23.9% | about 15.5% |
| GET_ACCESS_DATA | 35% | 62.5% | about 62% |
| UPDATE_SUBSCRIBER_DATA | 2% | 62.5% | about 63% |
| UPDATE_LOCATION | 14% | 100% | 100% |
| INSERT_CALL_FORWARDING | 2% | 31.25% | about 31% |
| DELETE_CALL_FORWARDING | 2% | 31.25% | about 31% |

The specification's 23.9% for GET_NEW_DESTINATION does not follow from its
own value rules: drawing the rows and keys the way the reference
implementation does finds a row in about 15% of the cases, and that is
what this harness measures too. `--mix read` runs only the three reads,
the `read_mix` of `upstream/tdf/example.tdf`.

**Keys.** Subscriber ids are drawn with the specification's non-uniform
distribution, `NURand(A, 1, P) = ((random(0, A) | random(1, P)) % P) + 1`,
with `A` = 65535 up to a million subscribers, so some subscribers are
asked for far more often than others. `--uniform` (`DISTRIBUTION=uniform`)
draws every subscriber equally often; results of the two cannot be
compared. Every connection has its own seeded stream of transactions.

**Errors and throughput.** A transaction that does not find its rows still
completes and counts towards MQTh. INSERT_CALL_FORWARDING fails with a
primary key or foreign key violation in about two thirds of the cases,
which the specification accepts: it is rolled back, counted as rejected
and left out of MQTh and the response times. Any other error stops the
run. MQTh is the number of transactions that started inside the measured
window and completed, divided by the window: the same wall time for every
engine and setting.

**Sizes and connections.** The SQLite VLDB paper runs TATP with one
client against 10K, 100K and 1M subscribers, and so does this, so the
panel for one connection can be read next to the paper's figure. The
other panels add 4 and 16 connections: TATP is mostly reads, which SQLite
runs side by side in WAL mode, so whether more connections help is a
different question here than for the write-only `perf/throughput`.

**Page cache.** Every connection of both engines gets a page cache of
`CACHE_SIZE_MB` (1024 MiB, through `PRAGMA cache_size`), the 1 GB the
paper gives its engines. That holds even the 1M-subscriber database, so
the size changes how deep the B-trees are, not whether a lookup has to
leave the engine. Both engines default to `cache_size = -2000`, about
2 MB, which would turn most lookups in the largest database into reads
from the operating system's cache.

**Load.** Closed loop, the flooding load of the specification: each
connection starts its next transaction the moment the previous one ends,
on its own thread. Each transaction is its own explicit transaction,
`BEGIN` to `COMMIT`.

**Durability.** Both engines run with `PRAGMA synchronous = FULL`, so every
commit that wrote something ends in an fsync.

**SQLite.** WAL mode, every connection through `rusqlite`. Writes start
with `BEGIN IMMEDIATE`, reads with `BEGIN`: a deferred `BEGIN` before a
write would fail with `SQLITE_BUSY_SNAPSHOT` instead of waiting when
another writer got in first. SQLite's busy handler is not fair: a writer
sleeps between tries and can keep missing the short moments the write
lock is free, so a write can wait out the whole busy timeout (60 s) while
others commit. It then tries again, every try counts as a restart, and
the whole wait stays in its response time.

**Turso.** MVCC (`PRAGMA journal_mode = mvcc`), every connection with its
own tokio runtime, `BEGIN CONCURRENT` for every transaction, and the
io_uring backend on Linux. Two transactions that change the same row
conflict, because the non-uniform keys make some subscribers hot; the
loser rolls back and starts over with the same keys, and the restarts
count towards its response time. The results say how many there were.

**Checkpointing.** Both engines checkpoint the way a server does: the
writers' auto-checkpoint is off and a separate connection runs
`PRAGMA wal_checkpoint(PASSIVE)` every `CHECKPOINTER` milliseconds (1000).

**Cost.** Once a second through every run, the process's own user and
system CPU time is sampled and recorded as a share of every hardware
thread on the machine; the summary also gives it per transaction, along
with what the disk under the database did.

**Runs.** After copying the database, the drive is told which blocks are
free (`fstrim`) and left idle for `IDLE` seconds (30). Then the operating
system's page cache is dropped and the database file is read once, so
every run starts with the database, and nothing else, in the operating
system's cache, and the engine's own cache empty. Each run measures 60 s
after a 10 s warmup, the same as the paper. Each configuration is run
`REPEATS` times (3); odd repeats walk the sizes and connection counts
upwards and even ones downwards, so an effect of the order shows up as a
difference between them.

## Configuring the benchmark

Every setting is an environment variable read by `scripts/bench.sh`, which
`run.sh` calls, so both take them:

| Variable | Default | Meaning |
|---|---|---|
| `SIZES` | `"10000 100000 1000000"` | Rows in the subscriber table, one database per size |
| `CONNS` | `"1 4 16"` | Connection counts to run |
| `CACHE_SIZE_MB` | `1024` | Page cache of every connection, in MiB |
| `MIX` | `standard` | `standard` (80% reads) or `read` (reads only) |
| `DISTRIBUTION` | `nurand` | `nurand` or `uniform` subscriber ids |
| `REPEATS` | `3` | Runs per configuration; every run writes its own files |
| `IDLE` | `30` | Seconds the drive is left idle before each run, after `fstrim` |
| `DURATION` | `60` | Seconds measured per run |
| `WARMUP` | `10` | Seconds run before measuring starts |
| `CHECKPOINTER` | `1000` | Milliseconds between checkpoints from a separate connection; `0` lets each writer checkpoint itself |
| `GROUP_COMMIT` | `on` | `off` makes every Turso transaction sync the logical log on its own |
| `OUT` | `plot/` | Where the results and figures go; a run refuses to overwrite files that exist |
| `DB_DIR` | `db/<timestamp>/` | Where the database files go |

A quick look, two runs of 10 seconds of the two smaller sizes:

```console
SIZES="10000 100000" REPEATS=2 DURATION=10 WARMUP=2 IDLE=5 ./scripts/run.sh
```

The harness can also be run by hand. `tatp populate --help` and
`tatp run --help` list its options, including `--mode immediate` to run
Turso with WAL and `BEGIN IMMEDIATE` like SQLite, and `--io syscall`:

```console
tatp populate --engine turso --db turso.db --subscribers 100000
tatp run --engine turso --db turso.db --subscribers 100000 --connections 8 --out-dir results
```

## The reference implementation

`upstream/` is release 1.1.1 of the TATP Benchmark Suite, from
<https://tatpbenchmark.sourceforge.net/>, unchanged except that the
prebuilt solidDB binaries in `bin/` are left out. It is an ODBC client
written in C and needs an ODBC driver for the database it tests, which
Turso does not have, so this harness implements the same workload in
Rust instead. The suite is copyright IBM Corporation 2004, 2011 and is
distributed under the Common Public License 1.0
(`../../licenses/perf/tatp-cpl-license.md`); its
`upstream/license_agreement.html` covers the parts it includes from others.
