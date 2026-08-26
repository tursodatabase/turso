# Write throughput

Throughput is committed rows per second: `inserts / elapsed_secs` from the
`mvcc-write-bench` CSV. Latency is BEGIN→COMMIT wall time on a closed loop
with occupancy equal to the worker count.

## How to run the cell

From the repo root:

```console
./perf/throughput/run.sh
```

That is `CELL=workers`. Turso runs `--topology io-pump --workers N` for N in
1, 2, 3, 4. Occupancy is N in-flight transactions on 1 SQL thread plus the
io-pump helper. SQLite is occupancy-1 via `mvcc-write-bench --engine sqlite`.

For N SQL threads, 1 worker per thread:

```console
CELL=threads ./perf/throughput/run.sh
```

That is `--topology threads-pump --threads N --workers-per-thread 1`. SQLite
occupancy-1 still comes from `mvcc-write-bench`. If `write-throughput-sqlite`
is in the workspace, the runner also records N-thread WAL from that binary
into a sidecar CSV with `source=write-throughput-sqlite`. Do not mix those
rates onto the Turso series or the occupancy-1 SQLite reference line.

Every Turso cell is paired: `truncate` and `passive` at the same other knobs
(`turso_pair`). Defaults are `--batch 100`, `--threshold disabled`,
`--duration 5s`, `--warmup 1s`, and `--repeats 3`. Override with `DURATION`,
`WARMUP`, and `REPEATS`.

CSV and plots land in `perf/throughput/plot/out/` (gitignored). The CSV
includes BEGIN→COMMIT `latency_p50_us` and `latency_p99_us`. Plot throughput
from that CSV:

```console
python3 perf/throughput/plot/plot-throughput-scaling.py --x workers \
    perf/throughput/plot/out/write-scaling.csv \
    perf/throughput/plot/out/throughput-scaling.png
```

Use `--x threads` when the CSV rows are `threads-pump`. Y is committed rows/s.
Truncate and passive are separate series. SQLite occupancy-1 is a reference
line.

`plot-txn-latency-ecdf.py` reads `*-ecdf.json` if the harness wrote them. This
crate does not take `--timeline-dir`, so that plot is a no-op until a later
harness emits those files.

## What not to use for the scaling plot

`write-throughput` (`perf/throughput/turso`) and `scripts/bench.sh` still
exist. Concurrent MVCC modes `join_all` every iteration, so occupancy equals
iterations, not 1 per thread. That is not a closed-loop cell. Do not plot
those rates as worker or SQL-thread scaling.

`plot-thread-scaling.py` and `plot-compute-impact.py` read that old CSV
shape (`system,threads,batch_size,compute,throughput`). Leave them for the
legacy binaries.
