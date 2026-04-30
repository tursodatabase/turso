# VACUUM benchmark

Compares in-place `VACUUM` performance between Turso and SQLite on databases
built from the same mixed write workload.

The workload is generated but not timed. Only the `VACUUM` operation itself is
measured. For each backend and target size, the benchmark reports:

- pre-VACUUM page count
- post-VACUUM page count
- pre/post WAL size
- measured duration
- the `VACUUM` statement duration
- checkpoint duration, when visible

Turso's in-place `VACUUM` publishes the replacement image to the source WAL and
then runs a TRUNCATE checkpoint before returning. SQLite's WAL-mode `VACUUM`
commits the replacement image and may leave the replacement pages in the WAL.
SQLite also enables `wal_autocheckpoint=1000` by default, which can hide a
PASSIVE checkpoint inside the `VACUUM` statement timing.

By default, this benchmark measures SQLite's normal WAL-mode behavior:
`locking_mode=NORMAL`, default auto-checkpointing, and no explicit
post-`VACUUM` checkpoint. To compare both engines with a WAL-truncated terminal
state, run with `--sqlite-auto-checkpoint off --sqlite-post-vacuum-checkpoint
truncate`. That row is labeled `sqlite+ckpt`.

The benchmark also inserts an untimed filesystem sync barrier before each
measured operation. The barrier syncs the generated benchmark database files and
case directory, then waits for storage to settle, so one backend is less likely
to absorb writeback from setup or from the other backend's run.

By default, when both backends are selected, each backend is populated and
measured in its own generated case. This avoids measuring one backend
immediately after the other's WAL/checkpoint-heavy `VACUUM`. Use `--paired` only
when intentionally investigating order effects in one shared comparison case.

The benchmark can perform an untimed source warmup before each measured backend.
The warmup scans table payloads and the workload index so the first backend is
not the only one benefiting from cache state left by population. The default is
no warmup.

Default target sizes:

- 5MB
- 50MB
- 100MB
- 250MB
- 500MB
- 750MB
- 1GB
- 1.5GB
- 2GB
- 3GB
- 5GB
- 8GB
- 10GB

Run a small sanity pass:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 5
```

Run repeated 50MB samples and alternate which backend is measured first:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 50 --batch-ops 64 --runs 6
```

Run the old back-to-back paired comparison explicitly:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 50 --batch-ops 64 --runs 6 --paired
```

Run one backend per process, which is useful for avoiding cross-backend storage
state even across generated cases inside a single benchmark process:

```console
for i in 1 2 3 4 5; do
  cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 250 --batch-ops 64 --turso
done

for i in 1 2 3 4 5; do
  cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 250 --batch-ops 64 --sqlite
done
```

Run the full matrix:

```console
cargo run -p vacuum-benchmark --profile bench-profile
```

Useful flags:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --help
```

Key flags for noisy storage environments:

- `--runs N`: recreate the same workload and measure it N times.
- `--sqlite` / `--turso`: run only one backend. By default both are run in the
  same process, but in separate generated cases.
- `--paired`: measure both backends back-to-back in the same generated case.
  This is useful for diagnosing order bias, but it is not the default comparison
  mode.
- `--backend-order alternate|sqlite-first|turso-first`: alternate by default.
  In isolated mode this controls which single-backend case runs first; in paired
  mode it controls measurement order inside the case.
- `--barrier-sleep-ms N`: tune the post-sync settle pause before each measured
  `VACUUM`. The default is 2000ms because shorter pauses allowed order bias on
  local WAL/checkpoint-heavy runs.
- `--sqlite-locking-mode normal|exclusive`: use normal SQLite WAL locking by
  default. `exclusive` is useful for isolating single-connection behavior, but
  it does not match SQLite's default concurrency behavior.
- `--sqlite-auto-checkpoint default|off`: leave SQLite's default
  `wal_autocheckpoint=1000` enabled or disable it before timing `VACUUM`.
- `--sqlite-post-vacuum-checkpoint truncate|none`: choose whether SQLite pays
  an explicit post-VACUUM TRUNCATE checkpoint cost. The default is `none`.
- `--warmup source-scan|none`: choose whether to warm table and index pages
  before each measured backend. The default is `none`.
- `--keep-files`: keep generated database files after each case. By default,
  completed case directories are removed to keep large runs from exhausting disk.
