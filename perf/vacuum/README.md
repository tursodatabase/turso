# VACUUM benchmark

Compares plain `VACUUM` performance between Turso and SQLite on databases built
from the same mixed write workload.

The workload is generated but not timed. Only the `VACUUM` operation itself is
measured. For each backend and target size, the benchmark reports:

- pre-VACUUM page count
- post-VACUUM page count
- `VACUUM` duration

The benchmark inserts an untimed filesystem sync barrier before each measured
`VACUUM` so one backend does not absorb writeback from setup or from the other
backend's run.

Default target sizes:

- 5MB
- 50MB
- 500MB
- 2GB
- 5GB

Run a small sanity pass:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --sizes-mb 5
```

Run the full matrix:

```console
cargo run -p vacuum-benchmark --profile bench-profile
```

Useful flags:

```console
cargo run -p vacuum-benchmark --profile bench-profile -- --help
```
