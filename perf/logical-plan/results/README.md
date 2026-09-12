# Logical-plan measurement records

The [protocol and interpretation](../../../docs/logical-plan-performance.md)
define the baseline, environment, fixed acceptance criteria and incomplete work.

`baseline/` contains seven native and three Callgrind runs for all 302 existing
prepare workloads. The isolated directories measure point lookup, CTE join and
correlated EXISTS with the same protocol. They diagnose individual changes and
do not replace the complete corpus. `comparison.csv` records every measured
workload, including failures.

Versioned `.txt` files retain each benchmark's native samples or instruction-run
workload listing. `.json` files retain every dump's event and instruction totals,
environment, command and exit status. Complete compressed Callgrind graphs remain
beside these records in the workspace and are ignored by Git because of their
size. The binaries used by each run are preserved under `target/logical-plan-*`.
An executable hash identifies each measured binary; older environment `commit`
fields identify the measurement checkout, not necessarily the binary's source.

`fuzz-12345-depth-3/` retains the earlier seed's SQL history, final schema and
coverage. Database and WAL files remain in the workspace. Later source revisions
can generate a different workload from the same seed; preserve the source
revision together with the seed, profile and nesting limit.

`dsl-isolated/` is the three-query comparison after generated rule integration.
`params-baseline/` and `params-derived/` retain the separate Criterion parameter
corpus, including every iteration count and elapsed-time sample. Their native
runs suspended the owned instruction runs; `params-scheduling.json` records that
interval. `execution-pilot/` checks all 57 prepared execution cases against
SQLite and retains their physical plans. `execution-boundary/` verifies one
execution dump per workload, excluding setup and preparation. Full execution
baseline/candidate records are collected separately.
