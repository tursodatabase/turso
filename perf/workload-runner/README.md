# Shared workload runner

This crate runs compiled Rust workloads against SQLite WAL, Turso WAL, and Turso MVCC.
It provides a library and a CLI without Criterion or Divan.
Workload code controls data, SQL, branching, coordination, and result assertions.
The runner controls connections, stages, arrival times, measurements, and shutdown.

## Run the compiled workloads

Run these commands from the repository root.
Each command creates a new output file and refuses to overwrite an existing file.
Database files are temporary and separate for every target and repetition.
The CLI runs targets sequentially and reverses their order on alternate repetitions.
Each repetition uses `seed + repetition`, with the same seed for its comparison targets.

```sh
cargo run -p turso-workload-runner -- \
  --connections 2 --batch-size 10 --duration 5 --warmup 1 \
  --repetitions 2 --output inserts.json

cargo run -p turso-workload-runner -- \
  --workload held-snapshot --connections 2 --count 100 --rate 100 \
  --checkpoint-ms 20 --raw-samples --output snapshot.json

cargo run -p turso-workload-runner -- \
  --workload noop --count 100000 --output overhead.json
```

`--targets sqlite-wal,turso-wal,turso-mvcc` selects the comparison targets.
`--rate` selects fixed arrival gaps and `--poisson` selects random exponential gaps.
The rate is the total across the workers that request admissions, not a rate per connection.
Without `--rate`, each worker starts its next operation after the previous operation finishes.
`--count` replaces the duration limit and disables warmup for that run.

The `insert` workload inserts a generated batch in one transaction.
The `held-snapshot` workload also holds a read transaction and runs periodic passive checkpoints.
The reader reads the table before it signals the writers, then reads it again before releasing its transaction.
The final stage compares every stored payload, the row count, and the sum of row IDs with committed inputs.
The `noop` workload uses the same admission and measurement path without database operations.
Its stage time per operation measures runner overhead, not engine speed.

## Write a workload in Rust

A `Group` supplies a worker count and a `Task` closure.
The runner opens one connection per worker on a dedicated OS thread.
Each thread owns a current-thread Tokio runtime.
SQLite calls execute synchronously on that thread, and Turso calls use the async driver.
The concrete adapter enums expose prepared statements, binding, row iteration, transaction state, and engine errors.

The task receives a borrowed `Connection` and an owned `Worker`.
It can retain prepared statements across calls to `worker.next_stage()`.
The runner does not start a stage until each worker in the previous stage requests its next stage or exits.
Preparation before the first request stays outside timing.
Preparation after dropping one stage and before requesting the next stays outside the next stage's timing.
Use a separate unmeasured stage when preparation depends on an earlier schema change.

```rust
use std::sync::Arc;
use turso_workload_runner::{db::Value, Group};

let readers = Group {
    name: "readers".into(),
    workers: 4,
    task: Arc::new(|connection, mut worker| Box::pin(async move {
        let mut statement = connection.prepare("SELECT ?").await?;
        while let Some(mut stage) = worker.next_stage().await {
            while let Some(admission) = stage.next().await {
                stage.measure("lookup", admission, async {
                    let mut rows = statement.query(&[Value::Integer(17)]).await?;
                    assert_eq!(rows.next().await?, Some(vec![Value::Integer(17)]));
                    assert_eq!(rows.next().await?, None);
                    Ok(())
                }).await?;
            }
        }
        Ok(())
    })),
};
```

A `Plan` is a sequence of named `Stage` values.
Each stage selects groups and defines its limit, load policy, measurement policy, timeout, and drain timeout.
There is no fixed setup, warmup, or verification stage type.
`Limit::Once` supplies one shared admission, `Count(n)` supplies up to `n`, and `Duration(d)` stops admissions after `d`.
Custom tasks can use `measure_custom` without consuming these admissions.
For custom tasks, the stage timeout and cancellation signals bound their lifetime.

Use `StageRun::repeat` for a repeated callback with mutable state.
Use `next` and `measure` when each operation needs generated inputs or result-dependent control flow.
Use `measure_with` and `Submeasurements::measure` for named measurements within one logical operation.
Submeasurements appear as `operation/submeasurement` and inherit the parent operation's measurement window.
Their scheduling delay is zero because they measure only the supplied future.

`Signal::wait` and `Signal::sleep` react to cancellation without losing an earlier signal.
At the admission deadline, `StageRun::cancellation()` stops background tasks and waits.
Admitted operations can continue until the drain timeout.
`StageRun::abort()` becomes set when that timeout expires or a worker fails.
Tasks must await their own background work before requesting the next stage.
Do not spawn detached threads or tasks from a workload.

For custom executable workloads, call `run` in a child process.
Use `process::run_child` in the parent with a command that starts that child and a hard timeout.
The built-in CLI follows this pattern and exchanges JSON through temporary files.
Rust cannot forcibly stop a synchronous call inside a thread, so an in-process timeout alone cannot guarantee shutdown.
If `run` reports an unfinished worker, terminate the child before starting another target.

## Interpret the measurements

The runner measures named logical operations rather than individual SQL statements.
`Measurement::Off` excludes a stage from metrics.
`Measurement::Windows` defines ordered, non-overlapping half-open windows inside a continuous stage.
Connections, prepared statements, load, and transactions can continue through those windows.
For continuous load, the admission time selects the window.
For paced load, the original scheduled arrival selects the window, even if execution starts later.

Execution latency runs from actual start to completion.
Scheduling delay runs from scheduled arrival to actual start.
Response latency includes both intervals.
Overdue arrivals keep their original times until the admission deadline.
The runner skips arrivals that cannot start by that deadline rather than admitting new work during drain.

Worker-local HDR histograms retain three significant digits.
Stage histograms merge the worker histograms before calculating percentiles.
Latency distributions include both successful and failed completed operations.
Throughput counts successful operations attributed to the measurement window, including their drain completions, divided by the window duration.
Count-limited stages use the common stage duration for all workers.
Raw samples are optional and remain in each worker's report.

`window_completions` finish before the selected measurement window ends.
`drain_completions` finish at or after that boundary.
`drain_ns` measures stage time after the admission deadline.
`backlog_at_deadline` includes unfinished admitted operations and skipped arrivals.
For continuous load there is no predetermined queue, so its skipped count is zero.
A run can finish with skipped arrivals, but missing count-limited work, failed operations, and unfinished work make it incomplete.

## Compare engine configuration explicitly

Journal mode and transaction mode are separate library configuration fields.
The CLI uses `BEGIN IMMEDIATE` for WAL and `BEGIN CONCURRENT` for Turso MVCC.
These compare different concurrency behavior, not identical isolation.
Capability validation rejects unsupported combinations before execution.
The output records transaction SQL, requested configuration, read-back pragmas, driver versions, Git revision, dirty state, and compiler version.

Journal mode and the MVCC checkpoint threshold apply at database scope.
Busy timeout, `synchronous=FULL`, and WAL autocheckpoint configuration apply to each connection.
WAL autocheckpoint thresholds count pages, while MVCC thresholds count logical-log bytes.
The held-snapshot workload disables automatic checkpoints and uses its own connection for passive checkpoints.
The engines do not promise identical checkpoint work or reclamation behavior.

`TransactionRunner` prepares transaction-control statements before measurement.
Its explicit retry helper receives one immutable input and reuses it after a classified conflict.
It rolls back an active failed transaction before another attempt, with limits on attempt count and retry time.
Pass the attempts counter to `measure_attempts` to include retries and backoff in logical latency.
The helper does not interrupt an in-progress database call, so the parent process still enforces the hard timeout.
Never use it to retry a task that can commit another transaction or produce external side effects.

## Commands and tests

This crate is additive. Existing benchmark crates, commands, scripts, and CI remain
unchanged and do not depend on it. The insert, held-snapshot, and no-op workloads
demonstrate the shared execution API; migrating existing benchmarks is separate work.

```sh
cargo test -p turso-workload-runner
cargo clippy -p turso-workload-runner --all-targets -- -D warnings
cargo fmt --all -- --check
```

Tests inject fake operation delays, startup failures, dropped futures, and retry conflicts.
They also run insert and held-snapshot scenarios in child processes against all three real targets.
These tests cover scheduling and correctness, not performance claims.
Use the no-op workload on the same build and machine to report the runner's overhead alongside benchmark results.
