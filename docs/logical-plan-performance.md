# Logical plan measurement protocol

Engine baseline: `a9a8779c1906247ae3ae78cd098ba713c27d8c9b`.
The first benchmark-only change introduces `measure_prepare`, a non-inlined
boundary around the same prepare-and-drop operation the existing Divan benchmarks
measure. Both revisions must use this boundary. No engine change is part of the
baseline. The staged input papers and prompt are not part of implementation commits.

## Fixed protocol

Record `rustc -Vv`, `cargo -V`, `uname -a`, `lscpu`, `valgrind --version`,
the baseline commit, and the exact commands alongside the results. Use Linux,
Rust 1.88.0, the repository's **dev** profile, default core features plus `bench`,
and the repository's rustflags. Do not use release or a profile inheriting release.
Measurements from this dev build describe that build; optimized production
performance and hosted CodSpeed remain separate required checks.

Build only the needed benchmark binaries:

```sh
CARGO_TARGET_DIR=target/logical-plan-build CARGO_BUILD_JOBS=4 \
  cargo build -p turso_core --bench prepare_benchmark \
  --bench prepare_params_benchmark --profile dev --features bench
```

Use the full existing `prepare_benchmark` corpus, including each scaling argument
and the vendored TPC-H, ClickBench, JOB and TPC-DS queries. Its in-memory schema
and indexes are created outside measurement. Tables are empty and there is no
ANALYZE. Do not silently drop failing workloads. Record failures separately.

* Native: seven sequential runs, 10 samples per workload, one prepare per sample,
  OS timer, pinned to the same permitted CPU, no concurrent builds or benchmarks.
  Preserve every benchmark's output, not just totals.
* Callgrind: three sequential runs with `--test`, collection initially disabled,
  toggled only within `prepare_benchmark::measure_prepare`. Zero counts on entry
  and dump on exit. This includes parsing, binding, rewriting, physical planning,
  emission and statement destruction, and excludes database/schema setup.
* Parameter Criterion corpus: retain the existing benchmark and CodSpeed naming.
  Run its test mode for correctness and native measurement separately.
* Execution measurements must use already-prepared statements, setup outside the
  interval, reset between iterations, and consume all rows. Record data generation,
  row counts, distinct bindings, indexes, NULLs, distributions, and modes.

The following acceptance rules are fixed **before** the first measurement:

1. For each workload, target zero instruction increase. Flag a final Callgrind
   count above the largest of that workload's three baseline counts. Report the
   median delta even when it lies within the baseline range.
2. For each workload, compare the median of the seven native run medians. Its
   fixed uncertainty is the greater of the baseline median range and three times
   the median absolute deviation of those seven medians. A final increase above
   that uncertainty fails. Do not widen uncertainty after implementation.
3. A failed workload, missing measurement, or reproducible regression is unfinished
   work. Gains elsewhere do not compensate. Hosted CodSpeed and optimized native
   confirmation are required before claiming final performance parity.

Raw results live under `perf/logical-plan/results/`. The runner accepts an output
directory and binary so the same protocol can be used on both revisions without
worktrees. Keep the baseline executable while compiling subsequent revisions.

## Environment encountered

The initial container has 14 aarch64 CPUs (Apple vendor), Linux 6.8.0-100-generic,
about 20 GiB RAM, Rust 1.88.0 / LLVM 20.1.5 and Valgrind 3.19.0. `/tmp` is on a
nearly full overlay filesystem; build artifacts are on the workspace volume.
An initial all-benches build exhausted `/tmp`; it is not a measurement. Only the
two prepare benchmarks are built for this protocol. The baseline emits existing
dead-code warnings in `core/index_method/backing_store.rs`.
