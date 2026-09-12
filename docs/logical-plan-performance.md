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
  Run its test mode for correctness and native measurement separately using
  `measure_params.py <saved-executable> <output> --source-revision <revision>`.
  Seven native runs use 10 samples, one-second warmup and one-second measurement,
  pinned to the same CPU with no concurrent builds or benchmarks. Keep raw
  Criterion JSON. Divide each sample's time by its iteration count, then use
  the same seven-run median and fixed baseline uncertainty formula below.
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

Seven native runs each contain the same 302 workloads. The point lookup's seven
medians are 51.41, 45.12, 228.9, 47.18, 49.31, 61.91, and 60.45 microseconds.
This host shows substantial timing variability, which limits conclusions about
small native changes. The fixed acceptance formula remains unchanged.

Three isolated Callgrind point-lookup checks each measured 520,038 instructions.
Only dumps triggered by `--dump-after=prepare_benchmark::measure_prepare` count;
CodSpeed metadata and process-termination dumps do not represent a workload.
`summarize.py` checks that all seven native and three instruction runs have the
same workload names and produces per-workload summaries and comparison CSVs.
The full baseline is complete. Full candidate comparisons remain outstanding.

The first full Callgrind run contains all 302 workloads and totals
121,723,783,645 measured prepare instructions. The seven-run native baseline and
three-run full instruction protocol remain the acceptance data. Additional
isolated runs of point lookup, EXISTS and CTE preparation diagnose individual
changes; they do not replace the full-corpus comparison. The second full baseline
instruction run was suspended while isolated native timings ran, then resumed;
instruction totals do not include the suspended interval.

The reproducible differential run `--seed 12345 --profile correlated-subqueries
--max-subquery-depth 3 -n 1000 --coverage --keep-files` executed 990 statements,
skipped 10 rejected during preparation, and found no warnings or failures. It
compared 118 distinct forced/disabled plans; another 102 eligible queries selected
the same operators. These are separate counts, not 220 successful rewrites.
The SQL history, schemas, databases, log and expression coverage are retained in
`results/fuzz-12345-depth-3/`. This run exercises both logical and legacy rules;
per-rule runtime coverage remains outstanding.

The first executable slice fails the isolated instruction criterion: CTE prepare
increased by 82 instructions (0.0043%), point lookup by 81 (0.0156%), and correlated
EXISTS by 374,066 (19.14%). The EXISTS median native time increased from 181.9 to
222.8 microseconds, exceeding its fixed 34.2-microsecond uncertainty. These are
unfinished performance work. `results/slice-2-isolated/comparison.csv` records
all three comparisons. The baseline and candidate binaries are preserved with
SHA-256 hashes in their measurement metadata.

Reusing catalog schema metadata and skipping an unnecessary resource walk removes
the point and CTE failures in the isolated set: point lookup measures 519,153
instructions versus 520,803, and CTE prepare measures 1,909,901 versus 1,912,037.
Keeping small logical column sets inline lowers EXISTS to 2,154,622 instructions,
still 10.253% above 1,954,251. Its native median is 225.9 microseconds, still
outside the original 34.2-microsecond uncertainty around 181.9 microseconds.
`results/column-sets-isolated/comparison.csv` retains these failures. This binary
precedes the generated-column effect guard and DSL; its hash identifies the
measured executable. Subsequent changes need their own comparison.

All three full baseline Callgrind rounds contain the same 302 workloads and
total 121,723,783,645, 121,723,929,052 and 121,723,426,890 instructions.
The complete per-workload samples, medians and fixed uncertainties are recorded
in `results/baseline/summary.json`. The `commit` field
in older measurement metadata identifies the checkout when the runner was
invoked, not the source of an already-preserved executable. Baseline executables
contain the original engine plus the benchmark wrapper; executable hashes and
the source notes above identify that distinction.

## Prepared execution corpus

`core/benches/unnesting_execution.rs` runs 19 data/query configurations in each of
automatic, forced and disabled unnesting modes. The 57 cases check their ordered
integer row IDs against SQLite before measurement. They include outer sizes of
16, 256 and 1024 rows, 16–256 distinct outer keys, repeated inner keys, absent
matches, NULLs, uniform and skewed keys, an optional covering index, inequalities,
disjunctions, aggregate and ordered scalar subqueries, nesting depths two and
four with distant references, and a limited derived input. Both engines receive
the same generated rows and ANALYZE. The `Case` fields define the full data set.

Only `unnesting_execution::measure_execution` is measured: it steps an already
prepared statement to completion, reads every result row, and resets it. Schema
and data loading, SQLite validation, preparation, and plan capture are outside
this boundary. A test-mode run has passed all 57 cases; timings and before/after
comparisons are separate work. Forced mode means every applicable rewrite is
enabled; some query classes still produce the same plan as disabled mode. Plan
records must distinguish those cases rather than count them as decorrelations.

```sh
CARGO_TARGET_DIR=target/logical-plan-build CARGO_BUILD_JOBS=4 \
  cargo build -p turso_core --bench unnesting_execution \
  --profile dev --features bench,simulator
python3 perf/logical-plan/measure.py <saved-executable> <output-directory> \
  --kind execution --phase native --source-revision <revision>
python3 perf/logical-plan/measure.py <saved-executable> <output-directory> \
  --kind execution --phase callgrind --source-revision <revision>
```

The same seven-native/three-instruction protocol and fixed acceptance formulas
apply. Execution uses the additional `simulator` feature for forced alternatives;
compare it only against an execution baseline with those same features. The
runner records each workload's configuration, SQL, checked row count and selected
physical JSON under `plans/`. The measurement parser reads the boundary from
metadata and excludes setup and prepare dumps. The original prepare protocol
and its existing baseline remain unchanged.

All seven native execution rounds are complete on both engines. All 57 workloads
pass the fixed native criterion; `execution-derived/native-comparison.json`
retains each workload's samples, delta and uncertainty. Full execution instruction
rounds use CPU 1 on both engines, while the existing prepare instruction run uses
CPU 0. Native timings ran separately on CPU 0 with the owned instruction runs
suspended. Instruction-count and optimized-build acceptance remain outstanding.

All three execution instruction rounds are now complete for the original engine
and `deebe5185`. The fixed criterion flags 50 of 57 cases even though native
timings pass. The largest instruction increase is disabled `anti_or_nulls`,
11,192,367 versus 11,180,031 (+0.1103%). Forced `exists_outer_1024` increases
0.0515%, and automatic `scalar_first_ordered` increases 0.0505%. These small
increases remain failures under the unchanged criterion and need investigation.
Automatic inequality execution improves from 18.83 to 5.904 milliseconds and
228,087,288 to 82,896,497 instructions; those gains do not cancel other failures.
The complete samples and comparisons are in `execution-baseline/summary.json`,
`execution-derived/summary.json`, and `execution-derived/comparison.csv`.

## Subsequent recorded comparisons

The generated-rule revision (`3dbf3c7ce`) passes the fixed native criterion for
all 302 prepare workloads. Its isolated EXISTS measurement is 2,138,380
instructions versus 1,954,251 (+9.422%), so it still fails the instruction
criterion. Its isolated native median is 189.2 microseconds versus 181.9, within
the original 34.2-microsecond uncertainty. Point lookup and CTE prepare pass both
isolated criteria. The first complete candidate instruction round flags 49 of
302 workloads, led by correlated EXISTS (+9.48%), TPC-H 22 (+2.90%), correlated
scalar preparation (+1.28%), and TPC-H 2 (+1.01%). These are provisional counts
until all three rounds finish; they are not performance parity.

The derived-input revision (`deebe5185`) passes the separate parameter corpus's
native criterion. Median prepare times for 200, 500 and 1000 parameters are
233.34, 563.37 and 1098.97 microseconds, versus baseline 238.28, 580.80 and
1107.65. The fixed baseline uncertainties are 11.53, 33.09 and 51.13 microseconds.
`params-baseline/` and `params-derived/` retain all samples and summaries, and
the latter contains the per-workload comparison. These measurements do not
replace the full prepare corpus or optimized-build confirmation.

The joined-equivalent fuzzer revision (`bc7569079`) ran seed 12345 at depth three
for 1000 generated statements: 993 executed, seven skipped, no warnings or
failures, 116 independent joined equivalents, 230 distinct forced/disabled plan
checks and 102 same-plan checks. `fuzz-joined-12345-depth-3/` records the source,
executable hash, SQL history, schema, coverage and counts.

Deriving scalar failure properties in the existing reference walk (`672665033`)
reduces isolated EXISTS preparation to 2,127,448 instructions in each of three
runs. Rejecting unsupported root shapes before scanning identities (`61bc1563c`)
measures 2,127,972 in that supported case. Both remain above the original
1,954,251 baseline. Point lookup stays at 519,153, and CTE preparation remains
below its original maximum. `scalar-walk-isolated/` and `early-shape-isolated/`
retain these diagnostic instruction samples; they have no native comparison yet.
