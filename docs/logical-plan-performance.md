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

The column-set profile at `d82658618` attributed preparation overhead to repeated
column sorting, deduplication, comparisons and copying during logical validation.
The replacement stores a bitset for each relation, including a separate rowid
bit, and preserves deterministic column ordering. Join predicates validate against
both inputs without copying the left outputs or merging right columns into the
output set of a semi/anti join. Validation remains enabled in the measured build.

The four-workload diagnostic retains seven native rounds and three instruction
rounds in `results/prepare-shared-producers-before`, `prepare-column-bitsets`, and
`prepare-join-bitsets`. Each candidate manifest records its exact source diff and
binary hash. The last candidate also includes the initial UPDATE FROM join-order
repair under review; these SELECT fixtures do not enter its write-origin branch.
`prepare-current-baseline` retains seven additional native runs of the original
saved executable, without replacing the fixed historical acceptance data.

| Workload | Original isolated instructions | Before column changes | After column and join changes |
|---|---:|---:|---:|
| Complex predicates | 2,195,824 | 2,194,073 | 2,194,073 |
| Index range with ordering/limit | 658,429 | 656,675 | 656,675 |
| Primary-key point lookup | 520,794 | 519,179 | 519,179 |
| Correlated EXISTS | 1,954,541 | 2,128,185 | 2,065,466 |

Every instruction round returned the same count for its workload. The EXISTS
reduction is 62,719 instructions, leaving a 110,925-instruction increase (5.68%)
against the original isolated result. Native medians for that query are 192.9
microseconds before, 189.0 with bitsets alone, and 200.1 with the join change;
the additional original-binary median is 167.9 microseconds. These timings do not
establish a native improvement. The comparison CSV retains failures under the
fixed criteria. Full preparation parity and the complete corpus remain unfinished.

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

`core/benches/unnesting_execution.rs` initially ran 19 data/query configurations in
automatic, forced and disabled unnesting modes. Those 57 cases check their ordered
integer row IDs against SQLite before measurement. They include outer sizes of
16, 256 and 1024 rows, 16–256 distinct outer keys, repeated inner keys, absent
matches, NULLs, uniform and skewed keys, an optional covering index, inequalities,
disjunctions, aggregate and ordered scalar subqueries, nesting depths two and
four with distant references, and a limited derived input. Both engines receive
the same generated rows and ANALYZE. The `Case` fields define the full data set.

The joined-input extension adds equality, inequality and anti joins inside EXISTS,
plus an indexed scalar COUNT with 16 outer rows and 4096 inner rows. These add 12
mode/case combinations, for 69 fixtures. The COUNT case selects indexed dependent
execution automatically; forced mode groups the inner input and joins it back.
The joined-input cases select a separate derived input automatically. Measure the
new cases against the original engine with the same extended harness and filter
`joined_input|scalar_count_indexed_small`; the original 57-case results retain
their original workload set and measurements.

The nested-filter extension adds depths two and four with immediate correlations
and a nested anti case. All three use 64 outer rows and 128 inner rows, 64/32
distinct keys and no index. They add nine combinations, bringing the corpus to
78 fixtures. The earlier nested cases retain their more distant references.
Use `nested_local` for the additional cases when comparing the same harness on
the original and changed engines.

The standard `codspeed` feature builds the 26 automatic cases. Enabling
`simulator` also builds forced and disabled alternatives, yielding all 78 cases.
The required feature is `bench`, which `codspeed` enables. This keeps the
execution target buildable in the existing CodSpeed workflow without enabling
simulator code in every prepare benchmark. Mode selection remains outside the
measured execution boundary. Serde is an explicit development dependency for
fixture metadata instead of relying on the simulator feature to enable it. Local
development builds and SQLite result checks at `71d01b3b9` pass for both feature
sets with the preceding 23/69 fixtures; their commands, hashes and 92 captured
plans are in `codspeed-execution-features/`.
This is feature/build validation, not a hosted CodSpeed measurement.

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
isolated criteria. All three complete candidate instruction rounds are now
recorded in `dsl/`. The fixed criterion flags 70 of 302 workloads, led by
correlated EXISTS (+9.48%), TPC-H 22 (+2.88%), correlated scalar preparation
(+1.28%), and TPC-H 2 (+1.01%). These failures prevent performance parity.

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

The execution corpus exposed an incorrect LIMIT cost reduction. A sorted outer
query estimated only 7.5 correlated filter calls where all 100 input rows must
be inspected. `d677aa75d` preserves the full call count through blocking operators
and OFFSET; streaming LIMIT still reduces it. The regression failed before the
fix, and 19 JSON tests, 461 SQL cases, strict core lint and all 57 execution
fixtures passed afterwards.

The fixed estimate changes the automatic `derived_limit` plan. One diagnostic
native run measured 6.444 milliseconds automatically, 6.408 forced and 19.59
disabled, compared with the earlier automatic median of 19.09 milliseconds.
`sort-cost-pilot/` retains its commands, binary hash, samples and all 57 checked
plans. This single run establishes the plan-choice effect, not full acceptance;
the seven-native/three-instruction comparison is still required.

The full `d677aa75d` execution comparison is now recorded in
`execution-sort-cost/`. Automatic `derived_limit` improves from 19.29 to 6.421
milliseconds (-66.71%) and from 232,604,364 to 87,351,550 instructions (-62.45%).
The unchanged acceptance criteria still flag 49 instruction cases and two native
cases: automatic `exists_nulls` at 6.076 versus 5.807 milliseconds (265-microsecond
uncertainty), and disabled `exists_low_selectivity` at 33.23 versus 31.61
milliseconds (1.18-millisecond uncertainty). These failures remain unfinished
work; the improved limited query does not establish overall parity.

The joined-input revision (`0625afdf7`) has a complete comparison of the 12
additional execution cases against the original engine with the same benchmark
source (`c0b1b8d08`). All native timings pass. Automatic joined-input equality,
inequality and anti queries improve from 25.30, 25.98 and 25.58 milliseconds to
13.09, 12.93 and 12.47 milliseconds. Their instruction counts fall by 43.17%,
44.36% and 45.33%. Automatic selection retains indexed dependent execution for
the small-outer COUNT case: 1.538 milliseconds versus 2.858 for forced unnesting,
with exactly the baseline's 19,000,464 instructions in all three automatic runs.

Three of these 12 cases still fail the instruction criterion: disabled joined
inequality, disabled indexed COUNT and forced indexed COUNT. The last case's
median decreases, but one candidate sample exceeds the largest baseline sample;
the protocol rejects that case. `execution-joined-baseline/` and
`execution-joined-input/` retain every sample, physical plan, executable and
harness hash, and comparison. `execution-joined-pilot/` retains result checks and
plans for all 69 fixtures. The original 57 fixtures still require a complete
comparison at this revision; their earlier failures remain open.

Seed 54321 at depth four on `0625afdf7` executed 990 of 1000 generated statements,
skipped ten, and had no warnings or errors. It checked 98 independent joined
equivalents, 192 distinct forced/disabled plans and 99 same-plan cases. An exact
repeat with `logical_optimizer=trace` recorded 539 `PullDependentFilter`
applications and zero applications of `PullDependentFilterOverJoin`: the
generator at that revision did not produce joined EXISTS inputs. Counts include
both query preparation and EXPLAIN, so they are not unique transformed queries.
Both runs are retained in `fuzz-joined-54321-depth-4/` and
`fuzz-joined-54321-depth-4-trace/`. This uncovered a generator coverage gap rather
than proving random coverage of the joined-input rule.

An isolated LIMIT preparation diagnostic compares the original engine, the
joined-input revision, and a change that skips correlated-call scaling when the
call list is empty. Each uses the same four-workload filter, seven native runs
and three instruction runs. `baseline-limit-isolated/`, `joined-limit-isolated/`
and `empty-call-scaling-isolated/` retain these measurements and exact source
manifests. The change reduces indexed range/ORDER BY/LIMIT preparation from
656,872 to 656,653 instructions. Complex-predicate preparation increases by 100
instructions to 2,194,047; point lookup stays at 519,145. All three remain below
the original engine's corresponding counts. Correlated EXISTS falls by 214
instructions to 2,127,404, still 8.84% above the original 1,954,541. Native samples
pass this diagnostic's original-engine uncertainty, but do not replace the
fixed complete-corpus comparison. The blocking-LIMIT call-count regression also
passes; no cost estimate changes for queries with correlated calls.

The dependency-inspection revision (`a5cfc53dd`) leaves all three ordinary-query
instruction counts unchanged in the same isolated diagnostic. Correlated EXISTS
adds 52 instructions, reaching 2,127,456 (+8.85% over the original engine). The
seven native runs remain inside this diagnostic's baseline uncertainty. Raw
samples, the exact source diff and the comparison are retained in
`dependency-diagnostics-isolated/`. This verifies that dependency traversal and
JSON construction stay off ordinary preparation; it does not resolve the
remaining preparation regression.

A nested LIMIT regression exposed a second call-count error: a scalar SELECT
invoked for 100 outer rows estimated just 2.5 calls to its own correlated COUNT
subquery. Its LIMIT 1 had been applied to all invocations combined. Multiplying
the limit by the invocation count restores the expected 100 calls and preserves
the query's 99 result rows. The before/after regression output is retained in
`limit-per-call-pilot/`; all 26 JSON tests, 469 SQL cases and strict core lint pass.
This estimate correction still needs an execution comparison at its revision.

The nested-filter extension passes 28 JSON, 22 logical/compiler/budget, 475 SQL
and 36 differential-fuzzer tests, formatting and strict core/fuzzer lint. A
before-change structural test leaves one dependency at depth two; afterwards
depths two and four remove every dependency for all tested EXISTS/NOT EXISTS
combinations. Empty inputs, NULLs, duplicates, parameter slots and effect guards
are covered. The 78 simulator fixtures and 26 standard CodSpeed-feature fixtures
also pass their SQLite result checks. Build manifests, captured plans and
validation logs are retained in `nested-filters-pilot/`. Execution timing and
instruction comparisons remain separate work.

The complete nine-case nested comparison at `815c96a70` is retained in
`execution-nested-baseline/` and `execution-nested-filters/`. Both engines use the
same 78-case harness, built on the original baseline and implementation branch
without a worktree. Automatic depth four improves from 12.72 to 7.138 milliseconds
(-43.88%) and reduces instructions by 41.49%. Automatic depth two and the nested
anti case regress by 19.34% and 17.31% in native time, and by 26.45% and 26.00% in
instructions. Forced depth two also fails the native criterion. Seven of nine
cases fail the instruction criterion, including small increases in all three
disabled cases. These are unfinished work. The automatic depth-two plan builds
two temporary indexes, while the dependent form can stop at each first match;
the current scan estimate still charges for reading every input row.

The limited-scan correction charges a correlated, unconstrained single-table
scan only for the estimated rows before its first result, retaining the first
page cost for every invocation. Sorts, aggregates, grouping, windows, DISTINCT,
OFFSET and constrained access methods do not receive this scan discount. The
JSON regression checks dependent scans for 16 outer rows, an ephemeral index for
1024 outer rows, scalar first-row limits and blocking operators. The two SQL
plan assertions now load data and ANALYZE before expecting semi-join indexes.

`execution-complete-baseline/` and `execution-scan-limit/` retain all 78 execution
cases under the original protocol. The scan correction fixes the automatic
shallow nested plan choices, but 64 cases still exceed the instruction bound,
mostly by small amounts in unchanged disabled plans. Three native cases fail.
These measurements do not establish final parity.

Callgrind isolates the small unchanged-plan increases to an opcode dispatch-table
copy in `Statement::_step`. For disabled scalar SUM inequality, both binaries
call `memcpy` from that function 18,181 times. The limited-scan binary spends
145,448 additional instructions in those copies, exactly eight per call. The
210-entry constant table occupies 1680 bytes; the dev build copies it when
indexing it by value. Making the immutable table static removes those calls.
`execution-shared-dispatch/dispatch-copy-profile.json` retains the caller counts
and costs from the three corresponding Callgrind dumps.

The static-table binary passes the instruction criterion for 76 of 78 execution
cases, including every automatic and disabled case. The remaining forced cases
are nested anti (48,258,527 versus 38,642,502 instructions, +24.88%) and depth two
(48,240,160 versus 38,492,326, +25.32%). They remain unfinished optimization
work. All 78 fixtures pass their SQLite result checks. The exact source diff,
binary hash, three instruction rounds, seven native rounds, plans and comparison
are retained in `execution-shared-dispatch/`.

That candidate's native samples fail 66 of 78 historical bounds, including
unchanged disabled plans. A same-session diagnostic reruns the saved original
binary and the candidate sequentially, with no concurrent build or benchmark.
Disabled scalar SUM inequality measures 55.39 milliseconds for the original and
55.17 for the candidate, compared with the historical original's 37.20. This
demonstrates a timing-environment change for that workload; it does not clear the
other native failures. The seven samples from each executable and their hashes
are retained in `execution-dispatch-timing-baseline/` and
`execution-dispatch-timing-candidate/`. The historical thresholds are unchanged.
Complete preparation parity, forced-plan regressions, current shared-producer
measurements, hosted CodSpeed and optimized-build validation remain open.
