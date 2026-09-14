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

The next profile identified repeated reconstruction of table masks while scoring
index candidates. `seek_constraint_masks` now combines the existing bitsets and
records consumed constraint positions in a bitset during the same pass. Hash-join
planning reuses its already computed left-input mask. Candidates whose cost cannot
win skip the residual-selectivity tie-break calculation; the original cost
tolerance and ordering bonus remain unchanged.

`results/prepare-seek-masks` retains that first mask change, and
`results/prepare-candidate-cost` retains the additional cost check. All three
instruction rounds agree. The final four-workload diagnostic passes both fixed
criteria against `baseline-limit-isolated`:

| Workload | Original isolated instructions | Final diagnostic instructions | Final native median, microseconds |
|---|---:|---:|---:|
| Complex predicates | 2,195,824 | 2,111,371 | 183.00 |
| Index range with ordering/limit | 658,429 | 639,837 | 60.63 |
| Primary-key point lookup | 520,794 | 505,464 | 50.94 |
| Correlated EXISTS | 1,954,541 | 1,942,305 | 186.90 |

The first native mask run overlapped a queued Cargo build. Its samples are kept
under `prepare-seek-masks/discarded-native` with the exclusion reason. The accepted
seven-round repeat ran after builds finished, with Callgrind suspended and then
resumed; `isolation.json` records that interval. The final candidate's native runs
also ran without concurrent builds or benchmarks.

Validation passed 2,493 core unit tests serially (17 ignored), 1,217 SQL cases
covering joins, constraints, index access and unnesting, and strict core/fuzzer
lint. These checks and benchmark manifests include the independently developed,
uncommitted UPDATE FROM lowering repair; its separate agent stopped while
packaging evidence. The completed full-corpus comparison uses the same saved binary in
`prepare-complete-mask-candidate`. All seven native runs and three instruction
runs contain 302 workloads. Six workloads exceed their original maximum
instruction count, and one exceeds the fixed native timing uncertainty:

| Workload | Maximum instructions above original maximum | Native median change | Failed criterion |
|---|---:|---:|---|
| TPC-DS 30 | 737,072 | +7.17% | instructions |
| TPC-DS 81 | 705,199 | +5.16% | instructions |
| CREATE INDEX | 11 | +8.20% | instructions |
| CREATE TABLE | 7 | -2.18% | instructions |
| Single parameterized INSERT | 35 | +3.56% | instructions |
| UPSERT | 119 | +2.08% | instructions |
| ClickBench 31 | none | +25.74% | native time |

The ClickBench median is 144.6 microseconds, versus 115.0 before; the original
uncertainty is 24.5 microseconds. The CREATE TABLE median instruction count is
one instruction lower, but its maximum still exceeds the acceptance limit.
These failures remain outstanding. This saved candidate predates the aggregate,
compound, VALUES and membership implementations. Final validation must measure
the finished source against the original baseline; the focused diagnostic and
this earlier full-corpus comparison do not establish final performance parity.

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


## Aggregate lowering prepare comparison

`results/prepare-aggregate-original/` and `results/prepare-aggregate-candidate/`
retain seven native and three Callgrind runs for five focused prepare workloads.
The candidate is `620da8e8e` plus the recorded, separate UPDATE FROM source diff;
its saved executable includes aggregate lowering and MIN/MAX row selection, but
precedes DISTINCT aggregate results and dependent aggregate EXISTS bodies.
`binary.json` identifies the exact source and executable. Native measurements
paused the full-corpus Callgrind process; `isolation.json` records that interval.

| Workload | Original full-corpus maximum instructions | Candidate maximum instructions | Original median ns | Candidate median ns | Fixed uncertainty ns |
|---|---:|---:|---:|---:|---:|
| GROUP BY and HAVING | 1,520,751 | 1,508,012 | 134,200 | 200,700 | 24,800 |
| CTE join | 1,912,943 | 1,852,670 | 188,200 | 273,600 | 62,300 |
| Correlated EXISTS | 1,951,691 | 1,944,472 | 192,300 | 283,500 | 60,700 |
| Aggregate derived input | 1,263,225 | 1,247,030 | 123,900 | 183,400 | 37,800 |
| Correlated scalar subquery | 2,099,903 | 2,034,168 | 208,200 | 326,100 | 64,200 |

All five instruction counts are below the original full-corpus maxima. All five
native medians exceed the original fixed limits. The saved original executable
also ran slower in this isolated measurement order; the paired diagnostic has no
failures against its newly observed spread. That spread does not replace the
original acceptance limits. `comparison-original-full.csv` records the five
outstanding native failures, and `comparison.csv` records the paired diagnostic.
These focused measurements do not establish parity for the full 302-workload
corpus or for subsequent changes.

## Scalar metadata and register allocation prepare comparison

The scalar-result metadata revision (`020097d97`) and a register allocation change
use the same four-workload invocation, seven native runs and three Callgrind runs.
The allocation change skips searching an empty register reuse list; allocation
order and reuse of nonempty ranges are unchanged. Saved binaries and source
manifests identify the unchanged deferred source drafts included in these builds.

| Workload | Fixed original maximum instructions | Before allocation change | After allocation change | Fixed original median ns | Candidate median ns | Fixed uncertainty ns |
|---|---:|---:|---:|---:|---:|---:|
| Parameterized INSERT | 476,548 | 476,524 | 473,677 | 39,880 | 63,760 | 7,840 |
| UPSERT | 741,720 | 742,138 | 737,539 | 61,520 | 91,600 | 15,260 |
| Primary-key lookup | 519,615 | 505,897 | 505,459 | 51,410 | 79,240 | 183,780 |
| Correlated scalar subquery | 2,099,903 | 2,034,359 | 2,030,272 | 208,200 | 325,600 | 64,200 |

All four candidate instruction counts pass the fixed original limits, including
UPSERT, which failed before this change. Three native medians fail the historical
limits. A sequential diagnostic with no concurrent builds or benchmarks reruns
the saved original, before-change and candidate binaries. Its original medians
are 61,880, 90,230, 82,250 and 340,800 ns respectively; candidate medians are
65,770, 89,410, 76,490 and 323,200 ns. The unchanged original also exceeds its
three historical limits. These samples show a timing-environment change, but
they do not replace the acceptance baseline or clear the native failures.

`prepare-row-metadata-baseline/` and `prepare-row-metadata-candidate/` retain the
initial isolated comparison. `prepare-register-allocation-candidate/` retains
the accepted source change's measurements, comparison with the fixed original
baseline, and validation. `prepare-register-allocation-timing-repeat/` retains
all 21 diagnostic native runs and executable hashes. The focused integration
suite passes 474 tests with seven ignored and one explicit host io_uring skip;
the forced/disabled form comparison also passes.

An intermediate experiment allocated explanation metadata only on demand. Its
extra allocation work increased INSERT and UPSERT instruction counts, so its
source change was discarded. `prepare-lazy-explain-candidate/` retains that
experiment's source diff, samples and failed outcome. None of these focused
comparisons establishes final preparation or execution parity.

## Duplicate filter normalization

`prepare-duplicate-filters-before/` and `prepare-duplicate-filters-grouped/` retain
seven native and three Callgrind rounds for thirteen prepare workloads. Eight new
fixtures vary repeated or distinct filters across 1, 8, 32 and 64 terms; each also
contains a correlated EXISTS. Five fixtures are from the fixed original corpus.
All three binaries in this investigation use the same benchmark harness. The
before-rule executable contains `23a84fbfa` and the added harness. The manifests
record the unchanged deferred source drafts present in all builds.

| Filter workload | Before-rule maximum instructions | Grouped search maximum instructions | Change |
|---|---:|---:|---:|
| 8 repeated | 3,835,359 | 3,291,920 | -14.17% |
| 32 repeated | 10,855,484 | 8,311,393 | -23.44% |
| 64 repeated | 20,498,109 | 15,329,706 | -25.21% |
| 8 distinct | 3,838,287 | 3,955,003 | +3.04% |
| 32 distinct | 10,866,234 | 11,313,199 | +4.11% |
| 64 distinct | 20,519,891 | 21,406,533 | +4.32% |

An initial implementation compared each expression with every earlier expression.
Its cost at 64 distinct filters increased by 16.28%; its source, correctness
results and measurements are retained in `prepare-duplicate-filters-candidate/`.
Grouping by expression hash reduces that cost, and exact bound-expression
comparison still decides whether to remove a predicate. The remaining distinct
filter overhead is unfinished performance work. Measuring the eight new fixtures
at the original baseline is also outstanding; the immediate before-rule
comparison does not replace that baseline.

All five existing workloads pass their fixed original instruction limits.
INSERT, UPSERT, correlated EXISTS and correlated scalar-subquery timings exceed
the historical native limits; primary-key lookup passes. None of the thirteen
workloads exceeds the immediate before-rule native spread. The historical limits
remain unchanged, and the four native failures remain open. Per-workload results
are in `comparison-fixed-original.json` and `comparison-before-rule.json` under
`prepare-duplicate-filters-grouped/`.

## Normalization inspection prepare comparison

`prepare-normalization-inspection-final/` retains seven native and three Callgrind
rounds with the same thirteen-workload filter as the preceding duplicate-filter
comparison. All five existing workloads have exactly the same maximum instruction
counts as `prepare-duplicate-filters-grouped/` and pass the fixed original limits.
INSERT, UPSERT, correlated EXISTS and correlated scalar-subquery native medians
still exceed the historical limits. All thirteen native medians remain within
the preceding candidate's spread; that diagnostic spread does not replace the
historical acceptance limits.

The 64-distinct-filter case increases by 1,514 instructions (0.0071%) against the
preceding candidate, and the 64-repeated-filter case increases by 10 instructions.
These increases are retained in `comparison-before-inspection.json`; their cause
is not established. Measuring the eight scaling fixtures at the original
baseline, the earlier distinct-filter overhead and full-corpus parity remain
outstanding. The generated normalization diagnostic function is absent from all
45 retained Callgrind function records, as recorded in `inspection-functions.json`.
It is called only while serializing logical inspection output.

`prepare-normalization-inspection/` retains an earlier generated form whose
repeated else-if branches failed strict lint. The final generator stops after the
first failed precondition and passes 52 JSON tests, 36 relational tests, formatting
and strict lint for the changed packages. Both executable manifests record their
source diffs and the unchanged deferred source drafts included in the builds.

## Scalar result operator prepare comparison

`prepare-scalar-result-operators/` retains seven native and three Callgrind rounds
for fifteen workloads. The five existing fixtures remain below their fixed
original instruction limits. Four native medians still exceed the historical
limits; the primary-key lookup remains within its original uncertainty.

| Workload | Fixed original maximum instructions | Candidate maximum instructions | Fixed original median ns | Candidate median ns |
|---|---:|---:|---:|---:|
| Parameterized INSERT | 476,548 | 473,701 | 39,880 | 61,980 |
| UPSERT | 741,720 | 737,917 | 61,520 | 89,830 |
| Primary-key lookup | 519,615 | 504,286 | 51,410 | 78,400 |
| Correlated EXISTS | 1,951,691 | 1,943,749 | 192,300 | 323,800 |
| Correlated scalar query | 2,099,903 | 2,028,042 | 208,200 | 342,000 |

All thirteen overlapping native medians remain within the immediately preceding
candidate's spread. Several instruction counts increase slightly relative to
that candidate; these remain visible in
`comparison-before-scalar-operators.json`. The eight filter-scaling fixtures still
need original-engine measurements. Neither the preceding candidate nor its timing
spread replaces the original acceptance data.

Two new prepare fixtures exercise the scalar-result operator beside EXISTS and
NOT EXISTS. Ordered first-row selection measures 3,221,826 instructions and a
503,100 ns native median. The empty-result fixture measures 3,010,837 instructions
and a 506,500 ns median. Their original-engine measurements remain outstanding.
`scalar-result-operators/` retains 483 passing integration tests, including 56 JSON
tests, 37 relational tests, 1,425 SQL cases, eight SQLite reference cases,
forced/disabled results, formatting and strict selected lint. These results
establish the bounded adapter's correctness, not general scalar decorrelation or
final performance parity.

## Original-engine measurements for the newer fixtures

`prepare-original-with-scalar-fixtures/` builds the fixed original revision
`a9a8779c1906247ae3ae78cd098ba713c27d8c9b` with the current prepare harness.
The harness is the only source difference; the original-engine executable
contains no deferred drafts. The implementation branch, five deferred files and
three staged inputs were restored before measurement, with exact file hashes
retained in `restored.json`. Seven native and three Callgrind rounds complete the
missing original-engine measurements for ten fixtures.

| New fixture | Original maximum instructions | Scalar-operator candidate maximum | Median instruction change |
|---|---:|---:|---:|
| 1 distinct filter | 1,810,040 | 1,830,918 | +1.15% |
| 8 distinct filters | 3,716,663 | 3,956,521 | +6.45% |
| 32 distinct filters | 10,394,239 | 11,314,745 | +8.85% |
| 64 distinct filters | 19,586,540 | 21,409,279 | +9.35% |
| 1 repeated filter | 1,808,460 | 1,828,487 | +1.11% |
| 8 repeated filters | 3,713,991 | 3,293,555 | -11.32% |
| 32 repeated filters | 10,385,692 | 8,309,783 | -19.99% |
| 64 repeated filters | 19,549,322 | 15,331,800 | -21.57% |
| Scalar empty result beside NOT EXISTS | 1,950,721 | 3,010,837 | +54.34% |
| Scalar ordered first row beside EXISTS | 2,067,829 | 3,221,826 | +55.81% |

Seven fixtures fail the instruction criterion. The two scalar-result fixtures
also exceed the original engine's measured native uncertainty. These are open
prepare regressions; successful execution and faster repeated-filter prepares do
not resolve them. The full comparison is in
`comparison-scalar-result-operators.json`.

Five existing fixtures were measured in the same invocation as additional
diagnostics. The candidate's native medians fit within those new samples' spread,
but the historical baseline and its four outstanding native failures remain
unchanged. The fixed acceptance formulas were not widened.

## Scalar results with parent ordering, limits and DISTINCT

`scalar-parent-operators/` records 484 passing integration tests, including 57
logical JSON tests, 37 relational tests, 1,433 SQL cases and eight new SQLite
reference cases. Forced/disabled comparisons, formatting and selected strict
lint pass. The initial JSON test failed because parent ordering retained legacy
binding. Four direct LIMIT/OFFSET cases failed before the emitter change and
pass afterward, including error precedence. Scalar dependencies remain; this
extension does not implement general scalar decorrelation.

`prepare-scalar-parent-operators/` records seven native and three Callgrind rounds
for seventeen workloads. Fifteen overlap the preceding scalar-operator candidate;
all overlapping native medians fit within that candidate's measured spread.
Four existing instruction counts are unchanged; the correlated scalar count
increases by 250 instructions and remains below its fixed original limit. The
filter-scaling counts change by between -524 and +2,749 instructions relative to
the preceding candidate. These small changes remain visible in the comparison;
their cause is not established.

`prepare-original-scalar-parent-fixtures/` measures the two new fixtures on the
fixed original engine with only the current benchmark harness. The build and
workspace restoration procedure preserves hashes for all thirteen modified files
and the three staged inputs. Original native measurements run after the build
and restoration; Callgrind overlaps the next expression-walker test build.
The candidate Callgrind run overlaps the original build. No native round
overlaps a build, and no benchmarks run concurrently.

| New parent form | Original maximum instructions | Candidate maximum instructions | Median instruction change |
|---|---:|---:|---:|
| DISTINCT | 2,041,689 | 3,209,053 | +57.18% |
| ORDER BY, LIMIT and OFFSET | 2,160,441 | 3,417,779 | +58.20% |

Nine of the seventeen fixtures fail their fixed original instruction criterion.
Seven native medians exceed their fixed original uncertainty: the four existing
INSERT/UPSERT/EXISTS/scalar failures, the scalar empty-result query and both new
parent forms. All original measurements for these seventeen fixtures are now
present. The failures and the broader final corpus comparison remain unfinished.
The separate expression-walker experiment is not included in these binaries.

## Expression traversal setup

`prepare-leaf-expression-walk/` measures direct visitor calls for immutable
column, rowid, literal and parameter expressions, avoiding a traversal stack
when the expression has no children. The visitor still runs once, errors still
propagate, and a skipped leaf still completes its walk successfully. The
general immutable and mutable tree traversal behavior is unchanged.

`leaf-expression-walk/` records two focused visitor tests, 198 translation tests,
484 integration tests with seven ignored and the explicit host io_uring skip,
1,433 SQL cases, forced/disabled comparisons, formatting and selected strict lint.

The seven native and three Callgrind rounds cover twenty-two workloads. All
seventeen cases measured for the preceding parent-scalar candidate use fewer
instructions, with median reductions of 0.68% to 2.40%.

| Workload | Before maximum instructions | After maximum instructions |
|---|---:|---:|
| insert_single_row_params | 473,701 | 467,199 |
| select_point_lookup_pk | 504,286 | 492,775 |
| subquery_distinct_filters/1 | 1,830,919 | 1,792,160 |
| subquery_scalar_empty_result_with_not_exists | 3,010,903 | 2,968,023 |
| subquery_scalar_parent_distinct | 3,209,053 | 3,131,966 |

The one-filter fixtures now pass their fixed original instruction criteria.
CREATE TABLE, CREATE INDEX and ClickBench 31 also pass the original instruction
limits. Nine cases still fail: TPC-DS 30 and 81, the three larger distinct-filter
fixtures and all four scalar-result fixtures. The scalar-result increases are
52.15% to 55.25% relative to the original engine; the small traversal saving does
not resolve them. Fourteen native medians exceed fixed original uncertainty.

Nine initial native medians also exceed the preceding candidate's uncertainty.
`prepare-leaf-walk-timing-repeat/` records seven interleaved before/after rounds
for the seventeen common workloads, alternating which executable runs first.
None of the paired median increases exceeds the preceding candidate's unchanged
uncertainty. This repeat does not reproduce the initial slowdown at that limit.
The initial samples, failures and fixed original criteria remain unchanged;
neither the repeat nor its spread replaces them. Native rounds run without
concurrent builds or benchmarks. Later Callgrind rounds overlap a SELECT
diagnostic build, with no other benchmark running. The diagnostic source changes
are absent from the measured executable.

## Repeated scalar planning across alternatives

`scalar-plan-call-counts/` records temporary SELECT diagnostics for the scalar
empty-result and first-row fixtures. The original query form estimates 160,000
calls for each child. Its rewritten form estimates 400,000 scalar calls. The
cache includes the exact call count, so it misses and plans the scalar body
again. Automatic selection keeps the original form in both fixtures.

The `single-filter-control/` experiment changes only whether the pure outer
filter appears once or twice. With one filter, both forms estimate 400,000 calls,
the scalar cache hits and automatic selection chooses the rewritten form. With
two identical filters, the original estimate drops to 160,000 while the rewritten
estimate stays at 400,000; the cache misses and selection changes to the original.
The transformed tree removes the duplicate filter before costing, while the
original tree still applies both filter selectivities. This inconsistency causes
repeated planning and changes the chosen alternative for equivalent filters.

These are diagnostic executions, not acceptance measurements. Exact source
diffs, executable hashes and outputs are retained. Temporary optimizer logging
and the control fixture were removed after building the saved executable, with
source hashes verified. The implementation and measurements below address the
inconsistent normalization; they do not resolve all scalar prepare costs.


## Normalize both query forms before costing

`normalized-alternatives/` records the normalization/exploration split. Initial
normalization now updates the original SELECT before creating and costing a
rewritten alternative. One, two and eight identical pure filters produce the
same selected physical nodes; the regression failed before this change.
Correlation filters stay separate from inner-join predicates until the dependent
rules can consume them, and removing an identity projection preserves eligibility
of a pure derived input. Both passes share the existing work and growth limits.

All 39 relational tests, 485 query-processing integration tests and 1,433 focused
SQL cases pass, along with forced/disabled comparisons, formatting and strict lint
for the changed packages. Seven integration tests remain ignored by the suite;
the host io_uring test remains explicitly excluded. The two initial rule-interaction
failures and their passing rerun are retained. The recorded source includes the
unchanged deferred drafts described in `source.json`; the commit excludes them.
No targeted deferred-bug reproduction was run.

`prepare-normalized-alternatives/` contains seven native and three Callgrind
rounds for the same twenty-two workloads as the preceding expression-walker
candidate. No benchmark overlaps a build or another benchmark.

| Workload | Before maximum instructions | After maximum instructions |
|---|---:|---:|
| repeated filters, 8 | 3,241,015 | 2,688,196 |
| repeated filters, 32 | 8,199,063 | 4,776,281 |
| repeated filters, 64 | 15,118,556 | 7,554,853 |
| distinct filters, 64 | 20,993,902 | 21,551,845 |
| scalar empty result | 2,968,023 | 2,991,636 |
| scalar first row | 3,165,350 | 3,149,550 |
| scalar parent DISTINCT | 3,131,966 | 3,114,652 |
| scalar parent ORDER/LIMIT | 3,354,158 | 3,368,114 |

The larger repeated-filter fixtures improve by 17.06% to 50.02% relative to the
preceding candidate. Distinct-filter fixtures increase by 1.64% to 2.66%; their
filters are checked again during exploration. Scalar changes range from -0.55%
to +0.80%. The cache correction therefore does not eliminate the scalar prepare
regression. CREATE TABLE, CREATE INDEX, INSERT and UPSERT instruction counts are
unchanged; point lookup and ClickBench 31 each add six instructions. Those six
instructions' cause has not been established.

No native median exceeds the preceding candidate's unchanged uncertainty.
Eleven workloads fail the fixed original instruction limits, including both
one-filter cases that previously passed. Thirteen native medians exceed the
fixed original uncertainty. Scalar-result instruction increases remain 52.31%
to 55.90% above the original engine. These failures, redundant second-pass work,
and the full-corpus comparison remain unfinished; the acceptance limits are
unchanged.


## Skip repeated normalization checks during exploration

`unchanged-normalization/` records exploration that normalizes new or changed
subtrees while skipping normalization checks on unchanged inputs. A changed
shared producer still makes its consumers eligible for normalization. The driver
continues to visit the same operators and uses the same work and growth limits.
All 40 relational tests, 485 integration tests, 1,433 focused SQL cases,
forced/disabled comparisons, formatting and selected strict lint pass. The
shared-producer test checks that a consumer filter becomes mergeable, and the
joined-input test checks normalization inside a newly constructed subtree.

`prepare-unchanged-normalization/` records seven native and three Callgrind
rounds for twenty-two workloads. The 8/32/64 distinct-filter fixtures use 1.48%,
1.98% and 2.07% fewer instructions than the preceding normalization candidate.
Their maximum counts are 3,913,629, 11,153,513 and 21,105,323. The large
repeated-filter savings remain. Scalar-result counts fall by 0.07% to 0.08%.
The existing correlated EXISTS fixture adds 184 instructions; the scalar-only
fixture and six ordinary statement/control counts are unchanged. TPC-DS counts
continue to vary slightly between rounds, with all samples retained.

Eleven fixtures still fail the fixed original instruction limits. Scalar-result
counts remain 52.20% to 55.79% above the original engine. No native median exceeds
its fixed original or preceding-candidate uncertainty in this run. Native medians
also fall substantially for unchanged controls, so the broad timing improvement
cannot be attributed to this source change. Earlier timing failures remain in the
record, and the full final corpus comparison is still required.

Native measurements run without concurrent builds or other benchmarks.
Callgrind overlaps compilation of the next correlated-membership structural
regression; that test is absent from the recorded validation and measured source.
The unchanged deferred source drafts remain documented in the source manifest
and excluded from the commit. No targeted deferred-bug reproduction was run.


## Correlated scalar and row membership filters

`correlated-membership/` records the extension of UnnestMembership to pure
projected filters with available outer columns. Local filters stay inside the
right input; comparison outputs and raw correlation columns remain distinct.
The slice passes 42 relational tests, 486 integration tests, 1,448 SQL cases,
the forced/disabled corpus including eight new distinct-plan cases, selected
strict lint and formatting. Fifteen focused cases also pass against SQLite
3.50.4. Separate preceding commits correct row-IN operand evaluation and legacy
IN comparison collation. Two older integration expectations were updated for
the now-supported NOT IN join and the projected input's search name.

`prepare-correlated-membership/` contains seven native and three Callgrind
rounds for twenty-six workloads. `prepare-original-membership-fixtures/`
builds the unchanged original engine with the current benchmark harness and
measures the three new prepare fixtures. All workspace files and user staging
are restored and checked after that temporary benchmark checkout.

| Workload | Original maximum instructions | Candidate maximum instructions | Increase |
|---|---:|---:|---:|
| correlated IN filter | 1,221,051 | 2,378,705 | 94.81% |
| correlated NOT IN filter | 1,196,268 | 2,707,764 | 126.35% |
| correlated row NOT IN filter | 1,338,644 | 3,375,737 | 152.18% |
| existing independent IN fixture | 1,029,074 | 2,105,393 | 104.69% |

Fifteen workloads fail the fixed original instruction limits; six fail its
native uncertainty limits. The preceding twenty-two-workload candidate still
has the same eleven instruction failures. Seven native medians exceed that
preceding candidate's uncertainty. Large repeated-filter improvements remain,
but scalar-result instruction increases remain 52.20% to 55.79%. Small changes
also occur in ordinary control instruction counts; their cause is not yet
established. No acceptance limit has changed.

`execution-correlated-membership/native-summary.json` records seven native
rounds for five workloads in automatic, forced and disabled modes. Every
workload is checked against SQLite before timing. Automatic IN inequality
execution takes 7.064 ms versus 79.670 ms with rewriting disabled; NOT IN takes
9.675 ms versus 79.520 ms. For the sixteen-row outer input and 4,096-row inner
input, automatic planning retains the correlated form (197.5 ms), avoiding
the slower forced projected join (419.7 ms). The row-NOT-IN/NULL workload shows
a costing miss: automatic execution retains the correlated form at 85.82 ms,
while the forced join takes 11.01 ms. These compare alternatives within the
current engine; original-engine execution measurements remain outstanding.
The existing `(k,v)` index is present in the indexed fixtures, but the retained
correlated predicate constrains `v` and therefore scans. These fixtures do not
establish indexed correlated execution acceptance.

The completed three-round execution Callgrind comparison confirms all five
forced plans use a semi/anti join and all disabled plans retain a subquery.
Automatic IN inequality uses 84,924,288 instructions versus 831,117,470 with
rewriting disabled (89.78% fewer). Automatic NOT IN uses 113,654,023 versus
831,749,570 (86.34% fewer). Forced row NOT IN uses 125,481,048 versus
955,306,922 (86.86% fewer), while automatic planning still selects the correlated
form. For the small outer input, the forced projected join uses 5,717,612,674
instructions, 181.11% above the disabled form. Automatic planning avoids that
join but its count is still 0.11% above disabled; the cause of that difference
has not been established. These results do not establish final acceptance.

Native measurements overlap neither builds nor other benchmarks. Prepare
Callgrind overlaps the execution-benchmark and original-engine builds. Execution
Callgrind overlaps compilation of the following direct-scan structural tests;
those edits are absent from the measured executable. Deferred source drafts
remain unchanged, recorded in the source manifest and excluded from the
commits; no targeted deferred-bug reproduction runs.

## Direct membership scans and nullable comparisons

`direct-membership-scans/` records lowering a pure membership projection over
one table into a direct semi/anti join. Constant NOT IN outputs or filters keep
their subquery boundary so the anti loop evaluates them in the right place.
Joined and shared inputs keep their existing projected column mapping.
`membership-null-checks/` then omits a comparison's NULL checks only when its
bound operand metadata proves they are unnecessary. Structural tests fail
before each change and pass after it. The final slice passes 45 relational
tests, 486 integration tests, 1,456 SQL cases, the forced/disabled corpus,
formatting and selected strict lint. Five new nullable-column cases also pass
against SQLite 3.50.4; three constant-result cases were checked in the direct
scan slice.

`prepare-direct-membership-scans/` retains three instruction rounds for the
initial scan change. That candidate increases scalar NOT IN preparation to
3,164,758 instructions and row NOT IN to 4,158,648. Removing unnecessary NULL
checks eliminates those increases. `prepare-membership-null-checks/` contains
the final seven native and three instruction rounds for all twenty-six fixtures.

| Workload | Projected-input maximum instructions | Direct scan with nullable checks |
|---|---:|---:|
| correlated IN filter | 2,378,705 | 2,059,293 |
| correlated NOT IN filter | 2,707,764 | 2,066,292 |
| correlated row NOT IN filter | 3,375,737 | 3,159,441 |
| existing independent IN fixture | 2,105,393 | 1,819,799 |

These reductions range from 6.41% to 23.69% against the preceding membership
implementation. They do not establish original-baseline parity: the four
fixtures remain 68.65%, 72.73%, 136.02% and 76.92% above their original instruction
counts, respectively. Fifteen workloads still fail the fixed original
instruction limits. Five native medians exceed the original uncertainty; none
exceeds the preceding membership candidate's uncertainty. The ordinary control
instruction counts are unchanged; other small instruction differences remain
recorded without an established cause. No acceptance limit changes.

`execution-membership-null-checks/native-summary.json` contains seven native
rounds for all five membership cases in automatic, forced and disabled modes.
Every workload checks ordered results against SQLite, and forced/disabled plans
have different executable forms. The small indexed case now searches `inner_key`
with both `k=?` and `v>?`: automatic execution takes 0.1238 ms, versus 196.4 ms
with rewriting disabled. The preceding projected-input implementation chose
the correlated form automatically at 197.5 ms. The other automatic scalar IN
and NOT IN cases take 7.179 to 9.676 ms, versus 78.55 to 79.84 ms disabled.
Automatic row NOT IN still chooses the correlated form (85.6 ms); its forced
join takes 10.65 ms. That cost-selection failure remains unfinished.

The completed three-round instruction comparison gives the small indexed case
1,125,038 instructions in both automatic and forced modes, versus 2,033,946,404
disabled. The preceding forced projected join took 5,717,612,674 instructions.
The other direct IN joins use about 0.03% fewer instructions than their
projected-input forms. Forced row NOT IN improves by 1.11%, to 124,090,870.
Scalar NOT IN instead increases by 0.85%, to 114,615,532 automatically and
114,620,096 forced; that difference remains unexplained. Small instruction
differences in unchanged correlated forms are also retained. No native median
exceeds the preceding membership implementation's uncertainty. These comparisons
do not establish original-engine execution acceptance; those measurements remain
outstanding.

Native runs overlap neither builds nor other benchmarks. Prepare instruction
collection overlaps the execution-benchmark build and compilation of a following
cost-selection test, which is absent from the measured executable and validation.
Execution instruction collection overlaps compilation of the following
cost-selection correction, also absent from the saved executable.
Function-level profiling annotations are inconsistent, so they are not used to
attribute the regression; the standard measurement boundary totals are retained.
Deferred source drafts remain unchanged and excluded from this change.

## Account for membership result indexes when selecting a plan

`membership-result-cost/` records adding the cost of inserting IN/NOT IN
subquery results into their temporary index. The estimate uses the existing
index-build formula and the estimated result rows per call, multiplied by the
number of calls. It applies to SELECT children with row estimates; compound
children and children without estimates keep their previous cost calculation.
The regression returns the same 248 ordered rows as SQLite before and after
the change, but its anti-join assertion fails before the correction. All 45
relational tests, 487 integration tests, 1,456 SQL cases, the forced/disabled
corpus, formatting and selected strict lint pass.

`execution-membership-result-cost/` records seven native and three instruction
rounds for all fifteen membership alternatives. Automatic row NOT IN now
selects the join, reducing its median from 85.6 ms to 10.46 ms and its maximum
instruction count from 955,250,820 to 124,093,136 (87.01% fewer). The other four
automatic instruction counts are unchanged. Forced indexed IN has a small
maximum increase of 812 instructions with an unchanged median; disabled indexed
IN decreases by 794. No native median exceeds the preceding candidate's fixed
uncertainty. Every measured alternative checks its ordered rows against SQLite;
every forced plan uses a semi/anti join while disabled plans retain subqueries.
Original-engine execution comparisons and the preceding scalar NOT IN
instruction increase remain unfinished.

`prepare-membership-result-cost/` contains the full seven native and three
instruction rounds for twenty-six fixtures. The four membership fixtures each
add 204 instructions. The ordinary control counts are unchanged. Fifteen
fixtures still exceed their original instruction limits and five exceed the
original native uncertainty. Correlated IN also exceeds the preceding
candidate's native uncertainty in this run: 207.6 to 305.6 microseconds, with
82.5 microseconds of fixed uncertainty.

The two `prepare-membership-*-native-repeat/` directories retain a diagnostic
sequence of seven interleaved before/after rounds using the saved binaries and
the same twenty-six fixtures. Correlated IN measures 242.0 microseconds before
and 241.9 after; none of the twenty-six median differences exceeds the preceding
candidate's fixed uncertainty. The slowdown therefore does not reproduce in
this diagnostic. Both the initial failure and these additional samples remain
recorded; neither the formal measurements nor their limits are replaced.

Native runs overlap neither builds nor other benchmarks. Instruction collection
overlaps work on the next membership projection extension, which is absent
from these saved binaries and validation. Deferred source drafts remain
unchanged and excluded from the commit.

## Outer values in membership projections

`membership-outer-projection/` records pure membership outputs that use
available outer columns over one independent scan. The outputs become join
comparison expressions. NOT IN still requires every output and filter to
reference the inner scan; other dependent projections remain outstanding.
All 46 relational tests, 487 integration tests, 1,468 SQL cases, the
forced/disabled corpus with eight new distinct-plan cases, formatting and
selected strict lint pass. Twelve focused cases also pass against SQLite
3.50.4 and the preceding compiler.

`prepare-membership-outer-projection/` contains seven native and three
instruction rounds for twenty-nine fixtures. The unchanged original engine
with the same harness supplies the three new fixture baselines in
`prepare-original-outer-projection-fixtures/`.

| New prepare workload | Original maximum instructions | Candidate maximum instructions | Increase |
|---|---:|---:|---:|
| IN with an outer value in its output | 991,072 | 1,594,810 | 60.92% |
| NOT IN with an outer value in its output | 1,244,736 | 2,921,723 | 134.73% |
| row NOT IN with an outer value in its output | 1,385,993 | 4,031,877 | 190.90% |

The original fifteen instruction failures remain, and the three new fixtures
also fail, for eighteen failures among twenty-nine workloads. Seven native
medians exceed the fixed original uncertainty, including the three new fixtures;
none of the twenty-six comparable fixtures exceeds the preceding candidate's
uncertainty. Ordinary controls have small instruction differences, including
413 more for the point lookup and 413 fewer for CREATE INDEX. No cause is
established for these changes. The complete per-workload comparisons retain
all increases and decreases. No acceptance limit changes.

`execution-membership-outer-projection/` contains seven native and three
instruction rounds for eight workloads in automatic, forced and disabled modes.
Every measured case checks its ordered results against SQLite. All forced
plans use semi/anti joins and all disabled plans retain subqueries. Automatic
planning also selects joins for the three new cases.

| New execution workload | Automatic instructions | Disabled instructions | Automatic median | Disabled median |
|---|---:|---:|---:|---:|
| IN with an outer value in its output | 316,065,715 | 5,067,047,807 | 26.20 ms | 477.30 ms |
| NOT IN with an outer value in its output | 134,204,117 | 821,425,381 | 10.76 ms | 74.65 ms |
| row NOT IN with an outer value in its output | 140,802,014 | 971,101,254 | 11.30 ms | 83.41 ms |

The preceding fifteen execution alternatives have small instruction changes,
retained individually in the comparison. None exceeds the preceding candidate's
native uncertainty. Original-engine execution measurements are recorded
separately in `execution-original-membership/`; these disabled alternatives
are not substitutes for that baseline.

The completed original-engine comparison covers all eight automatic workloads.
Every candidate automatic plan uses fewer instructions and stays within the
original native uncertainty. The new IN case falls from 5,140,973,048 to
316,065,715 instructions and from 459.1 to 26.2 ms. The new scalar NOT IN case
falls from 827,675,572 to 134,204,117 instructions and from 73.14 to 10.76 ms;
row NOT IN falls from 975,339,385 to 140,802,014 instructions and from 80.28 to
11.30 ms. All forced and disabled instruction counts also remain below the
matching original automatic count.

One disabled native alternative exceeds the original uncertainty in the formal
run: IN with an outer value in its output takes 477.3 ms versus 459.1 ms, with
12.3 ms of fixed original uncertainty. Seven interleaved diagnostic rounds using
the saved executables give medians of 448.1 ms original and 445.7 ms disabled,
so that slowdown does not reproduce. The initial failure, diagnostic samples
and original limits all remain recorded.

Native phases overlap no builds or other benchmarks. Candidate prepare
instruction collection overlaps the first generator regression build. Candidate
execution instruction collection overlaps the original-engine build and the
following generator changes and checks; those generator changes are absent
from these saved executables. The temporary original checkout restores the
working branch, file bytes and user staging, with recorded hash checks.
Deferred engine drafts remain unchanged and excluded from the commit.

## Reusing child plans that remain in the alternative

`prepare-components/` records an isolated correlated-IN profile with the saved
outer-projection executable. Consecutive function-entry dumps split its complete
2,065,215-instruction prepare into 411,713 instructions before logical binding,
124,700 through binding, 34,263 through normalization, 108,085 through exploration
and copying before resource extraction, and 1,386,454 for the remaining lowering,
physical planning, emission and destruction. The intervals sum exactly to the
complete-prepare control. They include caller work between the named entries;
they are not inclusive function costs. This isolated workload does not replace
the fixed twenty-nine-workload comparison.

The earlier function-return dumps include later operations: the binding dump
contains membership join construction, and binding, normalization and exploration
report the same ending basic-block position. Those profiles are retained and
marked unusable for attributing costs to their named functions. Parsed self-cost
totals match each retained interval; inclusive call-edge costs remain unreliable.

The child-plan cache previously copied every completed correlated child while
costing the original form, including membership bodies absent from the join
alternative. The optimizer now saves only children whose identities remain in
that alternative. Retained scalar and FROM children still use the cache, and
correlated reuse still requires an identical call count. The regression test
fails before this change because a removed membership child remains cached. Its
four final cases cover removed membership, retained scalar results and retained
derived inputs. The first derived fixture incorrectly used LIMIT, which prevents
this membership rewrite; the corrected fixture uses a plain projection.

All seven native rounds report 337 allocations for correlated IN, down from 352.
The other five correlated membership fixtures remove twelve to eighteen
allocations per prepare. `subquery-cache-copies/` retains the samples, source
hashes, regression result and validation. Validation passes 62 optimizer and
relational unit tests, 487 integration tests, 1,468 SQL cases, the forced/disabled
corpus, formatting and selected strict lint.

`prepare-subquery-cache-copies/` contains seven native and three instruction
rounds for the same twenty-nine workloads. The six correlated membership
fixtures remove 27,544–39,504 instructions per prepare, a reduction of
0.98–1.73% from the preceding candidate. The single-filter repeated and distinct
fixtures also fall below their original instruction limits: 1,784,415 versus
1,808,460, and 1,784,458 versus 1,810,040. Sixteen workloads still exceed the
original instruction limits, and seven exceed the original native uncertainty.
No native median exceeds the preceding candidate's uncertainty.

Four ordinary controls increase slightly from the preceding candidate: CREATE
INDEX by 413 instructions, CREATE TABLE by one, parameterized INSERT by 415 and
point lookup by 381. They still pass the fixed original limits. No cause is
established for those small changes; the comparisons retain them individually.
Neither these results nor the two resolved instruction failures establish final
performance parity. Original limits remain unchanged.

All 102 prepared-execution fixtures return their SQLite-checked results, and
every saved physical plan is byte-identical before and after the cache change.
The SELECT-only differential run uses seed 57291020 and depth one. It executes
1,000 statements with no skips, errors, warnings or oracle failures, compares
233 different forced/disabled plans and validates 94 independent joined
equivalents. Another 212 checks select the same plan. Its SQL history is
byte-identical to the retained generator run; the outcome records that file and
its hash. Native measurement, instruction measurement, execution checks and
fuzzing run sequentially after all builds finish.

## Copying a legacy alternative at its first mutation

When logical rewriting has no alternative, legacy rule checks now borrow the
current SELECT until a rule first mutates it. Later changes reuse the same owned
copy. The existing change flags still decide whether an alternative is planned;
ownership alone does not decide that. Nine regression cases cover declined
scalar, membership and limited-EXISTS checks, successful EXISTS and aggregate
rewrites, and repeated scalar aggregates. The test fails with eager copying under
the adapted interface, then passes with copying moved to the mutation sites.

`lazy-legacy-copy/` records the source, regression and validation. Validation
passes 63 optimizer and relational tests, 487 integration tests, 1,468 SQL cases,
the forced/disabled corpus, formatting and selected strict lint. All 102
SQLite-checked execution fixtures preserve their saved plans byte for byte. The
SELECT-only seed 57291020 again executes 1,000 statements without skips, errors,
warnings or oracle failures: 233 comparisons use different plans, 212 use the
same plan, and 94 independent joined equivalents pass. Its SQL history matches
the retained generator run.

The fixed twenty-nine-workload comparison in `prepare-lazy-legacy-copy/` still
has sixteen original instruction failures and seven original native failures.
Those subquery fixtures already receive an alternative and therefore do not
exercise the avoided copy. They add approximately 650–875 instructions relative
to the preceding candidate. Self-cost comparison for correlated IN accounts for
its 658-instruction increase, including 470 additional memcpy instructions and
70 in Cow dereferencing. The raw profiles remain in the two prepare directories;
these are self costs, not inclusive function costs.

Three additional fixtures measure actual declined rewrites. Both executables use
the same extended benchmark source, the same deferred drafts and the same build
settings. The earlier executable uses the optimizer files from before this
change; those files are restored before building the candidate. The paired
results are in `prepare-lazy-legacy-declined-before/` and
`prepare-lazy-legacy-declined-after/`:

| Prepare fixture | Before maximum instructions | After maximum instructions | Allocations before → after |
|---|---:|---:|---:|
| IN in the projection | 948,229 | 901,076 | 196 → 174 |
| Scalar first row | 1,077,494 | 1,023,463 | 236 → 210 |
| SUM in the projection | 1,226,415 | 1,157,518 | 280 → 246 |

The instruction reduction is 4.97–5.62%; allocation counts are identical across
all seven rounds for each executable. Native medians move from 97.19 to 91.86,
101.9 to 101.6, and 115.8 to 116.2 microseconds, respectively. All differences are
within the measured before-run uncertainty, so these timing samples do not
establish a wall-time improvement. Original-engine measurements for the three
new fixtures remain required.

The formal twenty-nine-case timing run also exceeded the preceding candidate's
uncertainty for TPC-DS 30 and scalar COUNT. Seven interleaved pairs include both
controls: medians are 1,480 versus 1,478 microseconds for TPC-DS 30 and 189.3
versus 198.3 for COUNT, within their paired uncertainties of 107 and 13
microseconds. This diagnostic does not replace the formal result or change any
historical limit. The small instruction increases for both controls remain
recorded. No preparation benchmark overlaps a build, another benchmark or the
execution and differential checks.

## Membership projections over joined inputs

Membership outputs can now read available outer columns above an independent
joined, shared or derived input. The rewrite projects the required raw inner
columns through a FROM boundary, leaving local filters inside that input and
evaluating the original comparison expressions in the semi/anti join. NOT IN
still requires every comparison output to read the inner input. This does not
implement propagation through dependent aggregates, ordering or limits.

`membership-joined-projections/` records the failing-before structural regression,
source hashes and validation. Four structural cases check correlation removal,
raw column identities and the one-node growth allowance. Sixteen added SQL cases
cover NULLs, duplicate rows, empty inputs, row comparisons, collation, disjunction,
MATERIALIZED CTEs and derived VALUES. Thirteen added oracle cases require distinct
forced/disabled plans and SQLite-compatible results. Validation passes 64 optimizer
and relational tests, 487 integration tests, 1,484 selected SQL cases, formatting
and selected strict lint. The host cannot run the io_uring busy-snapshot test;
that existing exclusion remains explicit in the command record.

All 114 execution fixtures pass their SQLite result checks. Seven new physical
plans change; the existing 102 remain byte-identical. The SELECT-only seed
57291020 executes 1,000 statements without skips, warnings, errors or oracle
failures. It compares 236 different plans, 209 identical plans and 94 independently
constructed joined equivalents. Its SQL history matches the retained generator
run; rule trace counts include repeated preparation and inspection.

Seven native rounds are complete for the original engine, the preceding candidate
and this implementation. The same added benchmark source is used throughout.
The original baseline is still `a9a8779c1906247ae3ae78cd098ba713c27d8c9b`.
The original checkout excludes deferred drafts; both candidate executables
include the same unchanged drafts, recorded in their source manifests. The
temporary checkout restores the working branch, bytes and staged user inputs.

| Automatic execution fixture | Original median, ms | Preceding median, ms | Current median, ms |
|---|---:|---:|---:|
| Joined-projection IN | 914.4 | 963.9 | 270.7 |
| Joined-projection NOT IN | 111.7 | 119.1 | 20.66 |
| Joined-projection row NOT IN | 142.2 | 148.4 | 39.70 |
| Joined-projection IN, small indexed outer input | 166.3 | 177.1 | 179.2 |

The small indexed query retains correlated execution. Its current median exceeds
the original 6.8 ms uncertainty and remains an unresolved timing failure. Forcing
the new alternative takes 5,583 ms, which demonstrates why automatic planning
must retain the indexed correlated choice. The other three automatic cases
improve execution, but their preparation regresses:

| Prepare fixture | Original median, µs | Preceding median, µs | Current median, µs |
|---|---:|---:|---:|
| Joined-projection IN | 194.7 | 200.6 | 373.4 |
| Joined-projection NOT IN | 168.3 | 213.4 | 402.8 |
| Joined-projection row NOT IN | 175.4 | 205.4 | 440.4 |

These three preparation timings exceed their fixed original limits. The native
summaries retain every workload and sample; execution gains do not compensate
for these failures. The thirty-five-workload prepare instruction comparison is
complete: nineteen workloads exceed the original instruction limit, and nine
exceed the original native limit. The new joined-projection IN, NOT IN and row
NOT IN cases use maxima of 3,911,069, 4,356,976 and 4,595,821 instructions,
respectively, compared with original maxima of 1,891,525, 1,867,151 and 1,900,059.
The execution instruction comparison is complete for all twelve candidate modes
and four original automatic workloads. All four automatic plans use fewer
instructions than the original. The small indexed case uses 1,825,834,855
instructions against 1,871,471,697 originally, but its native median of 179.2 ms
still exceeds the original 166.3 ms plus the fixed 6.8 ms uncertainty. That native
failure remains unresolved. Forcing the decorrelated alternative for this case
uses 81,321,451,505 instructions and 5,583 ms; automatic selection retains indexed
correlated execution. The full final corpus comparison remains required.

Original instruction measurements are complete for the three declined-rewrite
fixtures introduced with delayed legacy copying. Maximum original counts are
976,384 for IN in a projection, 1,125,257 for scalar first row and 1,274,522 for
scalar SUM. The saved delayed-copy candidate uses 901,076, 1,023,463 and 1,157,518,
respectively; all three also pass the original native limits. These measurements
in `prepare-original-joined-projection-fixtures/` complete that earlier missing
baseline comparison without changing any historical limits.

No build, test or other benchmark overlaps native measurement. Instruction
counting overlaps the additional CTE/derived-input test build, SQL checks and
selected lint. The saved engine and benchmark binaries remain unchanged;
`overlap.json` records this later validation alongside the initial isolation
snapshot. Later LEFT JOIN regression builds also overlap the final instruction
round. The saved binaries remain unchanged. The recorded checkout and execution
check pauses suspended Callgrind and both measurement drivers together, then
resumed them. No second benchmark overlaps instruction collection.

## Reusing membership comparison operands

Membership comparison construction now moves its operands into equality instead
of copying both expression trees first. NOT IN copies an operand only when its
NULL check also needs that expression. Optional NULL operands use the boxes that
their final expressions require, keeping the temporary values small.

The eight-case allocation regression fails before this change because positive
IN discards both original operand allocations. It passes after the change for
both operators and all input-nullability combinations. Validation passes 65
optimizer and relational tests, 487 integration tests, 1,484 SQL cases, the
forced/disabled corpus, formatting and selected strict lint. All 114 execution
fixtures retain their physical plans byte for byte. Seed 57291020 executes 1,000
statements without skips, errors, warnings or mismatches; it again compares 236
different plans, 209 identical plans and 94 independent joined equivalents.

`membership-comparison-copies/` records the source, regression, validation and
saved binary hashes. Its thirty-five-workload comparison is complete after seven
native and three Callgrind rounds. Moving the operands reduces preparation
instructions by 0.64% for IN with an outer-dependent projection, 0.34% for IN
with a joined projection, and 0.06–0.10% for the corresponding NOT IN cases.
The improvement is small: nineteen workloads still exceed the fixed original
instruction limit, and ten exceed the original native limit. Five workloads
also exceed the preceding candidate's native uncertainty. These failures remain
in the per-workload comparison; allocation reuse does not establish parity.
The native rounds ran alone. LEFT JOIN builds and correctness checks overlapped
instruction counting, with no second benchmark running.

## Direct SELECT subquery results

The direct-output adapter adds MarkJoin for EXISTS, NOT EXISTS, scalar IN/NOT IN
and row IN/NOT IN. The marker remains dependent while a sibling filter can be
rewritten and lowered. The existing Relation enum stays at 56 bytes in this
build, verified from the before and after debug information.

`mark-projections/` retains the failing-before regression, 16 SQL additions,
17 SQLite comparisons with distinct forced/disabled filter plans, and the source
and binary hashes. Validation passes 66 optimizer/relational tests, 490 query
integration tests, 1,500 SQL cases, formatting and selected strict lint. All 123
candidate execution checks match SQLite, including every returned integer or
NULL column; the 114 earlier plan files are unchanged. The nine new original
engine checks also match SQLite and have the same plans in every mode. The
1,000-statement seed has no skips, errors, warnings or mismatches, with 238
different-plan comparisons, 207 identical-plan checks and 94 joined equivalents.

The full serial core run has 2,545 passes, 17 ignored tests and two failures:
the host denies io_uring setup, and a passive-MVCC transfer test observes an
incorrect total. That transfer test passes alone but remains unresolved. A
separate test-isolation fix corrects registry resets that disrupted an attached
reader during parallel tests; its deterministic regression and three successful
34-test parallel runs are recorded in `mvcc-test-registry/`.

Three prepare and three execution fixtures have saved original and candidate
binaries. Their measurements remain pending. Callgrind was paused for the
original-engine checkout and for benchmark correctness checks, then resumed;
the two pause records preserve those intervals. Fixed native and instruction
acceptance limits remain unchanged. This adapter does not establish marker
decorrelation or final performance parity.

## LEFT JOIN inputs

The LEFT JOIN adapter preserves ON predicates, nullable right-side outputs and
USING metadata while allowing rewrites within either input. The initial
integration regression fails on the prior compiler's legacy-path response.
The implementation passes 67 optimizer/relational tests, 493 query-processing
integration tests, 1,729 SQL cases, formatting and selected strict lint.
Ten new SQLite comparisons exercise forced, disabled and automatic planning,
including at least two cases with different forced and disabled physical plans.

All 129 candidate execution fixture modes match SQLite, and the previous 123
physical plan files remain unchanged. The six original-engine checks for the two
new fixtures also match SQLite. Seed 57291020 executes 1,000 statements without
skips, errors, warnings or mismatches, with 238 different-plan comparisons, 207
same-plan comparisons and 94 independent joined equivalents. Its SQL history
matches the preceding run byte for byte.

`left-join/` retains the source, test results, saved binaries, SQL references and
preservation checks for the deferred drafts. Two prepare and two execution
fixtures have identical harnesses on the original engine and this candidate;
their performance comparison remains pending behind the mark-result series.
The original build and execution checks paused that series and then resumed it;
the recorded intervals affect no native timings. The earlier full-core passive
MVCC mismatch remains unresolved, and this adapter does not establish general
outer-join decorrelation or final performance parity.
