# Tuning the optimizer cost parameters

The query optimizer scores each candidate plan with the constants in
[`core/translate/optimizer/cost_params.rs`](../../core/translate/optimizer/cost_params.rs).
The constants decide which plan wins, so a wrong constant makes the optimizer
choose a slow plan even when the cost model itself is correct.

This directory holds the procedure that set those constants from measurements
instead of from judgement. A learned model reads the measurements and proposes
the next parameter set to try.

## What the procedure changes

The procedure changes values in `CostModelParams::new()`. A parameter that the
benchmarks cannot move keeps its old value, and no plan-selection code changes.

It changed one formula, once, and only because the measurements forced it: two
uses of `cpu_cost_per_seek` wanted opposite values, so the second use got a
constant of its own. Step 7b tells that story. Read a parameter that two
benchmarks pull in opposite directions as a sign that it stands for two things,
not as a number to compromise on.

## Requirements

- A `tursodb` binary built with the `optimizer_params` feature. The feature adds
  the `TURSO_OPTIMIZER_PARAMS` variable, which points at a JSON file of cost
  parameters. Without the feature the binary uses the compiled constants and
  ignores the variable.
- Python 3.11 with `numpy`, `scipy` and `scikit-learn`.
- `valgrind`, for the instruction counts.
- The TPC-H database and the ClickBench database.

```bash
cargo build --release -p turso_cli --bin tursodb --features optimizer_params
uv venv tuner-venv && uv pip install --python tuner-venv/bin/python numpy scipy scikit-learn
```

## Step 1: get the two benchmark databases

TPC-H comes from the download in `perf/tpc-h/benchmark.sh`, at scale factor 1:
8 tables, 6,001,215 rows in `LINEITEM`, and two automatic indexes from the
primary keys of `LINEITEM` and `PARTSUPP`.

```bash
./perf/tpc-h/benchmark.sh    # downloads perf/tpc-h/TPC-H.db, then times the queries
```

ClickBench is one table of 1,000,000 rows with 104 columns and one automatic
index from the primary key `(CounterID, EventDate, UserID, EventTime, WatchID)`.
`perf/clickbench/benchmark.sh` downloads the rows from
`datasets.clickhouse.com`. When that host is not available, `gen_clickbench_data.py`
writes a `hits.csv` with the same row count, the same distinct-value counts and
the same share of empty strings as the first 1,000,000 rows of the published
file, and `import_clickbench.py` imports it with the bundled schema:

```bash
./perf/clickbench/benchmark.sh    # preferred: real rows

# only when datasets.clickhouse.com cannot be reached
python perf/optimizer-tuning/gen_clickbench_data.py --rows 1000000
python perf/optimizer-tuning/import_clickbench.py
```

The numbers in this document come from the generated rows, because the machine
that ran them could not reach `datasets.clickhouse.com`. `CounterID = 62` covers
9.15 percent of the generated rows and the primary-key index drives queries 37
to 43, as it does on the real ones. Run the benchmark again on the real rows
before you rely on a ClickBench number here.

Both query sets run as the benchmarks ship them. Neither database gets `ANALYZE`,
because that is the state a new database is in, and it is the state in which the
fallback constants decide every plan. Set `TURSO_TPCH_DB` or
`TURSO_CLICKBENCH_DB` to point the harness at another copy, such as one that has
run `ANALYZE`.

## Step 2: make one evaluation cheap

A full pass over the 64 queries takes about 100 seconds, and a search needs
hundreds of passes. Two facts make that affordable.

A cost parameter changes runtime only when it changes the bytecode a query
compiles to. So `bench.py` runs `EXPLAIN` for every query, hashes the bytecode,
and measures a query only when its hash is new. One `EXPLAIN` pass over all 64
queries takes 0.7 seconds.

The hash ignores the schema cookie in the `Transaction` opcode. TPC-H query 15
builds a view and removes it again, so the cookie counts up as the benchmark
runs, and leaving it in would make every hash new on every pass.

A parameter set whose 64 hashes are all in the cache needs no measurement at
all. Late in the search most proposals are free.

`tursodb` takes an exclusive lock on the database file, so only one harness may
run against a database at a time. The harness stops with an error rather than
record a locked run as a slow plan.

The harness reads the `Execution:` time the CLI reports for a statement with
`.timer on`, so process start-up is not part of a measurement. That number is
not wall time: `next_row` in `cli/app.rs` stops the execution timer while the
statement waits on I/O, and the CLI reports I/O on a line of its own. On these
two databases the difference is nothing, because both fit in the page cache of
the machine and the CLI prints `I/O: No samples available` for every query, with
`Execution:` equal to the `total:` line. On a database larger than memory the
two would part, and the harness would then be measuring the wrong thing for an
I/O cost model. Read the seconds in this document as execution time.

The harness uses the default syscall backend, not the `io_uring` backend that
`perf/tpc-h/run.sh` selects. Both sides of every comparison use the same
backend, and the choice does not change which plan the optimizer picks.

## Step 3: find the parameters the workload can move

```bash
python perf/optimizer-tuning/tune.py screen --screen-out screen.json
```

The screen sweeps each parameter over its range, from the defaults and from
three random parameter sets, and counts how many queries change plan. Ten of the
22 parameters never change a plan on this workload:

| Parameter | Why the workload cannot move it |
| --- | --- |
| `sel_eq_indexed` | Both databases index only their primary keys, and every equality on those keys is a full unique key, which takes the point-lookup path instead. |
| `sel_is_null`, `sel_is_not_null` | No query has an `IS NULL` or `IS NOT NULL` term. |
| `sel_not_like` | No query has a `NOT LIKE` term. |
| `sel_other` | The remaining terms are all handled by a more specific selectivity. |
| `in_subquery_rows` | No query has an `IN` subquery of unknown size. |
| `hash_cpu_cost`, `hash_insert_cost`, `hash_lookup_cost`, `hash_bytes_per_row` | No hash join wins on these queries, so its cost never decides. |

These ten keep their old values. The search runs over the other 12.

The same screen shows which queries the search can move. Only 16 of the 64
queries compile to different bytecode anywhere in the parameter space: TPC-H
queries 2, 3, 5, 7, 8, 9, 10, 11, 15, 16, 17, 18, 20, 21 and 22, and ClickBench
query 28. TPC-H supplies almost all of the signal. ClickBench keeps the
same plan for every parameter set tried, so it works as a check that the change
leaves those queries alone, not as a source of improvement.

## Step 4: the score

The score of a parameter set is the geometric mean, over the queries, of

```
(runtime + 25 ms) / (runtime with the current defaults + 25 ms)
```

A geometric mean keeps the slowest query from deciding the result on its own. A
plain sum of runtimes would let TPC-H query 1 decide the result for all 42
ClickBench queries together. The 25 ms addition stops a 2 ms difference on a
fast query from looking like a 2x regression. A query that cannot finish within
30 seconds counts as eight times slower, which is a strong penalty in the
geometric mean without being infinite.

Every measurement keeps the fastest of three runs. Other work on the machine can
only make a run slower, so the fastest run is the most repeatable summary of a
plan's cost.

## Step 5: search with a learned model

```bash
python perf/optimizer-tuning/tune.py search \
    --active-params active.json --initial 64 --rounds 30 --batch 4 \
    --out best-params.json
```

The search is sequential model-based optimization:

1. A scrambled Sobol sequence gives 64 starting parameter sets, which spread over
   the 12-dimensional unit cube more evenly than random points do. The count is a
   power of two, because a Sobol sequence spreads evenly only over such a batch.
2. A `RandomForestRegressor` of 200 trees learns the map from a parameter set to
   its score. A forest fits this problem because the score is a step function:
   the score only moves when a plan flips, and a forest is built from steps.
3. The spread of the 200 tree predictions gives the uncertainty at a point.
   Expected improvement combines the predicted score and that uncertainty into
   one number, which is high where the model expects a better score and where
   the model does not yet know.
4. Each round scores 8,000 candidates with the forest, measures the best four by
   expected improvement, and fits the forest again. Half the candidates are
   random and half sit near the best parameter set found so far, so each round
   both explores and refines.

Each parameter is searched on the scale it acts on. A multiplier such as
`cpu_cost_per_row` is searched in log space, because a step from 0.001 to 0.002
matters as much as a step from 0.1 to 0.2. `space.py` holds the range and the
scale of each parameter.

Run the search more than one time, with a different `--seed` each time, and keep
the best result. The plan cache is written to disk and read back, so a second
search pays for a plan only when it finds one the first search did not. The
first search here took 24 minutes and measured 92 plans; the second reused that
cache and evaluated six parameter sets in 14 seconds.

## Step 6: read what the search actually learned

```bash
python perf/optimizer-tuning/tune.py shortlist \
    --history history.json,history2.json --max-regression 1.05 --out picked.json
python perf/optimizer-tuning/tune.py shrink --params picked.json --out shrunk.json
python perf/optimizer-tuning/tune.py shrink --params shrunk.json --out shrunk2.json
python perf/optimizer-tuning/tune.py polish --params shrunk2.json --out polished.json
```

The number the search minimizes is not the whole of what matters, and the
parameter set it reports is not shippable as it stands. Three steps read more
out of the same measurements.

`shortlist` scores every parameter set either search tried, out of the plan
cache, for the cost of one EXPLAIN pass each and no new measurement. It then
applies two rules the search does not know about. Drop any set that makes one
query more than `--max-regression` times slower. Of the sets within
`--tolerance` of the lowest total runtime, take the one that moves the
parameters least. The last rule matters: runtime only says which side of a plan
flip a value is on, so very different parameter sets reach the same runtime and
the search has no reason to prefer the moderate one.

`shrink` moves each parameter back toward its default as far as the plans allow,
largest change first, with a binary search on the distance. Two parameter sets
that compile every query to the same bytecode run at the same speed, so this
costs nothing. The step is greedy, so run it until it stops moving. Here it put
five of the twelve parameters back on their defaults exactly.

`polish` rounds what is left to three significant digits, again only while the
plans hold, and then puts back any parameter that still ends within `--snap` of
its default.

Reading the result of all this was the useful part, not the parameter set it
produced. Three searches disagreed on how far to move each parameter but
agreed on which way: `rows_per_table_fallback` down, `rows_per_table_page` up,
`cpu_cost_per_row` up, `cpu_cost_per_seek` up, `sel_range` down.

The parameter set itself was not shippable. The last `polish` left two
parameters differing from their defaults by less than one percent, which means
they were winning an exact cost tie rather than stating anything about
databases. Putting them back cost 4 seconds of the 6 the set had won, and made
TPC-H query 16 2.5 times slower. A result that depends on a value in its fifth
digit is tuned to these 64 queries and to nothing else.

## Step 7: turn the directions into round numbers

```bash
python perf/optimizer-tuning/tune.py grid --values values.json --max-regression 1.10
```

`--values` names a JSON file that offers a few round values for each parameter
the searches agreed on, `{"parameter": [value, ...]}`. `grid` measures every
combination and picks by three rules: no query more than `--max-regression`
times slower, within `--tolerance` of the lowest total runtime, and, of those,
the fewest parameters changed.

This is the step that produced the values in the change. Every value is a round
number, each of which a reader can weigh against what the parameter is supposed
to mean, and the measurement behind it is the same measurement the search used.

A first grid of 324 combinations found almost nothing, because it held
`index_bonus` and `closed_range_selectivity_factor` at their defaults and
stopped `rows_per_table_page` at 200. Give the grid the full range the search
used, or it cannot reach what the search found.

`apply_params.py` writes the result into `CostModelParams::new()`.

## Step 7b: when no value of a parameter is right, split the parameter

The round-number grid gave four changes and 98.1 s, and that set broke
`test_fts_join_order_optimization`: a five-row table reached by its primary key
turned into a scan repeated for every outer row. The cause was
`cpu_cost_per_seek`, which the grid had raised from 0.01 to 0.3. Bisecting both
sides showed two windows that do not overlap. The test needs 0.25 or less.
TPC-H query 11 needs 0.28 or more.

The reason is that one constant priced two different things. A seek into an
index that exists pays it one time, in `estimate_index_cost`. Building an
in-memory index pays it for every row, times the depth of the part already
built, in `estimate_ephemeral_index_build_cost`. Query 11 is decided by the
second, and the FTS test by the first, so no single value can be right for both.

The change adds `ephemeral_index_build_cost` for the second use. A parameter
that two benchmarks push in opposite directions is worth reading as a sign that
it stands for two things.

## Step 7c: measure the grid again after the split

With the two uses apart, the grid ran again over `rows_per_table_fallback`,
`rows_per_table_page`, `cpu_cost_per_row` and the new
`ephemeral_index_build_cost`. It found that one change carries the whole gain:

```bash
python perf/optimizer-tuning/tune.py grid --values values3.json --max-regression 1.05
```

`ephemeral_index_build_cost` 0.01 to 0.5, and the other three parameters back on
their old values. The four-parameter set from step 7 and this one-parameter set
reach the same runtime, so the rule that takes the fewest changes takes this one.
The three parameters the first grid moved were paying for a mispriced index
build, which is now priced directly.

The grid offered 0.5, and 0.5 turned out to break a second test:
`two-identical-averages-use-one-grouped-table` in
`sqlite-sqltests/unnest-correlated.sqltest`, which reads two copies of the same
correlated average out of one grouped table. Bisecting both cases gives a window
rather than a point. TPC-H query 11 takes the fast plan from about 0.402 up, and
the grouped table holds to about 0.452. The change ships 0.43, the middle of
that window. Every benchmark query compiles to the same bytecode anywhere in it,
so the measurements above hold for the whole window.

## Step 8: measure the result

Wall time moves with whatever else the machine is doing, so the final numbers
come from two measurements.

```bash
python perf/optimizer-tuning/compare.py --after best-params.json --out compare.json
python perf/optimizer-tuning/callgrind.py --params before.json --out cg-before.json
python perf/optimizer-tuning/callgrind.py --out cg-after.json
```

`compare.py` runs the old and the new parameters one after the other on each
query and keeps the fastest of five runs of each, so a machine that gets slower
during the run slows both sides by the same amount.

`callgrind.py` counts the instructions each query runs. The count does not move
with machine load, so it shows a change that timing noise would hide, and two
jobs can share a machine without disturbing each other. Callgrind runs the
binary about 70 times slower, so one pass over both benchmarks takes about two
hours of processor time. Split it with `--only` and a list of query keys, give
each job its own copy of the database because of the file lock, and balance the
lists by the wall times from `compare.py`. Splitting by position leaves one job
running long after the others stop.

Both measurements use one binary and change only `TURSO_OPTIMIZER_PARAMS`, so
nothing but the parameters differs between the two sides.

## What the measurements said

One constant changed, and one was added:

| Parameter | Before | After | What it says |
| --- | ---: | ---: | --- |
| `ephemeral_index_build_cost` | (shared `cpu_cost_per_seek`, 0.01) | 0.43 | What one key comparison costs while an in-memory index is built, against one page read. |

Everything else keeps its old value, `cpu_cost_per_seek` included.

Almost all of the gain is one query. TPC-H query 11 joins `partsupp`,
`supplier` and `nation` with no index on the join columns. The old number
priced a copy of `partsupp` into an in-memory index below three primary-key
seeks per row, so the optimizer built that copy twice, once for the query and
once for its subquery:

```
HASH JOIN supplier
SEARCH partsupp USING COVERING INDEX ephemeral_partsupp_t1 (PS_SUPPKEY=?)
SCAN nation
```

The new number prices the copy above the seeks:

```
SCAN partsupp USING INDEX sqlite_autoindex_partsupp_1
SEARCH supplier USING INTEGER PRIMARY KEY (rowid=?)
SEARCH nation USING INTEGER PRIMARY KEY (rowid=?)
```

| Query | Time before | Time after | Instructions before | Instructions after |
| --- | ---: | ---: | ---: | ---: |
| TPC-H 11 | 5.65 s | 0.95 s (-83%) | 30.97 G | 6.28 G (-80%) |
| TPC-H 17 | 6.01 s | 5.72 s (-5%) | 33.86 G | 31.77 G (-6%) |

Those are the only two queries whose instruction count moves by more than one
percent. Every other query stays within 0.01 percent, so the rest of the timing
differences are the noise of the machine, not the change.

| Benchmark | Time before | Time after | Change | Instructions before | Instructions after | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H (22 queries) | 68.71 s | 63.72 s | -7.3% | 403.4 G | 376.6 G | -6.6% |
| ClickBench (42 queries) | 32.34 s | 32.33 s | -0.0% | 214.5 G | 214.5 G | +0.0% |

Both columns cover the same queries. ClickBench query 29 is left out of both,
because `REGEXP_REPLACE` is not available and the query fails to parse on either
side.

## Was the learned model worth it here?

No. An independent review of this experiment counted the cost, and the honest
answer is that the search did not find the value that ships.

- The parameter that ships did not exist while the searches ran.
  `ephemeral_index_build_cost` was added to `space.py` after both searches
  finished, so neither search could ever have proposed it.
- The effect is one threshold in one dimension. In the 108-point grid that found
  it, the total takes four values, one per setting of
  `ephemeral_index_build_cost`, and the other three parameters never improve on
  their defaults. 104 of the 108 points are redundant. A four-point sweep finds
  the same answer.
- The model rounds proposed 440 parameter sets between the two searches and
  found 14 plans the Sobol points had not already found. The 64 Sobol points of
  the first search found 87 of the 92 plans it ever saw. Most of the search was
  quasi-random sampling, and the model added little to it.
- By the score the search minimizes, the shipped change is worse than what the
  search found: 0.9728 against 0.9646 and 0.9505. Its own answers were thrown
  away, for the reasons in step 6.

What did the work was the cheap part. `screen` sweeps one parameter at a time
and takes seven minutes, and it already named `cpu_cost_per_seek` as one of four
parameters that move TPC-H query 11. From there a four-point sweep and two
bisections reach 0.43 in about two minutes of measurement, against ninety
minutes for the searches.

Use the search where the screen shows many parameters moving many queries, and
where the interactions between them matter. For a workload where 16 of 64
queries can change plan at all, and where the answer is one threshold, sweep the
parameters the screen names and skip the model.

## Limits of this procedure

- Every query is timed with the database already in the page cache of the
  operating system. Both benchmark databases together are smaller than the
  memory of the machine, so a repeated query reads no disk. A plan that reads
  the same pages again and again thus looks better here than it would on a
  database larger than memory, and the tuned value reflects that.
- The result is tuned for two analytic workloads on a database without
  `ANALYZE`. A write-heavy or small-table workload is not represented.
- Both databases index only their primary keys. A schema with many secondary
  indexes would exercise `sel_eq_indexed` and the index-choice parameters much
  harder than this workload does.
- The search optimizes measured runtime. It does not prove that a cost formula
  is right; a parameter can be pushed away from its physical meaning to
  compensate for a formula that is wrong. 0.43 is one such value. Against a
  `cpu_cost_per_row` of 0.003, it says one key comparison during an index build
  costs 140 rows of work, which nothing about the code supports.
- What it is compensating for is visible in `estimate_index_cost`. A repeated
  scan of an inner table gets the `cache_reuse_factor` discount, and a repeated
  seek into one does not: `seek_cost` is `input_cardinality * tree_depth` page
  reads no matter how many times the same small table is read. So the model
  over-prices the nested loop that TPC-H query 11 wants, and the only lever the
  parameters give is to over-price its competitor by the same amount. Giving
  repeated seeks the same cache discount as repeated scans would price both
  directly, and is the change worth making next.
- The value sits in a window about 12 percent wide, between the plan flip that
  TPC-H query 11 needs and the one that `two-identical-averages-use-one-grouped-table`
  needs. A later change to a cost formula can move either edge past it.
- The score is a geometric mean over queries. It accepts a small loss on many
  queries in exchange for a large gain on a few.

## Files

| File | Purpose |
| --- | --- |
| `bench.py` | Reads the two query sets, hashes plans, measures runtimes. |
| `space.py` | The range and scale of each parameter. |
| `tune.py` | The screen, the search, the shortlist, the grid, the shrink and the rounding. |
| `apply_params.py` | Writes a parameter set into `CostModelParams::new()`. |
| `compare.py` | Before and after wall time, interleaved. |
| `callgrind.py` | Before and after instruction counts. |
| `gen_clickbench_data.py` | A stand-in ClickBench dataset, for when the real one cannot be downloaded. |
| `import_clickbench.py` | Imports `hits.csv` without the sqlite3 shell. |
