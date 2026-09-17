# Tuning the optimizer cost parameters

The query optimizer scores each candidate plan with the constants in
[`core/translate/optimizer/cost_params.rs`](../../core/translate/optimizer/cost_params.rs).
The constants decide which plan wins, so a wrong constant makes the optimizer
choose a slow plan even when the cost model itself is correct.

This directory holds the procedure that set those constants from measurements
instead of from judgement. A learned model reads the measurements and proposes
the next parameter set to try.

## What the procedure changes

Only the values in `CostModelParams::new()` change. No cost formula changes, and
no plan-selection code changes. A parameter that the benchmarks cannot move
keeps its old value.

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

The harness reads the time the CLI reports for a statement with `.timer on`, so
process start-up is not part of a measurement. It uses the default syscall
backend, not the `io_uring` backend that `perf/tpc-h/run.sh` selects. Both sides
of every comparison use the same backend, and the choice does not change which
plan the optimizer picks.

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

## Step 6: cut the result down to few digits

```bash
python perf/optimizer-tuning/tune.py polish --params best-params.json --out tuned.json
```

The search reports eight digits, which claims a precision that 64 timed queries
cannot support. The score only moves when a plan flips, so a rounded value is as
good as the searched one while every query still compiles to the same bytecode.
`polish` rounds each changed parameter to three significant digits, checks the
plans, and keeps more digits only for a parameter that needs them.
`apply_params.py` then writes the result into `CostModelParams::new()`.

## Step 7: measure the result

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
with machine load, so it shows a change that timing noise would hide. Callgrind
runs the binary about 70 times slower, so a full pass takes about two hours.
Give each parallel callgrind job its own copy of the database, because of the
file lock.

Both measurements use one binary and change only `TURSO_OPTIMIZER_PARAMS`, so
nothing but the parameters differs between the two sides.

## Files

| File | Purpose |
| --- | --- |
| `bench.py` | Reads the two query sets, hashes plans, measures runtimes. |
| `space.py` | The range and scale of each parameter. |
| `tune.py` | The screen, the model-based search, and the rounding step. |
| `apply_params.py` | Writes a parameter set into `CostModelParams::new()`. |
| `compare.py` | Before and after wall time, interleaved. |
| `callgrind.py` | Before and after instruction counts. |
| `gen_clickbench_data.py` | A stand-in ClickBench dataset, for when the real one cannot be downloaded. |
| `import_clickbench.py` | Imports `hits.csv` without the sqlite3 shell. |

## Limits of this procedure

- The result is tuned for two analytic workloads on a database without
  `ANALYZE`. A write-heavy or small-table workload is not represented.
- Both databases index only their primary keys. A schema with many secondary
  indexes would exercise `sel_eq_indexed` and the index-choice parameters much
  harder than this workload does.
- The search optimizes measured runtime. It does not prove that a cost formula
  is right; a parameter can be pushed away from its physical meaning to
  compensate for a formula that is wrong.
- The score is a geometric mean over queries. It accepts a small loss on many
  queries in exchange for a large gain on a few.
