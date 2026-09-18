# Tuning the optimizer cost defaults against the bundled TPC-H workload

This is the record of one experiment. It measured query execution time, not
estimated cost. Every number below comes from a run in this repository at
commit `ae6e1561de1e2a331eff3ba94a1cfc78c9f7989c`.

## What changed

One default, in `CostModelParams::new()`:

| Parameter | Old | New | Ratio |
|-----------|-----|-----|-------|
| `sel_eq_unindexed` | 0.1 | 0.04 | x0.4 |

The search moved all 22 parameters. It found larger gains. Those gains are not
in this patch, because each one either made a common query shape much slower or
sat within 2% of a cliff that did. The section "Why the patch is one parameter"
gives the measurements.

## Result

Two ordinary release builds, no `optimizer_params` feature,
`TURSO_OPTIMIZER_PARAMS` unset. One carries the old default, one the new. Five
paired rounds over the whole query set in both statistics regimes, in turn,
with the order turned around on every other round.

| Regime | Old default | New default | Ratio |
|--------|-------------|-------------|-------|
| No `ANALYZE` | 56.28 s | 53.91 s | 0.9578 |
| After `ANALYZE` | 57.12 s | 56.45 s | 0.9881 |

`J = 0.9730`, 95% bootstrap interval over the five rounds `[0.9680, 0.9780]`,
standard deviation 0.0066. Per-round values: 0.9638, 0.9730, 0.9792, 0.9697,
0.9792. Geometric mean of the 44 per-query ratios: 0.9790, a speedup of 1.021x.

The same two builds under the conditions `perf/tpc-h/run.sh` uses, which drop
the page cache before every query, run the `tursodb` shell with the io_uring
backend, and time process start, parsing, preparation and printing the rows:

| Regime | Old default | New default | Ratio |
|--------|-------------|-------------|-------|
| No `ANALYZE` | 67.25 s | 62.86 s | 0.9346 |
| After `ANALYZE` | 65.99 s | 65.42 s | 0.9913 |

`J = 0.9629` over three paired rounds (0.9646, 0.9641, 0.9601). 264 runs, no
failures. Geometric mean of the per-query ratios: 0.9770, a speedup of 1.024x.

## The one regression

Query 5 without statistics gets slower. This is not noise and it does not go
away at any other value of the parameter.

| Query | Regime | Old | New | Ratio |
|-------|--------|-----|-----|-------|
| Q8 | no `ANALYZE` | 4.681 s | 1.132 s | **0.242** |
| Q20 | after `ANALYZE` | 3.044 s | 2.207 s | **0.725** |
| Q5 | no `ANALYZE` | 1.114 s | 2.319 s | **2.082** |

Every other query moves by less than 2%. The cold-cache regime agrees: Q8
7.078 s -> 1.868 s, Q20 3.280 s -> 2.457 s, Q5 1.627 s -> 2.808 s.

Q8 and Q5 are the same trade in opposite directions. Both queries join
`orders` and `lineitem`. The old default drives a nested loop from `orders`
and seeks `lineitem` once per row; the new one builds a hash table on
`lineitem` and looks up `orders` by rowid.

| | Q8 old | Q8 new | Q5 old | Q5 new |
|---|---|---|---|---|
| time | 5152 ms | 1184 ms | 1176 ms | 2371 ms |
| rows read | 3,329,417 | 6,202,474 | 1,679,992 | 6,015,294 |
| b-tree seeks | 4,155,184 | **76,751** | 1,087,784 | 1,397,003 |
| hash probes | 0 | 177,430 | 0 | 1,857,133 |

Q8 wins because it stops doing 4.1 million index seeks. Q5 loses because its
`orders` scan already carries a date filter, so its nested loop was cheap and
the hash build now reads the whole of `lineitem`.

Q20 wins for a different reason: the old plan builds an ephemeral covering
index over `nation`, a table of 25 rows, and reads 31.1 million rows. The new
plan scans `nation` directly and reads 7.4 million.

Q9, Q11 and Q15 change plan and keep their runtime within 2%.

The less regressive alternative is the old default: no gain, no regression.
There is no value of `sel_eq_unindexed` between the two.

## Why the patch is one parameter

The search found much better TPC-H numbers. They do not survive a check that
the TPC-H objective cannot make.

### The best vector the search found

`artifacts/best-unconstrained-do-not-ship.json` moves 8 parameters and scores
`J = 0.9407` with a 95% interval of `[0.9370, 0.9444]` over five paired
rounds, a geometric-mean speedup of 1.057x, and **no per-query regression
above 10%** in either regime. Ordinary release builds gave `J = 0.9413` and
the cold-cache io_uring regime gave `J = 0.9358`.

It is not in this patch. Against a two-table join with no statistics, a narrow
range on the driving table's primary key and a covering index on a table 20
times larger, it is **265 times slower**:

```sql
SELECT c.name, COUNT(o.order_id) FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id BETWEEN 10 AND 20
GROUP BY c.customer_id, c.name;
```

10,000 customers, 200,000 orders, no `ANALYZE`. The old default drives from the
11-row range and reads 231 rows in 0.09 ms. That vector scans the whole
`orders` index and reads 200,000 rows in 23.9 ms.

### No vector reaches that score without paying for it

`perf/optimizer-tuning/guards.py` holds ten query shapes of this kind with data
of a realistic size. 392 candidate vectors spread across the objective range
from 0.938 to 0.965 were run against all ten guards and against the whole
conformance corpus:

* 0 of 392 leave every recorded plan in the corpus unchanged.
* 41 of 392 keep every plan shape the corpus asserts.
* **1 of 392** also does the same work on all ten guards. It scores 0.9567.

That one vector sits on a cliff. Moving **any one** of five of its parameters
by **+2%** costs 865x on the same guard:

| Parameter moved by +2% | Worst guard |
|------------------------|-------------|
| `cache_reuse_factor` | 865.8x |
| `cpu_cost_per_row` | 865.8x |
| `rows_per_table_fallback` | 865.8x |
| `sel_eq_indexed` | 865.8x |
| `sel_range` | 865.8x |

A default that a 2% change turns into an 865x regression is not a setting. It
is a position on a cliff, and the next change to any other part of the cost
model will push it off.

`sel_eq_unindexed = 0.04` is not on that cliff. Every value from 0.02 to 0.08
reads exactly the same number of rows on all ten guards
(`artifacts/guards.csv`), and one guard reads half as many.

### What the cliff says about the cost model

With no `ANALYZE` statistics every table is assumed to hold
`rows_per_table_fallback` rows. A `BETWEEN` on the driving table is then
estimated at `rows_per_table_fallback * sel_range^2 *
closed_range_selectivity_factor`, which is 32,000 rows at the compiled
defaults. The child table, 20 times larger in reality, is assumed to hold the
same 1,000,000 rows as the parent. The two plans therefore price almost the
same, and the compiled defaults happen to sit on the correct side by a thin
margin.

This is a cardinality problem, not a coefficient that wants a better value. It
is recorded here and left alone, because fixing it belongs in its own change.

## Method

### Workload

All 22 files in `perf/tpc-h/queries`. No file carries a `LIMBO_SKIP`
directive, so Turso runs all 22. Three files carry `SQLITE_SKIP`, which says
the reference engine cannot run them; they stay in Turso's objective.
`15.sql` holds three statements: it creates a view, selects from it, and drops
it. The execution runner used to prepare only the first statement, which on a
read-only database failed with `attempt to write a readonly database`. The
runner now splits a query file with the engine's own parser and runs every
statement in order.

Database: TPC-H scale factor 1 from the URL `perf/tpc-h/benchmark.sh` uses,
1,252,864,000 bytes, SHA-256 `be2a81a5...8eb8bc8`. The download carries no
statistics tables, so it is the no-`ANALYZE` snapshot unchanged. The analyzed
snapshot is that file after `ANALYZE`, SHA-256 `7a9d950b...61cc8a4`. Its
`sqlite_stat1` holds 8 rows: a row count for each of the 8 tables and a
per-prefix average for the two automatic indexes.

### Objective

```text
T_m(theta) = sum over the 22 queries of the median execution time in mode m
J(theta)   = 0.5 * T_no_stats(theta) / T_no_stats(defaults)
           + 0.5 * T_analyzed(theta) / T_analyzed(defaults)
```

The defaults score 1. This is the driver's objective, not an official TPC-H
score. Baseline: no `ANALYZE` 56.29 s, after `ANALYZE` 57.36 s
(`artifacts/baseline.json`).

### Search

Search protocol: one process per query, 1 unmeasured run then 3 measured runs,
median of the 3, timeout `max(30 s, 8x the baseline median)`, plus a process
watchdog. `PlatformIO` (the syscall backend), warm page cache.

| Stage | Vectors |
|-------|---------|
| One-at-a-time and grouped plan probes | 46 |
| Cost-only random search, seed 1 | 64 |
| Estimation-heuristic random search, seed 2 | 64 |
| Both together, seed 3 | 64 |
| Local refinement, seeds 4 and 5 | 96 |
| Hand-built hybrids and single-field ablations | 18 |
| Plan-only sweeps, seeds 21, 22, 23 | 60,040 |
| Measured shortlist and finalists | 100+ |

The plan-only sweep costs about 35 ms per vector because it collects plans
without executing anything. A vector whose 44 programs were all measured
before gets a predicted objective for free.

### Attribution

| Group | Best J | Fields |
|-------|--------|--------|
| Execution-cost weights only | 0.9771 | 3 |
| Estimation heuristics only | 0.9605 | 5 |
| Both | 0.9385 | 8 |

No single field produces the gain. The best single-field change scored 0.9924.
The two groups are complementary: the cost-weight winner helps the analyzed
regime, the heuristics winner helps the no-statistics regime, and their
combination beats both. Combining them is not additive: one hybrid of the two
best parents scored 1.0012, worse than either parent.

### Correctness

Every plan that any finalist introduces was checked without timing it. Result
sets were compared row for row against the default plan's output, keeping
multiplicity, keeping NULL apart from the empty string and integers apart from
floats, and allowing floats to differ by 1e-9 relative or 1e-6 absolute
because a different join order adds the same values in a different order.
44 changed plans, 0 mismatches.

The same results were compared against SQLite 3.45.1 for the 19 queries the
query files do not mark `SQLITE_SKIP`, in both regimes: 0 mismatches.

### Conformance

`make -C sqlite/conformance run-rust` passes: 15,244 passed, 0 failed,
346 skipped. The change needed four test expectations updated:

* `snapshot_tests/tpch/tpch.sqltest`: the recorded plans for Q5 and Q8. These
  are the two plan changes the measurements above explain.
* `join/memory.sqltest`: two patterns that quote an estimated
  `rows_per_input` and `access_cost`. The plan text they also assert is
  unchanged.
* `explain-query-plan-json.sqltest`: three golden JSON documents that carry
  estimated rows and costs. Their plan text is unchanged; only numbers inside
  `estimate` objects moved.

## Reproducing this

```bash
# 1. Build the tuning binaries.
cargo build --release -p turso-join-benchmark --features optimizer_params
cargo build --release -p turso_cli --bin tursodb --features turso_core/optimizer_params
cargo build --release -p sqltest -p turso_core --features turso_core/optimizer_params

# 2. Get the data. DATA is any directory outside the repository.
curl -sL -o "$DATA/TPC-H.db" \
  https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db
python3 - <<'PY'
import sqlite3, shutil, os
D = os.environ["DATA"]
shutil.copyfile(f"{D}/TPC-H.db", f"{D}/tpch-nostats.db")
c = sqlite3.connect(f"{D}/tpch-nostats.db")
c.execute("DROP TABLE IF EXISTS sqlite_stat1")
c.execute("DROP TABLE IF EXISTS sqlite_stat4"); c.commit(); c.close()
shutil.copyfile(f"{D}/tpch-nostats.db", f"{D}/tpch-analyzed.db")
c = sqlite3.connect(f"{D}/tpch-analyzed.db"); c.execute("ANALYZE"); c.commit(); c.close()
for name in ("tpch-nostats", "tpch-analyzed"):
    shutil.copyfile(f"{D}/{name}.db", f"{D}/{name}-rw.db")
    shutil.copyfile(f"{D}/{name}.db", f"{D}/{name}-cli.db")
PY

# 3. Measure the defaults and freeze the workload. About 8 minutes.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" baseline

# 4. See which parameters change a plan. Seconds.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" probe --group all

# 5. Search. About 10 minutes for the cost-only group, an hour for the others.
for g in cost heuristics all; do
  python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" \
    search --group $g --seed 1 --candidates 64 --tag $g
done

# 6. Sweep plans only and predict. About 6 minutes for 20,000 vectors.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" \
  sweep --seed 21 --candidates 20000 --group all --workers 4 --write-top SHORTLIST

# 7. Reject a candidate that makes a common query shape slower.
python3 perf/optimizer-tuning/guards.py --binary target/release/turso-join-benchmark \
  --data-dir GUARDS --reference artifacts/baseline-vector.json --vectors SHORTLIST/*.json

# 8. Reject a candidate that breaks a plan the corpus asserts.
python3 perf/optimizer-tuning/conformance_filter.py --runner target/release/sqltest \
  --conformance-dir sqlite/conformance --vectors SHORTLIST/*.json

# 9. Check the results of every new plan, untimed.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" \
  correctness --vectors best.json

# 10. Re-measure in fresh paired rounds. About 18 minutes per round pair.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir "$DATA" \
  validate --vectors best.json --rounds 5

# 11. Compare two ordinary release builds, with the override feature absent.
python3 perf/optimizer-tuning/compiled_defaults.py \
  --binaries stock=/path/to/old proposed=/path/to/new \
  --data-dir "$DATA" --out RUN/compiled.jsonl --rounds 5

# 12. Compare them again with the page cache dropped and the io_uring backend.
python3 perf/optimizer-tuning/target_regime.py \
  --builds stock=/path/to/old-tursodb proposed=/path/to/new-tursodb \
  --data-dir "$DATA" --out RUN/target.jsonl --rounds 3 --vfs io_uring
```

Seeds: 1, 2, 3 for the random searches; 4 and 5 for local refinement; 21, 22
and 23 for the plan sweeps; 31, 41 and 51 for the stability probes; 17 for the
bootstrap.

## Machine and build

Intel Xeon at 2.80 GHz, 4 cores, 15 GiB of memory, one virtual disk, Ubuntu
24.04.4, Linux 6.18.44. rustc 1.88.0, cargo 1.88.0, Python 3.11.

Every timing came from a `--release` build. This is a task-specific exception
to the guidance in `AGENTS.md` against release builds, granted because a debug
build cannot choose a production cost default.

## Limits of this result

* The gain was measured on one dataset, at one scale factor, with one query
  set, on one machine. It is not evidence that the new default is better
  everywhere.
* The no-`ANALYZE` regime carries the whole gain (0.958) and almost all of
  the regression. The analyzed regime moves by 1.2%.
* Q5 without statistics is 2.08 times slower. The patch trades it for Q8,
  which is 4.13 times faster, on the same pair of tables.
* `ANALYZE` was run by the Python `sqlite3` module, SQLite 3.45.1. The shell
  harness installs 3.50.4. Turso reads only `sqlite_stat1`, whose contents are
  listed above, but the two versions were not compared directly, because
  sqlite.org is not reachable from the machine that ran this.
* The ten guards are the query shapes this experiment happened to need. They
  are not a complete set. A candidate that passes them can still be slower on
  a shape nobody wrote down.
* The search proxy used a warm page cache and the syscall I/O backend. The
  cold-cache io_uring regime confirmed the final result but was not used
  during the search.
