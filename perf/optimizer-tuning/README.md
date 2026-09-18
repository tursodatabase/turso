# Optimizer cost tuning driver

This directory holds the automation that searches for optimizer cost parameters
that make the bundled TPC-H workload run faster. It measures execution time. It
does not optimize estimated cost, the agreement between estimated cost and
runtime, or preparation time.

The engine already has the override mechanism: build with the
`optimizer_params` feature and point `TURSO_OPTIMIZER_PARAMS` at a JSON file.
This driver reuses that mechanism and adds the parts an experiment needs:
checked candidate files, one fresh process per vector, reuse of a measurement
when the program is the same, timeouts, and an append-only record of every
trial.

## What it measures

For statistics mode `m`:

```text
T_m(theta) = sum over the frozen query manifest of the median execution time
             of that query in that mode

J(theta)   = 0.5 * T_no_stats(theta) / T_no_stats(defaults)
           + 0.5 * T_analyzed(theta) / T_analyzed(defaults)
```

The defaults score 1. Lower is better. Both statistics regimes weigh the same.
This is the driver's own objective, not an official TPC-H score.

## Why every measurement starts a new process

`CostModelParams` is read once per process into a `LazyLock`. Changing the
environment variable, rewriting the JSON file, opening another connection or
preparing another statement inside a running process has no effect. The driver
therefore launches one process per vector and sets the environment before the
process starts.

## Why the driver checks the vector it wrote

`CostModelParams::load_from_file` returns the compiled defaults when the file
is missing, unreadable, malformed or out of domain. A search that trusted that
path would measure the defaults again and again and report them as candidates.
The driver therefore:

* checks every vector against the domains in `cost_vector.py` before it writes
  the file, and refuses unknown keys, non-finite values and out-of-domain
  values;
* writes the file to a read-only path named after the digest of its contents;
* reads the effective vector back out of a fresh engine process with
  `--print-params` and stops when it differs from the vector it asked for.

`cost_vector.py` adds two checks the engine does not make:

* `rows_per_table_page` must be more than 1, because `estimate_btree_depth`
  divides by its natural logarithm.
* `index_bonus` is kept at or below 10. The bonus is subtracted from every
  index cost, and the result is clamped to 0.001, so a large bonus makes all
  small index accesses cost the same and the optimizer can no longer tell them
  apart.

## Reusing a measurement

Two vectors that pick the same program for a query run the same work. The
driver keys its measurements on the bytecode of the program, together with the
regime, the database, the engine build and the run context. The bytecode holds
the join order, the access paths, the sorters and the ephemeral indexes. The
estimated rows and costs in the structured plan are left out, because they move
with the parameters without changing what runs.

One operand is normalized away: the P3 operand of `Transaction` carries the
schema version, which the query file that creates a view raises on every run.

Reused scores are provisional, as the prompt for this work requires. The
`validate` command always measures again.

## The query that creates a view

`perf/tpc-h/queries/15.sql` holds three statements: it creates a view, selects
from it and drops it. The execution runner used to prepare only the first
statement, which on a read-only database failed with `attempt to write a
readonly database`. The runner now splits a query file with the engine's own
parser and runs every statement in file order, and this query runs against a
private writable copy of the snapshot. Its measured interval covers the whole
sequence and therefore includes preparation, which is what the shell harness
also times. The record carries `"includes_preparation": true`.

No query file carries a `LIMBO_SKIP` directive, so Turso runs all 22. Three
files carry `SQLITE_SKIP`, which says the reference engine cannot run them.
They stay in Turso's objective.

## Commands

```bash
# Build the tuning binaries.
cargo build --offline --release -p turso-join-benchmark --features optimizer_params
cargo build --offline --release -p turso_cli --bin tursodb \
  --features turso_core/optimizer_params

# Freeze the workload and measure the compiled defaults.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir DATA baseline

# Which parameters change a plan at all (plan-only, seconds).
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir DATA \
  probe --group all

# Search the execution-cost weights.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir DATA \
  search --group cost --seed 1 --candidates 64

# Re-measure the finalists in fresh paired rounds.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir DATA \
  validate --vectors best.json --rounds 5

# Compare the results of every new plan against the default plan.
python3 perf/optimizer-tuning/tune.py --out-dir RUN --data-dir DATA \
  correctness --vectors best.json
```

`DATA` must hold `tpch-nostats.db`, `tpch-analyzed.db` and a writable copy of
each one named `tpch-nostats-rw.db` and `tpch-analyzed-rw.db`.

## What the run directory holds

| File | Contents |
|------|----------|
| `state.json` | Workload manifest, build identity, database identity, baseline |
| `measurements.jsonl` | One record per measured query, with every sample |
| `candidates.jsonl` | One record per scored vector |
| `plans.jsonl` | One record per distinct program, with its plan text |
| `probes.jsonl` | Which plans each one-at-a-time probe changed |
| `validation.jsonl` | One record per configuration per validation round |
| `correctness.jsonl` | One record per checked plan |
| `vectors/` | Every candidate file, read-only, named after its digest |
| `logs/` | The standard error of every process |

## Tests

```bash
python3 -m unittest discover -s perf/optimizer-tuning
```
