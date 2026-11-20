---
name: differential-fuzzer
description: How to run the differential fuzzer, which compares Turso against SQLite on generated SQL, and how to reproduce and minimize its failures
---

# Differential Fuzzer

The differential fuzzer compares Turso results against SQLite for generated SQL statements to find correctness bugs.

## Location

`testing/differential-oracle/fuzzer/`

## Running the Fuzzer

```bash
cargo run --bin differential_fuzzer -- --seed 12345 --keep-files   # reproducible run, keep .db files
cargo run --bin differential_fuzzer -- loop 50                     # 50 runs with random seeds; omit the count to run forever
```

Run with `--help` for all options (statement count, tables, columns, `--profile`, `--verbose`).

## Output Files

All output goes to `simulator-output/` directory:

| File | Description |
|------|-------------|
| `test.sql` | All executed SQL statements. Failed statements prefixed with `-- FAILED:`, errors with `-- ERROR:` |
| `schema.json` | Database schema at end of run (or at failure) |
| `test.db` | Turso database file (only with `--keep-files`) |
| `test-sqlite.db` | SQLite database file (only with `--keep-files`) |

## Reproducing Errors

1. **Find the seed and profile** in the error output:
   ```
   INFO: Starting differential_fuzzer with config: SimConfig { seed: 12345, ..., weight_profile: Writes }
   ```

2. **Re-run with that seed and profile** (a seed only replays under the same profile):
   ```bash
   cargo run --bin differential_fuzzer -- --seed 12345 --profile writes --verbose --keep-files
   ```

3. **Read the minimized reproduction first.** On an oracle failure the fuzzer
   writes these files to `simulator-output/`:
   - `minimized.sql` - a shrunken state script plus the shrunken failing
     statement, produced automatically. Start here.
   - `turso-state.sql` / `sqlite-state.sql` - each engine's full state as a
     replayable script, when you need more than the minimized version kept.
   - `test.sql` - every executed statement (the failing one is marked
     `-- FAILED:`). The minimizer falls back to replaying this history when
     the failure depends on how the state was built, not just its contents.
   - `schema.json` - table structure at failure time.

4. **Probe the reproduction with `differential_probe`.** It runs a
   statement-per-line script on Turso and SQLite side by side, prints both
   outcomes for every statement, marks divergences, and compares the final
   table contents. Exit code 1 means something diverged.
   ```bash
   cargo run -q -p differential-fuzzer --bin differential_probe -- \
       simulator-output/minimized.sql
   ```
   Use it instead of piping SQL into the two shells: the tursodb shell cannot
   `ATTACH ':memory:' AS aux`, so fuzzer reproductions with an `aux` schema
   only run correctly through the probe. Reading from stdin also works:
   `echo "SELECT ~X'96';" | cargo run -q -p differential-fuzzer --bin differential_probe`.

5. **Bisect by editing the script.** Copy `minimized.sql`, simplify one thing
   at a time (replace an expression with a constant, drop a column, drop a
   state line), and re-run the probe after each edit. The divergence marker
   tells you immediately whether the edit kept the bug. This loop usually
   ends at a one-line statement you can hand to `EXPLAIN` on both engines.

6. The minimized failure becomes a `*.sqltest` regression (see the testing skill). A Rust test is for behavior `.sqltest` cannot express.

## Oracle Warnings

A LIMIT without ORDER BY that returns different rows in the two engines is a warning, not a failure.

## Tracing

Set `RUST_LOG` for more detailed output:

```bash
RUST_LOG=debug cargo run --bin differential_fuzzer -- --seed 12345
```
