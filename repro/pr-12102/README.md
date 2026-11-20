# Reproduction kit for the measurements on PR 12102

This directory contains the scripts, SQL files and tests that I used to examine the
claims of [PR 12102](https://github.com/tursodatabase/turso/pull/12102)
("core/mvcc: fix skipped cursor rows and slow indexed pulls"). It also contains my
results in `results/`.

## What the scripts compare

| Variant | Source |
|---|---|
| `pr` | PR head `a4c5e653`. The file `core/mvcc/cursor.rs` is the same at the later head `8f3b7ef7`. |
| `base` | The same tree, with `core/mvcc/cursor.rs` from the merge base `91d399c4`. |

The PR description uses the same method: the two variants differ only in
`core/mvcc/cursor.rs`. Both variants also contain `cursor_repro_tests.rs`, which
`build.sh` appends to `core/mvcc/database/tests.rs`.

## Requirements

- Linux or macOS, with git, bash and python3.
- The Rust toolchain of `rust-toolchain.toml` (Rust 1.88). rustup installs it.
- About 15 GB of free disk space.
- Access to github.com and crates.io.

## Procedure

1. Clone the repository and get this branch:

   ```bash
   git clone https://github.com/tursodatabase/turso.git
   ```

   ```bash
   cd turso && git checkout claude/wizardly-sagan-y9mner
   ```

2. Build the two variants. On a 4-core VM, this takes about 20 minutes:

   ```bash
   repro/pr-12102/build.sh
   ```

   The scripts put the build tree, the binaries and the outputs in
   `../pr-12102-repro-work`. To use a different directory, set `WORK` for each script.

3. Run the cursor tests (about 3 minutes):

   ```bash
   repro/pr-12102/run_cursor_tests.sh
   ```

4. Run the plain SQL check (a few seconds):

   ```bash
   repro/pr-12102/run_mixed_sql.sh
   ```

5. Run the SQL-only slowdown tests (about 3 minutes):

   ```bash
   repro/pr-12102/run_sql_perf.sh
   ```

6. Run the join test (about 1 minute):

   ```bash
   repro/pr-12102/run_join.sh
   ```

7. Run the pull benchmark of the PR. On a 4-core VM, this takes about 60 minutes.
   Do not run other work at the same time:

   ```bash
   repro/pr-12102/run_pull_bench.sh
   ```

   For a short check, set `SIZES`:

   ```bash
   SIZES=1000,2000 repro/pr-12102/run_pull_bench.sh
   ```

8. Optional: run the unindexed cases 3 more times (about 3 minutes):

   ```bash
   repro/pr-12102/run_unindexed_repeats.sh
   ```

## My results

Machine: Linux VM, 4 cores of an Intel Xeon at 2.1 GHz, 15 GB of memory.
The benchmark and the CLI use `bench-profile`. The tests use the dev profile.
The PR used an Apple M5, which is faster: on my VM, base is 4 to 7 times slower
and the PR build is about 3 times slower.

### Cursor bugs

The fixture of the PR has B-tree rows 10, 30, 50 and MVCC-only rows 20, 40.

| Tests | base | pr |
|---|---|---|
| 35 tests of the four claims (`cursor_repro_tests.rs`) | 5 ok, 30 failed | 35 ok |
| 89 new unit-test cases of the PR | 13 ok, 76 failed | 89 ok |
| 6 new integration tests of the PR | 6 failed | 6 ok |

Examples of what base returns:

| Sequence | base | Correct |
|---|---|---|
| Exact lookup of 30, then `next()` | 30, 50 | 30, 40, 50 |
| Exact lookup of 20, then `next()` | 20 | 20, 30, 40, 50 |
| `exists(20)`, then `next()` | 20 | 20, 30, 40, 50 |
| Insert 35, then `next()` | 35, 10, 30, 40, 50 | 35, 40, 50 |
| Range seek 30, `next()`, then `prev()` | 40, then 50 | 40, then 30 |

No plain SQL statement shows these bugs. `run_mixed_sql.sh` runs 15 queries and
writes on the same kind of mixed state. Both variants give the same output, and
I examined each row of it by hand (`results/mixed_sql_expected.txt`). The built-in
FTS and vector index methods do not use these cursor sequences.

### Delete, then insert again, in one transaction

50,000 checkpointed rows and five indexes (`run_sql_perf.sh`, part 1). Times in seconds.

| N | INSERT, base | INSERT, pr | Integrity check after the commit, base | Same, pr |
|---:|---:|---:|---:|---:|
| 1,000 | 0.584 | 0.048 | 0.988 | 0.632 |
| 2,000 | 2.340 | 0.049 | 2.790 | 0.593 |
| 4,000 | 8.856 | 0.084 | 9.812 | 0.564 |
| 8,000 | 35.508 | 0.192 | 37.152 | 0.612 |

On base, the INSERT time increases 4 times when N doubles.

### Slowdown that the PR adds

After each exact seek, the PR clears the saved direction (`peeks_valid_for`). The
first `next()` after the seek then seeks both sources again.

| Test | base | pr |
|---|---:|---:|
| Join on a non-unique index, median of 15 runs (`run_join.sh`) | 430 ms | 561 ms |
| One lookup on a non-unique index among hidden entries (`run_sql_perf.sh`, part 2) | 0.54 ms | 0.85 ms |

### Pull benchmark

Five indexes. Pull time in seconds (`run_pull_bench.sh`). The last column gives the
numbers of the PR description, for the automatic policy.

| Updated rows | base, automatic | pr, automatic | base, checkpoint after each pull | pr, checkpoint after each pull | PR description (M5) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.68 | 0.07 | 0.68 | 0.07 | 0.216 → 0.028 |
| 2,000 | 3.73 | 0.22 | 2.67 | 0.15 | 1.257 → 0.080 |
| 4,000 | 13.2 | 0.48 | 14.8 | 0.54 | 3.480 → 0.166 |
| 10,000 | 114.5 | 1.52 | 80.1 | 1.29 | 20.49 → 0.436 |
| 20,000 | 166.0 | 2.70 | 278.8 | 3.41 | 43.72 → 0.938 |
| 40,000 | 622.8 | 6.96 | 363.0 | 5.72 | 88.37 → 1.947 |

With 23 generated columns added, the PR is 60 to 100 times faster at 10,000 to
40,000 rows. All the data is in `results/pull_bench/comparison.csv`.

Without indexes, the two variants have the same speed. For 40,000 rows, the medians
of 3 runs differ by less than 3% (`results/pull_bench_unindexed/comparison.csv`).

## Notes

- A parallel build changes the benchmark times very much. In one of my runs, a
  build made a 10,000-row base pull take 122 s instead of 83 s.
- The order of the two replicas has an effect. From 2,000 rows up, the second pull of
  each size was always slower, in both variants. Thus the run order, not the checkpoint
  policy, causes most of the difference between the two checkpoint columns. The PR
  description shows the same effect at 20,000 rows (43.7 s, then 70.6 s).
- I measured the times in `results/` before I wrote these scripts. The scripts run the
  same SQL and the same commands.
