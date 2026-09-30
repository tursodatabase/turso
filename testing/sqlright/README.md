# SQLRight Fuzzer for Turso

Coverage-guided SQL fuzzer using [SQLRight](https://github.com/PSU-Security-Universe/sqlright) to find logical bugs and
crashes in Turso.

SQLRight requires Linux.

## Docker

All commands run from the **repo root**.

```bash
docker build -f testing/sqlright/Dockerfile -t limbo-sqlright .

docker run --rm -it limbo-sqlright                           # single core, default oracle (NoREC)
docker run --rm -it limbo-sqlright --oracle TLP              # TLP oracle
docker run --rm -it limbo-sqlright --oracle INDEX            # INDEX oracle
docker run --rm -it limbo-sqlright --oracle INDEX            # ROWID oracle
docker run --rm -it limbo-sqlright --timeout 3600            # stop after 1 hour
```

### Persist results and resume

Mount a volume so results survive container restarts:

```bash
# First run — results saved to the volume:
docker run --rm -it -v sqlright-results:/limbo/testing/sqlright/results limbo-sqlright --cores 4

# Resume from where you left off:
docker run --rm -it -v sqlright-results:/limbo/testing/sqlright/results limbo-sqlright --cores 4 --resume
```

## Manual run (if on Linux)

Prerequisites:

```bash
sudo apt-get install -y build-essential bison flex libreadline-dev zlib1g-dev
cargo install cargo-afl
cargo afl config --build --force
rustup component add llvm-tools-preview  # for coverage reports
```

### Install SQLRight

This clones SQLRight at a pinned commit, applies patches (MAP_SIZE increase for Turso's coverage guards, GCC 13
compatibility fixes), and builds `afl-fuzz`.

```bash
./setup.sh
cargo afl build --profile fuzzing --bin tursodb
./run.sh # see available flags in the Docker section
```

Results are saved to `results/run_{ORACLE}_{TIMESTAMP}/` and per-instance stats are printed on exit.

### Coverage report

```bash
./collect_coverage.sh                                     # auto-finds most recent run
./collect_coverage.sh results/run_NOREC_20260217/primary/queue  # specific run
```

Generates `coverage_report/summary.txt`, `coverage_report/html/index.html`, and `coverage_report/coverage.lcov`.

### Crash analysis

See the crash reports [README](crash_reports/README.md) for instructions on how to analyze the fuzzer's results. This
can be done concurrently while the fuzzer is running.

## Multi-Core Fuzzing

I was only able to get this working efficiently with 1 core, but this was still enough to provide results. This section
describes SQLRight's support for parallel fuzzing.

`--cores N` launches AFL in primary/secondary mode:

- **Primary** (`-M primary`): deterministic mutations, syncs with secondaries
- **Secondaries** (`-S secondary_N`): random mutations, sync interesting inputs from primary

## Resuming

`--resume` finds the most recent run directory for the selected oracle. You can change the core count between runs. If
no previous session exists for that oracle, falls back to a fresh start.

## Environment Variables

| Variable                                | Value     | Why                                                                      |
|-----------------------------------------|-----------|--------------------------------------------------------------------------|
| `AFL_OLD_FORKSERVER`                    | `1`       | SQLRight uses AFL's old fork server protocol                             |
| `AFL_MAP_SIZE`                          | `2097152` | Turso has ~1.1M coverage guards → needs 2^21 map                         |
| `AFL_SKIP_CPUFREQ`                      | `1`       | Skip CPU governor check                                                  |
| `AFL_SKIP_CRASHES`                      | `1`       | Skip inputs that crash during calibration (prevents infinite crash loop) |
| `AFL_HANG_TMOUT`                        | `30000`   | 30-second hang threshold (allows slow queries to complete)               |
| `AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES` | `1`       | Continue if crashes directory is missing                                 |

