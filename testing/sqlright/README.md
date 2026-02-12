# SQLRight Fuzzer for Turso

Coverage-guided SQL fuzzer using [SQLRight](https://github.com/PSU-Security-Universe/sqlright) to find logical correctness bugs in Turso.

SQLRight uses SQL-aware mutations and oracle-based bug detection (NOREC, TLP) to test that query results are correct — not just that they don't crash.

## Setup

### Prerequisites

```bash
sudo apt-get install -y build-essential bison flex libreadline-dev zlib1g-dev
cargo install cargo-afl
cargo afl config --build --force
rustup component add llvm-tools-preview  # for coverage reports
```

### Install SQLRight

```bash
./setup.sh
```

This clones SQLRight at a pinned commit, applies patches (MAP_SIZE increase for Turso's coverage guards, GCC 13 compatibility fixes), and builds `afl-fuzz`.

### Build instrumented tursodb

```bash
cargo afl build --bin tursodb
```

## Running

### Quick run

```bash
./run.sh                     # single core
./run.sh --cores 4           # 4 parallel instances
./run.sh --resume            # resume previous session
./run.sh --cores 4 --resume  # resume with 4 cores
```

### Production run

```bash
# NOREC oracle, 4 cores, 1 hour:
./run_production.sh --oracle NOREC --cores 4 --timeout 3600

# TLP oracle, run forever:
./run_production.sh --oracle TLP --cores 2

# Resume most recent NOREC run:
./run_production.sh --oracle NOREC --cores 4 --resume
```

Production runs are saved to `results/run_{ORACLE}_{TIMESTAMP}/` and print per-instance stats on exit.

### Coverage report

```bash
./collect_coverage.sh /tmp/sqlright_test/primary/queue
```

Generates `coverage_report/summary.txt`, `coverage_report/html/index.html`, and `coverage_report/coverage.lcov`.

## Seed Corpus

Seeds are automatically extracted from Turso's `.sqltest` test suite at the start of each fresh (non-resume) fuzzing run:

```bash
cargo run --bin test-runner -- extract-sql testing/runner/tests/ --output-dir seeds/
```

This produces ~3,000 self-contained SQL files covering CTEs, joins, window functions, aggregations, triggers, JSON, and more. Each file includes resolved `@setup` SQL prepended to the test SQL.

Files using `:default:` databases, tests with `@skip`, and tests with `.dbconfig` dot-commands are excluded (not self-contained).

## Multi-Core Fuzzing

`--cores N` launches AFL in primary/secondary mode:

- **Primary** (`-M primary`): deterministic mutations, syncs with secondaries
- **Secondaries** (`-S secondary_N`): random mutations, sync interesting inputs from primary

All instances share one output directory. `Ctrl+C` stops them all.

```
output/
├── primary/
│   ├── queue/         # discovered test cases
│   ├── crashes/       # oracle violations
│   └── fuzzer_stats
├── secondary_2/
└── secondary_3/
```

## Monitoring

```bash
cat /tmp/sqlright_test/primary/fuzzer_stats
```

Key fields:
- `bitmap_cvg` — percentage of coverage bitmap hit
- `unique_crashes` — oracle violations found (correctness bugs)
- `execs_per_sec` — throughput
- `paths_found` — new coverage paths from mutations

## How It Works

```
SQLRight's afl-fuzz (SQL-aware mutations + oracle)
    │
    ├── Mutates SQL using grammar-aware transformations
    ├── Forks instrumented tursodb via fork server
    │
    ▼
tursodb (instrumented with PCGUARD coverage)
    │
    ├── Executes SQL from stdin
    ├── Reports coverage via shared memory bitmap
    │
    ▼
afl-fuzz compares results
    ├── NOREC: optimized vs unoptimized query → same results?
    ├── TLP: partitioned WHERE clauses → same combined results?
    ├── New coverage → save to queue
    └── Oracle mismatch → save as "crash" (correctness bug)
```

## Oracles

- **NOREC** (Non-optimizing Reference Engine Construction): rewrites queries to disable optimizations, compares results
- **TLP** (Ternary Logic Partitioning): splits `WHERE` into `TRUE/FALSE/NULL` partitions, verifies union equals original

Oracle violations are reported as AFL "crashes" — `unique_crashes` is the number of distinct correctness bugs.

## Resuming

`--resume` uses AFL's `-i -` to continue from the existing output directory, skipping seed calibration. You can change the core count between runs. If no previous session exists, falls back to a fresh start.

## Directory Layout

```
testing/sqlright/
├── setup.sh              # clone, patch, build SQLRight
├── run.sh                # quick fuzzing script
├── run_production.sh     # production fuzzing script
├── collect_coverage.sh   # coverage report generation
├── patches/              # git patches applied to SQLRight
│   └── 0001-turso-increase-map-size-and-fix-gcc13.patch
├── README.md             # this file
├── .gitignore
├── build/                # [gitignored] SQLRight clone + compiled binaries
├── seeds/                # [gitignored] auto-extracted SQL seeds
├── results/              # [gitignored] production fuzzing results
└── coverage_report/      # [gitignored] coverage reports
```

## Environment Variables

| Variable | Value | Why |
|----------|-------|-----|
| `AFL_OLD_FORKSERVER` | `1` | SQLRight uses AFL's old fork server protocol |
| `AFL_MAP_SIZE` | `2097152` | Turso has ~1.1M coverage guards → needs 2^21 map |
| `AFL_SKIP_CPUFREQ` | `1` | Skip CPU governor check |

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `setup.sh` fails on `make` | Check `g++ --version` ≥ 13, install `bison flex` |
| "fork server" error | Verify `cargo afl build` was used (not plain `cargo build`) |
| `bitmap_cvg: 0.00%` | MAP_SIZE mismatch — re-run `setup.sh` |
| Coverage profdata version mismatch | Use `rustup component add llvm-tools-preview`, not system llvm |
| Resume says "no previous run" | Output dir must have `primary/queue/` subdirectory |
