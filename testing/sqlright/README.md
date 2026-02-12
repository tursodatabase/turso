# SQLRight Fuzzer for Turso

Coverage-guided SQL fuzzer using [SQLRight](https://github.com/PSU-Security-Universe/sqlright) to find logical correctness bugs in Turso.

SQLRight uses SQL-aware mutations and oracle-based bug detection (NOREC, TLP, INDEX) to test that query results are correct — not just that they don't crash.

## Setup

### Platform Support

**SQLRight is designed for Linux (Ubuntu 20.04).**

- **Linux**: Native support, follow instructions below
- **macOS**: **Not supported** - use Docker or Linux VM (see below)
- **Windows**: Use WSL2 or Docker

#### macOS Users

SQLRight uses AFL which has extensive Linux-specific dependencies. To run on macOS:

1. **Docker (Recommended)**: Use SQLRight's official Docker image
   ```bash
   docker pull steveleungsly/sqlright_sqlite:version1.2
   # Adapt for Turso (requires custom Dockerfile)
   ```

2. **Linux VM**: Run in a Linux VM (UTM, Parallels, VMware, etc.)

Partial macOS compatibility patches are included (`patches/0002-*.patch`, `0003-*.patch`) but native builds require extensive additional AFL/macOS type fixes.

### Prerequisites (Linux)

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
cargo afl build --profile fuzzing --bin tursodb
```

## Running

### Quick run

```bash
./run.sh                              # single core, NOREC oracle
./run.sh --oracle TLP                 # TLP oracle (broader seed compatibility)
./run.sh --oracle INDEX               # INDEX oracle (optimizer bugs)
./run.sh --cores 4                    # 4 parallel instances
./run.sh --resume                     # resume previous session
./run.sh --cores 4 --oracle TLP --resume
```

### Production run

```bash
# NOREC oracle, 4 cores, 1 hour:
./run_production.sh --oracle NOREC --cores 4 --timeout 3600

# TLP oracle, run forever:
./run_production.sh --oracle TLP --cores 2

# INDEX oracle (catches optimizer bugs around index usage):
./run_production.sh --oracle INDEX --cores 2

# Resume most recent NOREC run:
./run_production.sh --oracle NOREC --cores 4 --resume
```

Production runs are saved to `results/run_{ORACLE}_{TIMESTAMP}/` and print per-instance stats on exit.

### Coverage report

```bash
./collect_coverage.sh /tmp/sqlright_test/primary/queue
```

Generates `coverage_report/summary.txt`, `coverage_report/html/index.html`, and `coverage_report/coverage.lcov`.

## Experimental Features

All experimental Turso features are enabled during fuzzing to maximize attack surface:

| Feature | Flag | Why |
|---------|------|-----|
| Views | `--experimental-views` | Materialized view correctness |
| Strict tables | `--experimental-strict` | Type enforcement edge cases |
| Triggers | `--experimental-triggers` | Trigger execution correctness |
| Index methods | `--experimental-index-method` | Custom index optimizer bugs |
| Autovacuum | `--experimental-autovacuum` | Page management under vacuum |
| Attach | `--experimental-attach` | Multi-database query correctness |

These are enabled automatically by `run.sh` and `run_production.sh`.

## Seed Corpus

Seeds are automatically extracted from Turso's `.sqltest` test suite at the start of each fresh (non-resume) fuzzing run:

```bash
cargo run --bin test-runner -- extract-sql testing/runner/tests/ --output-dir seeds/
```

This produces ~3,000 self-contained SQL files covering CTEs, joins, window functions, aggregations, triggers, JSON, and more. Each file includes resolved `@setup` SQL prepended to the test SQL.

Files using `:default:` databases, tests with `@skip`, and tests with `.dbconfig` dot-commands are excluded (not self-contained).

## Oracles

SQLRight supports 6 oracles for detecting different classes of bugs:

| Oracle | What it tests | Best for |
|--------|---------------|----------|
| **NOREC** | Rewrites `SELECT COUNT(*) FROM t WHERE expr` to disable optimizations, compares counts | Optimizer correctness |
| **TLP** | Splits `WHERE` into TRUE/FALSE/NULL partitions, verifies union equals original | General WHERE clause bugs |
| **INDEX** | Runs queries with and without `CREATE INDEX`, compares results | Index scan vs table scan mismatches |
| **ROWID** | Compares results using rowid-based access patterns | Rowid handling bugs |
| **LIKELY** | Adds `LIKELY()`/`UNLIKELY()` hints, compares results | Hint-related optimizer bugs |
| **OPT** | Toggles various optimizer flags, compares results | Optimizer pass correctness |

### Oracle selection guidance

- **NOREC**: Requires seeds matching `SELECT COUNT(*) FROM t WHERE expr` — only ~1% of auto-extracted seeds qualify. Best when combined with SQLRight's init_lib seeds.
- **TLP**: Works with any `SELECT ... WHERE ...` query — broadest seed compatibility. Good default choice.
- **INDEX**: Works when seeds contain `CREATE INDEX` — ~13% of extracted seeds qualify. Catches index-related optimizer bugs.

Oracle violations are reported as AFL "crashes" — `unique_crashes` is the number of distinct correctness bugs.

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

### Multi-core fixes applied

**File contention fix (critical):** Each fuzzer instance gets its own isolated working directory to prevent deadlock on `map_id_triggered.txt`. The SQLite port of SQLRight was missing per-core file logic that MySQL/PostgreSQL versions have.

**Crash handling fix (critical):** `AFL_SKIP_CRASHES=1` prevents AFL from spending hours on deterministic fuzzing of inputs that reliably crash. Without this, the fuzzer gets stuck exploring crash variations instead of discovering new bugs.

**Timeout tuning:** 5-second execution timeout (`-t 5000`) and 30-second hang threshold (`AFL_HANG_TMOUT=30000`) allow slow SQL queries (large joins, complex aggregations) to complete instead of being marked as hangs.

### Recommended core counts

- **1 core:** Simplest, no coordination overhead, good baseline
- **4-8 cores:** Good balance for most machines
- **16-32 cores:** Diminishing returns, ensure adequate I/O bandwidth
- **64+ cores:** Only on high-end systems with fast storage

Performance scales sub-linearly due to queue sync overhead and corpus sharing.

## Monitoring

### Real-time status (recommended)

```bash
./whatsup.sh [OUTPUT_DIR]
```

Shows per-instance stats (alive/dead, execs, coverage, crashes, hangs) plus aggregate summary. Default output dir: `/tmp/sqlright_test`

Example output:
```
>>> primary          alive=yes  0d 0h 10m  cycles=0  execs=2078  eps=5.2  cov=3.41%  crashes=4  hangs=0
>>> secondary_2      alive=yes  0d 0h 10m  cycles=0  execs=1523  eps=4.8  cov=3.40%  crashes=2  hangs=0

Summary:
  Fuzzers alive  : 8
  Total execs    : 12453
  Total crashes  : 15
  Max coverage   : 3.41%
```

### Manual inspection

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
    ├── All experimental features enabled
    │
    ▼
afl-fuzz compares results
    ├── NOREC: optimized vs unoptimized query → same results?
    ├── TLP: partitioned WHERE clauses → same combined results?
    ├── INDEX: with index vs without index → same results?
    ├── New coverage → save to queue
    └── Oracle mismatch → save as "crash" (correctness bug)
```

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
│   ├── 0001-turso-increase-map-size-and-fix-gcc13.patch
│   ├── 0002-turso-fix-macos-bison-compatibility.patch (macOS only, incomplete)
│   └── 0003-turso-fix-macos-compilation-errors.patch (macOS only, incomplete)
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
| `AFL_SKIP_CRASHES` | `1` | Skip inputs that crash during calibration (prevents infinite crash loop) |
| `AFL_HANG_TMOUT` | `30000` | 30-second hang threshold (allows slow queries to complete) |
| `AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES` | `1` | Continue if crashes directory is missing |

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `setup.sh` fails on `make` | Check `g++ --version` ≥ 13, install `bison flex` |
| "fork server" error | Verify `cargo afl build` was used (not plain `cargo build`) |
| `bitmap_cvg: 0.00%` | MAP_SIZE mismatch — re-run `setup.sh` |
| Coverage profdata version mismatch | Use `rustup component add llvm-tools-preview`, not system llvm |
| Resume says "no previous run" | Output dir must have `primary/queue/` subdirectory |
| 0 oracle violations after long run | Try TLP oracle (broadest seed compatibility) or INDEX oracle |
| **Multi-core fuzzing stuck/frozen** | See below ⬇️ |

### Multi-core specific issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| All instances frozen at low execs, ~9% CPU | File contention deadlock on `map_id_triggered.txt` | **Fixed in latest version** - per-instance working directories |
| Many "hangs" detected, slow progress | Timeout too aggressive for slow SQL | **Fixed in latest version** - 5s timeout + 30s hang threshold |
| Queue explodes (4000+ items), stuck at low test case numbers | AFL fuzzing crashing inputs indefinitely | **Fixed in latest version** - `AFL_SKIP_CRASHES=1` enabled |
| High CPU but `execs_per_sec: 0.00` | Calibration in progress (normal) | Wait for "All test cases processed" |
| `execs_since_crash: 0` on all instances | Target has bugs that crash during fuzzing | Expected - crashes are saved, fuzzing continues |
| Resume picks up old crashes/hangs | Leftover state from previous runs | Delete output dir before fresh start: `rm -rf /tmp/sqlright_test` |

### Performance expectations

- **Calibration time:** 2-5 minutes for 2500 seeds (depends on core count)
- **Normal execs/sec:** 5-50 per instance (SQL fuzzing is slow compared to binary fuzzing)
- **Queue growth:** Normal, especially early in fuzzing (new coverage paths discovered)
- **Crashes found:** If target has bugs, expect crashes within first hour

Use `./whatsup.sh` to monitor progress and diagnose issues.
