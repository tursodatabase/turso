# SQLRight Fuzzer for Turso

Coverage-guided SQL fuzzer using [SQLRight](https://github.com/PSU-Security-Universe/sqlright) to find logical correctness bugs in Turso.

SQLRight uses SQL-aware mutations and oracle-based bug detection (NOREC, TLP, INDEX) to test that query results are correct — not just that they don't crash.

## Setup

### Platform Support

**SQLRight requires Linux (Ubuntu 20.04 or later).**

- **Linux**: Native support, follow instructions below
- **macOS**: Not supported - use Docker with Linux container or Linux VM
- **Windows**: Use WSL2

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

### Crash analysis

```bash
cd crash_reports
./collect_crashes.py /tmp/sqlright_test
```

Collects crashes from AFL instances, deduplicates by content hash, and tests against both tursodb and SQLite:
- Deduplication: 78 crash files → 12 unique crashes
- Classification: PANIC, CRASH, PARSE_ERROR, SUCCESS
- Differential testing: Identifies oracle bugs (incorrect results vs SQLite)
- Idempotent: Re-running only processes new crashes

Query crashes:

```bash
./query_crashes.py list                          # all crashes
./query_crashes.py bugs                          # real bugs only
./query_crashes.py export <crash_id> output.sql  # export specific crash
./query_crashes.py stats                         # summary statistics
```

Results stored in `crash_reports/crashes.db` with processing history for reproducibility. See `crash_reports/README.md` for full documentation.

### Integrity checking

Validates that databases written by tursodb are structurally correct by replaying AFL inputs through tursodb, then checking the resulting database with SQLite:

```bash
python3 check_integrity.py --output-dir /tmp/sqlright_test --every 10 -v

# Query failures
python3 crash_reports/query_crashes.py integrity --fails
```

Each database is validated with `PRAGMA integrity_check` (B-tree structure, page connectivity, index entry counts, orphaned pages) followed by extended checks:

| Check | What it catches |
|-------|----------------|
| Page count vs file size | Truncated/extended files where header page count doesn't match actual file size |
| Index ordering | Index entries ordered correctly by raw bytes but wrong by collation (exercises query engine comparator, not B-tree checker) |
| Round-trip consistency | Table scan vs index scan returning different rows (corrupt index pointers affecting query results) |

Extended checks use Python's `sqlite3` module for safe schema discovery (handles special characters in names), filter out partial and expression indexes, use `EXCEPT` for memory-efficient set comparison (works with WITHOUT ROWID tables), and run per-index for error isolation.

### Error handling and false positives

Early fuzzing runs revealed a critical issue with tursodb CLI error handling that caused a 67% false positive rate in crash reports. The CLI was writing all errors (parse errors, runtime errors, constraint violations) to stdout with exit code 0, causing the crash analysis system to classify errors as SUCCESS. The solution was to align tursodb's error handling with SQLite3's standard behavior: all errors now go to stderr with exit code 1, while successful query results go to stdout with exit code 0. This change ensures the fuzzer correctly distinguishes between genuine bugs (crashes, panics, incorrect results) and expected error cases (malformed SQL, constraint violations), eliminating false positives and enabling accurate bug detection.

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
- **63 cores (max):** Maximum supported on high-end systems with fast storage

Performance scales sub-linearly due to queue sync overhead and corpus sharing.

## Monitoring

### Real-time status (recommended)

```bash
./whatsup.sh [OUTPUT_DIR] [--verbose]
```

Compact monitoring view suitable for `watch`. Default output dir: `/tmp/sqlright_test`

Example output:
```
=== SQLRight Fuzzer Monitor ===
Output: /tmp/sqlright_test

SUMMARY: 63 instances | 12453 execs | 15 crashes | 320 eps | 3.41% cov

⚠️  2 instances with execs_since_crash=0 (crashing on every exec)

Stuck instances: 3 (showing first 5)
  secondary_15: execs=2 eps=0.00 crashes=2
  secondary_26: execs=1 eps=0.00 crashes=1
  secondary_31: execs=3 eps=0.00 crashes=2

Total crash files: 15

Run with --verbose for detailed crash analysis
```

Use `watch -n5 ./whatsup.sh` for live monitoring. Add `--verbose` flag for detailed per-instance crash distribution and source analysis.

During calibration (before fuzzer_stats exists), shows initialization progress: AFL processes running, last test execution time, seed processing status, and CPU usage.

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
├── whatsup.sh            # real-time fuzzer monitoring
├── check_affinity.sh     # CPU affinity verification
├── check_integrity.py    # integrity validation of tursodb-written databases
├── patches/              # git patches applied to SQLRight
│   └── 0001-turso-increase-map-size-and-fix-gcc13.patch
├── crash_reports/        # crash analysis and deduplication
│   ├── collect_crashes.py   # crash collection and testing
│   ├── query_crashes.py     # crash database queries
│   ├── schema.sql           # database schema
│   ├── lib/                 # analysis modules
│   └── README.md            # crash analysis documentation
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
| `AFL_NO_AFFINITY` | `1` | Disable automatic CPU binding |

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

- **Calibration time:** Scales with core count due to coordination overhead:
  - 1 core: <1 minute
  - 2-4 cores: 1-2 minutes
  - 8 cores: 2-3 minutes
  - 63 cores: 2-4 minutes
- **Normal execs/sec:** 5-50 per instance (SQL fuzzing is slow compared to binary fuzzing)
- **Queue growth:** Normal, especially early in fuzzing (new coverage paths discovered)
- **Crashes found:** If target has bugs, expect crashes within first hour

Use `./whatsup.sh` to monitor progress and diagnose issues.
