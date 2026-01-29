---
name: differential-fuzzer
description: Information about the differential fuzzer tool, how to run it and use it catch bugs in Turso. Always load this skill when running this tool
---

# Differential Fuzzer

Always load [Debugging skill for reference](../debugging/)

The differential fuzzer compares Turso results against SQLite for generated SQL statements to find correctness bugs.

## Location

`testing/differential-oracle/fuzzer/`

## Running the Fuzzer

### Single Run

```bash
# Basic run (100 statements, random seed)
cargo run --bin differential_fuzzer

# With specific seed for reproducibility
cargo run --bin differential_fuzzer -- --seed 12345

# More statements with verbose output
cargo run --bin differential_fuzzer -- -n 1000 --verbose

# Keep database files after run (for debugging)
cargo run --bin differential_fuzzer -- --seed 12345 --keep-files

# All options
cargo run --bin differential_fuzzer -- \
  --seed <SEED>           # Deterministic seed
  -n <NUM>                # Number of statements (default: 100)
  -t <NUM>                # Number of tables (default: 2)
  -c <NUM>                # Columns per table (default: 5)
  --verbose               # Print each SQL statement
  --keep-files            # Persist .db files to disk
```

### Continuous Fuzzing (Loop Mode)

```bash
# Run forever with random seeds
cargo run --bin differential_fuzzer -- loop

# Run 50 iterations
cargo run --bin differential_fuzzer -- loop 50
```

### Docker Runner (CI/Production)

```bash
# Build and run from repo root
docker build -f testing/differential-oracle/fuzzer/docker-runner/Dockerfile -t fuzzer .
docker run -e GITHUB_TOKEN=xxx -e SLACK_WEBHOOK_URL=xxx fuzzer
```

Environment variables for docker-runner:
- `TIME_LIMIT_MINUTES` - Total runtime (default: 1440 = 24h)
- `PER_RUN_TIMEOUT_SECONDS` - Per-run timeout (default: 1200 = 20min)
- `NUM_STATEMENTS` - Statements per run (default: 1000)
- `LOG_TO_STDOUT` - Print fuzzer output (default: false)
- `GITHUB_TOKEN` - For auto-filing issues
- `SLACK_WEBHOOK_URL` - For notifications

## Output Files

All output goes to `simulator-output/` directory:

| File | Description |
|------|-------------|
| `test.sql` | All executed SQL statements. Failed statements prefixed with `-- FAILED:`, errors with `-- ERROR:` |
| `schema.json` | Database schema at end of run (or at failure) |
| `test.db` | Turso database file (only with `--keep-files`) |
| `test-sqlite.db` | SQLite database file (only with `--keep-files`) |

## Reproducing Errors

Always follow these steps

1. **Find the seed** in the error output:
   ```
   INFO: Starting differential_fuzzer with config: SimConfig { seed: 12345, ... }
   ```

2. **Re-run with that seed**:
   ```bash
   cargo run --bin differential_fuzzer -- --seed 12345 --verbose --keep-files
   ```

3. **Check output files**:
   - `simulator-output/test.sql` - Find the failing statement (look for `-- FAILED:`)
   - `simulator-output/schema.json` - Check table structure at failure time

4. **Create a minimal reproducer**
   - Create reproducer in `.sqltest` or in `.rs` always load [Debugging skill for reference](../debugging/)

5. **Compare behavior manually**:
   If needed try to compare the behaviour and produce a report in the end.
   Always write to a tmp file first with Edit tool to test the sql and then pass it to the binaries.
   ```bash
   # Run failing SQL against SQLite
   sqlite3 :memory: < simulator-output/test.sql

   # Run against tursodb CLI
   tursodb :memory: < simulator-output/test.sql
   ```

## Understanding Failures

### Oracle Failure Types

1. **Row set mismatch** - Turso returned different rows than SQLite
2. **Turso errored but SQLite succeeded** - Turso rejected valid SQL
3. **SQLite errored but Turso succeeded** - Turso accepted invalid SQL
4. **Schema mismatch** - Tables/columns differ after DDL

### Warning (non-fatal)

- **Unordered LIMIT mismatch** - LIMIT without ORDER BY may return different valid rows

## Key Source Files

| File | Purpose |
|------|---------|
| `main.rs` | CLI parsing, entry point |
| `runner.rs` | Main simulation loop, executes statements on both DBs |
| `oracle.rs` | Compares Turso vs SQLite results |
| `schema.rs` | Introspects schema from both databases |
| `memory/` | In-memory IO for deterministic simulation |

## Tracing

Set `RUST_LOG` for more detailed output:

```bash
RUST_LOG=debug cargo run --bin differential_fuzzer -- --seed 12345
```
