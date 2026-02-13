# SQLRight Crash Report Collection System

Automated collection, deduplication, testing, and bug classification for SQLRight fuzzer crashes.

## Features

- **Automated Collection**: Scans fuzzing directories for crash files
- **Deduplication**: Uses MD5 content hashing to identify unique crashes
- **Differential Testing**: Tests crashes against both tursodb and SQLite
- **Bug Detection**: Identifies behavior divergences (panics, crashes, parse errors)
- **Idempotent**: Safe to run multiple times - only processes new crashes
- **O(n) Efficient**: Checkpoint-based processing skips already-processed files
- **SQLite Storage**: All data stored in a queryable SQLite database

## Quick Start

```bash
# Collect from active fuzzing session (automatically tests crashes)
./collect_crashes.py --scan /tmp/sqlright_test

# View statistics
./query_crashes.py stats

# List bugs
./query_crashes.py bugs

# Show detailed crash info
./query_crashes.py show <crash_id>
```

## Installation

No installation required. Dependencies:
- Python 3.7+
- Standard library only
- `tursodb` binary (auto-detected from common locations)
- `sqlite3` command (for differential testing)

## Usage

### Collecting Crashes

```bash
# Collect from specific directory
./collect_crashes.py --scan /tmp/sqlright_test

# Collect from multiple directories
./collect_crashes.py --scan /tmp/sqlright_test --scan ~/work/limbo-main/testing/sqlright/results

# Collect from all known locations
./collect_crashes.py --all

# Specify database location
./collect_crashes.py --scan /tmp/sqlright_test --db /path/to/crashes.db

# Specify tursodb path
./collect_crashes.py --scan /tmp/sqlright_test --tursodb-path /path/to/tursodb

# Verbose output
./collect_crashes.py --scan /tmp/sqlright_test -v
```

**Note**: Collection automatically includes testing against tursodb and SQLite. No separate testing step needed.

### Querying Results

```bash
# Show database statistics
./query_crashes.py stats

# List all bugs
./query_crashes.py bugs

# Show detailed crash information
./query_crashes.py show <crash_id>

# Export crash SQL to file
./query_crashes.py export <crash_id> output.sql

# List all crashes
./query_crashes.py list

# List crashes by classification
./query_crashes.py list --class PANIC
./query_crashes.py list --class PARSE_ERROR
```

## How It Works

### 1. Crash Discovery (O(n) Algorithm)

The scanner uses a checkpoint-based algorithm to only process new crashes:

1. Register fuzzing session in database
2. Get last processing checkpoint (if exists)
3. Find all crash files in session directory
4. Filter to only files newer than checkpoint
5. Double-check against database to ensure idempotency
6. Process in batches of 100 files
7. Update checkpoint after each batch

**Time Complexity**: O(new_crashes) not O(total_crashes)

### 2. Deduplication

Crashes are deduplicated by:
- MD5 hash of SQL content
- Signal number (SIGSEGV, SIGABRT, etc.)

This ensures that identical crashes from different fuzzer instances are treated as one unique crash.

### 3. Crash Testing

Each unique crash is tested against tursodb:

```python
tursodb -q -m list --experimental-all < crash.sql
```

Results are classified as:
- **PANIC**: Process panicked
- **CRASH**: Process received signal (SIGSEGV, SIGABRT)
- **PARSE_ERROR**: SQL parse/syntax error
- **SUCCESS**: Query executed successfully
- **TIMEOUT**: Process exceeded time limit (5 seconds)
- **ERROR**: Other error

### 4. Differential Testing

The same SQL is tested against SQLite:

```python
sqlite3 :memory: < crash.sql
```

Results are compared to identify bugs:

| Tursodb | SQLite | Bug Category |
|---------|--------|--------------|
| PANIC | any | TURSO_PANIC |
| CRASH | not CRASH | TURSO_CRASH |
| PARSE_ERROR | SUCCESS | TURSO_INCORRECT_PARSE_ERROR |
| SUCCESS | PARSE_ERROR | TURSO_INCORRECT_SUCCESS |
| TIMEOUT | not TIMEOUT | TURSO_TIMEOUT |

### 5. Database Schema

**Core Tables**:
- `crashes` - Unique crashes (deduplicated)
- `crash_instances` - Individual crash files
- `fuzzing_sessions` - Fuzzing runs
- `crash_tests` - Tursodb test results
- `differential_tests` - Differential testing results (SQLite comparison)
- `processing_checkpoints` - Idempotency tracking

**Views**:
- `v_crashes_with_bugs` - Crashes with bug information
- `v_bug_summary` - Bug category statistics
- `v_session_stats` - Per-session statistics

## Directory Structure

```
crash_reports/
├── crashes.db              # SQLite database (auto-created)
├── schema.sql              # Database schema
├── collect_crashes.py      # Main collection script
├── query_crashes.py        # Query interface
├── lib/
│   ├── __init__.py
│   ├── database.py         # DB operations
│   ├── scanner.py          # O(n) crash discovery
│   ├── executor.py         # Crash testing
│   └── parser.py           # AFL filename parsing
└── README.md               # This file
```

## Examples

### Initial Collection

```bash
$ ./collect_crashes.py --scan /tmp/sqlright_test

============================================================
Scanning: /tmp/sqlright_test
============================================================
Found 64 crash directories in /tmp/sqlright_test
Found 79 total crash files
No checkpoint - processing all files
Final unprocessed count: 79
Processing 79 crashes in batches of 100
Checkpoint updated: 79 files processed

============================================================
Testing crashes against tursodb and SQLite
============================================================
Bug found: TURSO_PANIC (crash_id=1)
Bug found: TURSO_INCORRECT_PARSE_ERROR (crash_id=2)

============================================================
SUMMARY
============================================================

Scanning:
  Files processed:     79
  Crashes added:       79
  Skipped:             0
  Errors:              0

Testing:
  Crashes tested:      3
  Bugs found:          2
  Test errors:         0

Database:
  Total crash files:   79
  Unique crashes:      3
  Total bugs:          2
  Dedup ratio:         3.8%

Classifications:
  PANIC                2
  PARSE_ERROR          1

Bug Categories:
  TURSO_PANIC                    1
  TURSO_INCORRECT_PARSE_ERROR    1
```

### Incremental Run (Idempotency)

```bash
$ ./collect_crashes.py --scan /tmp/sqlright_test

============================================================
Scanning: /tmp/sqlright_test
============================================================
Found 64 crash directories in /tmp/sqlright_test
Found 79 total crash files
Checkpoint filter: 0 files newer than checkpoint
Final unprocessed count: 0
No new crashes to process

============================================================
Testing crashes against tursodb and SQLite
============================================================
Testing 0 untested crashes

# Takes < 1 second (checkpoint-based skip)
```

### Query Bugs

```bash
$ ./query_crashes.py bugs

============================================================
BUGS FOUND (2 total)
============================================================

TURSO_PANIC:
  Crash #1: PANIC (instances: 42, size: 156 bytes)

TURSO_INCORRECT_PARSE_ERROR:
  Crash #2: PARSE_ERROR (instances: 35, size: 89 bytes)
```

### Show Crash Details

```bash
$ ./query_crashes.py show 1

============================================================
CRASH #1
============================================================

Metadata:
  Content hash:        a1b2c3d4e5f6...
  Signal number:       6
  Instance count:      42
  First seen:          2026-02-13 10:30:45
  Last seen:           2026-02-13 12:15:22

Tursodb Test:
  Classification:      PANIC
  Stderr:              thread 'main' panicked at 'assertion failed...

SQLite Comparison:
  Classification:      SUCCESS
  Is bug:              True
  Bug category:        TURSO_PANIC

SQL Content (156 bytes):
------------------------------------------------------------
CREATE VIEW v AS SELECT * FROM t;
SELECT * FROM v AS alias;
------------------------------------------------------------

Instances (5 shown):
  /tmp/sqlright_test/primary/crashes/id:000042,sig:06,src:000001
  /tmp/sqlright_test/secondary_01/crashes/id:000015,sig:06,sync:primary
```

## Performance

- **First run** (79 crashes): ~8 seconds (collection + testing)
- **Incremental run** (0 new): < 1 second (checkpoint-based skip)
- **Large dataset** (10,000 crashes): ~5 minutes
- **Database size**: ~1KB per unique crash

## Edge Cases Handled

- Deleted crashes during scan (skip with warning)
- Concurrent fuzzing (WAL mode for concurrent reads)
- Timeout hangs (5-second timeout with SIGKILL)
- Binary crashes during testing (catch all signals)
- Invalid crash filenames (skip with warning)
- Empty crash files (skip as invalid)
- Disk full (abort with clear message)
- Interrupted processing (resume from checkpoint)

## Troubleshooting

### "tursodb binary not found"

Specify path manually:
```bash
./collect_crashes.py --scan /tmp/sqlright_test --tursodb-path /path/to/tursodb
```

### "Database is locked"

Another process may be using the database. The database uses WAL mode for concurrent reads, but only one writer at a time.

### High memory usage

Process crashes in smaller batches:
```python
# Edit collect_crashes.py
stats = scan_session(path, db, batch_size=50)  # Default is 100
```

### Missing crashes

Run with `--verbose` to see what's being skipped:
```bash
./collect_crashes.py --scan /tmp/sqlright_test -v
```

## Integration with Fuzzing Workflow

### Manual Collection

```bash
# Run fuzzer
./run_sqlright.sh

# Periodically collect crashes
./collect_crashes.py --scan /tmp/sqlright_test
./query_crashes.py bugs
```

### Automated Collection (Cron)

```bash
# Add to crontab for hourly collection
0 * * * * cd ~/work/limbo-main/testing/sqlright/crash_reports && ./collect_crashes.py --all
```

### CI/CD Integration

```bash
# In GitHub Actions or similar
./collect_crashes.py --scan /tmp/sqlright_test
if ./query_crashes.py bugs | grep -q "BUGS FOUND"; then
    echo "New bugs found!"
    exit 1
fi
```

## Database Queries

Direct SQL queries on crashes.db:

```sql
-- Find all panics
SELECT crash_id, sql_content FROM crashes c
JOIN crash_tests ct ON c.crash_id = ct.crash_id
WHERE ct.classification = 'PANIC';

-- Bug breakdown by category
SELECT bug_category, COUNT(*) as count
FROM differential_tests
WHERE is_bug = 1
GROUP BY bug_category;

-- Crashes with most instances (likely easiest to trigger)
SELECT crash_id, instance_count, LENGTH(sql_content) as size
FROM crashes
ORDER BY instance_count DESC
LIMIT 10;

-- Recent crashes
SELECT c.crash_id, c.first_seen, ct.classification
FROM crashes c
JOIN crash_tests ct ON c.crash_id = ct.crash_id
ORDER BY c.first_seen DESC
LIMIT 20;
```

## License

Part of the Turso/Limbo project.
