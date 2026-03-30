# SQLRight Crash Report Collection System

Automated collection, deduplication, testing, and bug classification for SQLRight fuzzer crashes.

```bash
# Collect from specific directory
# This is idempotent and will pick up from a previous collection left off, testing only the new crashes
./collect_crashes.py --scan ../results/run_NOREC_*

# Collect from multiple directories
./collect_crashes.py --scan ../results/run_NOREC_* --scan ~/work/limbo-main/testing/sqlright/results

# Collect from all known locations
./collect_crashes.py --all

# Specify database location
./collect_crashes.py --scan ../results/run_NOREC_* --db /path/to/crashes.db

# Specify tursodb path
./collect_crashes.py --scan ../results/run_NOREC_* --tursodb-path /path/to/tursodb

# Verbose output
./collect_crashes.py --scan ../results/run_NOREC_* -v
```

## Querying Results

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

## Bug classification

| Tursodb     | SQLite      | Bug Category                |
|-------------|-------------|-----------------------------|
| PANIC       | any         | TURSO_PANIC                 |
| CRASH       | not CRASH   | TURSO_CRASH                 |
| PARSE_ERROR | SUCCESS     | TURSO_INCORRECT_PARSE_ERROR |
| SUCCESS     | PARSE_ERROR | TURSO_INCORRECT_SUCCESS     |
| TIMEOUT     | not TIMEOUT | TURSO_TIMEOUT               |

## Database Queries

Direct SQL queries on crashes.db:

```sql
-- Find all panics
SELECT crash_id, sql_content
FROM crashes c
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
ORDER BY c.first_seen DESC LIMIT 20;
```
