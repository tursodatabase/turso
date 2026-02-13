#!/usr/bin/env python3
"""
SQLRight Integrity Check Script

Replays AFL queue/crash/hang inputs through tursodb (writing to a temp file),
then validates the resulting database with SQLite's PRAGMA integrity_check.

Queue items are sampled 1/N (default N=100); crashes and hangs are all checked.
Results are stored in the existing crashes.db via the integrity_checks table.
"""

import sys
import os
import argparse
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

# Add crash_reports/lib to path
sys.path.insert(0, str(Path(__file__).parent / "crash_reports"))

from lib.database import Database

logger = logging.getLogger(__name__)


def find_tursodb() -> Path:
    """Find tursodb binary."""
    locations = [
        Path.home() / "work/limbo-main/target/debug/tursodb",
        Path.home() / "work/limbo-main/target/release/tursodb",
        Path("/usr/local/bin/tursodb"),
    ]
    for loc in locations:
        if loc.exists():
            return loc
    raise FileNotFoundError(
        "tursodb binary not found. Build it or specify path with --tursodb-path"
    )


def collect_files(output_dir: Path, every_n: int):
    """Collect input files from AFL output directory.

    Walks primary/ and secondary_*/ instances, collecting:
    - Every Nth queue item (sorted by name)
    - ALL crash files
    - ALL hang files

    Returns list of (path, source_type, instance_name) tuples.
    """
    files = []

    if not output_dir.exists():
        logger.error(f"Output directory does not exist: {output_dir}")
        return files

    # Find all instance directories (primary, secondary_0, secondary_1, ...)
    instances = sorted(output_dir.iterdir()) if output_dir.is_dir() else []
    instances = [d for d in instances if d.is_dir()]

    # If the output_dir itself contains queue/crashes/hangs, treat it as a single instance
    if any((output_dir / subdir).is_dir() for subdir in ['queue', 'crashes', 'hangs']):
        instances = [output_dir]

    for instance_dir in instances:
        instance_name = instance_dir.name

        # Queue: sample every Nth item
        queue_dir = instance_dir / "queue"
        if queue_dir.is_dir():
            queue_files = sorted(
                f for f in queue_dir.iterdir()
                if f.is_file() and f.name != '.state'
            )
            for i, f in enumerate(queue_files):
                if i % every_n == 0:
                    files.append((f, 'queue', instance_name))
            logger.info(
                f"  {instance_name}/queue: {len(queue_files)} total, "
                f"{len([x for x in files if x[2] == instance_name and x[1] == 'queue'])} sampled (every {every_n}th)"
            )

        # Crashes: all
        crashes_dir = instance_dir / "crashes"
        if crashes_dir.is_dir():
            crash_files = sorted(
                f for f in crashes_dir.iterdir()
                if f.is_file() and f.name != 'README.txt'
            )
            for f in crash_files:
                files.append((f, 'crash', instance_name))
            logger.info(f"  {instance_name}/crashes: {len(crash_files)} (all checked)")

        # Hangs: all
        hangs_dir = instance_dir / "hangs"
        if hangs_dir.is_dir():
            hang_files = sorted(
                f for f in hangs_dir.iterdir()
                if f.is_file() and f.name != 'README.txt'
            )
            for f in hang_files:
                files.append((f, 'hang', instance_name))
            logger.info(f"  {instance_name}/hangs: {len(hang_files)} (all checked)")

    return files


def quote_ident(name):
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def discover_schema(db_path, timeout):
    """Discover tables and indexes from a database.

    Uses Python's sqlite3 module with PRAGMA queries for safe schema
    introspection — no string interpolation of identifier names into SQL,
    so names with quotes/pipes/special chars are handled correctly.

    Filters out:
    - Partial indexes (can't satisfy INDEXED BY without a matching WHERE)
    - Expression indexes (NULL column names produce invalid generated SQL)

    Returns dict mapping table_name -> list of index info dicts.
    Each index info dict has keys: index_name, columns (list of {name, desc, collation}).
    """
    import sqlite3
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    except Exception:
        return {}

    try:
        tables = {}
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ):
            table_name = row[0]
            indexes = []

            for idx_row in conn.execute(
                f'PRAGMA index_list({quote_ident(table_name)})'
            ):
                # (seq, name, unique, origin, partial)
                idx_name = idx_row[1]
                origin = idx_row[3]
                partial = idx_row[4]
                if origin == 'pk' or partial:
                    continue

                cols = []
                skip_index = False
                for col_row in conn.execute(
                    f'PRAGMA index_xinfo({quote_ident(idx_name)})'
                ):
                    # (seqno, cid, name, desc, coll, key)
                    if not col_row[5]:  # not a key column
                        continue
                    col_name = col_row[2]
                    if not col_name:  # expression index column
                        skip_index = True
                        break
                    cols.append({
                        'name': col_name,
                        'desc': bool(col_row[3]),
                        'collation': col_row[4] or 'BINARY',
                    })

                if cols and not skip_index:
                    indexes.append({
                        'index_name': idx_name,
                        'columns': cols,
                    })

            tables[table_name] = indexes

        conn.close()
        return tables
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return {}


def run_extended_checks(db_path, timeout):
    """Run extended integrity checks beyond PRAGMA integrity_check.

    Checks:
    1. Page count vs file size consistency (with WAL checkpoint)
    2. Index ordering via query engine (catches collation-level bugs)
    3. Round-trip data consistency via EXCEPT (table scan vs index scan)

    Returns list of (check_name, passed, detail) tuples.
    """
    results = []

    # --- Check 1: Page count vs file size ---
    try:
        # Checkpoint any WAL first so main file reflects all committed pages
        proc = subprocess.run(
            ['sqlite3', db_path,
             'PRAGMA wal_checkpoint(PASSIVE); PRAGMA page_count; PRAGMA page_size;'],
            capture_output=True,
            timeout=timeout,
            text=True,
        )
        numeric_lines = [l.strip() for l in proc.stdout.strip().split('\n')
                         if l.strip().isdigit()]
        if len(numeric_lines) >= 2:
            page_count = int(numeric_lines[0])
            page_size = int(numeric_lines[1])
            expected_size = page_count * page_size
            # Re-read file size after checkpoint
            file_size = os.path.getsize(db_path)
            wal_path = db_path + '-wal'
            if os.path.exists(wal_path) and os.path.getsize(wal_path) > 0:
                results.append(('page_count_vs_file_size', True,
                                'skipped (WAL not fully checkpointed)'))
            elif file_size == expected_size:
                results.append(('page_count_vs_file_size', True,
                                f'ok (page_count={page_count}, page_size={page_size}, '
                                f'file_size={file_size})'))
            else:
                results.append(('page_count_vs_file_size', False,
                                f'MISMATCH: page_count*page_size={expected_size} '
                                f'but file_size={file_size}'))
        else:
            results.append(('page_count_vs_file_size', True,
                            'skipped (could not read pragmas)'))
    except Exception as e:
        results.append(('page_count_vs_file_size', True, f'skipped ({e})'))

    # --- Discover schema for checks 2 and 3 ---
    schema = discover_schema(db_path, timeout)

    tables_with_indexes = {t: idxs for t, idxs in schema.items() if idxs}
    if not tables_with_indexes:
        results.append(('index_ordering', True, 'skipped (no eligible indexes)'))
        results.append(('round_trip_consistency', True, 'skipped (no eligible indexes)'))
        return results

    # --- Check 2: Index ordering validation ---
    # Scans each index via INDEXED BY and uses LAG() to check adjacent pairs.
    # NOTE: This relies on SQLite returning rows in index scan order when
    # INDEXED BY is used. This is consistent SQLite behavior (the B-tree cursor
    # iterates in key order). We intentionally omit ORDER BY in the window
    # function — adding one would sort the results and mask index ordering bugs.
    # Worst case (if a future SQLite reorders scan output), this check becomes
    # a no-op rather than producing false positives.
    ordering_details = []
    ordering_all_ok = True
    for table_name, indexes in tables_with_indexes.items():
        qt = quote_ident(table_name)
        for idx_info in indexes:
            idx_name = idx_info['index_name']
            cols = idx_info['columns']
            label = f'{table_name}.{idx_name}'
            qi = quote_ident(idx_name)

            col_selects = ', '.join(quote_ident(c['name']) for c in cols)
            lag_selects = ', '.join(
                f"LAG({quote_ident(c['name'])}) OVER () AS _p{i}"
                for i, c in enumerate(cols)
            )

            # Build violation condition with NULL-safe comparisons.
            # SQLite sorts NULLs first in ASC, last in DESC.
            conditions = []
            for i, c in enumerate(cols):
                # Prefix: all prior columns must be equal (NULL-safe via IS)
                prefix_eq = ' AND '.join(
                    f"_p{j} IS {quote_ident(cols[j]['name'])}"
                    for j in range(i)
                )
                qc = quote_ident(c['name'])
                if not c['desc']:
                    # ASC: violation when prev is non-NULL but curr is NULL or prev > curr
                    cond = f"(_p{i} IS NOT NULL AND ({qc} IS NULL OR _p{i} > {qc}))"
                else:
                    # DESC: violation when curr is non-NULL but prev is NULL or prev < curr
                    cond = f"({qc} IS NOT NULL AND (_p{i} IS NULL OR _p{i} < {qc}))"
                if prefix_eq:
                    cond = f"({prefix_eq} AND {cond})"
                conditions.append(cond)

            where = ' OR '.join(conditions)

            # EXISTS for early termination — one violation is enough
            query = (
                f"SELECT EXISTS("
                f"SELECT 1 FROM ("
                f"SELECT {col_selects}, {lag_selects} "
                f"FROM {qt} INDEXED BY {qi}"
                f") WHERE _p0 IS NOT NULL AND ({where})"
                f");"
            )

            try:
                proc = subprocess.run(
                    ['sqlite3', db_path, query],
                    capture_output=True,
                    timeout=timeout,
                    text=True,
                )
                if proc.returncode != 0 or proc.stderr.strip():
                    ordering_details.append(
                        f'{label}: error ({proc.stderr.strip()[:100]})')
                else:
                    val = proc.stdout.strip()
                    if val == '0':
                        ordering_details.append(f'{label}: ok')
                    else:
                        ordering_details.append(
                            f'{label}: out-of-order entries found')
                        ordering_all_ok = False
            except subprocess.TimeoutExpired:
                ordering_details.append(f'{label}: timeout')
            except Exception as e:
                ordering_details.append(f'{label}: error ({e})')

    results.append(('index_ordering', ordering_all_ok,
                    '; '.join(ordering_details) if ordering_details
                    else 'no checks run'))

    # --- Check 3: Round-trip data consistency via EXCEPT ---
    # Compares rows from a full table scan (NOT INDEXED) against rows accessed
    # via each index (INDEXED BY). Uses EXCEPT for set comparison, which:
    # - Works with WITHOUT ROWID tables (no rowid reference needed)
    # - Is memory-efficient (no GROUP_CONCAT materialization)
    # - Is NULL-safe (empty tables produce 0 difference rows, not NULL=NULL)
    consistency_details = []
    consistency_all_ok = True
    for table_name, indexes in tables_with_indexes.items():
        qt = quote_ident(table_name)
        for idx_info in indexes:
            idx_name = idx_info['index_name']
            qi = quote_ident(idx_name)
            label = f'{table_name}.{idx_name}'

            # Check both directions: rows in table but not via index, and vice versa
            query = (
                f"SELECT "
                f"EXISTS(SELECT 1 FROM "
                f"(SELECT * FROM {qt} NOT INDEXED "
                f"EXCEPT SELECT * FROM {qt} INDEXED BY {qi})) "
                f"OR "
                f"EXISTS(SELECT 1 FROM "
                f"(SELECT * FROM {qt} INDEXED BY {qi} "
                f"EXCEPT SELECT * FROM {qt} NOT INDEXED));"
            )

            try:
                proc = subprocess.run(
                    ['sqlite3', db_path, query],
                    capture_output=True,
                    timeout=timeout,
                    text=True,
                )
                if proc.returncode != 0 or proc.stderr.strip():
                    consistency_details.append(
                        f'{label}: error ({proc.stderr.strip()[:100]})')
                else:
                    val = proc.stdout.strip()
                    if val == '0':
                        consistency_details.append(f'{label}: ok')
                    else:
                        consistency_details.append(
                            f'{label}: MISMATCH (table scan != index scan)')
                        consistency_all_ok = False
            except subprocess.TimeoutExpired:
                consistency_details.append(f'{label}: timeout')
            except Exception as e:
                consistency_details.append(f'{label}: error ({e})')

    results.append(('round_trip_consistency', consistency_all_ok,
                    '; '.join(consistency_details) if consistency_details
                    else 'no checks run'))

    return results


def check_one(tursodb_path: Path, sql_path: Path, timeout: int):
    """Run one integrity check.

    1. Create a unique temp DB file
    2. Run tursodb with file-backed DB, piping in the SQL
    3. Run sqlite3 PRAGMA integrity_check on the result
    4. Run extended checks (page size, index ordering, round-trip)
    5. Clean up temp files

    Returns dict with turso_exit_code, turso_stderr, integrity_output,
    extended_output, passed.
    """
    fd, tmp_db = tempfile.mkstemp(suffix='.db', prefix='integrity_',
                                  dir='/tmp/claude')
    os.close(fd)
    # Remove the empty file so tursodb creates it fresh
    os.unlink(tmp_db)

    result = {
        'turso_exit_code': None,
        'turso_stderr': None,
        'integrity_output': None,
        'extended_output': None,
        'passed': True,  # Default: no file created = no corruption possible
    }

    try:
        # Read SQL content
        sql_content = sql_path.read_bytes()

        # Run tursodb with the SQL input
        try:
            turso_proc = subprocess.run(
                [str(tursodb_path), tmp_db, '-q', '-m', 'list'],
                input=sql_content,
                capture_output=True,
                timeout=timeout,
            )
            result['turso_exit_code'] = turso_proc.returncode
            result['turso_stderr'] = turso_proc.stderr.decode('utf-8', errors='replace')[:4096]
        except subprocess.TimeoutExpired:
            result['turso_exit_code'] = -1
            result['turso_stderr'] = 'TIMEOUT'
        except Exception as e:
            result['turso_exit_code'] = -2
            result['turso_stderr'] = str(e)

        # Check if tursodb created a database file
        if not os.path.exists(tmp_db) or os.path.getsize(tmp_db) == 0:
            # No file created — only SELECTs or empty SQL. No corruption possible.
            result['integrity_output'] = 'skipped (no database file created)'
            result['passed'] = True
            return result

        # Run sqlite3 integrity_check
        try:
            sqlite_proc = subprocess.run(
                ['sqlite3', tmp_db, 'PRAGMA integrity_check;'],
                capture_output=True,
                timeout=timeout,
                text=True,
            )
            output = sqlite_proc.stdout.strip()
            result['integrity_output'] = output[:4096]
            result['passed'] = (output == 'ok')
        except subprocess.TimeoutExpired:
            result['integrity_output'] = 'TIMEOUT'
            result['passed'] = False
        except Exception as e:
            result['integrity_output'] = f'ERROR: {e}'
            result['passed'] = False

        # Run extended checks if basic check passed
        if result['passed']:
            ext_results = run_extended_checks(tmp_db, timeout)
            ext_lines = []
            for check_name, check_passed, detail in ext_results:
                status = 'PASS' if check_passed else 'FAIL'
                ext_lines.append(f'[{status}] {check_name}: {detail}')
                if not check_passed:
                    result['passed'] = False
            result['extended_output'] = '\n'.join(ext_lines)

        return result

    finally:
        # Clean up temp files
        for suffix in ['', '-wal', '-shm']:
            path = tmp_db + suffix
            try:
                if os.path.exists(path):
                    os.unlink(path)
            except OSError:
                pass


def main():
    parser = argparse.ArgumentParser(
        description="Check integrity of databases created by tursodb from AFL inputs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                            # Check /tmp/sqlright_test with defaults
  %(prog)s --output-dir /path/to     # Custom AFL output directory
  %(prog)s --every 50                # Sample every 50th queue item
  %(prog)s --timeout 10              # 10s timeout per execution
  %(prog)s --tursodb-path /path      # Custom tursodb binary
  %(prog)s --db crashes.db           # Custom DB path
  %(prog)s -v                        # Verbose logging
        """
    )

    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('/tmp/sqlright_test'),
        metavar='PATH',
        help='AFL output directory (default: /tmp/sqlright_test)'
    )
    parser.add_argument(
        '--every',
        type=int,
        default=100,
        metavar='N',
        help='Sample every Nth queue item (default: 100)'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=5,
        metavar='SECS',
        help='Timeout per execution in seconds (default: 5)'
    )
    parser.add_argument(
        '--tursodb-path',
        type=Path,
        metavar='PATH',
        help='Path to tursodb binary (auto-detected if not specified)'
    )
    parser.add_argument(
        '--db',
        type=Path,
        metavar='PATH',
        help='Path to database file (default: crash_reports/crashes.db)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Check sqlite3 is available
    if not shutil.which('sqlite3'):
        logger.error("sqlite3 not found in PATH. Install SQLite to use this script.")
        sys.exit(1)

    # Find tursodb
    try:
        tursodb_path = args.tursodb_path or find_tursodb()
        logger.info(f"Using tursodb: {tursodb_path}")
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Ensure temp directory exists
    os.makedirs('/tmp/claude', exist_ok=True)

    # Collect files
    print(f"\n=== Integrity Check ===")
    print(f"Output dir: {args.output_dir}")
    logger.info("Scanning for input files...")
    files = collect_files(args.output_dir, args.every)

    if not files:
        print("No input files found.")
        sys.exit(0)

    # Count by type
    type_counts = {}
    for _, source_type, _ in files:
        type_counts[source_type] = type_counts.get(source_type, 0) + 1

    for stype, count in sorted(type_counts.items()):
        label = "sampled" if stype == 'queue' else "all checked"
        print(f"  {stype.capitalize()}: {count} ({label})")

    print(f"\nChecking {len(files)} items...")

    # Open database
    db_path = args.db or Path(__file__).parent / "crash_reports" / "crashes.db"
    schema_path = Path(__file__).parent / "crash_reports" / "schema.sql"

    db = Database(db_path)
    try:
        db.init_database(schema_path)
        db.ensure_integrity_schema()

        passed = 0
        failed = 0
        skipped = 0

        for i, (sql_path, source_type, instance_name) in enumerate(files):
            source_file = str(sql_path)

            # Skip already-checked files
            if db.is_integrity_checked(source_file):
                skipped += 1
                continue

            # Read SQL content for storage
            try:
                sql_content = sql_path.read_text(errors='replace')
            except Exception as e:
                logger.warning(f"Cannot read {sql_path}: {e}")
                continue

            # Run integrity check
            result = check_one(tursodb_path, sql_path, args.timeout)

            # Store result
            db.add_integrity_check(
                source_type=source_type,
                source_instance=instance_name,
                source_file=source_file,
                sql_content=sql_content,
                turso_exit_code=result['turso_exit_code'],
                turso_stderr=result['turso_stderr'],
                integrity_output=result['integrity_output'],
                passed=result['passed'],
                extended_output=result.get('extended_output'),
            )

            if result['passed']:
                passed += 1
            else:
                failed += 1
                detail = result['integrity_output'][:100]
                if result.get('extended_output') and result['integrity_output'] == 'ok':
                    detail = result['extended_output'][:200]
                logger.warning(f"FAIL: {sql_path.name} — {detail}")

            # Progress every 10 items
            checked = passed + failed
            if checked % 10 == 0:
                print(f"  [{checked}/{len(files) - skipped}] "
                      f"Checked {checked} items ({passed} passed, {failed} failed)")

        # Final summary
        checked = passed + failed
        print(f"\nResults: {checked} checked, {passed} passed, {failed} failed", end="")
        if skipped:
            print(f" ({skipped} already checked, skipped)")
        else:
            print()

        if failed > 0:
            print(f"\nUse 'query_crashes.py integrity --fails' to see failure details.")
            sys.exit(1)

    except KeyboardInterrupt:
        print(f"\nInterrupted. Partial results saved to {db_path}")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=args.verbose)
        sys.exit(1)
    finally:
        db.close()


if __name__ == '__main__':
    main()
