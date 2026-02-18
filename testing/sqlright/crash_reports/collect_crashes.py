#!/usr/bin/env python3
"""
SQLRight Crash Report Collection Script

Collects, deduplicates, and tests crash files from SQLRight fuzzing sessions.
Idempotent and incremental - safe to run multiple times.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add lib to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.database import Database  # noqa: E402
from lib.executor import test_crashes  # noqa: E402
from lib.scanner import scan_session  # noqa: E402


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


def find_tursodb() -> Path:
    """Find tursodb binary."""
    # Try common locations
    locations = [
        Path.home() / "work/limbo-main/target/debug/tursodb",
        Path.home() / "work/limbo-main/target/release/tursodb",
        Path("/usr/local/bin/tursodb"),
    ]

    for loc in locations:
        if loc.exists():
            return loc

    raise FileNotFoundError(
        "tursodb binary not found. Please build it or specify path with --tursodb-path"
    )


def collect_from_paths(paths: list, db: Database, tursodb_path: Path, verbose: bool = False):
    """
    Collect crashes from multiple paths.

    Args:
        paths: List of paths to scan
        db: Database instance
        tursodb_path: Path to tursodb binary
        verbose: Enable verbose logging
    """
    logger = logging.getLogger(__name__)

    total_stats = {
        "files_processed": 0,
        "crashes_added": 0,
        "skipped": 0,
        "errors": 0
    }

    # Scan each path
    for path_str in paths:
        path = Path(path_str).expanduser().resolve()

        if not path.exists():
            logger.warning(f"Path does not exist: {path}")
            continue

        if not path.is_dir():
            logger.warning(f"Not a directory: {path}")
            continue

        logger.info(f"\n{'=' * 60}")
        logger.info(f"Scanning: {path}")
        logger.info(f"{'=' * 60}")

        try:
            stats = scan_session(path, db, batch_size=100)

            total_stats["files_processed"] += stats["files_processed"]
            total_stats["crashes_added"] += stats["crashes_added"]
            total_stats["skipped"] += stats["skipped"]
            total_stats["errors"] += stats["errors"]

            logger.info(f"Session stats: {stats}")

        except Exception as e:
            logger.error(f"Error scanning {path}: {e}", exc_info=verbose)
            total_stats["errors"] += 1

    # Test crashes
    logger.info(f"\n{'=' * 60}")
    logger.info("Testing crashes against tursodb and SQLite")
    logger.info(f"{'=' * 60}")

    try:
        test_stats = test_crashes(db, tursodb_path, timeout=5)
        logger.info(f"Test stats: {test_stats}")
    except Exception as e:
        logger.error(f"Error testing crashes: {e}", exc_info=verbose)

    # Print summary
    print_summary(db, total_stats, test_stats)


def print_summary(db: Database, scan_stats: dict, test_stats: dict):
    """Print summary statistics."""
    stats = db.get_stats()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print("\nScanning:")
    print(f"  Files processed:     {scan_stats['files_processed']}")
    print(f"  Crashes added:       {scan_stats['crashes_added']}")
    print(f"  Skipped:             {scan_stats['skipped']}")
    print(f"  Errors:              {scan_stats['errors']}")

    print("\nTesting:")
    print(f"  Crashes tested:      {test_stats.get('tested', 0)}")
    print(f"  Bugs found:          {test_stats.get('bugs_found', 0)}")
    print(f"  Test errors:         {test_stats.get('errors', 0)}")

    print("\nDatabase:")
    print(f"  Total crash files:   {stats.get('total_files', 0)}")
    print(f"  Unique crashes:      {stats.get('unique_crashes', 0)}")
    print(f"  Total bugs:          {stats.get('bugs_found', 0)}")

    if stats.get("unique_crashes", 0) > 0:
        dedup_ratio = 100.0 * stats["unique_crashes"] / stats["total_files"]
        print(f"  Dedup ratio:         {dedup_ratio:.1f}%")

    if stats.get("classifications"):
        print("\nClassifications:")
        for classification, count in sorted(stats["classifications"].items()):
            print(f"  {classification:20s} {count}")

    if stats.get("bug_categories"):
        print("\nBug Categories:")
        for category, count in sorted(stats["bug_categories"].items()):
            print(f"  {category:30s} {count}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Collect and test SQLRight crash reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect from active fuzzing session
  %(prog)s --scan /tmp/sqlright_test

  # Collect from historical results
  %(prog)s --scan ~/work/limbo-main/testing/sqlright/results/run_*

  # Collect from all known locations
  %(prog)s --all

  # Verbose output
  %(prog)s --scan /tmp/sqlright_test -v
        """
    )

    parser.add_argument(
        "--scan",
        action="append",
        metavar="PATH",
        help="Path to fuzzing session to scan (can be specified multiple times)"
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Scan all known fuzzing locations"
    )

    parser.add_argument(
        "--db",
        type=Path,
        metavar="PATH",
        help="Path to database file (default: ./crashes.db)"
    )

    parser.add_argument(
        "--tursodb-path",
        type=Path,
        metavar="PATH",
        help="Path to tursodb binary (auto-detected if not specified)"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Determine paths to scan
    scan_paths = []

    if args.scan:
        scan_paths.extend(args.scan)

    if args.all:
        # Add known fuzzing locations
        known_locations = [
            "/tmp/sqlright_test",
            Path.home() / "work/limbo-main/testing/sqlright/results"
        ]
        for loc in known_locations:
            if Path(loc).exists():
                scan_paths.append(str(loc))

    if not scan_paths:
        parser.error("No paths specified. Use --scan or --all")

    # Determine database path
    db_path = args.db or Path(__file__).parent / "crashes.db"
    schema_path = Path(__file__).parent / "schema.sql"

    # Find tursodb
    try:
        tursodb_path = args.tursodb_path or find_tursodb()
        logger.info(f"Using tursodb: {tursodb_path}")
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Initialize database
    db = Database(db_path)
    try:
        db.init_database(schema_path)
        logger.info(f"Database: {db_path}")

        # Collect crashes
        collect_from_paths(scan_paths, db, tursodb_path, args.verbose)

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=args.verbose)
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
