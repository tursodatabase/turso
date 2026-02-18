#!/usr/bin/env python3
"""
SQLRight Crash Report Query Interface

Query and analyze collected crash reports.
"""

import argparse
import sys
from pathlib import Path

# Add lib to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.database import Database  # noqa: E402


def cmd_stats(db: Database):
    """Show database statistics."""
    stats = db.get_stats()

    print("\n" + "=" * 60)
    print("CRASH REPORT STATISTICS")
    print("=" * 60)

    print("\nOverall:")
    print(f"  Total crash files:   {stats.get('total_files', 0)}")
    print(f"  Unique crashes:      {stats.get('unique_crashes', 0)}")
    print(f"  Bugs found:          {stats.get('bugs_found', 0)}")

    if stats.get("unique_crashes", 0) > 0 and stats.get("total_files", 0) > 0:
        dedup_ratio = 100.0 * stats["unique_crashes"] / stats["total_files"]
        print(f"  Deduplication:       {dedup_ratio:.1f}%")

    if stats.get("classifications"):
        print("\nClassifications:")
        for classification, count in sorted(stats["classifications"].items()):
            print(f"  {classification:20s} {count}")

    if stats.get("bug_categories"):
        print("\nBug Categories:")
        for category, count in sorted(stats["bug_categories"].items()):
            print(f"  {category:30s} {count}")

    # Session stats
    conn = db.get_connection()
    cursor = conn.execute("SELECT * FROM v_session_stats ORDER BY session_id")
    sessions = cursor.fetchall()

    if sessions:
        print("\nSessions:")
        for session in sessions:
            print(f"  [{session['session_id']}] {session['session_path']}")
            print(f"      Files: {session['total_files']}, Unique: {session['unique_crashes']}")

    print()


def cmd_bugs(db: Database):
    """List all bugs."""
    bugs = db.get_bugs()

    if not bugs:
        print("No bugs found.")
        return

    print("\n" + "=" * 60)
    print(f"BUGS FOUND ({len(bugs)} total)")
    print("=" * 60)

    current_category = None
    for bug in bugs:
        if bug["bug_category"] != current_category:
            current_category = bug["bug_category"]
            print(f"\n{current_category}:")

        print(f"  Crash #{bug['crash_id']}: {bug['turso_classification']} "
              f"(instances: {bug['instance_count']}, size: {bug['sql_length']} bytes)")

    print()


def cmd_show(db: Database, crash_id: int):
    """Show detailed information about a crash."""
    crash = db.get_crash(crash_id)

    if not crash:
        print(f"Crash #{crash_id} not found.")
        return

    print("\n" + "=" * 60)
    print(f"CRASH #{crash_id}")
    print("=" * 60)

    print("\nMetadata:")
    print(f"  Content hash:        {crash['content_hash']}")
    print(f"  Signal number:       {crash['signal_number']}")
    print(f"  Instance count:      {crash['instance_count']}")
    print(f"  First seen:          {crash['first_seen']}")
    print(f"  Last seen:           {crash['last_seen']}")

    print("\nTursodb Test:")
    print(f"  Classification:      {crash['turso_classification']}")
    if crash["turso_stderr"]:
        stderr_preview = crash["turso_stderr"][:200]
        if len(crash["turso_stderr"]) > 200:
            stderr_preview += "..."
        print(f"  Stderr:              {stderr_preview}")

    print("\nSQLite Comparison:")
    print(f"  Classification:      {crash['sqlite_classification']}")
    print(f"  Is bug:              {bool(crash['is_bug'])}")
    if crash["bug_category"]:
        print(f"  Bug category:        {crash['bug_category']}")

    print(f"\nSQL Content ({len(crash['sql_content'])} bytes):")
    print("-" * 60)
    print(crash["sql_content"])
    print("-" * 60)

    # Show instances
    conn = db.get_connection()
    cursor = conn.execute(
        """
        SELECT file_path, file_name, afl_id, discovered_at
        FROM crash_instances
        WHERE crash_id = ?
        ORDER BY discovered_at
        LIMIT 5
        """,
        (crash_id,)
    )
    instances = cursor.fetchall()

    if instances:
        print(f"\nInstances ({len(instances)} shown):")
        for inst in instances:
            print(f"  {inst['file_path']}")

    print()


def cmd_export(db: Database, crash_id: int, output_file: str):
    """Export crash SQL to file."""
    crash = db.get_crash(crash_id)

    if not crash:
        print(f"Crash #{crash_id} not found.")
        return

    output_path = Path(output_file)
    output_path.write_text(crash["sql_content"])

    print(f"Exported crash #{crash_id} to {output_path}")
    print(f"  Classification: {crash['turso_classification']}")
    print(f"  Is bug:         {bool(crash['is_bug'])}")
    print(f"  Size:           {len(crash['sql_content'])} bytes")


def cmd_list(db: Database, classification: str = None, limit: int = 50):
    """List crashes with optional filtering."""
    conn = db.get_connection()

    if classification:
        cursor = conn.execute(
            """
            SELECT c.crash_id, ct.classification, c.instance_count, LENGTH(c.sql_content) AS sql_length
            FROM crashes c
            JOIN crash_tests ct ON c.crash_id = ct.crash_id
            WHERE ct.classification = ?
            ORDER BY c.crash_id
            LIMIT ?
            """,
            (classification.upper(), limit)
        )
    else:
        cursor = conn.execute(
            """
            SELECT c.crash_id, ct.classification, c.instance_count, LENGTH(c.sql_content) AS sql_length
            FROM crashes c
            LEFT JOIN crash_tests ct ON c.crash_id = ct.crash_id
            ORDER BY c.crash_id
            LIMIT ?
            """,
            (limit,)
        )

    crashes = cursor.fetchall()

    if not crashes:
        print("No crashes found.")
        return

    print("\n" + "=" * 60)
    print(f"CRASHES ({len(crashes)} shown)")
    print("=" * 60)

    for crash in crashes:
        classification = crash["classification"] or "UNTESTED"
        print(f"  #{crash['crash_id']:4d}  {classification:15s}  "
              f"instances={crash['instance_count']:3d}  size={crash['sql_length']:6d}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Query SQLRight crash reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  stats              Show database statistics
  bugs               List all bugs
  show <id>          Show detailed crash information
  export <id> <file> Export crash SQL to file
  list               List all crashes
  list --class=PANIC List crashes by classification

Examples:
  %(prog)s stats
  %(prog)s bugs
  %(prog)s show 42
  %(prog)s export 42 crash_42.sql
  %(prog)s list --class PANIC
        """
    )

    parser.add_argument(
        "command",
        choices=["stats", "bugs", "show", "export", "list"],
        help="Command to execute"
    )

    parser.add_argument(
        "args",
        nargs="*",
        help="Command arguments"
    )

    parser.add_argument(
        "--db",
        type=Path,
        metavar="PATH",
        help="Path to database file (default: ./crashes.db)"
    )

    parser.add_argument(
        "--class",
        dest="classification",
        metavar="CLASS",
        help="Filter by classification (for list command)"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        metavar="N",
        help="Limit number of results (default: 50)"
    )

    args = parser.parse_args()

    # Determine database path
    db_path = args.db or Path(__file__).parent / "crashes.db"

    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run collect_crashes.py first to create the database.")
        sys.exit(1)

    # Open database
    db = Database(db_path)

    try:
        dispatch_command(parser, args, db)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


def dispatch_command(parser: argparse.ArgumentParser, args: argparse.Namespace, db: Database):
    """Dispatch to the appropriate command handler."""
    if args.command == "stats":
        cmd_stats(db)
    elif args.command == "bugs":
        cmd_bugs(db)
    elif args.command == "show":
        if not args.args:
            parser.error("show command requires crash_id")
        cmd_show(db, int(args.args[0]))
    elif args.command == "export":
        if len(args.args) < 2:
            parser.error("export command requires crash_id and output_file")
        cmd_export(db, int(args.args[0]), args.args[1])
    elif args.command == "list":
        cmd_list(db, args.classification, args.limit)


if __name__ == "__main__":
    main()
