#!/usr/bin/env python3
"""
Binary search to find the earliest WAL frame that introduces corruption.

This script performs a binary search through WAL frames, running SQLite's
integrity check at each step to find the exact frame where corruption begins.

Usage:
    ./find_corrupt_frame.py <db-file>
    ./find_corrupt_frame.py <db-file> --verbose
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.wal import create_truncated_wal, get_frame_count, get_page_size_from_wal


def check_integrity(db_path: str, wal_path: str, num_frames: int, page_size: int, verbose: bool = False) -> bool:
    """Check integrity of DB with truncated WAL. Returns True if OK."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_db = os.path.join(tmpdir, "test.db")
        tmp_wal = os.path.join(tmpdir, "test.db-wal")

        # Copy the database file
        shutil.copy2(db_path, tmp_db)

        # Create truncated WAL
        if num_frames > 0:
            create_truncated_wal(wal_path, num_frames, tmp_wal, page_size)

        # Run integrity check using sqlite3 CLI
        try:
            result = subprocess.run(
                ["sqlite3", tmp_db, "PRAGMA integrity_check;"], capture_output=True, text=True, timeout=60
            )
            output = result.stdout.strip()

            if verbose and output != "ok":
                print(f"    Integrity check output: {output[:200]}")

            return output == "ok"

        except subprocess.TimeoutExpired:
            print("  Warning: integrity check timed out", file=sys.stderr)
            return False
        except FileNotFoundError:
            print("Error: sqlite3 CLI not found. Please install SQLite.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"  Error checking integrity: {e}", file=sys.stderr)
            return False


def binary_search_corrupt_frame(db_path: str, wal_path: str, verbose: bool = False) -> int:
    """
    Find the earliest frame that causes corruption.

    Returns:
        Frame number (1-indexed) that introduces corruption, or -1 if not found
    """
    page_size = get_page_size_from_wal(wal_path)
    total_frames = get_frame_count(wal_path, page_size)

    print(f"Database: {db_path}")
    print(f"WAL file: {wal_path}")
    print(f"WAL has {total_frames} frames")
    print()

    # First check: is 0 frames OK?
    print("Checking with 0 frames (DB file only)...", end=" ", flush=True)
    if not check_integrity(db_path, wal_path, 0, page_size, verbose):
        print("CORRUPT")
        print("\nDatabase file is already corrupt without any WAL frames!")
        return -1
    print("OK")

    # Check if full WAL is actually corrupt
    print(f"Checking with all {total_frames} frames...", end=" ", flush=True)
    if check_integrity(db_path, wal_path, total_frames, page_size, verbose):
        print("OK")
        print("\nFull WAL passes integrity check - no corruption found!")
        return -1
    print("CORRUPT")
    print()

    # Binary search for first corrupting frame
    low = 0  # Last known good
    high = total_frames  # Known bad

    iterations = 0
    while high - low > 1:
        mid = (low + high) // 2
        iterations += 1
        print(f"[{iterations}] Testing {mid} frames (range {low}-{high})...", end=" ", flush=True)

        if check_integrity(db_path, wal_path, mid, page_size, verbose):
            print("OK")
            low = mid
        else:
            print("CORRUPT")
            high = mid

    print()
    print("=" * 60)
    print(f"Found: Frame {high} (0-indexed: {high - 1}) introduces corruption")
    print(f"Last good state: {low} frames")
    print(f"Binary search completed in {iterations} iterations")

    # Calculate byte offset of the corrupting frame
    from lib.wal import FRAME_HEADER_SIZE, WAL_HEADER_SIZE

    frame_offset = WAL_HEADER_SIZE + (high - 1) * (FRAME_HEADER_SIZE + page_size)
    print(f"Corrupting frame byte offset: {frame_offset} (0x{frame_offset:x})")

    return high


def main():
    parser = argparse.ArgumentParser(description="Binary search for the first WAL frame that introduces corruption")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show integrity check output on failures")
    args = parser.parse_args()

    db_path = args.db_path
    wal_path = db_path + "-wal"

    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(wal_path):
        print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
        sys.exit(1)

    corrupt_frame = binary_search_corrupt_frame(db_path, wal_path, args.verbose)
    sys.exit(0 if corrupt_frame > 0 else 1)


if __name__ == "__main__":
    main()
