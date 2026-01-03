#!/usr/bin/env python3
"""
Track when a rowid appears/disappears in index or table pages.

Usage:
    ./track_rowid.py <db-file> <rowid> --pages 26,42  # Track in specific pages
    ./track_rowid.py <db-file> <rowid> --all-index    # Track in all index pages
"""

import argparse
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.page import get_page_from_db, parse_page_header
from lib.record import get_index_rowids, get_table_rowids
from lib.wal import (
    FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
    get_page_size_from_wal,
)


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Track when a rowid appears/disappears in pages")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("rowid", type=int, help="Rowid to track")
    parser.add_argument("--pages", type=str, help="Comma-separated list of page numbers to track")
    parser.add_argument("--all-index", action="store_true", help="Track in all index leaf pages")
    parser.add_argument("--all-table", action="store_true", help="Track in all table leaf pages")
    args = parser.parse_args()

    db_path = args.db_path
    wal_path = db_path + "-wal"
    target_rowid = args.rowid

    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(wal_path):
        print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
        sys.exit(1)

    page_size = get_page_size_from_wal(wal_path)
    frame_size = FRAME_HEADER_SIZE + page_size

    # Determine which pages to track
    if args.pages:
        target_pages = set(int(p.strip()) for p in args.pages.split(","))
    elif args.all_index or args.all_table:
        target_pages = None  # Will be determined dynamically
    else:
        print("Error: Specify --pages, --all-index, or --all-table", file=sys.stderr)
        sys.exit(1)

    print(f"Tracking rowid {target_rowid}")
    print("=" * 70)

    # Read WAL
    with open(wal_path, "rb") as f:
        wal_data = f.read()

    num_frames = (len(wal_data) - WAL_HEADER_SIZE) // frame_size

    # Track state per page: page_num -> (has_rowid, last_frame)
    page_states = {}

    # Initialize from DB file
    db_size = os.path.getsize(db_path)
    db_pages = db_size // page_size

    if target_pages is None:
        # Scan DB file for index/table leaf pages
        target_pages = set()
        for page_num in range(1, db_pages + 1):
            page_data = get_page_from_db(db_path, page_num, page_size)
            page_offset = 100 if page_num == 1 else 0
            header = parse_page_header(page_data, page_offset)

            if args.all_index and header.page_type == 0x0A:
                target_pages.add(page_num)
            elif args.all_table and header.page_type == 0x0D:
                target_pages.add(page_num)

    print(f"Tracking pages: {sorted(target_pages)}")
    print()

    # Initialize state from DB file
    for page_num in target_pages:
        page_data = get_page_from_db(db_path, page_num, page_size)
        page_offset = 100 if page_num == 1 else 0
        header = parse_page_header(page_data, page_offset)

        if header.page_type == 0x0A:  # Index leaf
            rowids = get_index_rowids(page_data, page_offset)
            has_rowid = target_rowid in rowids
        elif header.page_type == 0x0D:  # Table leaf
            rowids = get_table_rowids(page_data, page_offset)
            has_rowid = target_rowid in rowids
        else:
            has_rowid = False

        page_states[page_num] = (has_rowid, "DB")

        if has_rowid:
            print(f"  Page {page_num:3d} (DB file): rowid {target_rowid} PRESENT")

    print()
    print("WAL changes:")
    print("-" * 70)

    # Scan WAL for changes
    events = []

    for frame_idx in range(num_frames):
        offset = WAL_HEADER_SIZE + frame_idx * frame_size
        frame_page_num = struct.unpack(">I", wal_data[offset : offset + 4])[0]
        db_size_field = struct.unpack(">I", wal_data[offset + 4 : offset + 8])[0]

        if frame_page_num not in target_pages:
            continue

        page_data = wal_data[offset + FRAME_HEADER_SIZE : offset + frame_size]
        page_offset = 100 if frame_page_num == 1 else 0
        header = parse_page_header(page_data, page_offset)

        if header.page_type == 0x0A:  # Index leaf
            rowids = get_index_rowids(page_data, page_offset)
            has_rowid = target_rowid in rowids
        elif header.page_type == 0x0D:  # Table leaf
            rowids = get_table_rowids(page_data, page_offset)
            has_rowid = target_rowid in rowids
        else:
            continue

        frame_num = frame_idx + 1
        prev_has_rowid, _ = page_states.get(frame_page_num, (False, None))

        if has_rowid != prev_has_rowid:
            is_commit = "COMMIT" if db_size_field > 0 else ""
            if has_rowid:
                action = "APPEARS"
            else:
                action = "DISAPPEARS"

            events.append((frame_num, frame_page_num, action, is_commit))
            print(f"  Frame {frame_num:5d}: Page {frame_page_num:3d} - rowid {target_rowid} {action}  {is_commit}")

        page_states[frame_page_num] = (has_rowid, frame_num)

    print()
    print("Summary:")
    print("-" * 70)

    for page_num in sorted(target_pages):
        has_rowid, last_source = page_states.get(page_num, (False, "DB"))
        status = "present" if has_rowid else "absent"
        print(f"  Page {page_num:3d}: rowid {target_rowid} is {status} (last updated: {last_source})")

    # Show timeline if there were changes
    if events:
        print()
        print("Timeline:")
        print("-" * 70)
        for frame_num, page_num, action, is_commit in events:
            print(f"  Frame {frame_num}: {action} in page {page_num}  {is_commit}")


if __name__ == "__main__":
    main()
