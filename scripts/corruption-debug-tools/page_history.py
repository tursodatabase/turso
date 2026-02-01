#!/usr/bin/env python3
"""
Show all WAL writes to a specific page.

Usage:
    ./page_history.py <db-file> <page-num>
    ./page_history.py <db-file> <page-num> --track-key "some_key"
    ./page_history.py <db-file> <page-num> --track-rowid 661
"""

import argparse
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.page import get_page_from_db, parse_page_header
from lib.record import get_index_rowids
from lib.wal import (
    FRAME_HEADER_SIZE,
    WAL_HEADER_SIZE,
    get_page_size_from_wal,
)


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Show all WAL writes to a specific page")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("page_num", type=int, help="Page number (1-indexed)")
    parser.add_argument("--track-key", type=str, metavar="KEY", help="Track presence of a key (searches as bytes)")
    parser.add_argument("--track-rowid", type=int, metavar="ROWID", help="Track presence of a rowid (for index pages)")
    parser.add_argument("--limit", type=int, default=None, help="Limit to first N writes")
    args = parser.parse_args()

    db_path = args.db_path
    wal_path = db_path + "-wal"
    page_num = args.page_num

    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(wal_path):
        print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
        sys.exit(1)

    page_size = get_page_size_from_wal(wal_path)
    frame_size = FRAME_HEADER_SIZE + page_size
    page_offset = 100 if page_num == 1 else 0

    # Track key as bytes
    track_key = args.track_key.encode() if args.track_key else None

    # Get initial state from DB file
    db_page = get_page_from_db(db_path, page_num, page_size)
    db_header = parse_page_header(db_page, page_offset)

    print(f"Page {page_num} History")
    print("=" * 70)
    print(
        f"DB file state: {db_header.type_name}, {db_header.cell_count} cells, "
        f"content_start={db_header.cell_content_start}"
    )

    if track_key:
        has_key = track_key in db_page
        print(f"  Key '{args.track_key}': {'present' if has_key else 'absent'}")

    if args.track_rowid is not None and db_header.is_index and db_header.is_leaf:
        rowids = get_index_rowids(db_page, page_offset)
        has_rowid = args.track_rowid in rowids
        print(f"  Rowid {args.track_rowid}: {'present' if has_rowid else 'absent'}")

    print()
    print("WAL writes:")
    print("-" * 70)

    # Read WAL
    with open(wal_path, "rb") as f:
        wal_data = f.read()

    num_frames = (len(wal_data) - WAL_HEADER_SIZE) // frame_size
    writes_found = 0

    # Track previous state for change detection
    prev_key_present = track_key in db_page if track_key else None
    prev_rowid_present = None
    if args.track_rowid is not None and db_header.is_index:
        prev_rowid_present = args.track_rowid in get_index_rowids(db_page, page_offset)

    for frame_idx in range(num_frames):
        offset = WAL_HEADER_SIZE + frame_idx * frame_size
        frame_page_num = struct.unpack(">I", wal_data[offset : offset + 4])[0]
        db_size = struct.unpack(">I", wal_data[offset + 4 : offset + 8])[0]

        if frame_page_num != page_num:
            continue

        page_data = wal_data[offset + FRAME_HEADER_SIZE : offset + frame_size]
        header = parse_page_header(page_data, page_offset)

        frame_num = frame_idx + 1
        is_commit = "COMMIT" if db_size > 0 else ""

        line = (
            f"  Frame {frame_num:5d}: {header.cell_count:3d} cells, "
            f"content_start={header.cell_content_start:4d}  {is_commit}"
        )

        # Track key changes
        if track_key:
            has_key = track_key in page_data
            if has_key != prev_key_present:
                if has_key:
                    line += f"  KEY '{args.track_key}' APPEARS"
                else:
                    line += f"  KEY '{args.track_key}' DISAPPEARS"
            prev_key_present = has_key

        # Track rowid changes
        if args.track_rowid is not None and header.is_index and header.is_leaf:
            rowids = get_index_rowids(page_data, page_offset)
            has_rowid = args.track_rowid in rowids
            if has_rowid != prev_rowid_present:
                if has_rowid:
                    line += f"  ROWID {args.track_rowid} APPEARS"
                else:
                    line += f"  ROWID {args.track_rowid} DISAPPEARS"
            prev_rowid_present = has_rowid

        print(line)
        writes_found += 1

        if args.limit and writes_found >= args.limit:
            print(f"  ... (limited to {args.limit} writes)")
            break

    print()
    print(f"Total writes to page {page_num}: {writes_found}")


if __name__ == "__main__":
    main()
