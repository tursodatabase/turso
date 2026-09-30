#!/usr/bin/env python3
"""
Compare page states between two WAL frames.

Usage:
    ./page_diff.py <db-file> <page-num> --before 5127 --after 5133
    ./page_diff.py <db-file> <page-num> --before 5127 --after 5133 --rowids
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.diff import compare_cell_pointers, compare_pages, compare_rowids
from lib.page import get_page_at_frame, parse_page_header
from lib.record import get_index_keys
from lib.wal import get_page_size_from_wal


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Compare page states between two WAL frames")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("page_num", type=int, help="Page number (1-indexed)")
    parser.add_argument("--before", type=int, required=True, help="Frame number for 'before' state")
    parser.add_argument("--after", type=int, required=True, help="Frame number for 'after' state")
    parser.add_argument("--rowids", action="store_true", help="Compare index rowids")
    parser.add_argument("--keys", action="store_true", help="Show lost/gained keys (for index pages)")
    parser.add_argument("--hex", action="store_true", help="Show hex diff of changed regions")
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
    page_offset = 100 if page_num == 1 else 0

    # Get page states
    before = get_page_at_frame(db_path, wal_path, page_num, args.before, page_size)
    after = get_page_at_frame(db_path, wal_path, page_num, args.after, page_size)

    # Parse headers
    before_header = parse_page_header(before, page_offset)
    parse_page_header(after, page_offset)

    print(f"Page {page_num} Diff: Frame {args.before} -> Frame {args.after}")
    print("=" * 60)

    # Basic comparison
    diff = compare_pages(before, after)

    if diff.identical:
        print("Pages are identical - no changes")
        return

    print(f"Changed bytes:     {diff.bytes_changed} / {diff.total_bytes}")
    print(f"Cell count:        {diff.cell_count_before} -> {diff.cell_count_after}", end="")
    if diff.cell_count_before != diff.cell_count_after:
        change = diff.cell_count_after - diff.cell_count_before
        print(f" ({'+' if change > 0 else ''}{change})")
    else:
        print()

    print(f"Content start:     {diff.content_start_before} -> {diff.content_start_after}", end="")
    if diff.content_start_before != diff.content_start_after:
        change = diff.content_start_after - diff.content_start_before
        print(f" ({'+' if change > 0 else ''}{change})")
    else:
        print()

    # Cell pointer comparison
    ptr_diff = compare_cell_pointers(before, after, page_offset)
    if ptr_diff.pointers_lost or ptr_diff.pointers_gained:
        print("\nCell Pointers:")
        print(f"  Lost:            {sorted(ptr_diff.pointers_lost) or 'none'}")
        print(f"  Gained:          {sorted(ptr_diff.pointers_gained) or 'none'}")
        if ptr_diff.first_diff_index >= 0:
            print(f"  First diff at:   index {ptr_diff.first_diff_index}")

    # Rowid comparison for index pages
    if args.rowids and before_header.is_index and before_header.is_leaf:
        rowid_diff = compare_rowids(before, after, page_offset)
        print("\nRowids:")
        print(f"  Lost:            {sorted(rowid_diff.rowids_lost) or 'none'}")
        print(f"  Gained:          {sorted(rowid_diff.rowids_gained) or 'none'}")
        print(f"  Unchanged:       {len(rowid_diff.rowids_unchanged)} rowids")

    # Key comparison for index pages
    if args.keys and before_header.is_index and before_header.is_leaf:
        before_keys = get_index_keys(before, page_offset)
        after_keys = get_index_keys(after, page_offset)

        before_by_rowid = {k.rowid: k for k in before_keys}
        after_by_rowid = {k.rowid: k for k in after_keys}

        lost_rowids = set(before_by_rowid.keys()) - set(after_by_rowid.keys())
        gained_rowids = set(after_by_rowid.keys()) - set(before_by_rowid.keys())

        if lost_rowids:
            print("\nLost Keys:")
            for rowid in sorted(lost_rowids)[:20]:
                key = before_by_rowid[rowid]
                key_str = ", ".join(repr(v) for v in key.key_values)
                print(f"  rowid {rowid}: {key_str}")
            if len(lost_rowids) > 20:
                print(f"  ... and {len(lost_rowids) - 20} more")

        if gained_rowids:
            print("\nGained Keys:")
            for rowid in sorted(gained_rowids)[:20]:
                key = after_by_rowid[rowid]
                key_str = ", ".join(repr(v) for v in key.key_values)
                print(f"  rowid {rowid}: {key_str}")
            if len(gained_rowids) > 20:
                print(f"  ... and {len(gained_rowids) - 20} more")

    # Hex diff
    if args.hex:
        print("\nHex Diff (first 10 changed regions):")
        print("-" * 60)
        regions = []
        i = 0
        while i < len(before) and i < len(after):
            if before[i] != after[i]:
                start = i
                while i < len(before) and i < len(after) and before[i] != after[i]:
                    i += 1
                regions.append((start, i))
            else:
                i += 1

        for start, end in regions[:10]:
            length = end - start
            print(f"  Offset {start}-{end} ({length} bytes):")
            print(f"    Before: {before[start : min(end, start + 32)].hex()}")
            print(f"    After:  {after[start : min(end, start + 32)].hex()}")

        if len(regions) > 10:
            print(f"  ... and {len(regions) - 10} more regions")


if __name__ == "__main__":
    main()
