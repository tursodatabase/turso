#!/usr/bin/env python3
"""
Verify if a page state looks like it was created from a stale page read.

This script checks if a "corrupt" page state could be explained by reading
a stale version of the page and then performing an insertion/update.

Usage:
    ./verify_stale.py <db-file> <page-num> --stale-frame 5098 --corrupt-frame 5133
    ./verify_stale.py <db-file> <page-num> --stale-frame 5107 --corrupt-frame 5131 --gained-rowid 663
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.diff import compare_pages
from lib.page import get_page_at_frame, parse_page_header
from lib.record import get_index_rowids
from lib.wal import get_page_size_from_wal


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Verify if corruption looks like a stale page read")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("page_num", type=int, help="Page number (1-indexed)")
    parser.add_argument("--stale-frame", type=int, required=True, help="Frame that might be the stale source")
    parser.add_argument("--corrupt-frame", type=int, required=True, help="Frame containing the corrupt page")
    parser.add_argument(
        "--good-frame", type=int, default=None, help="Frame containing the expected good state (optional)"
    )
    parser.add_argument("--gained-rowid", type=int, default=None, help="Rowid that was gained in the corrupt state")
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
    stale_page = get_page_at_frame(db_path, wal_path, page_num, args.stale_frame, page_size)
    corrupt_page = get_page_at_frame(db_path, wal_path, page_num, args.corrupt_frame, page_size)

    good_page = None
    if args.good_frame:
        good_page = get_page_at_frame(db_path, wal_path, page_num, args.good_frame, page_size)

    # Parse headers
    stale_header = parse_page_header(stale_page, page_offset)
    corrupt_header = parse_page_header(corrupt_page, page_offset)

    print(f"Stale Page Analysis for Page {page_num}")
    print("=" * 70)

    print(f"\nStale source (frame {args.stale_frame}):")
    print(f"  Type:          {stale_header.type_name}")
    print(f"  Cell count:    {stale_header.cell_count}")
    print(f"  Content start: {stale_header.cell_content_start}")

    print(f"\nCorrupt state (frame {args.corrupt_frame}):")
    print(f"  Type:          {corrupt_header.type_name}")
    print(f"  Cell count:    {corrupt_header.cell_count}")
    print(f"  Content start: {corrupt_header.cell_content_start}")

    if good_page:
        good_header = parse_page_header(good_page, page_offset)
        print(f"\nExpected good state (frame {args.good_frame}):")
        print(f"  Type:          {good_header.type_name}")
        print(f"  Cell count:    {good_header.cell_count}")
        print(f"  Content start: {good_header.cell_content_start}")

    # For index pages, compare rowids
    if stale_header.is_index and stale_header.is_leaf:
        stale_rowids = get_index_rowids(stale_page, page_offset)
        corrupt_rowids = get_index_rowids(corrupt_page, page_offset)

        print("\nRowid Analysis:")
        print(f"  Stale rowids:    {len(stale_rowids)}")
        print(f"  Corrupt rowids:  {len(corrupt_rowids)}")

        if good_page:
            good_rowids = get_index_rowids(good_page, page_offset)
            print(f"  Good rowids:     {len(good_rowids)}")

            lost_from_good = good_rowids - corrupt_rowids
            gained_vs_good = corrupt_rowids - good_rowids

            print(f"\n  Lost vs good:    {sorted(lost_from_good)}")
            print(f"  Gained vs good:  {sorted(gained_vs_good)}")

        # Check stale page hypothesis
        print("\n--- Stale Page Hypothesis ---")

        if args.gained_rowid:
            # Expected: corrupt = stale + gained_rowid
            expected_if_stale = stale_rowids | {args.gained_rowid}

            print(f"  If corrupt = stale + rowid {args.gained_rowid}:")
            print(f"    Expected rowids: {len(expected_if_stale)}")
            print(f"    Actual rowids:   {len(corrupt_rowids)}")

            if corrupt_rowids == expected_if_stale:
                print("\n  >>> STALE PAGE HYPOTHESIS CONFIRMED <<<")
                print(f"  Corrupt state exactly matches: stale + rowid {args.gained_rowid}")
            else:
                missing = expected_if_stale - corrupt_rowids
                extra = corrupt_rowids - expected_if_stale
                print(f"\n    Missing: {sorted(missing)}")
                print(f"    Extra:   {sorted(extra)}")

                # Check if it's close
                overlap = len(corrupt_rowids & expected_if_stale)
                total = len(corrupt_rowids | expected_if_stale)
                similarity = (overlap / total * 100) if total > 0 else 0
                print(f"    Similarity: {similarity:.1f}%")

        else:
            # General comparison
            common = stale_rowids & corrupt_rowids
            only_stale = stale_rowids - corrupt_rowids
            only_corrupt = corrupt_rowids - stale_rowids

            print(f"  Common rowids:     {len(common)}")
            print(f"  Only in stale:     {len(only_stale)} {sorted(only_stale)[:10]}")
            print(f"  Only in corrupt:   {len(only_corrupt)} {sorted(only_corrupt)[:10]}")

            # Check if corrupt looks like stale + insertions
            if only_stale:
                print("\n  Note: Stale has rowids not in corrupt - suggests B-tree rebalance")
            if len(only_corrupt) <= 5:
                print(f"\n  Corrupt looks like stale + {only_corrupt} (possible insertions)")

    # Byte-level comparison
    print("\n--- Byte Comparison ---")

    stale_diff = compare_pages(stale_page, corrupt_page)
    stale_pct = 100 - stale_diff.bytes_changed / stale_diff.total_bytes * 100
    print(f"  Stale vs Corrupt: {stale_diff.bytes_changed} bytes differ ({stale_pct:.1f}% similar)")

    if good_page:
        good_diff = compare_pages(good_page, corrupt_page)
        good_pct = 100 - good_diff.bytes_changed / good_diff.total_bytes * 100
        print(f"  Good vs Corrupt:  {good_diff.bytes_changed} bytes differ ({good_pct:.1f}% similar)")

        stale_good_diff = compare_pages(stale_page, good_page)
        sg_pct = 100 - stale_good_diff.bytes_changed / stale_good_diff.total_bytes * 100
        print(f"  Stale vs Good:    {stale_good_diff.bytes_changed} bytes differ ({sg_pct:.1f}% similar)")


if __name__ == "__main__":
    main()
