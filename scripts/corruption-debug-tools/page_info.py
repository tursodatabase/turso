#!/usr/bin/env python3
"""
Show detailed information about a database page.

Usage:
    ./page_info.py <db-file> <page-num>
    ./page_info.py <db-file> <page-num> --frame 5127  # Show state at frame N
    ./page_info.py <db-file> <page-num> --cells       # Show cell details
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.page import (
    get_cell_pointers,
    get_interior_children,
    get_page_at_frame,
    get_page_from_db,
    parse_page_header,
)
from lib.record import get_index_keys, get_index_rowids, get_table_rowids
from lib.wal import get_page_size_from_wal


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="Show detailed information about a database page")
    parser.add_argument("db_path", help="Path to database file")
    parser.add_argument("page_num", type=int, help="Page number (1-indexed)")
    parser.add_argument("--frame", type=int, default=None, help="Show page state at this WAL frame (0 = DB file only)")
    parser.add_argument("--cells", action="store_true", help="Show cell pointer details")
    parser.add_argument("--keys", action="store_true", help="Show index keys (for index pages)")
    args = parser.parse_args()

    db_path = args.db_path
    wal_path = db_path + "-wal"
    page_num = args.page_num

    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Determine page size
    if os.path.exists(wal_path):
        page_size = get_page_size_from_wal(wal_path)
    else:
        page_size = 4096  # Default

    # Get page data
    if args.frame is not None:
        if not os.path.exists(wal_path):
            print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
            sys.exit(1)
        page_data = get_page_at_frame(db_path, wal_path, page_num, args.frame, page_size)
        source = f"after frame {args.frame}"
    else:
        if os.path.exists(wal_path):
            from lib.wal import get_frame_count

            total_frames = get_frame_count(wal_path, page_size)
            page_data = get_page_at_frame(db_path, wal_path, page_num, total_frames, page_size)
            source = f"after all {total_frames} frames"
        else:
            page_data = get_page_from_db(db_path, page_num, page_size)
            source = "from DB file"

    # Page 1 has 100-byte database header
    page_offset = 100 if page_num == 1 else 0

    # Parse header
    header = parse_page_header(page_data, page_offset)

    print(f"Page {page_num} ({source})")
    print("=" * 60)
    print(f"  Type:            0x{header.page_type:02x} ({header.type_name})")
    print(f"  Cell count:      {header.cell_count}")
    print(f"  Content start:   {header.cell_content_start}")
    print(f"  First freeblock: {header.first_freeblock}")
    print(f"  Fragmented:      {header.fragmented_bytes} bytes")

    if header.rightmost_ptr is not None:
        print(f"  Rightmost ptr:   {header.rightmost_ptr}")

    # For interior pages, show children
    if header.is_interior:
        children = get_interior_children(page_data, page_offset)
        print(f"\n  Child pages:     {children}")

    # Show rowids for leaf pages
    if header.is_leaf:
        if header.is_index:
            rowids = get_index_rowids(page_data, page_offset)
            print(f"\n  Index entries:   {len(rowids)} rowids")
            if rowids:
                sorted_rowids = sorted(rowids)
                if len(sorted_rowids) <= 20:
                    print(f"  Rowids:          {sorted_rowids}")
                else:
                    print(f"  Rowids (first 10): {sorted_rowids[:10]}")
                    print(f"  Rowids (last 10):  {sorted_rowids[-10:]}")
        else:  # Table
            rowids = get_table_rowids(page_data, page_offset)
            print(f"\n  Table rows:      {len(rowids)} rowids")
            if rowids:
                sorted_rowids = sorted(rowids)
                if len(sorted_rowids) <= 20:
                    print(f"  Rowids:          {sorted_rowids}")
                else:
                    print(f"  Rowids (first 10): {sorted_rowids[:10]}")
                    print(f"  Rowids (last 10):  {sorted_rowids[-10:]}")

    # Show cell pointers if requested
    if args.cells:
        pointers = get_cell_pointers(page_data, page_offset)
        print(f"\nCell Pointers ({len(pointers)}):")
        print("-" * 40)
        for i, ptr in enumerate(pointers):
            print(f"  [{i:3d}] offset {ptr}")

    # Show index keys if requested
    if args.keys and header.is_index and header.is_leaf:
        keys = get_index_keys(page_data, page_offset)
        print(f"\nIndex Keys ({len(keys)}):")
        print("-" * 40)
        for i, key in enumerate(keys[:50]):  # Limit to first 50
            key_str = ", ".join(repr(v) for v in key.key_values)
            print(f"  [{i:3d}] rowid={key.rowid}: {key_str}")
        if len(keys) > 50:
            print(f"  ... and {len(keys) - 50} more")


if __name__ == "__main__":
    main()
