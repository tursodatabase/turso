#!/usr/bin/env python3
"""
Show WAL file header and summary information.

Usage:
    ./wal_info.py <wal-file>
    ./wal_info.py <db-file>  # Will use <db-file>-wal
"""

import argparse
import os
import sys

# Add lib to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.wal import (
    WAL_HEADER_SIZE,
    get_frame_count,
    iter_frames,
    parse_wal_header,
)


def main():
    parser = argparse.ArgumentParser(description="Show WAL file header and summary information")
    parser.add_argument("path", help="WAL file or database file")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show additional details")
    args = parser.parse_args()

    # Determine WAL path
    if args.path.endswith("-wal"):
        wal_path = args.path
    else:
        wal_path = args.path + "-wal"

    if not os.path.exists(wal_path):
        print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
        sys.exit(1)

    # Read and parse header
    with open(wal_path, "rb") as f:
        header_data = f.read(WAL_HEADER_SIZE)

    header = parse_wal_header(header_data)
    frame_count = get_frame_count(wal_path, header.page_size)

    # Count commits
    commit_count = 0
    page_writes = {}  # page_num -> count

    for frame in iter_frames(wal_path, header.page_size):
        if frame.header.is_commit:
            commit_count += 1
        page_num = frame.header.page_num
        page_writes[page_num] = page_writes.get(page_num, 0) + 1

    # Print summary
    print("WAL Header")
    print("=" * 50)
    print(f"  File:            {wal_path}")
    print(f"  Size:            {os.path.getsize(wal_path):,} bytes")
    print(f"  Magic:           0x{header.magic:08x}", end="")
    if header.is_big_endian:
        print(" (big-endian checksums)")
    elif header.is_little_endian:
        print(" (little-endian checksums)")
    else:
        print(" (unknown)")
    print(f"  Version:         {header.version}")
    print(f"  Page size:       {header.page_size} bytes")
    print(f"  Checkpoint seq:  {header.checkpoint_seq}")
    print(f"  Salt:            0x{header.salt1:08x} 0x{header.salt2:08x}")
    print(f"  Checksum:        0x{header.checksum1:08x} 0x{header.checksum2:08x}")
    print()
    print("Summary")
    print("=" * 50)
    print(f"  Total frames:    {frame_count}")
    print(f"  Commit frames:   {commit_count}")
    print(f"  Unique pages:    {len(page_writes)}")

    if args.verbose and page_writes:
        print()
        print("Pages by write count (top 20)")
        print("-" * 30)
        sorted_pages = sorted(page_writes.items(), key=lambda x: -x[1])
        for page_num, count in sorted_pages[:20]:
            print(f"  Page {page_num:5d}: {count:5d} writes")


if __name__ == "__main__":
    main()
