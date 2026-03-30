#!/usr/bin/env python3
"""
List commit frames and transaction boundaries in a WAL file.

Usage:
    ./wal_commits.py <wal-file>
    ./wal_commits.py <wal-file> --around 5133  # Focus on specific frame
    ./wal_commits.py <wal-file> --last 10      # Show last N commits
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.wal import WAL_HEADER_SIZE, iter_frames, parse_wal_header


def main():  # noqa: C901
    parser = argparse.ArgumentParser(description="List commit frames and transaction boundaries")
    parser.add_argument("path", help="WAL file or database file")
    parser.add_argument("--around", type=int, metavar="FRAME", help="Focus analysis around a specific frame")
    parser.add_argument("--last", type=int, metavar="N", default=10, help="Show last N commits (default: 10)")
    parser.add_argument("--all", action="store_true", help="Show all commits")
    args = parser.parse_args()

    # Determine WAL path
    if args.path.endswith("-wal"):
        wal_path = args.path
    else:
        wal_path = args.path + "-wal"

    if not os.path.exists(wal_path):
        print(f"Error: WAL file not found: {wal_path}", file=sys.stderr)
        sys.exit(1)

    # Parse header
    with open(wal_path, "rb") as f:
        header = parse_wal_header(f.read(WAL_HEADER_SIZE))

    # Collect all frames and commits
    all_frames = list(iter_frames(wal_path, header.page_size))
    commits = [(f.index + 1, f) for f in all_frames if f.header.is_commit]

    print(f"WAL: {wal_path}")
    print(f"Total frames: {len(all_frames)}, Commits: {len(commits)}")
    print()

    if args.around:
        # Focus on frames around a specific point
        target_frame = args.around

        # Find the commit at or after target
        target_commit_idx = None
        for idx, (frame_num, frame) in enumerate(commits):
            if frame_num >= target_frame:
                target_commit_idx = idx
                break

        if target_commit_idx is None:
            print(f"No commit found at or after frame {target_frame}")
            return

        # Show the transaction containing target frame
        print(f"=== Analysis around frame {target_frame} ===")
        print()

        # Previous commit (transaction start)
        if target_commit_idx > 0:
            prev_frame_num, prev_frame = commits[target_commit_idx - 1]
            print(f"Previous commit: Frame {prev_frame_num}")
            print(f"  Page: {prev_frame.header.page_num}")
            print(f"  DB size: {prev_frame.header.db_size} pages")
            print()
            tx_start = prev_frame_num + 1
        else:
            tx_start = 1

        # Current commit
        curr_frame_num, curr_frame = commits[target_commit_idx]
        print(f"Target commit: Frame {curr_frame_num}")
        print(f"  Page: {curr_frame.header.page_num}")
        print(f"  DB size: {curr_frame.header.db_size} pages")
        print()

        # Show transaction frames
        print(f"Transaction frames ({tx_start} to {curr_frame_num}):")
        print("-" * 50)
        for frame in all_frames[tx_start - 1 : curr_frame_num]:
            frame_num = frame.index + 1
            is_commit = "COMMIT" if frame.header.is_commit else ""
            marker = " <-- TARGET" if frame_num == target_frame else ""
            print(f"  Frame {frame_num:5d}: page {frame.header.page_num:5d}  {is_commit}{marker}")

    else:
        # Show last N commits or all
        if args.all:
            show_commits = commits
            print("All commits:")
        else:
            show_commits = commits[-args.last :]
            print(f"Last {len(show_commits)} commits:")

        print("-" * 60)
        prev_commit_frame = 0
        for frame_num, frame in show_commits:
            tx_size = frame_num - prev_commit_frame
            print(
                f"  Frame {frame_num:5d}: page {frame.header.page_num:3d}, "
                f"db_size={frame.header.db_size:3d}, tx_frames={tx_size}"
            )
            prev_commit_frame = frame_num


if __name__ == "__main__":
    main()
