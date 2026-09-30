# Tursodb Corruption Debug Tools

A collection of Python scripts for analyzing WAL files and debugging database corruption issues.

## Prerequisites

- Python 3.8+
- SQLite CLI (`sqlite3`) for integrity checks

## Scripts

### WAL Analysis

#### `wal_info.py`
Show WAL file header and summary information.

```bash
./wal_info.py my_corrupted_database.db
./wal_info.py my_corrupted_database.db-wal
./wal_info.py my_corrupted_database.db -v  # Verbose: show pages by write count
```

Example output:
```
WAL Header
==================================================
  File:            my_corrupted_database.db-wal
  Size:            22,598,232 bytes
  Magic:           0x377f0682 (big-endian checksums)
  Page size:       4096 bytes
  Checkpoint seq:  0

Summary
==================================================
  Total frames:    5485
  Commit frames:   1459
  Unique pages:    67
```

#### `wal_commits.py`
List commit frames and transaction boundaries.

```bash
./wal_commits.py my_corrupted_database.db --last 20     # Last 20 commits
./wal_commits.py my_corrupted_database.db --around 5133 # Focus on specific frame
./wal_commits.py my_corrupted_database.db --all         # All commits
```

Example output (`--around 5137`):
```
=== Analysis around frame 5137 ===

Previous commit: Frame 5131
  Page: 64
  DB size: 64 pages

Target commit: Frame 5137
  Page: 60
  DB size: 64 pages

Transaction frames (5132 to 5137):
--------------------------------------------------
  Frame  5132: page     7
  Frame  5133: page    26
  Frame  5134: page    27
  Frame  5135: page    43
  Frame  5136: page    56
  Frame  5137: page    60  COMMIT <-- TARGET
```

### Corruption Detection

#### `find_corrupt_frame.py`
Binary search to find the earliest WAL frame that introduces corruption.

```bash
./find_corrupt_frame.py my_corrupted_database.db
./find_corrupt_frame.py my_corrupted_database.db -v  # Show integrity check output
```

Example output:
```
WAL has 5846 frames
Checking with 0 frames (DB file only)... OK
Checking with all 5846 frames... CORRUPT

[1] Testing 2923 frames (range 0-5846)... OK
[2] Testing 4384 frames (range 2923-5846)... OK
...
[13] Testing 5133 frames (range 5132-5134)... CORRUPT

Found: Frame 5133 (0-indexed: 5132) introduces corruption
Last good state: 5132 frames
```

### Page Analysis

#### `page_info.py`
Show detailed information about a database page.

```bash
./page_info.py my_corrupted_database.db 26                  # Current state
./page_info.py my_corrupted_database.db 26 --frame 5127     # State at frame 5127
./page_info.py my_corrupted_database.db 26 --cells          # Show cell pointers
./page_info.py my_corrupted_database.db 26 --keys           # Show index keys (for index pages)
```

Example output:
```
Page 26 (after frame 5127)
============================================================
  Type:            0x0a (leaf index)
  Cell count:      185
  Content start:   559
  First freeblock: 0
  Fragmented:      0 bytes

  Index entries:   185 rowids
  Rowids (first 10): [4, 17, 23, 45, 67, 89, 102, 115, 128, 141]
  Rowids (last 10):  [601, 614, 627, 640, 653, 661, 666, 679, 692, 705]
```

#### `page_diff.py`
Compare page states between two WAL frames.

```bash
./page_diff.py my_corrupted_database.db 26 --before 5127 --after 5133
./page_diff.py my_corrupted_database.db 26 --before 5127 --after 5133 --rowids  # Compare rowids
./page_diff.py my_corrupted_database.db 26 --before 5127 --after 5133 --keys    # Show key changes
./page_diff.py my_corrupted_database.db 26 --before 5127 --after 5133 --hex     # Show hex diff
```

Example output (`--rowids`):
```
Page 26 Diff: Frame 5131 -> Frame 5133
============================================================
Changed bytes:     1609 / 4096
Cell count:        185 -> 185
Content start:     559 -> 558 (-1)

Rowids:
  Lost:            [661]
  Gained:          [365]
  Unchanged:       184 rowids
```

#### `page_history.py`
Show all WAL writes to a specific page.

```bash
./page_history.py my_corrupted_database.db 26
./page_history.py my_corrupted_database.db 26 --track-key "dark_wall_716"  # Track key presence
./page_history.py my_corrupted_database.db 26 --track-rowid 661            # Track rowid
./page_history.py my_corrupted_database.db 26 --limit 50                   # Limit output
```

Example output (`--track-rowid 661`):
```
Page 26 History
======================================================================
DB file state: leaf index, 192 cells, content_start=428
  Rowid 661: absent

WAL writes:
----------------------------------------------------------------------
  Frame  5098: 193 cells, content_start=409
  Frame  5106: 185 cells, content_start=559  ROWID 661 APPEARS
  Frame  5133: 185 cells, content_start=558  ROWID 661 DISAPPEARS
  ...
```

### Rowid Tracking

#### `track_rowid.py`
Track when a rowid appears/disappears across pages.

```bash
./track_rowid.py my_corrupted_database.db 661 --pages 26,42     # Track in specific pages
./track_rowid.py my_corrupted_database.db 661 --all-index       # Track in all index pages
./track_rowid.py my_corrupted_database.db 661 --all-table       # Track in all table pages
```

Example output:
```
Tracking rowid 661
======================================================================
Tracking pages: [26, 42]

WAL changes:
----------------------------------------------------------------------
  Frame  5106: Page  26 - rowid 661 APPEARS
  Frame  5108: Page  42 - rowid 661 APPEARS
  Frame  5133: Page  26 - rowid 661 DISAPPEARS
  Frame  5145: Page  42 - rowid 661 DISAPPEARS

Timeline:
----------------------------------------------------------------------
  Frame 5106: APPEARS in page 26
  Frame 5108: APPEARS in page 42
  Frame 5133: DISAPPEARS in page 26
  Frame 5145: DISAPPEARS in page 42
```

### Stale Page Verification

#### `verify_stale.py`
Verify if corruption looks like it was caused by reading a stale page.

```bash
# Check if frame 5133 looks like frame 5098 + insertion of rowid 663
./verify_stale.py my_corrupted_database.db 26 --stale-frame 5098 --corrupt-frame 5133 --gained-rowid 663

# Compare against known good state
./verify_stale.py my_corrupted_database.db 26 --stale-frame 5098 --corrupt-frame 5133 --good-frame 5106
```

Example output:
```
Stale Page Analysis for Page 26
======================================================================

Stale source (frame 5105):
  Type:          leaf index
  Cell count:    193

Corrupt state (frame 5133):
  Type:          leaf index
  Cell count:    185

Rowid Analysis:
  Stale rowids:    193
  Corrupt rowids:  185

--- Stale Page Hypothesis ---
  Common rowids:     185
  Only in stale:     8 [324, 344, 448, 449, 588, 613, 640, 653]
  Only in corrupt:   0 []

  Note: Stale has rowids not in corrupt - suggests B-tree rebalance

--- Byte Comparison ---
  Stale vs Corrupt: 1911 bytes differ (53.3% similar)
  Good vs Corrupt:  1609 bytes differ (60.7% similar)
```

## Library Modules

The scripts use a shared library in `lib/`:

- `lib/wal.py` - WAL parsing (headers, frames, commits)
- `lib/page.py` - Page reading and header parsing
- `lib/record.py` - SQLite record format (varints, serial types)
- `lib/diff.py` - Comparison utilities

### Using the Library

```python
import sys
sys.path.insert(0, "/path/to/corruption-debug-tools")

from lib.wal import iter_frames, get_frame_count
from lib.page import get_page_at_frame, parse_page_header
from lib.record import get_index_rowids, get_index_keys
from lib.diff import compare_pages, compare_rowids

# Get page state at a specific frame
page = get_page_at_frame("db.db", "db.db-wal", page_num=26, up_to_frame=5127)

# Parse the page header
header = parse_page_header(page)
print(f"Type: {header.type_name}, Cells: {header.cell_count}")

# Get rowids from an index page
rowids = get_index_rowids(page)
print(f"Rowids: {rowids}")
```

## Typical Investigation Workflow

1. **Find the corrupt frame**:
   ```bash
   ./find_corrupt_frame.py my_corrupted_database.db
   # Output: Frame 5133 introduces corruption
   ```

2. **Analyze the corrupt transaction**:
   ```bash
   ./wal_commits.py my_corrupted_database.db --around 5133
   # Shows frames 5128-5133 in the transaction
   ```

3. **Compare page states**:
   ```bash
   ./page_diff.py my_corrupted_database.db 26 --before 5127 --after 5133 --rowids --keys
   # Shows: Lost rowid 661, Gained rowid 663
   ```

4. **Track the lost rowid**:
   ```bash
   ./track_rowid.py my_corrupted_database.db 661 --pages 26,42
   # Shows when rowid 661 appeared and disappeared
   ```

5. **Find the stale source**:
   ```bash
   ./page_history.py my_corrupted_database.db 26 --track-rowid 661
   # Find frames where 661 was present vs absent
   ```

6. **Verify stale page hypothesis**:
   ```bash
   ./verify_stale.py my_corrupted_database.db 26 --stale-frame 5098 --corrupt-frame 5133 --gained-rowid 663
   # Confirms if corruption matches stale + insertion pattern
   ```