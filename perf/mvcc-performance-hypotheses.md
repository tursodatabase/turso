# MVCC performance hypotheses

Working notes for instruction-count profiling of Turso's MVCC layer. Status
values are `hypothesis`, `measuring`, `confirmed`, `fixed`, `rejected`, and
`deferred`.

The primary metric is deterministic instructions per operation from
`scripts/mvcc-icount-callgrind.sh`. The script runs the same workload at two
iteration counts under Callgrind in a Linux ARM64 container and subtracts the
totals, so database setup is excluded. Criterion wall time and correctness tests
are secondary checks.

## Workloads

| Scenario | MVCC path |
|---|---|
| `point_read` | Primary-key seek and version visibility |
| `index_read` | Secondary-index seek, visibility, and table lookup |
| `scan_128` | Cursor merge over a fixed table |
| `point_update_rollback` | Update version creation and rollback cleanup |
| `insert_rollback` | Table/index version creation and rollback cleanup |
| `point_update_commit` | Update, conflict validation, logical log, and commit |
| `insert_commit` | Basic prepared autocommit insert and commit |

## H1. Establish instruction baselines and measured hot functions — `fixed`

**Measure:** Run every scenario with 200 and 2,200 iterations. The original
20/220 point-read window included one delayed allocator-reclamation event and
reported 37,262 instructions, while 5/25 reported 19,618. The longer window
includes repeated reclamation cycles and reports 20,000 instructions.

**Decision rule:** A code change needs a named Callgrind hot function, a clear
reason it does unnecessary work, a lower count in the identical workload, and
correctness coverage. Source complexity alone is not performance evidence.

Baseline at `3c1a304be` plus the measurement harness:

| Scenario | Instructions/operation | Leading measured work |
|---|---:|---|
| `point_read` | 20,000 | memcpy, SkipMap pin/search |
| `index_read` | 54,665 | UTF-8 validation, index comparison, allocation |
| `scan_128` | 329,292 | SkipMap pinning, cursor advance, row decode |
| `point_update_rollback` | 504,506 | version visibility and SkipMap lookup |
| `insert_rollback` | 345,661 | version visibility and rollback lookup |
| `point_update_commit` | 46,513 | memcpy, SkipMap pin/search, commit state machine |
| `insert_commit` | 156,555 | index-key comparison, allocation, commit state machine |

## H2. Transaction rollback retains every aborted version until GC — `fixed`

**Where:** `MvStore::rollback_tx_inner` changed versions created by the aborted
transaction to `(begin=None, end=None)`. GC eventually removed them, but before
the 16,384-version trigger every later operation on the same key scanned all of
them. The rollback workloads showed the resulting history-dependent cost in
`RowVersion::is_visible_to`.

**Fix:** Remove versions created by the aborted transaction while holding the
version-chain write lock. Restore the `end` field of older versions deleted by
the transaction. Update the approximate live-version count immediately.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `point_update_rollback` | 504,506 | 142,566 | -71.7% |
| `insert_rollback` | 345,661 | 165,157 | -52.2% |

The post-fix profiles no longer show `RowVersion::is_visible_to` among their
leading costs. A regression test performs 100 rollbacks on one key and verifies
that its version chain stays empty without a GC pass.

## H3. Scan cursor pays repeated transaction-map pins per row — `fixed`

**Where:** The 128-row scan spends about 20,773 instructions per operation in
the two `try_pin_loop` bodies, 6,400 in `MvccLazyCursor::next`, 6,400 in
`RowVersion::is_visible_to`, 4,258 in `refresh_current_position`, and 3,968 in
`btree_covers_chain_for_tx`.

`MvccLazyCursor` looked up its immutable transaction timestamp and WAL read mark
while opening the B-tree, then looked up the same transaction again while
advancing each MVCC row and again while copying that row into the output record.

**Fix:** Capture the transaction ID, begin timestamp, and read mark once when the
cursor opens. Resolve committed timestamp-only version chains directly from that
snapshot. Keep the original transaction lookup and dependency-registration path
for chains that still contain a transaction ID.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `scan_128` | 329,292 | 282,279 | -14.3% |
| `point_read` | 20,000 | 19,681 | -1.6% |
| `index_read` | 54,665 | 53,993 | -1.2% |

The two `try_pin_loop` bodies fell from about 20,773 to 5,711 instructions per
scan operation. Speculative-read and speculative-delete tests verify that
unresolved transaction IDs still use the dependency-aware visibility path.

## Wall-clock sanity baseline

Each native sample runs in a fresh process. Setup and statement preparation are
outside the timed region. The table compares the original implementation with
the branch after H2 and H3. Short read and scan workloads use nine samples;
rollback workloads use seven samples of 2,200 operations; committed writes use
eleven samples of 10,000 operations.

| Scenario | Original median | After H2/H3 | Change |
|---|---:|---:|---:|
| `point_read` | 1,029 ns | 1,012 ns | -1.7% |
| `index_read` | 2,992 ns | 2,976 ns | -0.5% |
| `scan_128` | 11,993 ns | 10,315 ns | -14.0% |
| `point_update_rollback` | 27,988 ns | 15,261 ns | -45.5% |
| `insert_rollback` | 22,126 ns | 15,274 ns | -31.0% |
| `point_update_commit` | 3,122 ns | 3,136 ns | +0.4% |
| `insert_commit` | 8,659 ns | 8,609 ns | -0.6% |

The committed-write differences are within run-to-run noise and their
instruction counts did not regress. The large scan and rollback instruction
wins also appear in elapsed time.

## H4. Index lookup repeatedly decodes and validates stored text keys — `fixed`

**Where:** One index lookup spends 5,103 instructions in UTF-8 validation,
1,557 in `cmp_in_column`, 1,114 and 961 in the two record-value decoders, and
944 in `SortableIndexKey::compare`.

**Fix:** Read each serialized column once during `SortableIndexKey` comparison.
For two ASCII text values under BINARY collation, compare their bytes directly;
UTF-8 string order and byte order are identical for ASCII. Non-ASCII text,
invalid UTF-8, numeric values, and other collations keep the checked generic
comparison path.

**Callgrind, 200/2,200 iterations:** `index_read` fell from 53,993 to 45,143
instructions per operation (-16.4%; -17.4% from the original baseline). UTF-8
validation fell from 6,383 to 255 instructions per operation. The same index
comparison path is part of inserting an indexed row: `insert_commit` fell from
156,555 to 116,806 instructions per operation (-25.4%).

**Wall clock:** Against the original tree, eleven fresh-process samples of
`index_read` fell from a 3,044 ns median to 2,525 ns (-17.0%). Eleven
fresh-process `insert_commit` samples fell from 8,897 ns to 6,868 ns (-22.8%).

## H5. Short MVCC operations contain avoidable MVCC allocations — `rejected`

`malloc`, `free`, and memcpy are leading self-costs in every short workload.
DHAT on 200 committed MVCC inserts reported 932,486 allocations overall, but
the leading sites were SQL parsing, schema construction, and catalog work in
the memory harness rather than the prepared-statement path used by this
benchmark.

The MVCC-filtered stacks contained about one allocation per committed row at
each expected ownership boundary: the new version, new row slot, write-set
entry, logical-log record, and I/O completion. That evidence does not support a
broad ownership or pooling change. Keep the concrete sites visible in profiles,
but do not trade simpler lifetimes for an unmeasured reduction.

## H6. Scan cursor clones its complete position on every advance — `fixed`

**Where:** `MvccLazyCursor::next` and `prev` called `get_current_pos`, which
cloned the row key and optional version-chain `Arc` just to inspect the current
position. The 128-row scan repeats that work for every row in the result.

**Fix:** Match on a shared reference to `current_pos`. Compute the two advance
flags before mutating either cursor, so the state-machine behavior is unchanged
and no owned position is needed.

**Callgrind, 200/2,200 iterations:** `scan_128` fell from 282,279 to 271,659
instructions per operation (-3.8%; -17.5% from the original baseline).

**Wall clock:** Eleven fresh-process samples fell from the original 11,993 ns
median to 9,820 ns (-18.1%).

## H7. Read-only cursor operations clone cached row state — `fixed`

**Where:** After H6, `current_row`, `rowid`, `is_empty`, and `has_record` still
called or reproduced the owned-position path. On a scan row this cloned the row
key and cached version-chain `Arc`, adding reference-count operations before the
VDBE could consume the row.

**Fix:** Borrow the position and version chain while reading them. Rust can
borrow the cursor's reusable record buffer separately, so row serialization no
longer needs a temporary `Arc` clone. Keep the owned clone in `delete`, which
must retain the row ID while it mutates the cursor and version store.

**Callgrind, 200/2,200 iterations:** `scan_128` fell from 271,659 to 255,644
instructions per operation (-5.9%; -22.4% from the original baseline).

**Wall clock:** Eleven fresh-process samples fell from the original 11,993 ns
median to 9,419 ns (-21.5%).
