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

## H4. Index lookup repeatedly decodes and validates stored text keys — `hypothesis`

**Where:** One index lookup spends 5,103 instructions in UTF-8 validation,
1,557 in `cmp_in_column`, 1,114 and 961 in the two record-value decoders, and
944 in `SortableIndexKey::compare`.

**Next measure:** Separate MVCC's in-memory index comparison from the B-tree
fallback and confirm whether the stored `SortableIndexKey` can compare its
serialized text without rebuilding validated values.

## H5. Short MVCC operations are allocation-heavy — `hypothesis`

`malloc`, `free`, and memcpy are leading self-costs in every short workload.
Before changing ownership or pooling, collect allocation-site evidence for the
specific scenario and confirm that the allocations originate in MVCC rather
than the statement or parser layers.
