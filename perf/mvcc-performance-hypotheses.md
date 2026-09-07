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

## H8. Move the winning dual-cursor peek instead of cloning it — `rejected`

The merge cursor retains two peek values. Moving the winner into `current_pos`
would avoid cloning its row key and optional version-chain `Arc`. It also changes
the state machine by replacing the consumed peek with `Uninitialized` before
the next advance.

The added state transition reduced `scan_128` from 255,644 to 254,366
instructions (-0.5%) and `index_read` from 45,143 to 44,994 (-0.3%), while
`point_read` increased from 19,681 to 19,715 (+0.2%). This trade is too small to
justify the wider state-machine surface, so the implementation change was
reverted.

## H9. Main-only rollback invalidates every prepared statement — `fixed`

**Where:** The prepared rollback workloads still showed SQL parser,
`ProgramBuilder`, and translation costs once per operation. Five benchmark
iterations reported reprepare counts `[4, 4, 4]` for the already-prepared
`BEGIN`, write, and `ROLLBACK` statements.

`Connection::rollback_attached_wal_txns` treated the temp pager like a changed
attached schema whenever it held transient query state. It cleared a schema
cache that temp does not use and bumped the connection's prepare generation.
The next execution then rebuilt every prepared statement.

**Fix:** Roll back only non-main WAL pagers that hold a transaction. Invalidate
the attached schema cache only for attached-database writers. Temp schema has
its own `schema_did_change` flag and rollback path, so ordinary temp pager
activity does not invalidate prepared code. The benchmark now reports
reprepare counts `[0, 0, 0]`.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change | From original |
|---|---:|---:|---:|---:|
| `point_update_rollback` | 142,098 | 38,742 | -72.7% | -92.3% |
| `insert_rollback` | 146,487 | 79,761 | -45.5% | -76.9% |

**Wall clock:** Eleven fresh-process samples reduced `point_update_rollback`
from the original 27,988 ns median to 3,094 ns (-88.9%) and `insert_rollback`
from 22,126 ns to 5,439 ns (-75.4%).

## H10. BINARY index comparison revalidates immutable text — `fixed`

**Where:** After H4, `insert_commit` still spent 5,461 instructions per
operation checking whether the same serialized text was ASCII during repeated
skip-list comparisons. Caching that fact in each key would enlarge every
`SortableIndexKey`.

**Fix:** Validate serialized text once when an index key is constructed. A
private zero-sized field prevents internal construction that skips validation.
After that check, BINARY collation can compare UTF-8 bytes directly: SQLite's
BINARY order and UTF-8 string order are both lexicographic byte order. This
also rejects malformed UTF-8 at construction instead of waiting for a later
comparison, and adds no per-key storage.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change | From original |
|---|---:|---:|---:|---:|
| `index_read` | 45,143 | 43,934 | -2.7% | -19.6% |
| `insert_commit` | 116,585 | 111,850 | -4.1% | -28.6% |
| `insert_rollback` | 79,761 | 78,381 | -1.7% | -77.3% |

**Wall clock:** Eleven fresh-process `insert_commit` samples fell from 6,868
ns to 6,714 ns (-2.2%; -24.5% from the original matched run). The shorter
`index_read` samples moved from 2,525 ns to 2,569 ns, within run-to-run noise;
the branch remains 15.6% below the original 3,044 ns median.

## H11. Fast-path one-byte record serial types — `rejected`

The remaining indexed workload profile attributes substantial cost to
`ValueIterator::next_serialized_value`. Most benchmark values have a one-byte
serial-type varint, so an explicit branch bypassed `read_varint` for bytes below
`0x80` while retaining the general parser for larger values.

`index_read` remained exactly 43,934 instructions per operation. The compiler
already reduces the inlined parser to the same code for this common case, so
the explicit branch and its added test were reverted.

## H12. Inline or peek serialized values during index comparison — `rejected`

Forcing `ValueIterator::next_serialized_value` into every caller reduced
`index_read` from 43,934 to 42,091 instructions (-4.2%) and `insert_commit`
from 111,850 to 104,636 (-6.4%). It also increased `point_update_commit` from
45,945 to 46,817 (+1.9%) and made the branch slightly slower than the refreshed
`origin/main` baseline for that workload.

Inlining only the outer comparator removed the gains. Peeking at the first
serialized value avoided iterator updates for unequal keys, but equal-key
updates still rose to 46,795 instructions because continuing after an equal
prefix needs more state. No tested variant improved lookup and insert without
regressing updates, so all implementation changes were reverted.

## H13. Version-store-only cursors allocate an unused B-tree cursor — `fixed`

**Where:** Tables and indexes keep negative roots until a checkpoint creates
their B-trees. `OpenRead` and `OpenWrite` still allocated a full `BTreeCursor`
for those roots before wrapping it in `MvccLazyCursor`. The MVCC cursor's
snapshot gate then refused to use it. Each unused cursor includes the fixed
depth page stack and all B-tree state-machine fields, so constructing its box
caused a large copy and an allocation on every statement.

**Fix:** Represent the base cursor as either a B-tree cursor or a pager for
version-store-only access. When root resolution remains negative, the VDBE
constructs the latter. The existing snapshot/read-mark check still guards every
B-tree operation, and a cursor created without a B-tree remains version-store-only
for its lifetime. If a concurrent checkpoint makes the root readable between
VDBE root resolution and cursor construction, construction returns
`SchemaUpdated` so the statement retries with the positive root. Positive,
checkpointed roots keep the existing cursor.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change | From original |
|---|---:|---:|---:|---:|
| `point_read` | 19,532 | 18,308 | -6.3% | -7.9% |
| `index_read` | 43,934 | 40,649 | -7.5% | -25.5% |
| `scan_128` | 255,494 | 254,258 | -0.5% | -23.2% |
| `point_update_rollback` | 38,744 | 37,411 | -3.4% | -92.6% |
| `insert_rollback` | 78,381 | 73,947 | -5.7% | -78.6% |
| `point_update_commit` | 45,945 | 44,731 | -2.6% | -3.9% |
| `insert_commit` | 111,850 | 107,988 | -3.5% | -31.0% |

**Wall clock:** Eleven fresh-process samples, run on the baseline first and the
branch second, measured 1,018 to 963 ns for `point_read` (-5.4%), 2,970 to
2,243 ns for `index_read` (-24.5%), and 8,557 to 6,689 ns for `insert_commit`
(-21.8%). These are final branch-to-baseline totals, not the isolated H13
effect.

## Final measured totals

The branch was rebased after `origin/main` gained unrelated planner work and an
MVCC checkpoint append optimization. The final comparison reran all seven
scenarios against the refreshed `origin/main` baseline at `869cb5dae`, with the
same measurement-harness commits applied to both trees. The earlier hypothesis
tables retain the measurements taken while each change was evaluated.

| Scenario | Original | Final | Change |
|---|---:|---:|---:|
| `point_read` | 19,880 | 18,308 | -7.9% |
| `index_read` | 54,562 | 40,649 | -25.5% |
| `scan_128` | 331,208 | 254,258 | -23.2% |
| `point_update_rollback` | 508,834 | 37,411 | -92.6% |
| `insert_rollback` | 345,221 | 73,947 | -78.6% |
| `point_update_commit` | 46,530 | 44,731 | -3.9% |
| `insert_commit` | 156,587 | 107,988 | -31.0% |

## Stopping point

The final point-read and commit profiles are led by memory copying and
allocation, plus the epoch pin and search work required by the concurrent
skip list. The scan profile is led by VDBE dispatch, record decoding, and the
visibility checks needed for each returned row. Indexed workloads still spend
time decoding serialized values, but H11 and H12 tested the local shortcuts:
the compiler already emits the one-byte fast path, and forced inlining trades
faster lookup and insert for slower updates.

No remaining measured hotspot has a bounded MVCC-local change that improves
the workload matrix without weakening ownership or concurrency rules. Further
gains need a wider record, VDBE, allocator, or skip-list design change and
should start with their own matched benchmark set.

The earlier native wall-clock spot check, before H13, used eleven fresh-process
samples per tree and ran the baseline and branch sequentially:

| Scenario | `origin/main` median | Branch median before H13 | Change |
|---|---:|---:|---:|
| `scan_128` | 12,467 ns | 9,477 ns | -24.0% |
| `point_update_rollback` | 47,787 ns | 3,118 ns | -93.5% |
| `insert_commit` | 8,842 ns | 6,914 ns | -21.8% |
