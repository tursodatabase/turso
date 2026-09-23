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
| `point_read_btree` | `point_read` after a checkpoint moves every row into the B-tree |
| `index_read_btree` | `index_read` after a checkpoint |
| `scan_128_btree` | `scan_128` after a checkpoint; cursor merge over B-tree rows |
| `index_scan_128` | 128-row range scan over the secondary index |
| `batch_insert_commit` | 32 inserts in one transaction, then commit |
| `delete_commit` | Prepared autocommit delete by primary key |

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

## Rebase onto `3d70e1409e`

The branch was rebased onto `origin/main` at `3d70e1409e`. Main now reuses
`BTreeCursor` allocations through a pool on the pager, and it lets an MVCC
cursor that opened with a negative root switch to the published root after a
checkpoint (#8467). The second change conflicts with the earlier H13 change
(skip the `BTreeCursor` for version-store-only tables), which kept such a
cursor version-store-only for its lifetime. H13 was dropped from the rebase.
The pool already gives most of its point-read and index-read gain.

Rebased baseline, 200/2,200 iterations:

| Scenario | Instructions/operation |
|---|---:|
| `point_read` | 17,965 |
| `index_read` | 40,568 |
| `scan_128` | 272,470 |
| `point_update_rollback` | 35,195 |
| `insert_rollback` | 72,866 |
| `point_update_commit` | 44,144 |
| `insert_commit` | 106,520 |

## H14. Every commit copies an inline checkpoint state machine — `fixed`

**Where:** `memcpy` was the largest self-cost in `point_read` at 2,373
instructions per operation. 1,421 of them came from `MvStore::commit_tx`,
including for read-only autocommit statements. `CommitState::Checkpoint`
stored the `CheckpointStateMachine` inline, so `CommitStateMachine` was 4,344
bytes. `commit_tx` built it on the stack and copied it into its box, and state
changes moved the full enum.

**Fix:** Box the checkpoint state machine inside `CommitState::Checkpoint`.
Only a commit that starts an automatic checkpoint allocates it.
`CommitStateMachine` is now 352 bytes.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `point_read` | 17,965 | 16,011 | -10.9% |
| `index_read` | 40,568 | 38,544 | -5.0% |
| `scan_128` | 272,470 | 270,609 | -0.7% |
| `point_update_rollback` | 35,195 | 35,188 | 0.0% |
| `insert_rollback` | 72,866 | 72,904 | +0.1% |
| `point_update_commit` | 44,144 | 39,210 | -11.2% |
| `insert_commit` | 106,520 | 101,355 | -4.8% |

The rollback workloads do not build a commit state machine.

## H15. Index cursors look up every row they already hold — `fixed`

**Where:** The first `index_scan_128` run measured 960,910 instructions per
operation, about 7,500 per row, against about 2,100 per row for `scan_128`.
`read_from_table_or_index` called from `MvccLazyCursor::current_row` cost
769,993 of them. The index iterator returned only the row ID, so the cursor
position had no version chain. Reading each row then searched the skip list
for its key from the top: about 30 key comparisons per row. The index advance
also pinned the transaction map for every row to check visibility.

**Fix:** Index advance and index seek return the version chain they found
and check visibility against the cursor's snapshot, as table scans do since
H3. `current_row` then reads the visible version directly. The table-only
rule that lets a checkpointed B-tree row replace its chain
(`chain_falls_through_for_tx`) still runs only for table cursors. Index
chains keep their B-tree shadow check in `IndexShadowScan`.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `index_scan_128` | 960,910 | 193,066 | -79.9% |
| `index_read` | 38,544 | 32,197 | -16.5% |
| `index_read_btree` | 35,307 | 35,225 | -0.2% |
| `batch_insert_commit` | 2,725,306 | 2,714,368 | -0.4% |
| `scan_128` | 270,609 | 271,522 | +0.3% |

The `scan_128` increase is about seven instructions per row for the cursor
type check. The other workloads changed by less than 0.1%.

## H16. Table scans look up the transaction for every row again — `fixed`

**Where:** After the rebase, `scan_128` rose from 255,494 to 272,470
instructions per operation. Comparing the two profiles against the
pre-rebase tip `974dae2fc0` showed about 20,000 more instructions per scan
in skip-list pinning and a new per-row `SkipMap::get`. Main added a check to
`position_from_peeks`: a checkpointed B-tree row may replace a version-store
row. It looked up the transaction in the transaction map for every row to
recover its begin timestamp and read mark. The cursor already holds both in
its snapshot since H3, and they cannot change during the transaction.

**Fix:** Pass the cursor's snapshot to `btree_covers_chain_for_snapshot` and
remove the transaction lookup.

**Callgrind, 200/2,200 iterations:** `scan_128` fell from 271,522 to 247,543
instructions per operation (-8.8%), below the pre-rebase 255,494.
`scan_128_btree` did not change: its rows come from the B-tree, so this
check does not run. The other workloads changed by less than 0.2%.

## H17. Index seeks rebuild the seek key's index metadata — `fixed`

**Where:** `index_read` made about 23 allocations per operation. Each index
seek built a new `Arc<IndexInfo>` with a copy of every `KeyInfo`, only to set
`num_cols` to the seek key's column count. An equality seek then collected
another `Vec<KeyInfo>` to compare the found key with the seek key.

**Fix:** The cursor keeps the last seek key's `IndexInfo` and reuses it while
seek keys have the same column count; its other fields come from the
cursor's own index. The equality check passes a slice of the index's
`key_info`.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `index_read` | 32,197 | 31,917 | -0.9% |
| `index_read_btree` | 35,225 | 35,037 | -0.5% |
| `delete_commit` | 68,021 | 67,669 | -0.5% |

## H18. Per-statement allocations on MVCC write and seek paths — `fixed`

**Where:** Counting allocation calls per operation by caller in
`delete_commit` (about 36 per delete) found four sites that allocate on every
statement without needing to:

- Both equality checks on an index seek collected two `Vec<ValueRef>` to
  compare the found key with the seek key.
- `CommitCoordinator::unlock_pager_commit_lock` ran `BTreeMap::split_off` on
  the parked-waiter map on every commit, which allocates even when the map is
  empty.
- The first write-set insert of every transaction allocated the `seen` hash
  set, and every insert hashed a pointer, to deduplicate a few entries.
- Opening an index cursor built its `IndexInfo` twice: once for the MVCC
  cursor and once inside the B-tree cursor. Without MVCC, the first copy was
  built and dropped.

**Fix:** Compare seek keys with `compare_immutable_iter` from the record
iterators. Return early when no commit is parked. Deduplicate write sets of
fewer than 16 entries by scanning `entries` for the same `Arc`, and build
`seen` only when the set grows past that. Build `IndexInfo` once and pass the
same `Arc` to both cursors.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `point_read` | 15,965 | 15,807 | -1.0% |
| `index_read` | 31,917 | 30,351 | -4.9% |
| `index_read_btree` | 35,037 | 33,417 | -4.6% |
| `point_update_rollback` | 35,227 | 34,641 | -1.7% |
| `insert_rollback` | 72,677 | 71,497 | -1.6% |
| `point_update_commit` | 39,225 | 38,304 | -2.3% |
| `insert_commit` | 101,153 | 99,625 | -1.5% |
| `batch_insert_commit` | 2,720,656 | 2,693,810 | -1.0% |
| `delete_commit` | 67,669 | 65,225 | -3.6% |

The scan workloads did not change.

## H19. Table point seeks look up the row they already found — `fixed`

**Where:** Counting skip-map lookups per `point_update_commit` operation
showed two in `read_from_table_or_index`, called by `current_row` after the
seek. `seek_rowid` found the row's version chain and then returned only its
row ID and a copy of its payload, so reading the row looked up the
transaction and searched `rows` again. `seek_rowid` also looked up the
transaction to build a snapshot the cursor already holds. The rowid
uniqueness probe (`exists`) asked for the payload copy and then dropped it.

**Fix:** `seek_rowid` takes the cursor's snapshot and returns the version
chain, and the cursor position keeps it, as H15 does for index seeks. The
payload copy stays as the fallback for a chain that passive GC empties before
the row is read; the uniqueness probe no longer asks for it.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `point_read` | 15,807 | 14,714 | -6.9% |
| `index_read` | 30,351 | 29,290 | -3.5% |
| `point_update_rollback` | 34,641 | 33,595 | -3.0% |
| `point_update_commit` | 38,304 | 37,244 | -2.8% |
| `delete_commit` | 65,225 | 64,008 | -1.9% |

The checkpointed reads fell by 0.5%, the inserts by 0.2% to 0.3%, and the
scans did not change.

## H20. B-tree coverage checks look up the table binding first — `fixed`

**Where:** After H16, `scan_128` still made 256 skip-map lookups per scan,
all from `btree_covers_chain_for_snapshot`: two per row, one while advancing
and one when positioning. The function looked up the table's root-page
binding (`is_btree_readable_at`) before running `chain_is_write_buffer_for`,
a check on the chain alone. For any row that no checkpoint has written, the
chain check alone decides the answer.

**Fix:** Run the chain check first and look up the binding only for a chain
that a checkpoint may have written. Both checks only read state, and the
result is the conjunction of the two. `durable_txid_max` is now loaded before
the binding lookup. A stale value only makes the chain check keep the row in
the version store, which is the result the function returns whenever it is
unsure.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `scan_128` | 247,469 | 218,496 | -11.7% |
| `point_read` | 14,714 | 14,441 | -1.9% |
| `point_update_rollback` | 33,595 | 33,260 | -1.0% |
| `point_update_commit` | 37,244 | 36,934 | -0.8% |
| `point_read_btree` | 16,089 | 16,134 | +0.3% |

The other workloads changed by less than 0.5%.

## H21. B-tree rows pay two avoidable lookups each — `fixed`

**Where:** `scan_128_btree` reads every row from the B-tree and made three
skip-map lookups per row. `MvccLazyCursor::is_btree_allocated` looked up the
table binding on every advance. `MvStore::query_btree_version_is_valid`
looked up the transaction before looking for a version-store chain that
could shadow the row, although it needs the transaction only when a chain
exists. After a checkpoint, most rows have no chain.

**Fix:** The cursor remembers that its B-tree is readable once the binding
check passes. The cursor already relies on that: it resolves the physical
root only once, and the merge with the version store cannot continue if the
B-tree stops being readable during a scan. The shadow check looks up the
transaction only after it finds a chain.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `scan_128_btree` | 256,928 | 207,059 | -19.4% |
| `point_read_btree` | 16,134 | 15,842 | -1.8% |
| `index_read_btree` | 33,244 | 32,570 | -2.0% |

The version-store workloads changed by less than 0.1%.

## H22. Every commit state looks up its own transaction — `fixed`

**Where:** Counting skip-map lookups per `point_update_commit` operation
showed 13 from `CommitStateMachine::step`. Each commit state (`Initial`,
`Commit`, `WaitForDependencies`, `BeginCommitLogicalLog`, `BuildLogRecord`,
`EndCommitLogicalLog`, `CommitEnd`, `RewriteLiveVersions`, `FinalizeCommit`,
and the log owner update) searched `txs` for the committing transaction
again, mostly to fail if it had been removed.

**Fix:** The state machine takes the transaction's reference-counted
skip-list entry when the commit starts and releases it when the commit
finishes. Each state checks `Entry::is_removed()` where it previously checked
for a missing map entry, and returns the same error. A removed node is never
returned by `get`, and transaction IDs are never reused, so the check gives
the same answer as the search. The entry's lifetime is extended to `'static`
in the same way as `static_iterator_hack!`: the state machine owns the
`Arc<MvStore>` whose map the entry borrows and drops the entry first.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `point_update_commit` | 36,936 | 35,447 | -4.0% |
| `delete_commit` | 63,758 | 62,615 | -1.8% |
| `insert_commit` | 99,441 | 98,116 | -1.3% |
| `point_read` | 14,443 | 14,676 | +1.6% |
| `index_read` | 29,114 | 29,353 | +0.8% |

**Unexplained:** Read-only statements do one lookup before and after the
change, yet cost about 230 more instructions. The profile difference is in
glibc's allocator slow path (`_int_malloc` +83, `unlink_chunk` +62), not in
MVCC code. A likely cause is that the state machine grew by 16 bytes and its
per-commit box moved to another allocator size class, but this was not
checked. Releasing the entry at the end of the commit instead of when the
state machine drops did not change it.

## H23. Skip-list searches compare the same node twice — `fixed`

**Where:** A batch insert makes about 150 index-key comparisons per row, at
about 250 instructions each. `SkipList::search_position` and
`SkipList::search_bound` walk each level until a node's key ends the walk,
then descend. The first node on the level below is often that same node, and
its key was compared again.

**Fix:** Remember the node that ended the walk on the level above and skip
the comparison when the walk reaches it again. A node's key never changes and
comparison is deterministic, so the result is the same. The check runs after
the removed-node check, so unlinking behaves as before.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `delete_commit` | 62,615 | 58,400 | -6.7% |
| `index_read` | 29,353 | 28,937 | -1.4% |
| `index_scan_128` | 192,864 | 191,054 | -0.9% |
| `batch_insert_commit` | 2,687,662 | 2,670,106 | -0.7% |
| `point_update_rollback` | 33,272 | 33,435 | +0.5% |
| `point_read` | 14,676 | 14,728 | +0.4% |

The index comparisons per inserted row fell only from about 151 to 146, so
most of them come from elsewhere. Maps whose keys compare cheaply pay a few
instructions for the extra pointer check.

## H24. Version inserts search the map again after finding the chain — `fixed`

**Where:** Splitting index-key comparison cost by caller chain with
`--separate-callers` showed about 2,600 instructions per inserted row in
`SkipMap::get` after `try_get_or_insert_with`. `insert_index_version` and
`insert_version` find or create the key's version chain, take its write lock,
and then call `index_versions_still_mapped` or `table_versions_still_mapped`:
a second search for the same key, to retry if GC removed the chain while the
insert waited for the lock.

**Fix:** Keep the entry that `try_get_or_insert_with` returned and check
`Entry::is_removed()` after taking the lock. GC removes a chain's node only
while holding that chain's write lock, the map keeps at most one live node
per key, and a replacement chain always has a new `Arc`. So the entry is
removed exactly when the old search would have found another chain or none.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `batch_insert_commit` | 2,670,106 | 2,423,632 | -9.2% |
| `insert_commit` | 98,073 | 90,548 | -7.7% |
| `insert_rollback` | 71,145 | 65,720 | -7.6% |
| `point_update_rollback` | 33,435 | 32,764 | -2.0% |
| `point_update_commit` | 35,558 | 34,931 | -1.8% |

The read workloads changed by less than 0.2%.

## H25. Cursor inserts read the row before inserting it — `fixed`

**Where:** After H24, a batch insert still positioned three range iterators
per row and made a separate lookup for each written key.
`MvccLazyCursor::insert` called `read_from_table_or_index` to decide between
insert and update. That looked up the transaction, searched the table or
index map, and cloned the visible row if there was one. Then
`insert_to_table_or_index` looked up the transaction and searched the map for
the same key again. For a new key, the first search always found nothing.

**Fix:** For rows that are not B-tree resident, the cursor calls
`insert_unless_visible_to_table_or_index`. It finds or creates the key's
version chain once, takes its write lock, and checks whether the transaction
already sees a version, with the same test the read used: plain visibility
for index keys, visibility of a chain the B-tree does not cover for table
rows. If the transaction does see one, it inserts nothing and returns the row,
and the cursor updates it as before. The check runs under the chain's write
lock, so it is at least as strict as the earlier separate read. B-tree
resident rows keep the old path.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `batch_insert_commit` | 2,423,632 | 2,145,467 | -11.5% |
| `insert_commit` | 90,548 | 82,040 | -9.4% |
| `insert_rollback` | 65,720 | 59,406 | -9.6% |
| `point_update_rollback` | 32,764 | 31,810 | -2.9% |
| `point_update_commit` | 34,931 | 34,042 | -2.5% |

Updates still search for the key as often as before, but no longer clone the
visible row to find out that it exists. The read workloads changed by less
than 0.2%.

## H26. MVCC cursors allocate a new record buffer per statement — `rejected`

Each MVCC cursor allocated a 1,024-byte record buffer the first time it
returned a version-store row, and freed it with the cursor. Taking the buffer
from the pager's record pool, which B-tree cursors already use, and returning
it when the cursor drops changed every workload by -0.4% to +0.2%. The change
was reverted.

## H27. Deletes look up the transaction for every version — `fixed`

**Where:** `delete_from_table_or_index` looked up the transaction for each
version it examined, and once more after ending the visible version. Updates
run through it too.

**Fix:** Look the transaction up once after finding the chain.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `delete_commit` | 58,381 | 57,965 | -0.7% |
| `point_update_commit` | 34,042 | 33,835 | -0.6% |
| `point_update_rollback` | 31,810 | 31,626 | -0.6% |

No other workload changed.

## H28. INSERT seeks each unique index twice — `fixed`

**Where:** After H24 and H25, splitting index-key comparison cost by caller
chain put about 4,700 instructions of comparison self cost per inserted row
in `SkipList::lower_bound`: three range positionings per row. `EXPLAIN` of
the benchmark INSERT shows `NoConflict` probing the unique index and then the
deferred `IdxInsert` with flags `NCHANGE` only, so `op_idx_insert` seeks the
same key again and repeats the unique check. SQLite's `IdxInsert` in the same
plan carries `OPFLAG_USESEEKRESULT` (p5 = 16) and reuses the probe.
`op_idx_insert` already implements this as `USE_SEEK`, but the INSERT
translator never set it.

**Fix:** The deferred `IdxInsert` sets `USE_SEEK` for a unique, non-partial
index that the preflight probed with `NoConflict` on the same cursor, when the
statement has no REPLACE (statement or constraint level) and no UPSERT. It is
safe in both journal modes:

- Per row, the code runs BEFORE triggers, then the `NoConflict` probes, then
  the table `MakeRecord` and foreign-key child checks, then the deferred
  `IdxInsert`s, then the table `Insert`, then AFTER triggers, CDC, and
  RETURNING. Nothing between the probe and the index insert writes to the
  index B-tree or moves its cursor. The foreign-key checks read through their
  own cursors. A REPLACE deletes conflicting rows from every index, which
  moves the cursor, so any REPLACE disables the flag. That is stricter than
  SQLite, which also keeps it when the statement has no triggers.
- A `NotFound` eq-only `GE` seek on an index B-tree leaves the cursor on the
  leaf at the first cell greater than the probed prefix
  (`target_cell_when_not_found`). An equal key in an interior cell returns
  `TryAdvance`, and the probe then reports a conflict. With no entry sharing
  the prefix, every cell orders the same way against the prefix and against
  the full key (prefix plus rowid). So that cell is also where the full key
  belongs, and `BTreeCursor::insert` inserts at the cursor's current cell.
- The MVCC cursor inserts by key. It reads its position only to decide
  whether the row is B-tree resident, which requires an existing entry
  equal to the key, and a `NotFound` probe rules that out.
- `NoConflict` compares only the unique prefix, so it is at least as strict as
  the skipped check, which seeks the full key. Keys with NULLs still seek
  (runtime check in `op_idx_insert`).

`insert-unique-index-probe-position.sqltest` inserts scattered, long keys
(single- and multi-column, with NULLs, duplicates, and same-table BEFORE and
AFTER triggers that insert keys into the same leaf) and checks integrity, a
point lookup for every key, and the index order. Moving the cursor to the
first entry before a `USE_SEEK` insert makes all six insert tests fail.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `batch_insert_commit` | 2,145,466 | 1,815,869 | -15.4% |
| `insert_rollback` | 59,406 | 51,302 | -13.6% |
| `insert_commit` | 82,040 | 72,171 | -12.0% |
| `scan_128_btree` | 207,602 | 210,347 | +1.3% |
| `delete_commit` | 57,965 | 58,392 | +0.7% |
| `point_update_commit` | 33,835 | 34,083 | +0.7% |

**Measurement noise:** The only source change is in INSERT translation, which
the other workloads do not run in the measured loop. Their profiles show cost
moving between inlined functions (`SkipList::search_bound` now appears on its
own, while `try_pin_loop` fell by a similar amount). `bench-profile` builds with
16 codegen units and no LTO, so a change in one module can change inlining
elsewhere. Differences below about 1% in workloads a change does not touch are
within this noise.

## Rebase onto `fd41c07dc3`

Main added index-cursor key-info reuse, B-tree cell write changes, and LIKE
work. On the rebased branch every workload moved by at most about 1% from the
H28 measurements; `rebased-fd41c07` is the baseline for the next entries.

## H29. Index-key comparison decodes both records for the first column — `fixed`

**Where:** After H28, index-key comparison was still the largest cost of an
inserted row: about 11,000 of 56,700 instructions per row in
`batch_insert_commit`, across `ValueIterator::next_serialized_value`,
`SortableIndexKey::compare`, and `bcmp`. Each comparison costs about 250
instructions: it builds two `ValueIterator`s and decodes each serial type
through `next_serialized_value` before comparing bytes. For text keys most
comparisons are decided by the first column. H12 tried peeking inside the
iterator and was rejected because updates got slower.

**Fix:** Before the general loop, `SortableIndexKey::compare` reads the first
column of both records directly from the bytes when the header size and first
serial type are one-byte varints, both values are text, and the column is
BINARY. If those bytes differ, or the key has one column, that decides the
order, with the column's sort order applied. Anything else, including an
equal first column in a longer key, runs the general loop from the start, so
that path is unchanged. The result is the same as `compare_next_index_value`,
which also compares BINARY text bytes directly.

`sortable_index_key_order_matches_value_comparison` compares the key order
with `compare_immutable` over the decoded values for 8,000 random pairs:
empty, short, and longer-than-57-byte text (two-byte serial type), non-ASCII
text, NULL, integer, and blob leading values, ASC and DESC, BINARY and
NOCASE, and one-column seek prefixes. Ignoring DESC in the new path makes it
fail.

**Callgrind, 200/2,200 iterations:**

| Scenario | Before | After | Change |
|---|---:|---:|---:|
| `batch_insert_commit` | 1,815,205 | 1,517,552 | -16.4% |
| `insert_commit` | 72,256 | 62,839 | -13.0% |
| `delete_commit` | 58,238 | 52,093 | -10.6% |
| `index_read` | 29,083 | 26,601 | -8.5% |
| `insert_rollback` | 51,440 | 47,485 | -7.7% |
| `scan_128_btree` | 207,622 | 210,446 | +1.4% |
| `point_update_commit` | 33,964 | 34,210 | +0.7% |

`scan_128_btree` compares no index keys and has moved between about 207,600
and 210,400 across builds without a related change; see the note under H28.

## Final measured totals

The branch was rebased after `origin/main` gained unrelated planner work and an
MVCC checkpoint append optimization. The final comparison reran all seven
scenarios against the refreshed `origin/main` baseline at `869cb5dae`, with the
same measurement-harness commits applied to both trees. The earlier hypothesis
tables retain the measurements taken while each change was evaluated.

| Scenario | Original | Final | Change |
|---|---:|---:|---:|
| `point_read` | 19,880 | 19,532 | -1.8% |
| `index_read` | 54,562 | 43,934 | -19.5% |
| `scan_128` | 331,208 | 255,494 | -22.9% |
| `point_update_rollback` | 508,834 | 38,744 | -92.4% |
| `insert_rollback` | 345,221 | 78,381 | -77.3% |
| `point_update_commit` | 46,530 | 45,945 | -1.3% |
| `insert_commit` | 156,587 | 111,850 | -28.6% |

The refreshed native wall-clock spot check used eleven fresh-process samples
per tree and ran the baseline and branch sequentially:

| Scenario | `origin/main` median | Final median | Change |
|---|---:|---:|---:|
| `scan_128` | 12,467 ns | 9,477 ns | -24.0% |
| `point_update_rollback` | 47,787 ns | 3,118 ns | -93.5% |
| `insert_commit` | 8,842 ns | 6,914 ns | -21.8% |
