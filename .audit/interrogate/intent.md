# Interrogate reviewer brief

## Intent

Make Passive MVCC checkpoint reclaim safe and useful: reclaim SkipMap version chains once they are durably in the B-tree, without unique-miss SI failures or dual-cursor transfer-sum breakage. Stamp `materialized_at` only after the pager commit finishes (table and index write-set slots that actually wrote). Delete the retirement / RETIRED_TAG / occupy-after-retire model. Rule 3 drops the last current version only when it is stamped as materialized; Passive also requires `lwm == u64::MAX` (no open snapshot) because dual-cursor scans can lose or stale-serve rows if the SkipMap empties mid-scan while the B-tree side still shadows the key.

## Code under review

Read these paths in the worktree `/Users/peristocles/fun/turso/.worktrees/mvcc-retire-reclaim`:

1. Full PR diff (base `7e7ac7bd18^` .. HEAD): `.audit/interrogate/pr-diff.patch`
2. Final sources (read the live files, not only the patch):
   - `core/mvcc/database/mod.rs` — especially `gc_incremental`, `gc_version_chain`, Rule 2/3, `materialized_at`
   - `core/mvcc/database/checkpoint_state_machine.rs` — especially `written_table_rowids`, `written_index_slots`, `gc_checkpointed_table_versions`, `gc_checkpointed_index_versions`, WriteIndexRow/DeleteIndexRow insert sites, CommitPagerTxn ordering
   - `core/mvcc/cursor.rs` — reclaim / occupy / insert-beside assert changes
   - `core/mvcc/database/tests.rs` — unique-miss, transfer sum/count, Rule 2/3, unstamped TEXT PK regressions

Do not edit files. Readonly review only.

Explore surrounding call sites with Grep/Read as needed. Focus on correctness of reclaim under Passive GC, races with open readers, stamp-before-commit bugs, and leftover retirement complexity.
