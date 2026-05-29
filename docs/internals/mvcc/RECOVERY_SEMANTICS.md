# MVCC Recovery and Checkpoint Semantics

This document describes the MVCC recovery and checkpoint behavior as implemented in
`core/mvcc/database/mod.rs` and `core/mvcc/database/checkpoint_state_machine.rs`.

The checkpoint model is stop-the-world (blocking checkpoint lock).

## Durable Artifacts

Startup decisions use four durable artifacts:
- Main database file (`.db`)
- WAL file (`.db-wal`)
- MVCC logical log (`.db-log`)
- MVCC metadata table row: `__turso_internal_mvcc_meta(k='persistent_tx_ts_max')`

The logical-log header (56 bytes) contains format metadata and a CRC chain seed:
magic, version, flags, hdr_len, salt (u64), reserved, hdr_crc32c.
The salt is regenerated on each log truncation; frame CRCs are chained
(`crc32c_append(prev_frame_crc, data)`) with the initial seed derived from the salt.

`persistent_tx_ts_max` in `__turso_internal_mvcc_meta`is the durable replay boundary, stored inside the main database file/WAL.
Recovery replays logical-log frames only when `commit_ts > persistent_tx_ts_max`.

## Bootstrap Order

`MvStore::bootstrap()` runs in this order:
1. `maybe_complete_interrupted_checkpoint()`
2. `reparse_schema()`
3. Ensure metadata table exists (or fail closed in invalid states)
4. Build in-memory table-id/root-page mapping from schema
5. `maybe_recover_logical_log()`
6. Promote bootstrap connection to regular MVCC connection

Any committed WAL state is reconciled before logical-log replay.
The replay boundary comes from the metadata row.

## Startup Case Classification

Recovery classifies startup state using two checks:
- Does the WAL have committed frames? (`wal.get_max_frame_in_wal() > 0`)
- What does `try_read_header()` return? (`Valid`, `Invalid`, or `NoLog`)

| Case | Startup artifacts | Recovery behavior |
|---|---|---|
| 1 | WAL has committed frames + log header valid | Complete checkpoint: backfill WAL into DB, sync DB, write fresh log header, truncate WAL. Then run logical-log recovery with metadata cutoff. |
| 2 | WAL has committed frames + log header missing (`NoLog`) | Recover (same as case 1, minus reusing the old header): the committed WAL frames hold the durable B-tree state + the `mvcc_meta` boundary row; an empty/truncated log means no uncheckpointed ops. This is the normal steady state of a non-stop-the-world (Passive) checkpoint, which truncates the logical log to 0 but leaves the WAL non-empty. A deleted log on top of a committed WAL is indistinguishable from this and also recovers — no committed data is lost. |
| 3 | WAL has committed frames + log header invalid/torn | Fail closed with `Corrupt`. A header that is present but fails to decode is a torn write / genuine corruption, not a clean truncation. |
| 4 | WAL has no committed frames | Truncate/discard WAL tail bytes and continue logical-log recovery. |
| 5 | No WAL + log header invalid/torn | Fail closed with `Corrupt`. |
| 6 | No WAL + valid header, no frames (size <= `LOG_HDR_SIZE`) | No replay needed; timestamp state comes from metadata row. |
| 7 | No WAL + empty log (0 bytes / `NoLog`) | Timestamp state loaded from metadata row if present; no replay. |

Notes:
- After a *stop-the-world* checkpoint (WAL also truncated), restart is case 7.
  After a *Passive* checkpoint (log truncated to 0, WAL left non-empty),
  restart is case 2 — which now recovers.
- Torn tail in log body is treated as EOF (prefix frames remain valid).
- First invalid frame during forward scan terminates the scan (prefix preserved), matching SQLite WAL availability semantics.
- Missing or corrupt metadata row is treated as corruption when the metadata table is expected to exist.

## Checkpoint Sequence

The collection/write phase (steps 1-3) and the backfill/truncate phase
(steps 7-11) run *unlocked*; only the marker-publication window (steps 4-6)
holds `blocking_checkpoint_lock.write()`, which is what makes `begin_tx`
return `Busy`. Releasing the lock right after the durable boundary is
published keeps the WAL→DB backfill off the path of concurrent readers/writers.

1. Snapshot the checkpointable boundary (`snapshot_ts`) and collect committed
   MVCC table/index versions. Unlocked.
2. Begin pager transaction; write the collected versions into pager.
3. Upsert metadata row `persistent_tx_ts_max` in the same pager transaction.
4. **Take `blocking_checkpoint_lock.write()`.** Commit pager transaction. WAL
   now contains committed frames for both data and the metadata row.
5. Publish in-memory markers: advance `durable_txid_max`, drain staged root-page
   allocations, run the targeted GC of checkpointed versions.
6. **Release `blocking_checkpoint_lock`.** Everything after this is unlocked.
7. Checkpoint WAL — backfill WAL frames into the DB file. Auto-checkpoints use
   `Passive` (backfill the safe prefix without WAL writer exclusivity, do not
   reset the WAL); explicit `Truncate`/`Restart` backfill all frames with
   exclusivity and reset the WAL.
8. Fsync DB file (unless `SyncMode::Off`).
9. Truncate logical log to 0 (salt regenerated in memory; header written with next frame).
10. Fsync logical log (unless `SyncMode::Off`).
11. Truncate WAL file — **only under `Truncate`/`Restart`** (`should_restart_log()`).
    Under `Passive` this is a pass-through: the backfill did not reset
    `max_frame`, so zeroing the file would orphan it (`ShortReadWalFrame`); the
    WAL resets lazily via restart-on-write.

Until the DB file and logical-log cleanup are durable, the WAL remains the
authoritative recovery source. After a `Passive` checkpoint the WAL is left
non-empty by design — recovery treats that (with a truncated logical log) as
startup case 2 and reconciles it, so no committed data is lost.

## Correctness Invariants

1. Startup reaches one consistent state or fails closed; no best-effort ambiguity.
2. Committed WAL state is never ignored.
3. Invalid logical-log tail frames are never replayed.
4. Torn or invalid-tail bytes are never interpreted as committed operations.
5. Replay applies only frames with `commit_ts > persistent_tx_ts_max`.
6. `persistent_tx_ts_max` is advanced atomically with pager commit during checkpoint.
7. Same-process checkpoint retries resume from the pager-committed boundary even if later checkpoint phases fail.
8. Logical clock is reseeded to `max(persistent_tx_ts_max, max_replayed_log_commit_ts) + 1`.
9. After interrupted-checkpoint reconciliation, WAL is truncated.

## SyncMode::Off

`SyncMode::Off` skips fsync calls. This weakens durability but does not change
logical ordering or fail-closed validation behavior.

## Test Coverage

Key tests in `core/mvcc/database/tests.rs`:
- `test_bootstrap_completes_interrupted_checkpoint_with_committed_wal`
- `test_bootstrap_recovers_committed_wal_without_log_file`
- `test_bootstrap_rejects_torn_log_header_with_committed_wal`
- `test_bootstrap_handles_committed_wal_when_log_truncated`
- `test_bootstrap_ignores_wal_frames_without_commit_marker`
- `test_bootstrap_rejects_corrupt_log_header_without_wal`
- `test_empty_log_recovery_loads_checkpoint_watermark`
- `test_meta_checkpoint_case_10_metadata_upsert_is_atomic_with_pager_commit`
- `test_meta_checkpoint_case_11_auto_checkpoint_failure_after_commit_remains_recoverable`

Logical-log corruption and torn-tail tests are in `core/mvcc/persistent_storage/logical_log.rs`.
