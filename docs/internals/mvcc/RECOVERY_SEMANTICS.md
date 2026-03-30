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
| 1 | WAL has committed frames + log header valid | Complete interrupted checkpoint: backfill WAL into DB, sync DB, truncate WAL. Then run logical-log recovery with metadata cutoff. |
| 2 | WAL has committed frames + log header missing (`NoLog`) | Fail closed with `Corrupt`. |
| 3 | WAL has committed frames + log header invalid/torn | Fail closed with `Corrupt`. |
| 4 | WAL has no committed frames | Truncate/discard WAL tail bytes and continue logical-log recovery. |
| 5 | No WAL + log header invalid/torn | Fail closed with `Corrupt`. |
| 6 | No WAL + valid header, no frames (size <= `LOG_HDR_SIZE`) | No replay needed; timestamp state comes from metadata row. |
| 7 | No WAL + empty log (0 bytes / `NoLog`) | Timestamp state loaded from metadata row if present; no replay. |

Notes:
- After checkpoint, the log is truncated to 0 bytes. On restart this is case 7.
- Torn tail in log body is treated as EOF (prefix frames remain valid).
- First invalid frame during forward scan terminates the scan (prefix preserved), matching SQLite WAL availability semantics.
- Missing or corrupt metadata row is treated as corruption when the metadata table is expected to exist.

## Checkpoint Sequence (Blocking Model)

1. Acquire blocking checkpoint lock.
2. Begin pager transaction.
3. Write committed MVCC table/index versions into pager.
4. Upsert metadata row `persistent_tx_ts_max` in the same pager transaction.
5. Commit pager transaction. WAL now contains committed frames for both data and the metadata row. In-memory `durable_txid_max` advances on this transition.
6. Checkpoint WAL (backfill WAL frames into DB file).
7. Fsync DB file (unless `SyncMode::Off`).
8. Truncate logical log to 0 (salt regenerated in memory; header written with next frame).
9. Fsync logical log (unless `SyncMode::Off`).
10. Truncate WAL.
11. Finalize: GC checkpointed versions, release lock.

WAL truncation is last. Until the DB file and logical-log cleanup are durable,
the WAL remains the authoritative recovery source.

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
- `test_bootstrap_rejects_committed_wal_without_log_file`
- `test_bootstrap_rejects_torn_log_header_with_committed_wal`
- `test_bootstrap_handles_committed_wal_when_log_truncated`
- `test_bootstrap_ignores_wal_frames_without_commit_marker`
- `test_bootstrap_rejects_corrupt_log_header_without_wal`
- `test_empty_log_recovery_loads_checkpoint_watermark`
- `test_meta_checkpoint_case_10_metadata_upsert_is_atomic_with_pager_commit`
- `test_meta_checkpoint_case_11_auto_checkpoint_failure_after_commit_remains_recoverable`

Logical-log corruption and torn-tail tests are in `core/mvcc/persistent_storage/logical_log.rs`.
