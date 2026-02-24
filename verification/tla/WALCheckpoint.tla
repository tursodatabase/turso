--------------------------- MODULE WALCheckpoint ---------------------------
(*
 * TLA+ formal verification of the Turso multi-process WAL checkpoint protocol.
 *
 * Models the ordering of durability operations (write, fsync) across multiple
 * processes coordinated via an exclusive write lock. Verifies that no
 * committed data is lost after any combination of process or power crashes,
 * and that readers always see a consistent snapshot.
 *
 * Key mechanism: salt-based frame visibility. After restart_log generates a
 * new salt, old WAL frames become invisible during recovery (salt mismatch
 * with the durable WAL header). The critical invariant is that the new salt
 * must NOT be flushed to disk before the DB file is synced.
 *
 * Source files modeled:
 *   core/storage/multi_process_wal.rs  -- MultiProcessWal coordination
 *   core/storage/wal.rs               -- WalFile, restart_log, checkpoint
 *   core/storage/pager.rs             -- Pager checkpoint state machine
 *   core/storage/tshm.rs              -- TSHM reader slots
 *
 * Abstraction choices:
 *   - TSHM change-detection counter is omitted (performance optimization).
 *     BeginWrite always reads the durable WAL header, which is correct and
 *     necessary: the TSHM bump and header flush are not atomic, so TSHM-based
 *     detection has a race condition. See BeginWrite comment for details.
 *   - WAL frames are modeled as a set of [wid, salt] records, not sequences.
 *   - Write + sync are combined into one atomic step (WriteFrame).
 *   - Reader snapshots track max_frame at read start, modeling TSHM reader
 *     slots. Checkpoint safe-frame computation respects active readers.
 *   - Backfill copies all safe-to-backfill writes in one step. Partial
 *     backfill (crash mid-loop) is modeled via crash at any phase, but the
 *     restart_log guard requires all frames to be backfilled, matching the
 *     implementation's `nbackfills >= max_frame` check.
 *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Procs,          \* Set of process IDs, e.g. {p1, p2}
    MaxWrites       \* Maximum write transactions to model

ASSUME Procs # {}
ASSUME MaxWrites \in Nat \ {0}

None == "none"
WriteIds == 1..MaxWrites

\* Bound on salt values. With MaxWrites writes, we need at most MaxWrites
\* checkpoint restarts (one per batch of frames). Add headroom for crash
\* recovery cycles. This keeps the state space finite while still being
\* sufficient to explore all interesting interleavings.
MaxSalt == MaxWrites + 3

\* Process phases
Phases == {
    "idle",
    "reading",
    "writing",          \* Holds write lock, can write frames or start checkpoint
    "committing",       \* Writing frame, about to commit
    "ckpt_backfill",    \* Checkpoint: copying WAL to DB (buffered)
    "ckpt_restart",     \* Checkpoint: new salt in memory
    "ckpt_dbsync",      \* Checkpoint: DB fsync in progress
    "ckpt_truncwal",    \* Checkpoint (Truncate mode): truncating WAL file to 0
    "wal_hdr_flushing", \* Flushing WAL header (lazy, on next write after restart)
    "crashed"
}

\* All phases where the process holds the write lock.
WriterPhases == {
    "writing", "committing", "wal_hdr_flushing",
    "ckpt_backfill", "ckpt_restart", "ckpt_dbsync", "ckpt_truncwal"
}

VARIABLES
    (* === Durable storage (survives all crashes) === *)
    db_durable,         \* SUBSET WriteIds: pages synced to DB file
    wal_durable,        \* Set of [wid: Nat, salt: Nat]: synced WAL frames
    wal_hdr_durable,    \* [salt: Nat]: synced WAL header

    (* === Buffered storage (page cache, lost on power crash) === *)
    db_buffered,        \* SUBSET WriteIds: pages written to DB, not synced

    (* === Write lock (survives process crash via dead-owner detection) === *)
    wlock,              \* Procs \cup {None}: write lock holder

    (* === Per-process volatile state === *)
    pc,                 \* [Procs -> Phases]: current phase
    mem_salt,           \* [Procs -> Nat]: in-memory active salt
    wal_init,           \* [Procs -> BOOLEAN]: WAL header on disk matches memory
    read_snap,          \* [Procs -> Nat]: reader snapshot (max_frame at read start)

    (* === Global shared in-memory state (per WalFileShared) === *)
    shared_max_frame,   \* Nat: committed frame count (shared.max_frame)
    nbackfills,         \* Nat: frames checkpointed to DB

    (* === Bookkeeping === *)
    committed,          \* SUBSET WriteIds: acknowledged writes
    next_wid,           \* Nat: next write ID counter
    next_salt,          \* Nat: next salt counter
    ckpt_mode           \* [Procs -> {"restart", "truncate", "none"}]: checkpoint mode

vars == <<db_durable, wal_durable, wal_hdr_durable,
          db_buffered, wlock,
          pc, mem_salt, wal_init, read_snap,
          shared_max_frame, nbackfills,
          committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Helpers                                                            *)
(* ================================================================== *)

\* Write IDs visible through the durable WAL: frames whose salt matches
\* the durable WAL header. After restart_log changes the salt, old frames
\* become invisible during recovery.
VisibleWalWrites ==
    {f.wid : f \in {x \in wal_durable : x.salt = wal_hdr_durable.salt}}

\* Set of write IDs recoverable from durable state after any crash.
RecoverableWrites == db_durable \cup VisibleWalWrites

\* Minimum reader snapshot across all active readers (0 if no readers).
\* Models TSHM min_reader_frame_excluding() and determine_max_safe_checkpoint_frame().
\* The checkpointer must not backfill past this boundary.
MinReaderSnapshot ==
    LET active_readers == {p \in Procs : pc[p] = "reading" /\ read_snap[p] > 0}
    IN IF active_readers = {} THEN 0
       ELSE CHOOSE m \in {read_snap[p] : p \in active_readers} :
            \A p \in active_readers : m <= read_snap[p]

\* Safe checkpoint frame: min of shared_max_frame and MinReaderSnapshot.
\* If no readers, checkpoint can go up to shared_max_frame.
SafeCheckpointFrame ==
    LET min_reader == MinReaderSnapshot
    IN IF min_reader = 0 THEN shared_max_frame
       ELSE IF min_reader < shared_max_frame THEN min_reader
       ELSE shared_max_frame

\* WAL writes that are safe to backfill (respecting reader snapshots).
\* Only frames up to SafeCheckpointFrame can be copied to the DB file.
SafeBackfillWrites ==
    LET safe == SafeCheckpointFrame
        visible == {x \in wal_durable : x.salt = wal_hdr_durable.salt}
    IN {f.wid : f \in {x \in visible : x.wid <= safe}}

(* ================================================================== *)
(* Init                                                               *)
(* ================================================================== *)

Init ==
    /\ db_durable = {}
    /\ wal_durable = {}
    /\ wal_hdr_durable = [salt |-> 1]
    /\ db_buffered = {}
    /\ wlock = None
    /\ pc = [p \in Procs |-> "idle"]
    /\ mem_salt = [p \in Procs |-> 1]
    /\ wal_init = [p \in Procs |-> TRUE]
    /\ read_snap = [p \in Procs |-> 0]
    /\ shared_max_frame = 0
    /\ nbackfills = 0
    /\ committed = {}
    /\ next_wid = 1
    /\ next_salt = 2
    /\ ckpt_mode = [p \in Procs |-> "none"]

(* ================================================================== *)
(* Read Transaction                                                   *)
(* ================================================================== *)

\* Begin read: snapshot the current shared_max_frame.
\* Models try_begin_read_tx() acquiring a read mark in the WAL read locks,
\* and TSHM reader slot registration with [pid, max_frame].
BeginRead(p) ==
    /\ pc[p] = "idle"
    /\ read_snap' = [read_snap EXCEPT ![p] = shared_max_frame]
    /\ pc' = [pc EXCEPT ![p] = "reading"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* End read: release snapshot.
\* Models end_read_tx() releasing the read lock and TSHM reader slot.
EndRead(p) ==
    /\ pc[p] = "reading"
    /\ read_snap' = [read_snap EXCEPT ![p] = 0]
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Write Transaction                                                  *)
(* ================================================================== *)

\* Acquire write lock and read the durable WAL header.
\*
\* CRITICAL: We MUST read the durable header unconditionally here, not rely
\* on TSHM change detection. The reason: another process may have flushed a
\* new WAL header (changing the salt via prepare_wal_start after a checkpoint
\* restart) and then crashed before bumping the TSHM writer state counter.
\* The TSHM bump and header flush are NOT atomic. If we only check TSHM, we
\* might miss the header change and write frames with the wrong salt — making
\* them invisible after recovery (salt mismatch) and causing data loss.
\*
\* In the implementation (multi_process_wal.rs), this corresponds to calling
\* rescan_wal_from_disk() in begin_write_tx() after acquiring the write lock.
BeginWrite(p) ==
    /\ pc[p] = "reading"
    /\ wlock = None
    /\ wlock' = p
    \* Always read the durable WAL header to get the current salt.
    /\ mem_salt' = [mem_salt EXCEPT ![p] = wal_hdr_durable.salt]
    /\ wal_init' = [wal_init EXCEPT ![p] = TRUE]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Write a frame to the WAL and sync it. WAL header must be on disk first.
WriteFrame(p) ==
    /\ pc[p] = "writing"
    /\ next_wid <= MaxWrites
    /\ wal_init[p] = TRUE
    /\ LET frame == [wid |-> next_wid, salt |-> mem_salt[p]]
       IN
       /\ wal_durable' = wal_durable \cup {frame}
       /\ next_wid' = next_wid + 1
    /\ pc' = [pc EXCEPT ![p] = "committing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_salt, ckpt_mode>>

\* Commit: update shared_max_frame, record committed write.
\* Process stays in "writing" (still holds lock).
Commit(p) ==
    /\ pc[p] = "committing"
    /\ committed' = committed \cup {next_wid - 1}
    /\ shared_max_frame' = shared_max_frame + 1
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   nbackfills,
                   next_wid, next_salt, ckpt_mode>>

\* Release write lock and return to reading.
EndWrite(p) ==
    /\ pc[p] = "writing"
    /\ wlock' = None
    /\ pc' = [pc EXCEPT ![p] = "reading"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Checkpoint                                                         *)
(* ================================================================== *)

\* Backfill WAL frames to DB (buffered, not durable yet).
\* Copies ALL safe-to-backfill writes respecting active reader snapshots.
\* The safe boundary is determined by SafeCheckpointFrame, modeling
\* determine_max_safe_checkpoint_frame() in wal.rs.
\*
\* The checkpoint mode (Restart or Truncate) is chosen non-deterministically.
\* Both modes share the backfill and DB sync phases. Truncate adds a WAL
\* file truncation step after DB sync.
CkptBackfill(p) ==
    /\ pc[p] = "writing"
    /\ nbackfills < shared_max_frame
    /\ LET safe == SafeBackfillWrites
       IN /\ safe # {}
          /\ db_buffered' = db_buffered \cup safe
    \* Non-deterministically choose Restart or Truncate mode.
    /\ \E mode \in {"restart", "truncate"}:
         ckpt_mode' = [ckpt_mode EXCEPT ![p] = mode]
    /\ pc' = [pc EXCEPT ![p] = "ckpt_backfill"]
    /\ UNCHANGED <<db_durable, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

\* Restart the WAL log: new salt in memory, reset shared_max_frame and nbackfills.
\* CRITICAL: does NOT flush header to disk.
\*
\* Guard: VisibleWalWrites must all be in db_buffered (all frames backfilled).
\* This matches the implementation's check: should_restart_log() is only called
\* when nbackfills >= max_frame (see wal.rs checkpoint processing loop).
\* If readers blocked full backfill, CkptFinalizePartial fires instead.
\*
\* Models restart_wal_header() in wal.rs which sets both max_frame and
\* nbackfills to 0 atomically.
CkptRestart(p) ==
    /\ pc[p] = "ckpt_backfill"
    /\ VisibleWalWrites \subseteq db_buffered    \* All frames were backfilled
    /\ next_salt <= MaxSalt      \* Bound salt counter to keep state space finite
    /\ nbackfills' = 0
    /\ LET new_salt == next_salt
       IN
       /\ next_salt' = next_salt + 1
       /\ mem_salt' = [mem_salt EXCEPT ![p] = new_salt]
       /\ wal_init' = [wal_init EXCEPT ![p] = FALSE]
    /\ shared_max_frame' = 0
    /\ pc' = [pc EXCEPT ![p] = "ckpt_restart"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, read_snap,
                   committed, next_wid, ckpt_mode>>

\* Finalize a partial checkpoint (Passive-style): readers blocked full backfill,
\* so the WAL log is NOT restarted. Return to writing.
\* This models the case where determine_max_safe_checkpoint_frame() < max_frame
\* and the checkpoint completes without calling restart_log.
CkptFinalizePartial(p) ==
    /\ pc[p] = "ckpt_backfill"
    /\ ~(VisibleWalWrites \subseteq db_buffered)    \* NOT all frames backfilled
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

\* Fsync DB file: buffered DB pages become durable.
CkptSyncDb(p) ==
    /\ pc[p] = "ckpt_restart"
    /\ db_durable' = db_durable \cup db_buffered
    \* Truncate mode continues to ckpt_truncwal; Restart mode goes to ckpt_dbsync.
    /\ pc' = [pc EXCEPT ![p] = IF ckpt_mode[p] = "truncate"
                                THEN "ckpt_truncwal"
                                ELSE "ckpt_dbsync"]
    /\ UNCHANGED <<db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Truncate WAL file to zero bytes (Truncate mode only).
\* This happens AFTER CkptSyncDb has made the DB durable, so all backfilled
\* data is safe on disk. Truncating the WAL destroys ALL frames — this is
\* safe because every committed frame was already synced to db_durable via
\* the backfill+sync path (CkptRestart's guard ensures full backfill).
\*
\* In the implementation (pager.rs), this is the TruncateWalFile phase that
\* runs after SyncDbFile. See wal.rs truncate_log() which calls file.truncate(0).
CkptTruncateWal(p) ==
    /\ pc[p] = "ckpt_truncwal"
    /\ wal_durable' = {}
    /\ pc' = [pc EXCEPT ![p] = "ckpt_dbsync"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Finalize checkpoint: return to writing (still holding lock).
CkptFinalize(p) ==
    /\ pc[p] = "ckpt_dbsync"
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

(* ================================================================== *)
(* Lazy WAL Header Flush (prepare_wal_start on next write)            *)
(* ================================================================== *)

\* Write + sync new WAL header to disk. Called when wal_init is FALSE
\* (after restart_log changed the salt). This happens AFTER CkptSyncDb
\* has made the DB durable, so it is safe for the writing process.
\* Other processes pick up the new salt via BeginWrite's unconditional
\* header read.
FlushWalHeader(p) ==
    /\ pc[p] = "writing"
    /\ wal_init[p] = FALSE
    /\ wal_hdr_durable' = [salt |-> mem_salt[p]]
    /\ wal_init' = [wal_init EXCEPT ![p] = TRUE]
    /\ pc' = [pc EXCEPT ![p] = "wal_hdr_flushing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable,
                   wlock, mem_salt, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* After header flush, return to writing to write frames.
FlushWalHeaderDone(p) ==
    /\ pc[p] = "wal_hdr_flushing"
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Crash and Recovery                                                 *)
(* ================================================================== *)

\* Process crash: volatile state lost, write lock released.
\* db_buffered is NOT cleared because the OS page cache survives a process
\* crash — only a power crash (PowerCrash) destroys buffered DB pages.
ProcessCrash(p) ==
    /\ pc[p] # "crashed"
    /\ pc' = [pc EXCEPT ![p] = "crashed"]
    /\ wlock' = IF wlock = p THEN None ELSE wlock
    /\ mem_salt' = [mem_salt EXCEPT ![p] = 0]
    /\ wal_init' = [wal_init EXCEPT ![p] = FALSE]
    /\ read_snap' = [read_snap EXCEPT ![p] = 0]
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

\* Power crash: ALL non-durable state is lost.
PowerCrash ==
    /\ \E p \in Procs: pc[p] # "crashed"
    /\ db_buffered' = db_durable
    /\ wlock' = None
    /\ pc' = [p \in Procs |-> "crashed"]
    /\ mem_salt' = [p \in Procs |-> 0]
    /\ wal_init' = [p \in Procs |-> FALSE]
    /\ read_snap' = [p \in Procs |-> 0]
    /\ shared_max_frame' = 0
    /\ nbackfills' = 0
    /\ ckpt_mode' = [p \in Procs |-> "none"]
    /\ UNCHANGED <<db_durable, wal_durable, wal_hdr_durable,
                   committed, next_wid, next_salt>>

\* Recovery: rebuild from durable state.
\* Each process recovers independently. shared_max_frame is rebuilt from
\* the durable WAL (visible frames). In a real multi-process system,
\* rescan_wal_from_disk() is per-connection, but the shared WalFileShared
\* is rebuilt on first open. We model recovery as setting shared_max_frame
\* to the visible frame count, which is correct for the first recovering
\* process. Subsequent recoverers read the same shared state.
Recover(p) ==
    /\ pc[p] = "crashed"
    /\ mem_salt' = [mem_salt EXCEPT ![p] = wal_hdr_durable.salt]
    /\ wal_init' = [wal_init EXCEPT ![p] = TRUE]
    /\ shared_max_frame' = Cardinality(VisibleWalWrites)
    /\ nbackfills' = 0
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, read_snap,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Next-state relation                                                *)
(* ================================================================== *)

Next ==
    \/ \E p \in Procs:
        \/ BeginRead(p)
        \/ EndRead(p)
        \/ BeginWrite(p)
        \/ WriteFrame(p)
        \/ Commit(p)
        \/ EndWrite(p)
        \/ CkptBackfill(p)
        \/ CkptRestart(p)
        \/ CkptFinalizePartial(p)
        \/ CkptSyncDb(p)
        \/ CkptTruncateWal(p)
        \/ CkptFinalize(p)
        \/ FlushWalHeader(p)
        \/ FlushWalHeaderDone(p)
        \/ ProcessCrash(p)
        \/ Recover(p)
    \/ PowerCrash

Spec == Init /\ [][Next]_vars

(* ================================================================== *)
(* Safety Properties                                                  *)
(* ================================================================== *)

\* PRIMARY INVARIANT: No committed data is ever lost.
NoCrashDataLoss ==
    committed \subseteq RecoverableWrites

\* At most one process in a writer phase at a time.
WriterExclusion ==
    \A p, q \in Procs:
        (p # q) => ~(pc[p] \in WriterPhases /\ pc[q] \in WriterPhases)

\* No reader observes a torn snapshot: every frame in a reader's snapshot
\* range must be recoverable. If a reader holds a snapshot at frame S,
\* all committed writes with wid <= S must be in RecoverableWrites.
\* This ensures that checkpoint backfill does not overwrite DB pages that
\* a reader is still accessing via the WAL.
NoStaleReads ==
    \A p \in Procs:
        pc[p] = "reading" /\ read_snap[p] > 0 =>
            \A wid \in committed:
                wid <= read_snap[p] => wid \in RecoverableWrites

\* Type invariant
TypeOK ==
    /\ db_durable \subseteq WriteIds
    /\ db_buffered \subseteq WriteIds
    /\ committed \subseteq WriteIds
    /\ wlock \in Procs \cup {None}
    /\ \A p \in Procs: pc[p] \in Phases
    /\ \A p \in Procs: read_snap[p] \in Nat
    /\ \A p \in Procs: ckpt_mode[p] \in {"restart", "truncate", "none"}
    /\ next_wid \in 1..(MaxWrites + 1)
    /\ shared_max_frame \in Nat
    /\ nbackfills \in Nat
    /\ nbackfills <= shared_max_frame
    /\ wal_hdr_durable.salt \in 1..MaxSalt

=============================================================================
