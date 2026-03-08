--------------------------- MODULE WALCheckpoint ---------------------------
(*
 * TLA+ formal verification of the Turso multi-process WAL checkpoint protocol
 * with WalIndex shared mmap and TSHM seqlock-style reader validation.
 *
 * Models the ordering of durability operations (write, fsync) across multiple
 * processes coordinated via an exclusive write lock. Verifies that no
 * committed data is lost after any combination of process or power crashes,
 * and that readers always see a consistent snapshot.
 *
 * Key mechanisms:
 *
 * 1. Salt-based frame visibility. After restart_log generates a new salt,
 *    old WAL frames become invisible during recovery (salt mismatch with the
 *    durable WAL header). The critical invariant is that the new salt must
 *    NOT be flushed to disk before the DB file is synced.
 *
 * 2. WalIndex shared mmap. A zero-I/O frame lookup structure stored in
 *    shared memory. Writers append page->frame mappings and commit max_frame
 *    atomically. Readers trust the WalIndex for frame lookups — no fallback
 *    to WAL file scanning. The WalIndex has a generation counter that is
 *    bumped on clear(); readers cache this generation at begin_read_tx and
 *    assert it hasn't changed during find_frame.
 *
 * 3. TSHM writer_state + seqlock. Readers load the TSHM writer_state counter
 *    (S1), sync from WalIndex, acquire read locks, claim a TSHM reader slot,
 *    then re-load writer_state (S2). If S1 != S2, a writer modified the WAL
 *    concurrently — the reader releases everything and retries. This closes
 *    the TOCTOU gap between sync and slot claim.
 *
 * Source files modeled:
 *   core/storage/multi_process_wal.rs  -- MultiProcessWal coordination
 *   core/storage/wal.rs               -- WalFile, restart_log, checkpoint
 *   core/storage/pager.rs             -- Pager checkpoint state machine
 *   core/storage/tshm.rs              -- TSHM reader slots, writer_state
 *   core/storage/wal_index.rs          -- WalIndex shared mmap
 *
 * Abstraction choices:
 *   - WalIndex frame mappings are modeled as a set of [wid, salt] records.
 *     find_frame queries this set. Hash collisions, bucket layout, and the
 *     append-only log are abstracted away.
 *   - WalIndex generation is a monotonic counter bumped on clear().
 *   - TSHM writer_state is a monotonic counter bumped by the writer after
 *     committing frames or checkpointing. Readers use it as a seqlock.
 *   - sync_shared_from_wal_index is modeled as reading wi_max_frame and
 *     wi_salt from shared state (zero I/O).
 *   - The "never decrease max_frame" heuristic in sync_shared_from_wal_index
 *     is modeled.
 *   - WAL frames are modeled as a set of [wid, salt] records, not sequences.
 *   - Write + sync are combined into one atomic step (WriteFrame).
 *   - Backfill copies all safe-to-backfill writes in one step.
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
\* checkpoint restarts. Add headroom for crash recovery cycles.
MaxSalt == MaxWrites + 3

\* Bound on WalIndex generation and TSHM writer_state counters.
\* Each write commit bumps writer_state, and each checkpoint clear bumps
\* generation. Checkpoint actions now also bump writer_state BEFORE
\* modifying the WalIndex (seqlock protocol fix). Bound to keep state
\* space finite.
\* With MaxWrites=2: one full write+checkpoint cycle needs ~8 tshm bumps
\* (begin_write+1, commit+1, ckpt_restart+1, ckpt_truncwal+1, ckpt_finalize+1,
\* end_write+1, plus checkpoint rebuild bump). Two processes need ~16.
\* wi_generation needs ~4 per cycle. MaxWrites * 4 + 6 gives enough headroom.
MaxCounter == MaxWrites * 4 + 6

\* Process phases
Phases == {
    "idle",
    "read_sync",        \* begin_read_tx: synced from WalIndex, before slot claim
    "reading",          \* Active read transaction
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

    (* === WalIndex: shared mmap state (survives process crash, lost on power crash) === *)
    wi_frames,          \* Set of [wid: Nat, salt: Nat]: frame entries in WalIndex
    wi_max_frame,       \* Nat: committed max_frame visible to readers
    wi_salt,            \* Nat: salt stored in WalIndex header
    wi_generation,      \* Nat: generation counter (bumped on clear)

    (* === Per-process cached WalIndex generation === *)
    cached_wi_gen,      \* [Procs -> Nat \cup {MaxCounter+1}]: cached generation
                        \*   MaxCounter+1 is the sentinel (u32::MAX equivalent)

    (* === TSHM writer_state: shared counter (survives process crash, lost on power crash) === *)
    tshm_writer_state,  \* Nat: writer state counter
    cached_tshm_state,  \* [Procs -> Nat]: per-process cached writer_state

    (* === Seqlock: per-process S1 snapshot for begin_read_tx === *)
    read_s1,            \* [Procs -> Nat]: writer_state at start of begin_read_tx

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
          wi_frames, wi_max_frame, wi_salt, wi_generation,
          cached_wi_gen, tshm_writer_state, cached_tshm_state, read_s1,
          shared_max_frame, nbackfills,
          committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Constants                                                          *)
(* ================================================================== *)

\* Sentinel value for "no generation cached yet" (u32::MAX equivalent).
SentinelGen == MaxCounter + 1

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

\* WalIndex-visible writes: entries in the WalIndex whose salt matches
\* the WalIndex header salt AND whose wid <= wi_max_frame.
WalIndexVisibleWrites ==
    {f.wid : f \in {x \in wi_frames : x.salt = wi_salt /\ x.wid <= wi_max_frame}}

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
    /\ wi_frames = {}
    /\ wi_max_frame = 0
    /\ wi_salt = 1
    /\ wi_generation = 0
    /\ cached_wi_gen = [p \in Procs |-> SentinelGen]
    /\ tshm_writer_state = 0
    /\ cached_tshm_state = [p \in Procs |-> 0]
    /\ read_s1 = [p \in Procs |-> 0]
    /\ shared_max_frame = 0
    /\ nbackfills = 0
    /\ committed = {}
    /\ next_wid = 1
    /\ next_salt = 2
    /\ ckpt_mode = [p \in Procs |-> "none"]

(* ================================================================== *)
(* Read Transaction (seqlock-style)                                   *)
(* ================================================================== *)

\* Phase 1 of begin_read_tx: load writer_state (S1) and sync from WalIndex.
\*
\* Models the first half of the seqlock loop in begin_read_tx():
\*   1. Load S1 = tshm.writer_state()
\*   2. If S1 != cached, call sync_shared_from_wal_index()
\*   3. Call inner.begin_read_tx() (acquire inner read lock)
\*   4. Cache WalIndex generation
\*
\* sync_shared_from_wal_index() updates shared_max_frame from WalIndex
\* (only increasing, never decreasing) and syncs the salt. This is the
\* zero-I/O fast path — no WAL file reads.
\*
\* After this step, the process has synced state from the WalIndex mmap
\* and acquired the inner read lock, but has NOT yet claimed a TSHM reader
\* slot. A writer could still interleave.
BeginReadSync(p) ==
    /\ pc[p] = "idle"
    /\ LET s1 == tshm_writer_state
       IN
       \* sync_shared_from_wal_index: update shared_max_frame from WalIndex
       \* (only increase, never decrease — modeling the heuristic in the code).
       /\ shared_max_frame' = IF wi_max_frame > shared_max_frame
                              THEN wi_max_frame
                              ELSE shared_max_frame
       \* Sync salt from WalIndex: if WalIndex has a non-zero salt, adopt it.
       \* This models set_wal_salt() in sync_shared_from_wal_index.
       /\ mem_salt' = [mem_salt EXCEPT ![p] = IF wi_salt > 0
                                              THEN wi_salt
                                              ELSE mem_salt[p]]
       \* Snapshot WalIndex generation so find_frame can validate it.
       /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = wi_generation]
       \* Snapshot max_frame for the read transaction.
       \* Uses the post-sync value (max of wi_max_frame, shared_max_frame).
       /\ read_snap' = [read_snap EXCEPT ![p] =
                            IF wi_max_frame > shared_max_frame
                            THEN wi_max_frame
                            ELSE shared_max_frame]
       \* Remember S1 for the seqlock check in BeginReadValidate.
       /\ read_s1' = [read_s1 EXCEPT ![p] = s1]
       /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] = s1]
    /\ pc' = [pc EXCEPT ![p] = "read_sync"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, wal_init,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   tshm_writer_state,
                   nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Phase 2 of begin_read_tx: claim TSHM slot and validate seqlock.
\*
\* Models the second half of the seqlock loop:
\*   5. Claim TSHM reader slot with [pid, max_frame]
\*   6. Re-load S2 = tshm.writer_state()
\*   7. If S2 != S1: release slot, release inner lock, retry (go back to idle)
\*   8. If S2 == S1: commit — transition to "reading"
\*
\* The TSHM slot claim is modeled implicitly by the read_snap[p] value.
\* The slot claim publishes max_frame so that checkpoint respects this reader.
BeginReadValidate(p) ==
    /\ pc[p] = "read_sync"
    /\ LET s2 == tshm_writer_state
       IN IF s2 = read_s1[p]
          THEN
            \* Seqlock valid: writer_state didn't change during our sync.
            \* Commit to reading phase.
            /\ pc' = [pc EXCEPT ![p] = "reading"]
            /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                           wlock, mem_salt, wal_init, read_snap,
                           wi_frames, wi_max_frame, wi_salt, wi_generation,
                           cached_wi_gen, tshm_writer_state, cached_tshm_state,
                           read_s1,
                           shared_max_frame, nbackfills,
                           committed, next_wid, next_salt, ckpt_mode>>
          ELSE
            \* Seqlock INVALID: writer modified WAL concurrently.
            \* Release everything and go back to idle for retry.
            /\ read_snap' = [read_snap EXCEPT ![p] = 0]
            /\ pc' = [pc EXCEPT ![p] = "idle"]
            /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                           wlock, mem_salt, wal_init,
                           wi_frames, wi_max_frame, wi_salt, wi_generation,
                           cached_wi_gen, tshm_writer_state, cached_tshm_state,
                           read_s1,
                           shared_max_frame, nbackfills,
                           committed, next_wid, next_salt, ckpt_mode>>

\* End read: release snapshot and TSHM slot.
\* Models end_read_tx() releasing the read lock and TSHM reader slot.
EndRead(p) ==
    /\ pc[p] = "reading"
    /\ read_snap' = [read_snap EXCEPT ![p] = 0]
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, tshm_writer_state, cached_tshm_state,
                   read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Write Transaction                                                  *)
(* ================================================================== *)

\* Acquire write lock, rescan from disk, manage WalIndex, publish state.
\*
\* CRITICAL: We MUST read the durable header unconditionally here, not rely
\* on TSHM change detection. The reason: another process may have flushed a
\* new WAL header (changing the salt via prepare_wal_start after a checkpoint
\* restart) and then crashed before bumping the TSHM writer state counter.
\* The TSHM bump and header flush are NOT atomic.
\*
\* After acquiring the write lock, the writer also:
\*   1. Rescans WAL from disk (getting current salt)
\*   2. Detects WAL restart (salt mismatch) and clears WalIndex
\*   3. Repopulates WalIndex from durable WAL if WalIndex is empty
\*   4. Publishes writer_state so readers detect the change
\*
\* In the implementation (multi_process_wal.rs), this corresponds to
\* begin_write_tx() calling maybe_rescan_from_tshm() + inner.begin_write_tx()
\* + clear_wal_index/populate_wal_index + publish_writer_state().
BeginWrite(p) ==
    /\ pc[p] = "reading"
    /\ wlock = None
    /\ wlock' = p
    \* Always read the durable WAL header to get the current salt.
    /\ mem_salt' = [mem_salt EXCEPT ![p] = wal_hdr_durable.salt]
    /\ wal_init' = [wal_init EXCEPT ![p] = TRUE]
    \* Detect WAL restart: if WalIndex salt doesn't match the durable
    \* header salt, clear it and repopulate from durable WAL.
    /\ IF wi_salt # wal_hdr_durable.salt
       THEN
         \* Clear WalIndex with current salt and bump generation.
         \* Then repopulate from durable visible WAL frames.
         /\ LET new_gen == IF wi_generation < MaxCounter
                           THEN wi_generation + 1
                           ELSE wi_generation
                visible == {x \in wal_durable : x.salt = wal_hdr_durable.salt}
            IN
            /\ wi_frames' = visible
            /\ wi_max_frame' = Cardinality({f.wid : f \in visible})
            /\ wi_salt' = wal_hdr_durable.salt
            /\ wi_generation' = new_gen
            /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = new_gen]
       ELSE IF wi_max_frame = 0 /\ shared_max_frame > 0
       THEN
         \* WalIndex empty but WAL has frames: populate (bootstrap).
         /\ LET new_gen == IF wi_generation < MaxCounter
                           THEN wi_generation + 1
                           ELSE wi_generation
                visible == {x \in wal_durable : x.salt = wal_hdr_durable.salt}
            IN
            /\ wi_frames' = visible
            /\ wi_max_frame' = Cardinality({f.wid : f \in visible})
            /\ wi_salt' = wi_salt
            /\ wi_generation' = new_gen
            /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = new_gen]
       ELSE
         \* WalIndex is up-to-date.
         /\ wi_frames' = wi_frames
         /\ wi_max_frame' = wi_max_frame
         /\ wi_salt' = wi_salt
         /\ wi_generation' = wi_generation
         /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = wi_generation]
    \* Publish writer_state so readers detect this write-lock acquisition.
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   read_snap, read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Write a frame to the WAL (durable) and to the WalIndex (shared mmap).
\* WAL header must be on disk first (wal_init = TRUE).
WriteFrame(p) ==
    /\ pc[p] = "writing"
    /\ next_wid <= MaxWrites
    /\ wal_init[p] = TRUE
    /\ LET frame == [wid |-> next_wid, salt |-> mem_salt[p]]
       IN
       /\ wal_durable' = wal_durable \cup {frame}
       \* Append to WalIndex (writer only). The frame is written but
       \* wi_max_frame is not updated yet — that happens at Commit.
       /\ wi_frames' = wi_frames \cup {frame}
       /\ next_wid' = next_wid + 1
    /\ pc' = [pc EXCEPT ![p] = "committing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, tshm_writer_state, cached_tshm_state,
                   read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_salt, ckpt_mode>>

\* Commit: update shared_max_frame, WalIndex max_frame, and bump
\* TSHM writer_state so readers detect the commit.
\* Process stays in "writing" (still holds lock).
Commit(p) ==
    /\ pc[p] = "committing"
    /\ committed' = committed \cup {next_wid - 1}
    /\ shared_max_frame' = shared_max_frame + 1
    \* Commit WalIndex max_frame atomically so readers see the new frame.
    /\ wi_max_frame' = shared_max_frame + 1
    \* Bump TSHM writer_state so readers detect the commit.
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_frames, wi_salt, wi_generation,
                   cached_wi_gen, read_s1,
                   nbackfills,
                   next_wid, next_salt, ckpt_mode>>

\* Release write lock and return to reading.
\* Publish writer_state one final time before releasing lock.
EndWrite(p) ==
    /\ pc[p] = "writing"
    /\ wlock' = None
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "reading"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   mem_salt, wal_init, read_snap,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Checkpoint                                                         *)
(* ================================================================== *)

\* Backfill WAL frames to DB (buffered, not durable yet).
\* Copies ALL safe-to-backfill writes respecting active reader snapshots.
\* Before backfill, invalidate cached WalIndex generation (sentinel)
\* so find_frame falls through to inner during the checkpoint operation.
\*
\* The checkpoint mode (Restart or Truncate) is chosen non-deterministically.
CkptBackfill(p) ==
    /\ pc[p] = "writing"
    /\ nbackfills < shared_max_frame
    /\ LET safe == SafeBackfillWrites
       IN /\ safe # {}
          /\ db_buffered' = db_buffered \cup safe
    \* Non-deterministically choose Restart or Truncate mode.
    /\ \E mode \in {"restart", "truncate"}:
         ckpt_mode' = [ckpt_mode EXCEPT ![p] = mode]
    \* Invalidate WalIndex generation — bypass WalIndex during checkpoint.
    \* Models: cached_wal_index_generation.store(u32::MAX) in checkpoint().
    /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = SentinelGen]
    /\ pc' = [pc EXCEPT ![p] = "ckpt_backfill"]
    /\ UNCHANGED <<db_durable, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   tshm_writer_state, cached_tshm_state, read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

\* Restart the WAL log: new salt in memory, reset shared_max_frame
\* and nbackfills, clear WalIndex with the new salt and bump generation.
\* CRITICAL: does NOT flush WAL header to disk.
\*
\* Guard: VisibleWalWrites must all be in db_buffered (all frames backfilled).
\* If readers blocked full backfill, CkptFinalizePartial fires instead.
CkptRestart(p) ==
    /\ pc[p] = "ckpt_backfill"
    /\ VisibleWalWrites \subseteq db_buffered    \* All frames were backfilled
    /\ next_salt <= MaxSalt
    /\ nbackfills' = 0
    /\ LET new_salt == next_salt
           new_gen == IF wi_generation < MaxCounter
                      THEN wi_generation + 1
                      ELSE wi_generation
       IN
       /\ next_salt' = next_salt + 1
       /\ mem_salt' = [mem_salt EXCEPT ![p] = new_salt]
       /\ wal_init' = [wal_init EXCEPT ![p] = FALSE]
       \* Clear WalIndex with new salt and bump generation.
       /\ wi_frames' = {}
       /\ wi_max_frame' = 0
       /\ wi_salt' = new_salt
       /\ wi_generation' = new_gen
       /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = new_gen]
    /\ shared_max_frame' = 0
    \* Bump tshm_writer_state BEFORE WalIndex modification so readers
    \* detect the change via the seqlock. Without this, a reader could
    \* cache the new WalIndex generation between its sync and seqlock
    \* check without detecting the interleaving.
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "ckpt_restart"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, read_snap, read_s1,
                   committed, next_wid, ckpt_mode>>

\* Finalize a partial checkpoint: readers blocked full backfill.
\* Rebuild WalIndex from the remaining WAL frames.
\* The WalIndex was invalidated (sentinel gen) at CkptBackfill start;
\* now re-clear and repopulate with current WAL state.
CkptFinalizePartial(p) ==
    /\ pc[p] = "ckpt_backfill"
    /\ ~(VisibleWalWrites \subseteq db_buffered)    \* NOT all frames backfilled
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    \* Rebuild WalIndex after partial checkpoint.
    /\ LET new_gen == IF wi_generation < MaxCounter
                      THEN wi_generation + 1
                      ELSE wi_generation
           current_salt == mem_salt[p]
           remaining == {x \in wal_durable : x.salt = wal_hdr_durable.salt}
       IN
       /\ wi_frames' = remaining
       /\ wi_max_frame' = shared_max_frame
       /\ wi_salt' = current_salt
       /\ wi_generation' = new_gen
       /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = new_gen]
    \* Bump tshm_writer_state BEFORE WalIndex modification (seqlock protocol).
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap, read_s1,
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
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, tshm_writer_state, cached_tshm_state,
                   read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Truncate WAL file to zero bytes (Truncate mode only).
\* This happens AFTER CkptSyncDb has made the DB durable, so all backfilled
\* data is safe on disk. Also clear WalIndex since all frames are destroyed.
CkptTruncateWal(p) ==
    /\ pc[p] = "ckpt_truncwal"
    /\ wal_durable' = {}
    \* Clear WalIndex (frames destroyed by truncation).
    /\ LET new_gen == IF wi_generation < MaxCounter
                      THEN wi_generation + 1
                      ELSE wi_generation
       IN
       /\ wi_frames' = {}
       /\ wi_max_frame' = 0
       /\ wi_generation' = new_gen
       /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = new_gen]
    \* Bump tshm_writer_state BEFORE WalIndex modification (seqlock protocol).
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "ckpt_dbsync"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_salt, read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* Finalize checkpoint: publish writer_state and return to writing.
CkptFinalize(p) ==
    /\ pc[p] = "ckpt_dbsync"
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    \* Publish writer_state so readers detect the checkpoint.
    /\ tshm_writer_state' = IF tshm_writer_state < MaxCounter
                            THEN tshm_writer_state + 1
                            ELSE tshm_writer_state
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] =
                                IF tshm_writer_state < MaxCounter
                                THEN tshm_writer_state + 1
                                ELSE tshm_writer_state]
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, read_s1,
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
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, tshm_writer_state, cached_tshm_state,
                   read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

\* After header flush, return to writing to write frames.
FlushWalHeaderDone(p) ==
    /\ pc[p] = "wal_hdr_flushing"
    /\ pc' = [pc EXCEPT ![p] = "writing"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, mem_salt, wal_init, read_snap,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   cached_wi_gen, tshm_writer_state, cached_tshm_state,
                   read_s1,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Crash and Recovery                                                 *)
(* ================================================================== *)

\* Process crash: per-process volatile state lost, write lock released.
\* WalIndex (shared mmap) and TSHM writer_state survive process crash.
\* db_buffered is NOT cleared because the OS page cache survives a process
\* crash — only a power crash (PowerCrash) destroys buffered DB pages.
ProcessCrash(p) ==
    /\ pc[p] # "crashed"
    /\ pc' = [pc EXCEPT ![p] = "crashed"]
    /\ wlock' = IF wlock = p THEN None ELSE wlock
    /\ mem_salt' = [mem_salt EXCEPT ![p] = 0]
    /\ wal_init' = [wal_init EXCEPT ![p] = FALSE]
    /\ read_snap' = [read_snap EXCEPT ![p] = 0]
    /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = SentinelGen]
    /\ cached_tshm_state' = [cached_tshm_state EXCEPT ![p] = 0]
    /\ read_s1' = [read_s1 EXCEPT ![p] = 0]
    /\ ckpt_mode' = [ckpt_mode EXCEPT ![p] = "none"]
    \* Shared mmap state survives: wi_*, tshm_writer_state unchanged.
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   tshm_writer_state,
                   shared_max_frame, nbackfills,
                   committed, next_wid, next_salt>>

\* Power crash: ALL non-durable state is lost — including shared mmap.
\* Only db_durable, wal_durable, and wal_hdr_durable survive.
PowerCrash ==
    /\ \E p \in Procs: pc[p] # "crashed"
    /\ db_buffered' = db_durable
    /\ wlock' = None
    /\ pc' = [p \in Procs |-> "crashed"]
    /\ mem_salt' = [p \in Procs |-> 0]
    /\ wal_init' = [p \in Procs |-> FALSE]
    /\ read_snap' = [p \in Procs |-> 0]
    \* WalIndex lost (mmap backed by tmpfs / not durable).
    /\ wi_frames' = {}
    /\ wi_max_frame' = 0
    /\ wi_salt' = 0
    /\ wi_generation' = 0
    /\ cached_wi_gen' = [p \in Procs |-> SentinelGen]
    /\ tshm_writer_state' = 0
    /\ cached_tshm_state' = [p \in Procs |-> 0]
    /\ read_s1' = [p \in Procs |-> 0]
    /\ shared_max_frame' = 0
    /\ nbackfills' = 0
    /\ ckpt_mode' = [p \in Procs |-> "none"]
    /\ UNCHANGED <<db_durable, wal_durable, wal_hdr_durable,
                   committed, next_wid, next_salt>>

\* Recovery: rebuild from durable state.
\* shared_max_frame rebuilt from visible WAL frames. WalIndex populated
\* during the first writer's begin_write_tx (modeled by BeginWrite's
\* populate logic). For recovery, we set cached_wi_gen to sentinel —
\* the first BeginWrite will detect the stale WalIndex and repopulate.
Recover(p) ==
    /\ pc[p] = "crashed"
    /\ mem_salt' = [mem_salt EXCEPT ![p] = wal_hdr_durable.salt]
    /\ wal_init' = [wal_init EXCEPT ![p] = TRUE]
    /\ shared_max_frame' = Cardinality(VisibleWalWrites)
    /\ nbackfills' = 0
    /\ cached_wi_gen' = [cached_wi_gen EXCEPT ![p] = SentinelGen]
    /\ pc' = [pc EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<db_durable, db_buffered, wal_durable, wal_hdr_durable,
                   wlock, read_snap,
                   wi_frames, wi_max_frame, wi_salt, wi_generation,
                   tshm_writer_state, cached_tshm_state, read_s1,
                   committed, next_wid, next_salt, ckpt_mode>>

(* ================================================================== *)
(* Next-state relation                                                *)
(* ================================================================== *)

Next ==
    \/ \E p \in Procs:
        \/ BeginReadSync(p)
        \/ BeginReadValidate(p)
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
\* Every committed write must be recoverable from durable storage
\* (DB file + visible WAL frames) at all times.
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
\*
\* Extended for WalIndex: if a reader synced from WalIndex (via
\* sync_shared_from_wal_index), its snapshot must still be valid even
\* though the reader used the zero-I/O WalIndex path instead of reading
\* the WAL file directly. The seqlock guarantees this: if a writer
\* interleaved, the reader retries.
NoStaleReads ==
    \A p \in Procs:
        pc[p] = "reading" /\ read_snap[p] > 0 =>
            \A wid \in committed:
                wid <= read_snap[p] => wid \in RecoverableWrites

\* WalIndex coherence: when the WalIndex has frames (max_frame > 0),
\* every entry visible through the WalIndex must correspond to a durable
\* WAL frame. This ensures that WalIndex lookups never return frame IDs
\* pointing to non-existent or stale WAL data.
\*
\* Specifically: for every committed wid in the WalIndex (wid <= wi_max_frame
\* and matching salt), there must be a corresponding durable WAL frame.
WalIndexCoherence ==
    wi_max_frame > 0 =>
        \A wid \in WalIndexVisibleWrites:
            wid \in committed =>
                \E f \in wal_durable: f.wid = wid /\ f.salt = wi_salt

\* Seqlock safety: a reader in "reading" phase with a validated generation
\* (cached_wi_gen != SentinelGen) must have a generation <= the current
\* WalIndex generation. The cached generation can be < wi_generation if
\* a writer cleared+repopulated after the reader's begin_read_tx, but
\* the reader's snapshot (read_snap) is still valid because checkpoint
\* respects TSHM reader slots (MinReaderSnapshot boundary).
SeqlockSafety ==
    \A p \in Procs:
        (pc[p] = "reading" /\ cached_wi_gen[p] # SentinelGen) =>
            cached_wi_gen[p] <= wi_generation

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
    /\ wi_max_frame \in Nat
    /\ wi_generation \in 0..MaxCounter
    /\ tshm_writer_state \in 0..MaxCounter

=============================================================================
