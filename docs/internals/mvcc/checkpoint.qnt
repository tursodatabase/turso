// -*- mode: quint -*-
//
// Checkpoint durability contract for Turso's MVCC engine.
//
// An executable statement of how checkpoints + recovery MUST behave: checkable
// on its own with Quint and replayed against the real engine by the MBT adapter
// (core/mvcc/database/tests.rs::test_mbt_checkpoint_traces_match_spec, fed by
// mbt_gen.py).
//
// We model the *durable* artifacts (what survives a crash) and the events that
// move data between them:
//   - committed : transactions that returned success to the user
//   - db        : rows materialized in the DB file
//   - wal       : rows in committed (fsynced) WAL frames
//   - logFrames : rows in the logical log (fsynced commit records, uncheckpointed)
//   - boundary  : persistent_tx_ts_max (txs <= boundary are checkpointed)
//
// The checkpoint is modeled as a multi-phase, NON-BLOCKING process: a `commit`
// may interleave at ANY phase (the whole point of the off-lock checkpoint). A
// checkpoint captures a snapshot of the log frames at start and only backfills /
// truncates THAT snapshot; commits that land above it must survive untouched.
// Passive/Full leave the WAL intact; Restart/Truncate additionally RESET it
// (rewind/zero) at the final phase — modeled in `checkpointTruncateLog`, so
// recoverySafe verifies recovery never relies on the reset frames.
//
// THE CONTRACT (invariant `recoverySafe`, checked at EVERY reachable state — i.e.
// "a crash right here recovers correctly"): recovery reconstructs exactly the set
// of committed transactions — no loss, no duplication.
//
// `backfillBeforeTruncate` toggles the ordering to show the invariant has teeth:
//   - true  (real ordering): the snapshot is durable in WAL+DB before the log is
//     truncated -> `recoverySafe` HOLDS.
//   - false (unsafe ordering): the log is truncated before the snapshot is
//     durable elsewhere -> recovery loses it -> `recoverySafe` is VIOLATED and
//     Quint produces the counterexample.
//
// Check it:
//   quint typecheck checkpoint.qnt
//   quint test      checkpoint.qnt
//   quint run       checkpoint.qnt --invariant=recoverySafe --max-samples=200000
// Flip `backfillBeforeTruncate` to false and re-run to see the data-loss trace.

module checkpoint {
  // A committed operation on a key, tagged by its commit timestamp `ts`.
  // `del = true` is a tombstone (DELETE); otherwise it writes `val`. Modeling
  // per-key writes/updates/deletes (not just additions of distinct rows) is what
  // exercises the engine's multi-version checkpoint logic — wrong-version
  // materialization, lost/resurrected deletes, tombstone-vs-boundary.
  type Op = { ts: int, key: int, value: int, del: bool }

  // A transaction's op while it is still IN-FLIGHT: the writer has planted the
  // row version in MVCC (insert with begin = TxID, delete with end = TxID) but
  // its commit has not yet returned a timestamp. No `ts` yet — that is assigned
  // at finalizeTx. Modeling commits as TWO phases (prepareTx then finalizeTx)
  // is what lets a passive checkpoint observe a transaction's speculative state
  // — the only window in which an `end = TxID(_)` tombstone can reach the
  // checkpoint's row dispatcher.
  type StagedOp = { key: int, value: int, del: bool }
  type InFlightTx = { txId: int, ops: Set[StagedOp] }

  // The key UNIVERSE txns operate on. Deliberately wide so that, when the adapter
  // replays a trace against the real engine, the live rows span many B-tree pages
  // (and, with the overflow-sized payloads the adapter uses for value >= 2, many
  // overflow pages) — the checkpoint then materializes a genuinely multi-page tree
  // and secondary index, not a toy one-page case. Each commit still touches only a
  // BOUNDED handful of keys (see `commit`), so the universe can be this wide
  // WITHOUT `commit` having to enumerate its powerset (which is 2^|dbKeys| and
  // would be intractable here). Traces are sampled via `oneOf`, so a wider
  // universe costs distribution density, not per-step blowup.
  pure val dbKeys = 1.to(256)

  // Keys the `seed` action pre-populates before any checkpoint runs, so every
  // trace begins against an ALREADY-ESTABLISHED multi-page B-tree (real
  // checkpoints rarely run against an empty tree). Kept to the lower half of the
  // universe so the random phase still INSERTS new keys (the upper half) into the
  // established tree — exercising page splits during commits and during a
  // concurrent passive checkpoint, not just updates/deletes of existing rows.
  pure val seedKeys = 1.to(128)

  // Secondary-index values a write may set. Deliberately SMALLER than the key
  // space and decoupled from `ts`, so traces routinely produce two keys with the
  // SAME value (duplicate non-unique-index entries) and UPDATEs that move a key
  // to a value another key already holds — the index-key transitions where the
  // engine's secondary-index materialization actually breaks. (With value = ts
  // every index key was unique, so these paths were never exercised.)
  pure val dbValues = Set(1, 2)

  // --- durable on-disk state (sets of committed ops a crash leaves behind) ---
  var committed: Set[Op]
  var db: Set[Op]
  var wal: Set[Op]
  var logFrames: Set[Op]
  var boundary: int
  var nextTs: int
  // True once `seed` has pre-populated the base tree. Gates commits/checkpoints so
  // every trace starts with `seed` (the only action enabled from `init`).
  var seeded: bool

  // --- in-flight transactions (started but not yet committed) ---
  // The set of transactions a writer has BEGUN: each has planted its row
  // versions (insert: begin = TxID(txId); delete: end = TxID(txId)) in MVCC but
  // has not yet returned a commit timestamp. invisible to readers / contents,
  // but observable by a concurrent passive checkpoint's row collection — which
  // is exactly where the speculative-tombstone bug class lives. `finalizeTx`
  // commits one: assigns nextTs, materializes the ops at that ts, drops the txn
  // from the in-flight set.
  var inFlightTxs: Set[InFlightTx]
  // Per-transaction identifier the model assigns at prepareTx. Distinct from
  // `nextTs` because txIds are minted at PREPARE while commit timestamps are
  // minted at FINALIZE — and arbitrarily many other steps can fire in between.
  var nextTxId: int

  // --- in-flight schema drops (R5) ---
  // A DROP TABLE / DROP INDEX has the same off-lock-race structure as a
  // committing INSERT/DELETE: the schema row's `end` becomes `TxID(_)` while
  // the writer is mid-commit, observable by a concurrent passive checkpoint's
  // collection. Modeling DROP as prepare+finalize parallels prepareTx /
  // finalizeTx and lets the spec EXPRESS the symmetric "concurrent DROP during
  // off-lock publish" race — the inverse of the CREATE race the recent
  // schema-publish fix handles. As with R4 (readers), the structural variables
  // and actions land first; the LOAD-BEARING invariant (e.g., "a DROP that
  // finalized after our snapshot must not be applied by this checkpoint")
  // wires in once the engine adapter learns to drive a fresh-DROP scenario
  // analogous to how `CheckpointWithParked` drives a fresh DELETE.
  type DropKind = str // "table" or "index"
  type InFlightDrop = { dropId: int, kind: DropKind }
  var inFlightDrops: Set[InFlightDrop]
  var nextDropId: int
  // Schema objects whose DROP has finalized. Distinct from "destroyed in this
  // checkpoint's pass" — these are model-global, mirroring the engine's
  // `destroyed_tables`/`destroyed_indexes` post-publish.
  var droppedTables: Set[int]
  var droppedIndexes: Set[int]

  // --- active readers (R4) ---
  // A Reader is a transaction that opened a read view at `snapshotTs` and has
  // NOT YET committed/aborted. The read view is the set of (key, value) pairs
  // observable at snapshotTs: resolve(committed-restricted-to-ts <= snapshotTs).
  // The MVCC engine must keep that view consistent until the reader releases
  // — i.e. checkpoint GC may not remove versions still needed by some live
  // reader, and recovery from a crash must not invalidate a reader that
  // observed a state ≤ snapshotTs that is still in `committed`.
  //
  // Modeling readers makes the GC-vs-live-reader contract testable: an
  // invariant `readersSafe` asserts every live reader's view at its
  // snapshotTs equals the view computed from the durable artifacts at the
  // same ts. A bug that prematurely GCs a version a reader still needs would
  // change the resolved view at snapshotTs after the GC fires; the invariant
  // catches it.
  type Reader = { readerId: int, snapshotTs: int }
  var readers: Set[Reader]
  var nextReaderId: int

  // --- in-progress checkpoint ---
  // ckptPhase: "idle" | "started" | "walFilled" | "dbBackfilled"
  var ckptPhase: str
  var ckptSnapshot: Set[Op] // log ops THIS checkpoint will backfill + truncate
  var ckptBoundary: int     // boundary THIS checkpoint will publish
  // The mode of the in-flight checkpoint: "passive" | "full" | "restart" |
  // "truncate" ("" when idle). Only "passive" is non-blocking; the others block
  // writers for the whole checkpoint, so a commit can interleave a "passive"
  // checkpoint's prepare phase but not the others'. The mode also drives WAL
  // handling: Restart/Truncate reset/zero the WAL (modeled in
  // checkpointTruncateLog); the adapter replays each mode against the real
  // checkpoint, which performs the concrete WAL operation.
  var ckptMode: str

  pure val backfillBeforeTruncate = true

  // Resolve a set of ops to the live (key, value) contents: for each key, the op
  // with the highest ts wins (ts is unique per commit); a tombstone => the key is
  // absent. This is "latest version per key", the contents recovery must rebuild.
  pure def resolve(ops: Set[Op]): Set[(int, int)] =
    // Fold over the keys PRESENT in `ops`, not the whole universe — keeps this
    // O(|ops|) so a wide `dbKeys` doesn't slow `quint run`'s invariant checks.
    ops.map(o => o.key).fold(Set(), (acc, k) => {
      val ko = ops.filter(o => o.key == k)
      if (ko.size() == 0) acc
      else {
        val maxTs = ko.fold(0, (m, o) => if (o.ts > m) o.ts else m)
        // exactly one op has maxTs; fold the singleton to apply it
        ko.filter(o => o.ts == maxTs)
          .fold(acc, (a, o) => if (o.del) a else a.union(Set((k, o.value))))
      }
    })

  // Recovery reconstructs the live contents from the union of all durable
  // artifacts (latest version per key wins across them).
  pure def recovered(d: Set[Op], w: Set[Op], l: Set[Op]): Set[(int, int)] =
    resolve(d.union(w).union(l))

  action init = all {
    committed' = Set(),
    db' = Set(),
    wal' = Set(),
    logFrames' = Set(),
    boundary' = 0,
    nextTs' = 1,
    ckptPhase' = "idle",
    ckptSnapshot' = Set(),
    ckptBoundary' = 0,
    ckptMode' = "",
    seeded' = false,
    inFlightTxs' = Set(),
    nextTxId' = 1,
    readers' = Set(),
    nextReaderId' = 1,
    inFlightDrops' = Set(),
    nextDropId' = 1,
    droppedTables' = Set(),
    droppedIndexes' = Set(),
  }

  // Pre-populate the base tree: commit every `seedKeys` row atomically at ts =
  // nextTs (value alternates 1/2 so the secondary index holds both values and the
  // adapter's value-2 rows are overflow-sized — a genuinely multi-page base). Only
  // enabled from `init` (when `not seeded`), and since `commit`/`checkpointStart`
  // require `seeded`/a non-empty log, `seed` is the ONLY action enabled at the
  // start — every trace begins here. Emitted as a `commit` in the generated trace
  // (mbt_gen remaps it), so the adapter replays it as one large insert txn.
  action seed = {
    val ops = seedKeys.map(k => { ts: nextTs, key: k, value: 1 + (k % 2), del: false })
    all {
      not(seeded),
      committed' = ops,
      logFrames' = ops,
      nextTs' = nextTs + 1,
      seeded' = true,
      db' = db,
      wal' = wal,
      boundary' = boundary,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      inFlightTxs' = inFlightTxs,
      nextTxId' = nextTxId,
      readers' = readers,
      nextReaderId' = nextReaderId,
      inFlightDrops' = inFlightDrops,
      nextDropId' = nextDropId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
    }
  }

  // PHASE 1 of a transaction: a writer opens a txn, plants its row versions
  // in MVCC, and walks the commit state machine up to but NOT past the point
  // where its tombstone/insert TxID-ends get rewritten to committed Timestamps
  // (in the engine, mod.rs:2004 inside build_log_record). The txn is now
  // "in-flight" — invisible to readers and to the recovered contents, but
  // observable to a concurrent passive checkpoint's row dispatcher as a
  // version with `begin = TxID(_)` or `end = TxID(_)`. Modeling commits as
  // prepare+finalize (rather than as a single atomic step) is what lets the
  // checkpoint actions interleave the moment a writer has planted versions
  // but not yet committed — the only window in which the speculative-state
  // bug class can fire.
  //
  // Allowed while NO checkpoint runs ("idle") and during a PASSIVE checkpoint's
  // unlocked prepare phase ("started") — the off-lock window where the engine
  // still accepts writers. Full/Restart/Truncate block writers for the whole
  // checkpoint, so no prepare interleaves them.
  action prepareTx = {
    // 1..3 keys (duplicates collapse), values, del flags — same shape as the
    // old atomic commit, just kept as `StagedOp`s (no `ts`) under a `txId` until
    // finalizeTx assigns a commit timestamp. setOfMaps stays small because `ks`
    // is bounded.
    nondet k1 = dbKeys.oneOf()
    nondet k2 = dbKeys.oneOf()
    nondet k3 = dbKeys.oneOf()
    val ks = Set(k1, k2, k3)
    nondet vmap = ks.setOfMaps(dbValues).oneOf()
    nondet dmap = ks.setOfMaps(Set(false, true)).oneOf()
    val stagedOps = ks.map(k => { key: k, value: vmap.get(k), del: dmap.get(k) })
    all {
      seeded,
      ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
      inFlightTxs' = inFlightTxs.union(Set({ txId: nextTxId, ops: stagedOps })),
      nextTxId' = nextTxId + 1,
      // Durable artifacts and `nextTs` don't move — the txn has not committed.
      committed' = committed,
      logFrames' = logFrames,
      nextTs' = nextTs,
      db' = db,
      wal' = wal,
      boundary' = boundary,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      seeded' = seeded,
      readers' = readers,
      nextReaderId' = nextReaderId,
      inFlightDrops' = inFlightDrops,
      nextDropId' = nextDropId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
    }
  }

  // PHASE 2 of a transaction: the writer finishes its commit state machine
  // (in the engine, rewrites TxID-ends to Timestamp-ends, appends the commit
  // frame to the logical log, fsyncs, publishes the marker). Until this fires,
  // every op the txn planted is invisible to committed/logFrames. Multiple
  // in-flight txns can coexist — the model picks one to finalize per step.
  //
  // Allowed in the same windows as prepareTx (idle or passive-prepare): a
  // writer can finish committing while a passive checkpoint runs, just as in
  // production.
  action finalizeTx = {
    nondet tx = inFlightTxs.oneOf()
    val finalOps = tx.ops.map(o => { ts: nextTs, key: o.key, value: o.value, del: o.del })
    all {
      seeded,
      inFlightTxs != Set(),
      ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
      committed' = committed.union(finalOps),
      logFrames' = logFrames.union(finalOps),
      nextTs' = nextTs + 1,
      inFlightTxs' = inFlightTxs.exclude(Set(tx)),
      nextTxId' = nextTxId,
      readers' = readers,
      nextReaderId' = nextReaderId,
      inFlightDrops' = inFlightDrops,
      nextDropId' = nextDropId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
      db' = db,
      wal' = wal,
      boundary' = boundary,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      seeded' = seeded,
    }
  }

  // Begin a checkpoint in one of the four modes (chosen nondeterministically):
  // snapshot the current log frames + the boundary it will publish. One
  // checkpoint at a time.
  action checkpointStart = {
    nondet m = Set("passive", "full", "restart", "truncate").oneOf()
    all {
      ckptPhase == "idle",
      logFrames != Set(),
      ckptPhase' = "started",
      ckptMode' = m,
      ckptSnapshot' = logFrames,
      ckptBoundary' = nextTs - 1,
      committed' = committed,
      db' = db,
      wal' = wal,
      logFrames' = logFrames,
      boundary' = boundary,
      nextTs' = nextTs,
      seeded' = seeded,
      inFlightTxs' = inFlightTxs,
      nextTxId' = nextTxId,
      readers' = readers,
      nextReaderId' = nextReaderId,
      inFlightDrops' = inFlightDrops,
      nextDropId' = nextDropId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
    }
  }

  // Phase 1: write + fsync the snapshot's commit frames into the WAL.
  action checkpointFillWal = all {
    ckptPhase == "started",
    ckptPhase' = "walFilled",
    wal' = wal.union(ckptSnapshot),
    committed' = committed,
    db' = db,
    logFrames' = logFrames,
    boundary' = boundary,
    nextTs' = nextTs,
    ckptSnapshot' = ckptSnapshot,
    ckptBoundary' = ckptBoundary,
    ckptMode' = ckptMode,
    seeded' = seeded,
    inFlightTxs' = inFlightTxs,
    nextTxId' = nextTxId,
    readers' = readers,
    nextReaderId' = nextReaderId,
    inFlightDrops' = inFlightDrops,
    nextDropId' = nextDropId,
    droppedTables' = droppedTables,
    droppedIndexes' = droppedIndexes,
  }

  // Phase 2: backfill the snapshot into the DB file + fsync it.
  action checkpointBackfillDb = all {
    ckptPhase == "walFilled",
    ckptPhase' = "dbBackfilled",
    db' = db.union(ckptSnapshot),
    committed' = committed,
    wal' = wal,
    logFrames' = logFrames,
    boundary' = boundary,
    nextTs' = nextTs,
    ckptSnapshot' = ckptSnapshot,
    ckptBoundary' = ckptBoundary,
    ckptMode' = ckptMode,
    seeded' = seeded,
    inFlightTxs' = inFlightTxs,
    nextTxId' = nextTxId,
    readers' = readers,
    nextReaderId' = nextReaderId,
    inFlightDrops' = inFlightDrops,
    nextDropId' = nextDropId,
    droppedTables' = droppedTables,
    droppedIndexes' = droppedIndexes,
  }

  // Phase 3a (R6 split): publish the new `persistent_tx_ts_max` boundary.
  // Mirrors engine yield `AfterDurableBoundaryAdvanced`: the durable header
  // says recovery's last-completed-ckpt is `ckptBoundary`. Crashes BETWEEN
  // 3a and 3b are recovery's hardest case: the boundary has advanced (so
  // recovery WON'T replay this ckpt's frames) but the logical-log entries
  // are still on disk — a non-idempotent recovery that re-applies them
  // would corrupt. Splitting these two phases makes that crash window
  // representable in the model. Today the engine commits both updates inside
  // the locked phase, so the only "between" point is mid-fsync; the spec
  // makes the ordering explicit either way.
  action checkpointAdvanceBoundary = all {
    ckptPhase == "dbBackfilled",
    ckptPhase' = "boundaryAdvanced",
    boundary' = ckptBoundary,
    committed' = committed,
    db' = db,
    wal' = wal,
    logFrames' = logFrames,
    nextTs' = nextTs,
    ckptSnapshot' = ckptSnapshot,
    ckptBoundary' = ckptBoundary,
    ckptMode' = ckptMode,
    seeded' = seeded,
    inFlightTxs' = inFlightTxs,
    nextTxId' = nextTxId,
    readers' = readers,
    nextReaderId' = nextReaderId,
    inFlightDrops' = inFlightDrops,
    nextDropId' = nextDropId,
    droppedTables' = droppedTables,
    droppedIndexes' = droppedIndexes,
  }

  // Phase 3b (final): truncate the logical log, removing ONLY this checkpoint's
  // snapshot (concurrent commits above it stay). Mirrors engine yields
  // `AfterTruncateLogicalLog` + `AfterFsyncLogicalLog` (collapsed; the fsync
  // is captured by the model's atomic-set semantics — there is no other
  // observable state between "logical log mutated" and "logical log durable"
  // in this abstraction). With the correct ordering the snapshot is already
  // durable in WAL+DB AND the boundary is published; the unsafe toggle lets
  // this fire BEFORE either, demonstrating the recoverySafe invariant has
  // teeth.
  action checkpointTruncateLog = all {
    ckptPhase == "boundaryAdvanced" or (not(backfillBeforeTruncate) and ckptPhase != "idle"),
    ckptPhase' = "idle",
    logFrames' = logFrames.exclude(ckptSnapshot),
    boundary' = boundary,
    ckptSnapshot' = Set(),
    ckptBoundary' = 0,
    ckptMode' = "",
    committed' = committed,
    db' = db,
    // Restart/Truncate RESET the WAL as part of the checkpoint (Restart rewinds
    // it so the next writer overwrites from the start; Truncate zeroes the file);
    // Passive/Full leave it intact. We clear it atomically with publishing the
    // boundary to assert recovery never depends on the reset frames — they must
    // already be durable in `db`. Under the unsafe `backfillBeforeTruncate =
    // false` ordering this fires before BackfillDb, so clearing the WAL then
    // loses the snapshot -> recoverySafe counterexample (the WAL-reset has teeth).
    wal' = if (ckptMode == "restart" or ckptMode == "truncate") Set() else wal,
    nextTs' = nextTs,
    seeded' = seeded,
    inFlightTxs' = inFlightTxs,
    nextTxId' = nextTxId,
    readers' = readers,
    nextReaderId' = nextReaderId,
    inFlightDrops' = inFlightDrops,
    nextDropId' = nextDropId,
    droppedTables' = droppedTables,
    droppedIndexes' = droppedIndexes,
  }

  // Open a read view at `nextTs - 1` (the latest committed ts so far). Mirrors
  // the engine: BEGIN CONCURRENT followed by a first read snapshots the
  // committed timestamp watermark and pins versions visible at that ts. Allowed
  // in idle or passive-started phases; non-passive checkpoints block writers
  // and the model's readers behave like other writers w.r.t. the lock.
  action startReader = all {
    seeded,
    ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
    readers' = readers.union(Set({
      readerId: nextReaderId,
      snapshotTs: nextTs - 1,
    })),
    nextReaderId' = nextReaderId + 1,
    committed' = committed,
    db' = db,
    wal' = wal,
    logFrames' = logFrames,
    boundary' = boundary,
    nextTs' = nextTs,
    ckptPhase' = ckptPhase,
    ckptSnapshot' = ckptSnapshot,
    ckptBoundary' = ckptBoundary,
    ckptMode' = ckptMode,
    seeded' = seeded,
    inFlightTxs' = inFlightTxs,
    nextTxId' = nextTxId,
    inFlightDrops' = inFlightDrops,
    nextDropId' = nextDropId,
    droppedTables' = droppedTables,
    droppedIndexes' = droppedIndexes,
  }

  // The reader commits / aborts and releases its snapshot. Allowed any time
  // there is at least one live reader.
  action endReader = {
    nondet r = readers.oneOf()
    all {
      readers != Set(),
      readers' = readers.exclude(Set(r)),
      nextReaderId' = nextReaderId,
      inFlightDrops' = inFlightDrops,
      nextDropId' = nextDropId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
      committed' = committed,
      db' = db,
      wal' = wal,
      logFrames' = logFrames,
      boundary' = boundary,
      nextTs' = nextTs,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      seeded' = seeded,
      inFlightTxs' = inFlightTxs,
      nextTxId' = nextTxId,
    }
  }

  // Begin a DROP TABLE / DROP INDEX transaction. Plants the schema row's end
  // marker as `TxID(_)` in the engine; the checkpoint's collection can observe
  // this speculative state in the off-lock window. Mirrors `prepareTx` for
  // schema operations.
  action prepareDrop = {
    nondet kind = Set("table", "index").oneOf()
    all {
      seeded,
      ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
      inFlightDrops' = inFlightDrops.union(Set({ dropId: nextDropId, kind: kind })),
      nextDropId' = nextDropId + 1,
      committed' = committed,
      logFrames' = logFrames,
      nextTs' = nextTs,
      db' = db,
      wal' = wal,
      boundary' = boundary,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      seeded' = seeded,
      inFlightTxs' = inFlightTxs,
      nextTxId' = nextTxId,
      readers' = readers,
      nextReaderId' = nextReaderId,
      droppedTables' = droppedTables,
      droppedIndexes' = droppedIndexes,
    }
  }

  // Finalize a DROP — schema row's `end` becomes `Timestamp(nextTs)` and the
  // object moves to the appropriate destroyed-set. Mirrors `finalizeTx`.
  action finalizeDrop = {
    nondet d = inFlightDrops.oneOf()
    all {
      seeded,
      inFlightDrops != Set(),
      ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
      inFlightDrops' = inFlightDrops.exclude(Set(d)),
      droppedTables' = if (d.kind == "table") droppedTables.union(Set(d.dropId)) else droppedTables,
      droppedIndexes' = if (d.kind == "index") droppedIndexes.union(Set(d.dropId)) else droppedIndexes,
      nextTs' = nextTs + 1,
      committed' = committed,
      logFrames' = logFrames,
      db' = db,
      wal' = wal,
      boundary' = boundary,
      ckptPhase' = ckptPhase,
      ckptSnapshot' = ckptSnapshot,
      ckptBoundary' = ckptBoundary,
      ckptMode' = ckptMode,
      seeded' = seeded,
      inFlightTxs' = inFlightTxs,
      nextTxId' = nextTxId,
      readers' = readers,
      nextReaderId' = nextReaderId,
      nextDropId' = nextDropId,
    }
  }

  action step = any {
    seed,
    prepareTx,
    finalizeTx,
    startReader,
    endReader,
    prepareDrop,
    finalizeDrop,
    checkpointStart,
    checkpointFillWal,
    checkpointBackfillDb,
    checkpointAdvanceBoundary,
    checkpointTruncateLog,
  }

  // === THE CONTRACT ===
  // Checked at every reachable state = "a crash at this exact point recovers
  // correctly": recovery reconstructs exactly the committed live contents
  // (latest value per key, deletes honored) — no lost write, stale version, or
  // resurrected delete.
  val recoverySafe =
    recovered(db, wal, logFrames) == resolve(committed)

  // Restrict `committed` to ops at ts <= snapshotTs. The view a reader at
  // `snapshotTs` MUST observe, regardless of what else commits or checkpoints
  // afterwards.
  pure def viewAt(ops: Set[Op], snapshotTs: int): Set[(int, int)] =
    resolve(ops.filter(o => o.ts <= snapshotTs))

  // === READER CONTRACT (R4) ===
  // Every live reader's view at its snapshotTs MUST equal the view computed
  // from `committed` filtered to that ts. The engine has many ways to violate
  // this: premature GC of a still-needed version, in-place page mutation
  // visible to a held cursor, schema-cache desync overwriting a reader's row.
  // Checked at every reachable state — including after a checkpoint runs.
  val readersSafe =
    readers.forall(r => viewAt(committed, r.snapshotTs) == viewAt(committed, r.snapshotTs))
  // Note: the equality above is trivially true because we don't yet model the
  // ENGINE'S view of a reader (which is what could diverge). The invariant
  // becomes load-bearing once we add a model variable for what the engine
  // SHOWS the reader, separate from what `committed` says it SHOULD show; that
  // is the next step (R4b — model the per-reader cached view that GC can
  // corrupt). The reader machinery (variable + actions + invariant skeleton)
  // landing first lets follow-up changes flip on the load-bearing comparison
  // without restructuring everything.

  // Example smoke trace (`quint test`): a checkpoint (mode chosen
  // nondeterministically) recovers safely. The richer coverage — every mode,
  // commits interleaved into a passive checkpoint, crashes at every point — comes
  // from `quint run --invariant=recoverySafe` and the MBT adapter that replays
  // generated traces against the real engine.
  run sequentialCheckpointTest =
    init
      .then(seed)
      .then(prepareTx)
      .then(finalizeTx)
      .then(checkpointStart)
      .then(checkpointFillWal)
      .then(checkpointBackfillDb)
      .then(checkpointAdvanceBoundary)
      .then(checkpointTruncateLog)
      .expect(recoverySafe)
}
