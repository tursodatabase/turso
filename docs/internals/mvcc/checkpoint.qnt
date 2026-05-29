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
    }
  }

  // A transaction commits ATOMICALLY at ts = nextTs: it touches 1..3 keys, each
  // independently written (to a nondet value) or deleted. All of the txn's ops
  // share one ts and are appended to the (fsynced) logical log together — recovery
  // must apply them all-or-nothing. Modeling multi-row txns (not one row per
  // commit) exercises recovery atomicity across rows and multi-entry checkpoint
  // materialization, not just single-row paths. Allowed while NO checkpoint runs
  // ("idle") and during a PASSIVE checkpoint's unlocked prepare phase ("started")
  // — the off-lock window where the engine still accepts writers. Full/Restart/
  // Truncate block writers for the whole checkpoint, so no commit interleaves them.
  action commit = {
    // Pick 1..3 keys as INDEPENDENT draws from the wide universe (duplicates
    // collapse). This bounds per-txn fan-out so `setOfMaps` (|dbValues|^|ks| and
    // 2^|ks|) stays tiny no matter how large `dbKeys` is — the trick that lets the
    // universe be wide without `commit` enumerating a 2^|dbKeys| powerset.
    nondet k1 = dbKeys.oneOf()
    nondet k2 = dbKeys.oneOf()
    nondet k3 = dbKeys.oneOf()
    val ks = Set(k1, k2, k3)
    nondet vmap = ks.setOfMaps(dbValues).oneOf()
    nondet dmap = ks.setOfMaps(Set(false, true)).oneOf()
    val ops = ks.map(k => { ts: nextTs, key: k, value: vmap.get(k), del: dmap.get(k) })
    all {
    seeded,
    ckptPhase == "idle" or (ckptPhase == "started" and ckptMode == "passive"),
    committed' = committed.union(ops),
    logFrames' = logFrames.union(ops),
    nextTs' = nextTs + 1,
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
  }

  // Phase 3 (final): truncate the logical log, removing ONLY this checkpoint's
  // snapshot (concurrent commits above it stay), and publish the boundary. With
  // the correct ordering the snapshot is already durable in WAL+DB; the unsafe
  // toggle lets this fire before the snapshot is durable elsewhere -> recovery
  // loses it.
  action checkpointTruncateLog = all {
    ckptPhase == "dbBackfilled" or (not(backfillBeforeTruncate) and ckptPhase != "idle"),
    ckptPhase' = "idle",
    logFrames' = logFrames.exclude(ckptSnapshot),
    boundary' = ckptBoundary,
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
  }

  action step = any {
    seed,
    commit,
    checkpointStart,
    checkpointFillWal,
    checkpointBackfillDb,
    checkpointTruncateLog,
  }

  // === THE CONTRACT ===
  // Checked at every reachable state = "a crash at this exact point recovers
  // correctly": recovery reconstructs exactly the committed live contents
  // (latest value per key, deletes honored) — no lost write, stale version, or
  // resurrected delete.
  val recoverySafe =
    recovered(db, wal, logFrames) == resolve(committed)

  // Example smoke trace (`quint test`): a checkpoint (mode chosen
  // nondeterministically) recovers safely. The richer coverage — every mode,
  // commits interleaved into a passive checkpoint, crashes at every point — comes
  // from `quint run --invariant=recoverySafe` and the MBT adapter that replays
  // generated traces against the real engine.
  run sequentialCheckpointTest =
    init
      .then(seed)
      .then(commit)
      .then(checkpointStart)
      .then(checkpointFillWal)
      .then(checkpointBackfillDb)
      .then(checkpointTruncateLog)
      .expect(recoverySafe)
}
