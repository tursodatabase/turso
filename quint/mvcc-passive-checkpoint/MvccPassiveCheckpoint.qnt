// -*- mode: Bluespec; -*-
/**
 * Turso MVCC passive checkpoint.
 *
 * This specification models how a passive checkpoint moves committed row
 * versions from the in-memory version store into the B-tree while writers
 * commit and readers read at the same time. See DESIGN.md next to this file
 * for the design in prose and for the mapping to the Rust code in
 * core/mvcc/database/checkpoint_state_machine.rs and core/mvcc/database/mod.rs.
 *
 * Actors.
 *   Writers  commit one row change each: Prepare (take a commit timestamp
 *            and change the version chain), LogAppend (write the logical log
 *            record), Finish (raise lastCommitted).
 *   Readers  pin a WAL read mark, take a begin timestamp, read rows, end.
 *   The checkpoint runs one pass at a time: take a snapshot timestamp,
 *            collect, commit one WAL frame, publish, backfill the WAL into
 *            the database file, truncate the logical log, stamp the versions
 *            it wrote.
 *   GC       drops versions from the version store. It can run at any time,
 *            which covers the commit-path GC and the checkpoint GC.
 *
 * Two properties are checked.
 *   ReadsCorrect     Every read returns the value of the row at the
 *                    reader's begin timestamp.
 *   RecoveryCorrect  If the process crashes now, the database file, the WAL
 *                    and the logical log rebuild every logged commit.
 *
 * The constants named Guard* switch off one rule of the design each. The
 * main configuration sets all of them to true. Each variant configuration
 * in MvccPassiveCheckpointConfigs.qnt sets one of them to false, and the
 * model checker then finds the bug that the rule prevents.
 *
 * Row-level extensions (DESIGN-rows.md). Each one is behind a constant
 * whose "off" value gives the model above, state for state.
 *   Aborts         A Preparing writer can roll back before its log append.
 *                  Readers that read its versions speculatively must abort.
 *   DirtyKeys      Collection visits only keys in the dirty-key map, unless
 *                  a failed mark forced a full scan.
 *   GcSamplesFirst GC samples lwm and the floor first, then applies the
 *                  rules to a chain in a later step.
 *
 * Not modeled: schema changes and root-page bindings, indexes, sequences,
 * WAL restart, aborts of Active writers and aborts after the log append,
 * write-write conflicts between writers, fsync failures, and the TRUNCATE
 * checkpoint mode.
 *
 * Notes on the Quint code:
 *   - Keys and Readers are strings. The model checker sees every order of
 *     them.
 *   - Max, Min and "the one element of a set" use fold.
 *   - Lists start at index 0. WAL frame numbers still start at 1: frame i
 *     is wal[i - 1] (see Frame).
 *   - Every action assigns every variable.
 *   - "val" is a keyword in Quint, so the version field is "value".
 *   - Each argument that a step picks comes from a named nondet in Next.
 *     `quint run --mbt` then records the action and its picks in the
 *     trace. "to" is a built-in name in Quint, so the backfill pick is
 *     named upTo.
 */
module MvccPassiveCheckpoint {
  const Keys: Set[str]            // Row keys of one table.
  const Readers: Set[str]         // Reader connections.
  const MaxTs: int                // Bound on the logical clock (commits and reader begins).
  const MaxCheckpoints: int       // Bound on the number of checkpoint passes.

  // Rules of the design. true in the main configuration.
  const GuardSnapshotBelowPreparing: bool  // snapshot_ts < every Preparing commit ts
  const GuardGcWaitsForReaders: bool       // GC drops a version only when every reader
                                           // can read its effect from the B-tree
  const GuardDropCurrentOnlyWhenIdle: bool // GC Rule 3 needs "no open snapshot"
  const GuardBackfillStopsAtReaders: bool  // WAL backfill stops at the lowest read mark

  // Which reader marks bound the GC floor. One of:
  //   "code"       registered readers' marks and backfillFloor (what
  //                gc_incremental uses today)
  //   "registered" only readers registered in txs
  //   "pinned"     every reader that pinned a WAL read mark
  //   "backfill"   only backfillFloor
  const GcFloorRule: str

  // true: a read looks at the chain twice, once for each side of the dual
  // cursor (what MvccLazyCursor does). false: one look decides both sides.
  const ReadLooksAtChainTwice: bool

  // Aborts. false in the main configuration.
  const Aborts: bool                        // a Preparing writer can roll back
  const GuardReadersAbortOnDependency: bool // register_commit_dependency + abort_now
  const AbortKeepsMaterialized: bool        // false: set_end(None) resets
                                            // materialized_at (the code). true: keep it.

  // Dirty keys. false / 0 in the main configuration.
  const DirtyKeys: bool                      // collect only keys in the dirty map
  const GuardMarkBeforeCommitEnd: bool       // MarkDirtyKeys runs while Preparing
  const MaxMarkFailures: int                 // bound on failed re-marks
  const GuardFullScanUntilStampCovered: bool // clear only if snap >= lost stamp
  const GuardFullScanGenerationCas: bool     // clear only if no new failure

  // GC in two steps. false in the main configuration.
  const GcSamplesFirst: bool          // sample lwm and floor, then apply
  const GuardGcRechecksIdleLwm: bool  // gc_chain_now recomputes lwm = MAX

  // Trace replay only. Each one makes the spec take only the steps that the
  // engine can take. false in every configuration that is model checked.
  const BackfillToLimit: bool      // the backfill copies every frame up to
                                   // the limit, not any frame up to it
  const LogAppendOneAtATime: bool  // a writer appends its log record only when
                                   // no other writer appended and did not
                                   // finish (pager_commit_lock)
  const WriterBeginTicks: bool     // a writer takes a begin timestamp from
                                   // the clock before its commit timestamp
  const WritersPinMarks: bool      // a writer holds a WAL read mark from its
                                   // prepare to its finish, and the backfill
                                   // stops at it

  pure val Inf = 1000   // "no end timestamp", "no reader"
  pure val None = -1    // "no visible version"

  /* A version is { b, e, value, mat }:
       b      begin timestamp. 0 means "begin None": a marker for a row that
              was only in the B-tree when a writer deleted or replaced it.
       e      end timestamp, Inf while the version is current.
       value  the row value. Values are the commit timestamp of the insert.
       mat    materialized_at: the WAL frame whose B-tree shows this version's
              last event, 0 when no frame shows it yet. */
  type Version = { b: int, e: int, value: int, mat: int }
  type Write = { ts: int, k: str, v: int }
  // mark: the WAL mark of the writer's read transaction, 0 without
  // WritersPinMarks.
  type Inflight = { ts: int, k: str, v: int, logged: bool, marked: bool, omat: int,
                    mark: int }
  type WalFrame = { w: str -> int, meta: int }
  type Cand = { k: str, s: int }
  type CkState = { pc: str, snap: int, old: int, todo: Set[str], wset: str -> int,
                   frame: int, full: int, cand: Set[Cand], clearOk: bool }
  type RdState = { pc: str, mark: int, b: int, key: str, vis1: int, valid1: bool,
                   deps1: Set[int] }
  type ErrRec = { reader: str, key: str, begin: int, mark: int, got: Set[int],
                  want: int, deps: Set[int] }
  type LateMark = { k: str, ts: int }
  type GcState = { pc: str, lwm: int, floor: int }

  var clock: int                 // Last timestamp taken from the logical clock.
  var chain: str -> Set[Version] // chain[k]: set of versions of key k in the version store.
  var inflight: Set[Inflight]    // Commits in the Preparing state.
  var lastCommitted: int         // last_committed_tx_ts (a fetch_max high-water mark).
  var llog: Set[Write]           // Records in the logical log file.
  var wal: List[WalFrame]        // WAL frames written by checkpoints. Frame i is wal[i - 1].
  var nBackfill: int             // WAL frames 1..nBackfill are copied to the database file.
  var db: str -> int             // B-tree rows in the database file.
  var dbMeta: int                // persistent_tx_ts_max in the database file.
  var durable: int               // durable_txid_max in memory.
  var backfillFloor: int         // backfill_floor in memory.
  var ck: CkState                // State of the running checkpoint pass.
  var nCk: int                   // Number of checkpoint passes started.
  var rd: str -> RdState         // rd[r]: state of reader r.
  var writes: Set[Write]         // History: every prepared write. Used only by checks.
  var logged: Set[Write]         // History: every write that reached the logical log.
  var err: Set[ErrRec]           // History: reads that returned a wrong value.
  var aborted: Set[int]          // History: commit timestamps of aborted writers.
  var readDeps: Set[int]         // History: writers that a read depended on, recorded
                                 // only when readers ignore their dependencies.
  var dirty: str -> int          // dirty[k]: stamp in checkpoint_dirty_table_keys, 0 = none.
  var lateMarks: Set[LateMark]   // Dirty marks still to do after CommitEnd (variant only).
  var fullGen: int               // checkpoint_full_scan_generation, 0 = no full scan.
  var fullMin: int               // checkpoint_full_scan_min_snapshot_ts.
  var markFails: int             // Number of failed re-marks so far.
  var gcs: GcState               // Sampled GC values when GC runs in two steps.

  // ---------------------------------------------------------------------------
  // Helpers

  pure def Max(S: Set[int]): int = S.fold(-Inf - 1, (a, x) => if (x > a) x else a)
  pure def Min(S: Set[int]): int = S.fold(Inf + 1, (a, x) => if (x < a) x else a)

  // CHOOSE x \in c : TRUE. The fold keeps the first element it visits.
  pure def AnyVersion(c: Set[Version]): Version =
    c.fold({ b: -1, e: 0, value: 0, mat: 0 }, (a, x) => if (a.b == -1) x else a)

  pure val EmptyFn: str -> int = Map()

  val InflightTs: Set[int] = inflight.map(i => i.ts)

  // A timestamp is resolved when its commit left the Preparing state. The code
  // stores a TxID instead of a timestamp until then, and the checkpoint and GC
  // skip such versions.
  def Resolved(t: int): bool = not(InflightTs.contains(t))

  // Value of key k after the writes in W with a timestamp below t. 0 = no row.
  pure def LatestIn(W: Set[Write], k: str, t: int): int =
    val ws = W.filter(w => w.k == k and w.ts < t)
    if (ws == Set()) 0
    else ws.fold({ ts: -1, k: k, v: 0 }, (a, w) => if (w.ts > a.ts) w else a).v

  // The value that a reader with begin timestamp b must see.
  def Oracle(k: str, b: int): int = LatestIn(writes, k, b)

  pure def Visible(c: Set[Version], b: int): Set[Version] =
    c.filter(v => v.b != 0 and v.b < b and b < v.e)

  pure def VisibleValue(c: Set[Version], b: int): int =
    if (Visible(c, b) == Set()) None else AnyVersion(Visible(c, b)).value

  // The B-tree row is valid for the reader unless a version shadows it: a
  // visible version, or a version that ended before the reader began.
  pure def BtreeRowValid(c: Set[Version], b: int): bool = and {
    Visible(c, b) == Set(),
    not(c.exists(v => v.e < b)),
  }

  // WAL frame number i, counted from 1.
  def Frame(i: int): WalFrame = wal[i - 1]

  // The B-tree as seen through WAL read mark m: the newest frame at or below
  // m that wrote k, else the database file.
  def FramesWith(k: str, lo: int, hi: int): Set[int] =
    lo.to(hi).filter(i => Frame(i).w.keys().contains(k))

  def BtreeAt(k: str, m: int): int =
    if (FramesWith(k, 1, m) == Set()) db.get(k)
    else Frame(Max(FramesWith(k, 1, m))).w.get(k)

  val Pinned: Set[str] = Readers.filter(r => rd.get(r).pc != "idle")
  val Registered: Set[str] = Readers.filter(r => Set("active", "reading").contains(rd.get(r).pc))

  // CHOOSE k \in Keys : TRUE
  val AnyKey: str = Keys.fold("", (a, k) => if (a == "") k else a)

  // ---------------------------------------------------------------------------
  // Initial state

  action Init = all {
    clock' = 0,
    chain' = Keys.mapBy(k => Set()),
    inflight' = Set(),
    lastCommitted' = 0,
    llog' = Set(),
    wal' = List(),
    nBackfill' = 0,
    db' = Keys.mapBy(k => 0),
    dbMeta' = 0,
    durable' = 0,
    backfillFloor' = 0,
    ck' = { pc: "idle", snap: 0, old: 0, todo: Set(), wset: EmptyFn, frame: 0,
            full: 0, cand: Set(), clearOk: false },
    nCk' = 0,
    rd' = Readers.mapBy(r => { pc: "idle", mark: 0, b: 0, key: AnyKey, vis1: None,
                               valid1: false, deps1: Set() }),
    writes' = Set(),
    logged' = Set(),
    err' = Set(),
    aborted' = Set(),
    readDeps' = Set(),
    dirty' = Keys.mapBy(k => 0),
    lateMarks' = Set(),
    fullGen' = 0,
    fullMin' = 0,
    markFails' = 0,
    gcs' = { pc: "idle", lwm: 0, floor: 0 },
  }

  // Keeps the row variables as they are.
  action UnchangedRowVars = all {
    aborted' = aborted, readDeps' = readDeps, dirty' = dirty, lateMarks' = lateMarks,
    fullGen' = fullGen, fullMin' = fullMin, markFails' = markFails, gcs' = gcs,
  }

  action UnchangedFullScan = all {
    fullGen' = fullGen, fullMin' = fullMin, markFails' = markFails,
  }

  // ---------------------------------------------------------------------------
  // Writers

  // A commit takes its timestamp from the clock, ends the current version and
  // adds the new one. In the code the chain holds the TxID until the commit
  // rewrites it to the timestamp. Readers read Preparing versions
  // speculatively, which this model shows as plain visibility, because every
  // prepared commit finishes.
  action WritePrepare(k: str, isDelete: bool): bool = {
    val ts = if (WriterBeginTicks) clock + 2 else clock + 1
    val old = Oracle(k, ts)
    val v = if (isDelete) 0 else ts
    // A version with begin None and end None is rolled-back garbage.
    val cur = chain.get(k).filter(x => x.e == Inf and x.b != 0)
    val omat = if (AbortKeepsMaterialized and cur != Set()) AnyVersion(cur).mat else 0
    val ended =
      if (cur != Set()) cur.map(x => { ...x, e: ts, mat: 0 })
      else if (old != 0) Set({ b: 0, e: ts, value: old, mat: 0 })
      else Set()
    val added = if (isDelete) Set() else Set({ b: ts, e: Inf, value: ts, mat: 0 })
    all {
      ts <= MaxTs,
      not(inflight.exists(i => i.k == k)),
      not(isDelete and old == 0),
      clock' = ts,
      chain' = chain.set(k, chain.get(k).exclude(cur).union(ended).union(added)),
      inflight' = inflight.union(Set({ ts: ts, k: k, v: v, logged: false,
                                       marked: not(DirtyKeys and GuardMarkBeforeCommitEnd),
                                       omat: omat,
                                       mark: if (WritersPinMarks) wal.length() else 0 })),
      writes' = writes.union(Set({ ts: ts, k: k, v: v })),
      lastCommitted' = lastCommitted, llog' = llog, wal' = wal, nBackfill' = nBackfill,
      db' = db, dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor,
      ck' = ck, nCk' = nCk, rd' = rd, logged' = logged, err' = err,
      UnchangedRowVars,
    }
  }

  // MarkDirtyKeys: after the commit has its timestamp and before the log
  // append, raise the stamp of the key to the commit timestamp (fetch_max).
  // A failed mark fails the commit, which WriteAbort covers.
  action WriteMarkDirty(i: Inflight): bool = all {
    not(i.marked),
    dirty' = dirty.set(i.k, Max(Set(dirty.get(i.k), i.ts))),
    inflight' = inflight.exclude(Set(i)).union(Set({ ...i, marked: true })),
    clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
    wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, rd' = rd, writes' = writes,
    logged' = logged, err' = err,
    aborted' = aborted, readDeps' = readDeps, lateMarks' = lateMarks, UnchangedFullScan,
    gcs' = gcs,
  }

  action WriteLogAppend(i: Inflight): bool = all {
    not(i.logged),
    i.marked,
    not(LogAppendOneAtATime and inflight.exists(j => j.logged)),
    llog' = llog.union(Set({ ts: i.ts, k: i.k, v: i.v })),
    logged' = logged.union(Set({ ts: i.ts, k: i.k, v: i.v })),
    inflight' = inflight.exclude(Set(i)).union(Set({ ...i, logged: true })),
    clock' = clock, chain' = chain, lastCommitted' = lastCommitted, wal' = wal,
    nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, rd' = rd, writes' = writes,
    err' = err,
    UnchangedRowVars,
  }

  action WriteFinish(i: Inflight): bool = all {
    i.logged,
    inflight' = inflight.exclude(Set(i)),
    lastCommitted' = Max(Set(lastCommitted, i.ts)),
    lateMarks' = if (DirtyKeys and not(GuardMarkBeforeCommitEnd))
                   lateMarks.union(Set({ k: i.k, ts: i.ts }))
                 else lateMarks,
    clock' = clock, chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill,
    db' = db, dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor,
    ck' = ck, nCk' = nCk, rd' = rd, writes' = writes, logged' = logged, err' = err,
    aborted' = aborted, readDeps' = readDeps, dirty' = dirty, UnchangedFullScan,
    gcs' = gcs,
  }

  // Variant only: the dirty mark happens after CommitEnd.
  action WriteLateMark(m: LateMark): bool = all {
    dirty' = dirty.set(m.k, Max(Set(dirty.get(m.k), m.ts))),
    lateMarks' = lateMarks.exclude(Set(m)),
    clock' = clock, chain' = chain, inflight' = inflight, lastCommitted' = lastCommitted,
    llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta,
    durable' = durable, backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, rd' = rd,
    writes' = writes, logged' = logged, err' = err,
    aborted' = aborted, readDeps' = readDeps, UnchangedFullScan, gcs' = gcs,
  }

  // require_checkpoint_full_scan after a failed re-mark lost stamp s.
  action RequireFullScan(s: int): bool = all {
    fullMin' = Max(Set(fullMin, s)),
    fullGen' = fullGen + 1,
    markFails' = markFails + 1,
  }

  // unmark then mark again with the stamp that was there. The second mark
  // can fail, and then the stamp is lost and a full scan is required.
  action Remark(k: str, s: int): bool = any {
    all { dirty' = dirty, UnchangedFullScan },
    all {
      markFails < MaxMarkFailures,
      dirty' = dirty.set(k, 0),
      RequireFullScan(s),
    },
  }

  // rollback_tx of a Preparing writer before its log append. The versions it
  // began are removed, the version it ended is current again (set_end(None),
  // which resets materialized_at), and a marker of a B-tree row becomes
  // garbage with begin None and end None. The code first stores Aborted and
  // then edits each chain. In between, readers and the checkpoint resolve
  // the TxID to "not committed", which gives the same result as the edited
  // chain, so the model does both in one step.
  action WriteAbort(i: Inflight): bool = {
    val k = i.k
    val rolled = chain.get(k).filter(x => x.b != i.ts)
                   .map(v => if (v.e == i.ts) { ...v, e: Inf, mat: i.omat } else v)
    val keeps = rolled.exists(v => v.b != 0 or v.e != Inf)
    val actual = dirty.get(k)
    all {
      Aborts,
      not(i.logged),
      chain' = chain.set(k, rolled),
      // unmark_checkpoint_dirty_key_if_stamp, only when no version keeps the
      // key dirty.
      if (DirtyKeys and not(keeps) and actual != 0)
        if (actual == i.ts) all { dirty' = dirty.set(k, 0), UnchangedFullScan }
        else Remark(k, actual)
      else all { dirty' = dirty, UnchangedFullScan },
      inflight' = inflight.exclude(Set(i)),
      writes' = writes.filter(w => w.ts != i.ts),
      aborted' = aborted.union(Set(i.ts)),
      clock' = clock, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
      nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, rd' = rd, logged' = logged,
      err' = err,
      readDeps' = readDeps, lateMarks' = lateMarks, gcs' = gcs,
    }
  }

  // ---------------------------------------------------------------------------
  // Readers

  action UnchangedForReaders = all {
    clock' = clock, chain' = chain, inflight' = inflight, lastCommitted' = lastCommitted,
    llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta,
    durable' = durable, backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk,
    writes' = writes, logged' = logged,
  }

  // pager.wal_pos() is read before the clock lock in begin_tx, so a reader
  // holds its WAL read mark for a while before it is in txs.
  action ReaderPin(r: str): bool = all {
    rd.get(r).pc == "idle",
    rd' = rd.setBy(r, x => { ...x, pc: "pinned", mark: wal.length() }),
    UnchangedForReaders, err' = err,
    UnchangedRowVars,
  }

  action ReaderBegin(r: str): bool = all {
    rd.get(r).pc == "pinned",
    clock < MaxTs,
    clock' = clock + 1,
    rd' = rd.setBy(r, x => { ...x, pc: "active", b: clock + 1 }),
    chain' = chain, inflight' = inflight, lastCommitted' = lastCommitted, llog' = llog,
    wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
    logged' = logged, err' = err,
    UnchangedRowVars,
  }

  // A dual cursor merges the version store side and the B-tree side, and the
  // two sides look at the chain at different moments. ReaderReadStart takes
  // the first look, ReaderReadEnd the second, and both orders are checked.
  // Writers whose Preparing versions a read of k at begin b looks at. The
  // read uses them speculatively and takes a commit dependency on each one
  // (is_visible_to, is_btree_invalidating_version).
  def SpecDeps(k: str, b: int): Set[int] =
    if (Aborts) inflight.filter(j => j.k == k and j.ts < b).map(i => i.ts)
    else Set()

  action ReaderReadStart(r: str, k: str): bool = all {
    rd.get(r).pc == "active",
    rd' = rd.setBy(r, x => { ...x, pc: "reading", key: k,
                             vis1: VisibleValue(chain.get(k), x.b),
                             valid1: BtreeRowValid(chain.get(k), x.b),
                             deps1: SpecDeps(k, x.b) }),
    UnchangedForReaders, err' = err,
    UnchangedRowVars,
  }

  action ReaderReadEnd(r: str): bool = {
    val k = rd.get(r).key
    val b = rd.get(r).b
    val vis2 = VisibleValue(chain.get(k), b)
    val valid2 = BtreeRowValid(chain.get(k), b)
    val bt = BtreeAt(k, rd.get(r).mark)
    // Version store side first, B-tree side second.
    val resA = if (rd.get(r).vis1 != None) rd.get(r).vis1 else if (valid2) bt else 0
    // B-tree side first, version store side second.
    val resB = if (vis2 != None) vis2 else if (rd.get(r).valid1) bt else 0
    val want = Oracle(k, b)
    // One look decides both sides.
    val resOne = if (vis2 != None) vis2 else if (valid2) bt else 0
    val ok = if (ReadLooksAtChainTwice) resA == want and resB == want else resOne == want
    val deps = rd.get(r).deps1.union(SpecDeps(k, b))
    all {
      rd.get(r).pc == "reading",
      err' = if (ok) err
             else err.union(Set({ reader: r, key: k, begin: b, mark: rd.get(r).mark,
                                  got: Set(resA, resB), want: want, deps: deps })),
      readDeps' = if (GuardReadersAbortOnDependency) readDeps else readDeps.union(deps),
      rd' = rd.setBy(r, x => { ...x, pc: "active", deps1: Set() }),
      UnchangedForReaders,
      aborted' = aborted, dirty' = dirty, lateMarks' = lateMarks, UnchangedFullScan,
      gcs' = gcs,
    }
  }

  action ReaderEnd(r: str): bool = all {
    Set("pinned", "active").contains(rd.get(r).pc),
    rd' = rd.setBy(r, x => { ...x, pc: "idle" }),
    UnchangedForReaders, err' = err,
    UnchangedRowVars,
  }

  // ---------------------------------------------------------------------------
  // Checkpoint

  // checkpoint_snapshot_ts: under the clock lock, stay below every commit that
  // has a timestamp but did not finish.
  val SnapshotTs: int =
    if (GuardSnapshotBelowPreparing and inflight != Set())
      Min(Set(lastCommitted, Min(InflightTs) - 1))
    else lastCommitted

  action UnchangedForCheckpoint = all {
    clock' = clock, inflight' = inflight, lastCommitted' = lastCommitted, rd' = rd,
    writes' = writes, logged' = logged, err' = err,
  }

  action CheckpointStart: bool = all {
    ck.pc == "idle",
    nCk < MaxCheckpoints,
    ck' = { pc: "collect", snap: SnapshotTs, old: durable, todo: Keys, wset: EmptyFn,
            frame: 0, full: if (DirtyKeys) fullGen else 0, cand: Set(), clearOk: false },
    nCk' = nCk + 1,
    chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
    dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor,
    UnchangedForCheckpoint,
    UnchangedRowVars,
  }

  // The key changed in (old, snap]: some resolved begin or end is in that range.
  def ChangedSince(c: Set[Version], old: int, snap: int): bool =
    c.exists(v => or {
      v.b != 0 and Resolved(v.b) and old < v.b and v.b <= snap,
      v.e != Inf and Resolved(v.e) and old < v.e and v.e <= snap,
    })

  // The value of the row at snapshot ts t: the version that began at or
  // before t and did not end at or before t. Unresolved timestamps are in
  // the future of every snapshot.
  def ValueAt(c: Set[Version], t: int): int =
    val vs = c.filter(v => and {
      v.b != 0, Resolved(v.b), v.b <= t,
      v.e == Inf or not(Resolved(v.e)) or t < v.e,
    })
    if (vs == Set()) 0 else AnyVersion(vs).value

  // Collection looks at one key at a time while writers keep committing.
  // With dirty keys, it visits a key only if the key is in the dirty map at
  // that moment, unless this pass is a full scan. A visited key with a stamp
  // at or below snap is a prune candidate (record_prune_candidate; the
  // DirtyStamps phase does the same for a full scan).
  action CheckpointCollect(k: str): bool = {
    val visit = not(DirtyKeys) or ck.full != 0 or dirty.get(k) != 0
    all {
      ck.pc == "collect",
      ck.todo.contains(k),
      ck' = { ...ck,
              todo: ck.todo.exclude(Set(k)),
              wset: if (visit and ChangedSince(chain.get(k), ck.old, ck.snap))
                      ck.wset.put(k, ValueAt(chain.get(k), ck.snap))
                    else ck.wset,
              cand: if (DirtyKeys and dirty.get(k) != 0 and dirty.get(k) <= ck.snap)
                      ck.cand.union(Set({ k: k, s: dirty.get(k) }))
                    else ck.cand },
      chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
      dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
      UnchangedForCheckpoint,
      UnchangedRowVars,
    }
  }

  // One pager transaction writes the collected rows and the new boundary
  // (persistent_tx_ts_max) as one WAL frame.
  action CheckpointCommit: bool = {
    val boundary = Max(Set(ck.old, ck.snap))
    val write = ck.wset.keys() != Set() or boundary > ck.old
    all {
      ck.pc == "collect",
      ck.todo == Set(),
      wal' = if (write) wal.append({ w: ck.wset, meta: boundary }) else wal,
      ck' = { ...ck, pc: "committed", frame: if (write) wal.length() + 1 else 0 },
      chain' = chain, llog' = llog, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta,
      durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
      UnchangedForCheckpoint,
      UnchangedRowVars,
    }
  }

  action CheckpointPublish: bool = all {
    ck.pc == "committed",
    durable' = Max(Set(ck.old, ck.snap)),
    ck' = { ...ck, pc: "published" },
    chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
    dbMeta' = dbMeta, backfillFloor' = backfillFloor, nCk' = nCk,
    UnchangedForCheckpoint,
    UnchangedRowVars,
  }

  // PASSIVE WAL checkpoint: copy frames into the database file, but never past
  // a frame that a pinned reader does not see. Busy is the case upTo = nBackfill.
  action CheckpointBackfill: bool = {
    val marks = Pinned.map(r => rd.get(r).mark)
                  .union(if (WritersPinMarks) inflight.map(i => i.mark) else Set())
    val limit = if (GuardBackfillStopsAtReaders) Min(marks.union(Set(wal.length())))
                else wal.length()
    all {
      ck.pc == "published",
      // \E to \in nBackfill..limit is false when the range is empty. "to" is
      // a built-in name in Quint, so the pick is named upTo.
      nBackfill <= limit,
      nondet upTo = (if (BackfillToLimit) Set(limit) else nBackfill.to(limit)).oneOf()
      all {
        db' = Keys.mapBy(k =>
                if (FramesWith(k, nBackfill + 1, upTo) == Set()) db.get(k)
                else Frame(Max(FramesWith(k, nBackfill + 1, upTo))).w.get(k)),
        dbMeta' = if (upTo > nBackfill) Frame(upTo).meta else dbMeta,
        nBackfill' = upTo,
      },
      ck' = { ...ck, pc: "backfilled" },
      chain' = chain, llog' = llog, wal' = wal, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk,
      UnchangedForCheckpoint,
      UnchangedRowVars,
    }
  }

  // The logical log is all or nothing: it is cut only when every record in it
  // is at or below the boundary that the B-tree now holds.
  action CheckpointTruncateLog: bool = all {
    ck.pc == "backfilled",
    llog' = if (llog.forall(l => l.ts <= durable)) Set() else llog,
    ck' = { ...ck, pc: "truncated" },
    chain' = chain, wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta,
    durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
    UnchangedForCheckpoint,
    UnchangedRowVars,
  }

  // stamp_chain_materialized: on each written key, every version whose last
  // event is resolved and at or below snap is in the B-tree from this frame on.
  pure def LastEvent(v: Version): int = if (v.e != Inf) v.e else v.b

  action CheckpointStamp: bool = all {
    ck.pc == "truncated",
    backfillFloor' = nBackfill,
    chain' = Keys.mapBy(k =>
               if (ck.wset.keys().contains(k))
                 chain.get(k).map(v =>
                   if (Resolved(LastEvent(v)) and LastEvent(v) <= ck.snap) { ...v, mat: ck.frame }
                   else v)
               else chain.get(k)),
    ck' = { ...ck, pc: if (DirtyKeys) "prune" else "idle" },
    llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta,
    durable' = durable, nCk' = nCk,
    UnchangedForCheckpoint,
    UnchangedRowVars,
  }

  // PruneDirtyKeys, one candidate at a time: remove the key from the dirty
  // map. If a writer raised the stamp since collection, mark it again.
  action CheckpointPrune(c: Cand): bool = {
    val latest = dirty.get(c.k)
    all {
      ck.pc == "prune",
      ck.cand.contains(c),
      if (latest == 0) all { dirty' = dirty, UnchangedFullScan }
      else if (latest == c.s) all { dirty' = dirty.set(c.k, 0), UnchangedFullScan }
      else Remark(c.k, latest),
      ck' = { ...ck, cand: ck.cand.exclude(Set(c)) },
      chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
      dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
      UnchangedForCheckpoint,
      aborted' = aborted, readDeps' = readDeps, lateMarks' = lateMarks, gcs' = gcs,
    }
  }

  // clear_checkpoint_full_scan, first half: compare snap with the highest
  // lost stamp.
  action CheckpointPruneDone: bool = all {
    ck.pc == "prune",
    ck.cand == Set(),
    ck' = { ...ck, pc: if (ck.full != 0) "clear" else "idle",
                   clearOk: not(GuardFullScanUntilStampCovered) or ck.snap >= fullMin },
    chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
    dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
    UnchangedForCheckpoint,
    UnchangedRowVars,
  }

  // clear_checkpoint_full_scan, second half: compare_exchange(generation, 0).
  action CheckpointClearFullScan: bool = all {
    ck.pc == "clear",
    fullGen' = if (ck.clearOk and (not(GuardFullScanGenerationCas) or fullGen == ck.full)) 0
               else fullGen,
    ck' = { ...ck, pc: "idle" },
    chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill, db' = db,
    dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
    UnchangedForCheckpoint,
    aborted' = aborted, readDeps' = readDeps, dirty' = dirty, lateMarks' = lateMarks,
    fullMin' = fullMin, markFails' = markFails, gcs' = gcs,
  }

  // ---------------------------------------------------------------------------
  // Garbage collection of the version store

  // compute_lwm: the lowest begin timestamp of a registered reader.
  val Lwm: int = if (Registered == Set()) Inf else Min(Registered.map(r => rd.get(r).b))

  // The lowest read mark that GC must respect.
  val GcFloor: int =
    if (GcFloorRule == "code")
      Min(Registered.map(r => rd.get(r).mark).union(Set(backfillFloor, Inf)))
    else if (GcFloorRule == "registered")
      Min(Registered.map(r => rd.get(r).mark).union(Set(Inf)))
    else if (GcFloorRule == "pinned")
      Min(Pinned.map(r => rd.get(r).mark).union(Set(Inf)))
    else backfillFloor

  // Rule 2: a superseded version that no snapshot sees.
  def Rule2(v: Version, lwm: int, floor: int): bool = and {
    v.e != Inf, Resolved(v.e), v.e < lwm,
    not(GuardGcWaitsForReaders) or (v.mat != 0 and v.mat <= floor),
  }

  // Rule 3: the last, current version, once the B-tree has it.
  def Rule3(c: Set[Version], lwm: int, floor: int): bool = and {
    c.size() == 1,
    val v = AnyVersion(c)
    and {
      v.e == Inf, v.b != 0, Resolved(v.b),
      not(GuardGcWaitsForReaders) or (v.mat != 0 and v.mat <= floor),
      not(GuardDropCurrentOnlyWhenIdle) or lwm == Inf,
    },
  }

  // gc_version_chain. Rule 1 drops rolled-back garbage (begin None, end None).
  def GcChain(k: str, lwm: int, floor: int): Set[Version] =
    val afterRule1 = chain.get(k).filter(v => not(v.b == 0 and v.e == Inf))
    val afterRule2 = afterRule1.filter(v => not(Rule2(v, lwm, floor)))
    if (Rule3(afterRule2, lwm, floor)) Set() else afterRule2

  action UnchangedForGc = all {
    clock' = clock, inflight' = inflight, lastCommitted' = lastCommitted, llog' = llog,
    wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, rd' = rd, writes' = writes,
    logged' = logged, err' = err,
  }

  action Gc(k: str): bool = all {
    not(GcSamplesFirst),
    GcChain(k, Lwm, GcFloor) != chain.get(k),
    chain' = chain.set(k, GcChain(k, Lwm, GcFloor)),
    UnchangedForGc,
    UnchangedRowVars,
  }

  // sample_gc_lwm under the clock lock, and the floor, before any chain lock.
  action GcSample: bool = all {
    GcSamplesFirst,
    gcs.pc == "idle",
    gcs' = { pc: "sampled", lwm: Lwm, floor: GcFloor },
    chain' = chain,
    UnchangedForGc,
    aborted' = aborted, readDeps' = readDeps, dirty' = dirty, lateMarks' = lateMarks,
    UnchangedFullScan,
  }

  // gc_chain_now under the chain lock, with the sampled values. An lwm of
  // "no reader" is computed again, because a reader may have begun since.
  action GcApply(k: str): bool = {
    val lwm = if (GuardGcRechecksIdleLwm and gcs.lwm == Inf) Lwm else gcs.lwm
    all {
      gcs.pc == "sampled",
      GcChain(k, lwm, gcs.floor) != chain.get(k),
      chain' = chain.set(k, GcChain(k, lwm, gcs.floor)),
      UnchangedForGc,
      UnchangedRowVars,
    }
  }

  action GcEnd: bool = all {
    gcs.pc == "sampled",
    gcs' = { pc: "idle", lwm: 0, floor: 0 },
    chain' = chain,
    UnchangedForGc,
    aborted' = aborted, readDeps' = readDeps, dirty' = dirty, lateMarks' = lateMarks,
    UnchangedFullScan,
  }

  // ---------------------------------------------------------------------------
  // Specification

  // Each pick has a name, so `quint run --mbt` records it. A set can be empty, so a pick from it sits behind a check.
  action Next = any {
    nondet k = Keys.oneOf()
    nondet isDelete = Bool.oneOf()
    WritePrepare(k, isDelete),
    all {
      inflight != Set(),
      nondet i = inflight.oneOf()
      any { WriteMarkDirty(i), WriteLogAppend(i), WriteFinish(i), WriteAbort(i) },
    },
    all {
      lateMarks != Set(),
      nondet m = lateMarks.oneOf()
      WriteLateMark(m),
    },
    all {
      Readers != Set(),
      nondet r = Readers.oneOf()
      any {
        ReaderPin(r),
        ReaderBegin(r),
        nondet k = Keys.oneOf()
        ReaderReadStart(r, k),
        ReaderReadEnd(r),
        ReaderEnd(r),
      },
    },
    CheckpointStart,
    nondet k = Keys.oneOf()
    CheckpointCollect(k),
    CheckpointCommit,
    CheckpointPublish,
    CheckpointBackfill,
    CheckpointTruncateLog,
    CheckpointStamp,
    all {
      ck.cand != Set(),
      nondet c = ck.cand.oneOf()
      CheckpointPrune(c),
    },
    CheckpointPruneDone,
    CheckpointClearFullScan,
    nondet k = Keys.oneOf()
    Gc(k),
    GcSample,
    nondet k = Keys.oneOf()
    GcApply(k),
    GcEnd,
  }

  // ---------------------------------------------------------------------------
  // Properties

  // A reader that depended on a writer that aborted must abort too
  // (abort_now), so its reads do not count. A read whose writers are still
  // Preparing is not decided yet.
  val ReadsCorrect: bool =
    if (GuardReadersAbortOnDependency)
      err.forall(e => e.deps.intersect(aborted.union(InflightTs)) != Set())
    else err == Set() and readDeps.intersect(aborted) == Set()

  // Every key with a committed change in (old, snap] is in the frame. This is
  // the property that the dirty-key map must keep.
  val CollectsEveryChange: bool =
    ck.pc == "committed" implies
      Keys.forall(k => ChangedSince(chain.get(k), ck.old, ck.snap)
                         implies ck.wset.keys().contains(k))

  // Crash recovery: read the B-tree through every WAL frame, then replay the
  // logical log records above the boundary stored in the B-tree.
  val RecoveredMeta: int = if (wal == List()) dbMeta else Frame(wal.length()).meta

  def Recovered(k: str): int =
    val replay = llog.filter(l => l.k == k and l.ts > RecoveredMeta)
    if (replay == Set()) BtreeAt(k, wal.length())
    else replay.fold({ ts: -1, k: k, v: 0 }, (a, l) => if (l.ts > a.ts) l else a).v

  val RecoveryCorrect: bool = Keys.forall(k => Recovered(k) == LatestIn(logged, k, Inf))

  // The WAL rule that the B-tree side of every read depends on: no pinned
  // reader has a read mark below the backfilled frames.
  val BackfillBelowReaders: bool = Pinned.forall(r => nBackfill <= rd.get(r).mark)

  // backfillFloor is at or below every pinned read mark. So the reader marks
  // in the "code" GC floor never lower it: that floor equals backfillFloor.
  val BackfillFloorBelowReaders: bool = and {
    backfillFloor <= nBackfill,
    Pinned.forall(r => backfillFloor <= rd.get(r).mark),
  }

  val TypeOK: bool = and {
    clock.in(0.to(MaxTs)),
    nBackfill.in(0.to(wal.length())),
    wal.length() <= MaxCheckpoints,
    durable <= lastCommitted,
    Keys.forall(k => chain.get(k).forall(v => and {
      v.b.in(0.to(MaxTs)),
      v.e.in(1.to(MaxTs).union(Set(Inf))),
      v.mat.in(0.to(wal.length())),
    })),
  }

  // ---------------------------------------------------------------------------
  // Reachability checks. Each one must be violated, which proves that the
  // model reaches the state.

  val NeverDropsCurrentRow: bool =
    not(Keys.exists(k => chain.get(k) == Set() and LatestIn(writes, k, Inf) != 0))
  val NeverTruncatesLog: bool = not(llog == Set() and logged != Set())
  val NeverReadsBtreeAtOldMark: bool =
    not(Readers.exists(r => and {
      rd.get(r).pc == "reading", rd.get(r).mark < wal.length(), rd.get(r).mark > 0,
    }))
  val NeverBackfills: bool = nBackfill == 0
  // After a rollback, a current version that the B-tree already has is left
  // with materialized_at = ORIGIN, and no later checkpoint stamps it again.
  val NeverLeavesUnstampedCurrent: bool =
    not(and {
      ck.pc == "idle", inflight == Set(), nCk == MaxCheckpoints,
      Keys.exists(k => chain.get(k).exists(v =>
        v.e == Inf and v.b != 0 and v.b <= durable and v.mat == 0)),
    })
}
