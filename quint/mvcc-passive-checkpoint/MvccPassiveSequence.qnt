// -*- mode: Bluespec; -*-
/**
 * Turso MVCC sequences (nextval, setval) and their compaction by the
 * checkpoint.
 *
 * A sequence keeps its state in a backing table with `value INTEGER PRIMARY
 * KEY`. The current value of the sequence is the last row (Last) for an
 * ascending sequence and the first row (Rewind) for a descending one. A
 * non-CYCLE nextval inserts one new row and deletes nothing. A CYCLE nextval
 * and setval insert one row and delete every other row in the same
 * transaction. The checkpoint state CompactSequences deletes every row of a
 * non-CYCLE backing table except the watermark row. See DESIGN-sequences.md
 * for the mapping to the Rust code.
 *
 * This module reuses the row protocol of MvccPassiveCheckpoint.qnt: version
 * chains with begin, end and materialized_at, a WAL of checkpoint frames,
 * a logical log, the snapshot clamp, and the GC rules of the code. The keys
 * of the table are the sequence values.
 *
 * Actors.
 *   Txs        Connections. A connection begins a transaction (it pins a
 *              WAL read mark and takes a begin timestamp), reads the current
 *              value, or runs nextval or setval in an inner transaction and
 *              commits it. The commit validates first-committer-wins on every
 *              key that it writes, then takes a commit timestamp and writes
 *              the logical log record (Prepare), then finishes.
 *   The checkpoint runs one pass at a time: snapshot, collect, compact the
 *              sequence, commit one WAL frame, publish, backfill, truncate
 *              the logical log, stamp.
 *   GC         drops versions. It can run at any time.
 *
 * Properties.
 *   ReadsCorrect     Every read of the current value (by a reader or by
 *                    nextval) returns the current value at the reader's begin
 *                    timestamp, from the history of committed writes and
 *                    compaction deletes.
 *   CompactionKeepsCurrent  A compaction delete never ends the row that
 *                    holds the current value at its timestamp.
 *   UniqueValues     Without setval and CYCLE, no two committed nextval calls
 *                    return the same value.
 *   RecoveryCorrect  If the process crashes now, recovery rebuilds the
 *                    current value of the sequence.
 *   NoStrandedVersions  A version whose last event is at or below the
 *                    checkpoint boundary is in the B-tree, so GC can drop it.
 *
 * Not modeled: the sequence_allocations / sequence_watermark registry (it
 * serves forward-only cursor scans, not nextval), the is_called flag, the
 * initial row of CREATE SEQUENCE, several sequences, the exclusive-mode
 * path that skips the inner transaction, schema changes and root pages,
 * the dual-cursor double look (a read looks at each chain once), and the
 * GC rules of the blocking mode (all three modes use the passive rules).
 *
 * Notes on the Quint code:
 *   - Txs are strings. The model checker sees every order of them.
 *   - Max, Min and "the one element of a set" use fold. PresentIn states
 *     its tie rule in its comparison (see there), so the simulator, which
 *     can visit a set in another order, picks the same write.
 *   - Lists start at index 0. WAL frame numbers still start at 1: frame i
 *     is wal[i - 1] (see Frame).
 *   - Every action assigns every variable.
 *   - Each argument that a step picks comes from a named nondet in Next.
 *     `quint run --mbt` then records the action and its picks in the
 *     trace. "to" is a built-in name in Quint, so the backfill pick is
 *     named upTo.
 */
module MvccPassiveSequence {
  const MaxVal: int           // Sequence values are 1..MaxVal. They are the row keys.
  const Txs: Set[str]         // Connections.
  const MaxTs: int            // Bound on the clock for transaction begins and commits.
  const CheckpointOwnerReaders: bool
  const FixRefuseCheckpointOwner: bool
  const FixStampAtPagerCommit: bool
  const FixCleanupPublishes: bool
  const CheckpointFailures: bool // A pass can stop before it finishes.
  const MaxCheckpoints: int   // Bound on the number of checkpoint passes.
  const Ascending: bool       // true: increment 1, start 1. false: increment -1,
                              // start MaxVal.
  const Cycle: bool           // The sequence is CYCLE.
  const WithSetval: bool      // Connections can call setval.
  const Mode: str             // "passive", "blocking", or "truncateFlagOn".

  // Special cases in the code. true in the main configuration.
  const GuardWatermarkFollowsDirection: bool // Last for ascending, Rewind for descending
  const GuardDeferDeletes: bool              // passive: record deletes, apply them at publish
  const GuardSkipCycle: bool                 // CompactSequences skips CYCLE sequences
  const GuardBlockingPurge: bool             // blocking: purge the version chain inline

  // Proposed fix, false in the code today: the recorded delete ends the
  // live version only if that version is the one the B-tree held at the
  // snapshot (begin <= snapshot_ts). See NoSetvalRace in DESIGN-sequences.md.
  const FixStampSnapshotVersion: bool

  // The commit timestamp of a recorded delete. One of:
  //   "clock"     a new clock timestamp in the publish window (the code)
  //   "snapshot"  the snapshot timestamp (= durable_txid_max_new)
  const DeleteTsRule: str

  val Vals: Set[int] = 1.to(MaxVal)
  pure val Inf = 1000
  val Start: int = if (Ascending) 1 else MaxVal

  /* A version is { b, e, mat }:
       b    begin timestamp. 0 marks a row that was only in the B-tree when
            a commit or a recorded delete ended it.
       e    end timestamp, Inf while the version is current.
       mat  materialized_at: the WAL frame whose B-tree shows the last event
            of this version, 0 when no frame shows it yet. */
  type Version = { b: int, e: int, mat: int }
  type LogRec = { ts: int, k: int, p: bool }
  type Write = { ts: int, k: int, p: bool, cmp: bool }
  type Handed = { ts: int, v: int }
  type ErrRec = { tx: str, b: int, mark: int, got: int, want: int }
  type WalFrame = { w: int -> bool, meta: int }
  type PurgedVersion = { k: int, b: int, e: int, mat: int }
  type CkState = { pc: str, owner: str, auto: bool, history: Set[str], purged: Set[PurgedVersion],
                   snap: int, old: int, wset: int -> bool, frame: int,
                   seqdel: Set[int], pagerOwned: bool }
  type TxState = { pc: str, b: int, mark: int, c: int, ins: int, del: Set[int], nv: bool, outerB: int, outerMark: int, pinned: bool }

  var clock: int                 // Last timestamp taken from the logical clock.
  var chain: int -> Set[Version] // chain[k]: set of versions of key k.
  var lastCommitted: int         // last_committed_tx_ts.
  var llog: Set[LogRec]          // Records in the logical log file.
  var wal: List[WalFrame]        // WAL frames written by checkpoints. Frame i is wal[i - 1].
  var nBackfill: int             // WAL frames 1..nBackfill are in the database file.
  var db: int -> bool            // db[k]: the row k is in the database file.
  var dbMeta: int                // persistent_tx_ts_max in the database file.
  var durable: int               // durable_txid_max in memory.
  var backfillFloor: int         // backfill_floor in memory.
  var ck: CkState                // State of the running checkpoint pass.
  var nCk: int                   // Number of checkpoint passes started.
  var tx: str -> TxState         // tx[t]: state of connection t.
  var writes: Set[Write]         // History: every committed write { ts, k, p, cmp }, and
                                 // every compaction delete (cmp = true). Checks only.
  var handed: Set[Handed]        // History: every committed nextval { ts, v }. Checks only.
  var err: Set[ErrRec]           // History: reads that returned a wrong value.

  // ---------------------------------------------------------------------------
  // Helpers

  pure def Max(S: Set[int]): int = S.fold(-Inf - 1, (a, x) => if (x > a) x else a)
  pure def Min(S: Set[int]): int = S.fold(Inf + 1, (a, x) => if (x < a) x else a)

  // CHOOSE x \in c : TRUE. The fold keeps the first element it visits.
  pure def AnyVersion(c: Set[Version]): Version =
    c.fold({ b: -1, e: 0, mat: 0 }, (a, x) => if (a.b == -1) x else a)

  pure val EmptyFn: int -> bool = Map()

  // The current value of a set of rows: Last for ascending, Rewind for
  // descending. 0 = the table is empty.
  def Current(rows: Set[int]): int =
    if (rows == Set()) 0
    else if (Ascending) Max(rows) else Min(rows)

  // SequenceComputeNext with increment +1 or -1. 0 = the sequence is
  // exhausted. A CYCLE sequence wraps to its start.
  def NextOf(m: int): int =
    if (m == 0) Start
    else {
      val n = if (Ascending) m + 1 else m - 1
      if (Vals.contains(n)) n else if (Cycle) Start else 0
    }

  val InflightTs: Set[int] = Txs.filter(u => tx.get(u).pc == "preparing").map(t => tx.get(t).c)

  // A timestamp is resolved when its commit left the Preparing state.
  def Resolved(t: int): bool = not(InflightTs.contains(t))

  // Row k exists after the writes in W with a timestamp below t.
  // The write with the highest timestamp decides. A commit and a compaction
  // delete of k can have the same timestamp (the blocking mode deletes at
  // the snapshot timestamp). Then the commit (cmp = false) decides. The
  // comparison below states that rule.
  pure def PresentIn(W: Set[Write], k: int, t: int): bool =
    val ws = W.filter(w => w.k == k and w.ts < t)
    ws != Set() and
      ws.fold({ ts: -1, k: k, p: false, cmp: false }, (a, w) =>
        if (w.ts > a.ts or (w.ts == a.ts and (a.cmp and not(w.cmp)
                                              or (a.cmp == w.cmp and a.p and not(w.p))))) w
        else a).p

  def OracleRows(b: int): Set[int] = Vals.filter(k => PresentIn(writes, k, b))

  pure def Visible(c: Set[Version], b: int): Set[Version] =
    c.filter(v => v.b != 0 and v.b < b and b < v.e)

  pure def BtreeRowValid(c: Set[Version], b: int): bool = and {
    Visible(c, b) == Set(),
    not(c.exists(v => v.e < b)),
  }

  // WAL frame number i, counted from 1.
  def Frame(i: int): WalFrame = wal[i - 1]

  def FramesWith(k: int, lo: int, hi: int): Set[int] =
    lo.to(hi).filter(i => Frame(i).w.keys().contains(k))

  def BtreeAt(k: int, m: int): bool =
    if (FramesWith(k, 1, m) == Set()) db.get(k)
    else Frame(Max(FramesWith(k, 1, m))).w.get(k)

  // The read path of one key: the version store first, then the B-tree at
  // the reader's mark.
  def PresentFor(k: int, b: int, m: int): bool =
    if (Visible(chain.get(k), b) != Set()) true
    else BtreeRowValid(chain.get(k), b) and BtreeAt(k, m)

  def RowsFor(b: int, m: int): Set[int] = Vals.filter(k => PresentFor(k, b, m))

  val Registered: Set[str] = Txs.filter(t => tx.get(t).pc != "idle" or tx.get(t).outerB != 0)

  val Pinned: Set[str] = Txs.filter(t => tx.get(t).pinned)
  val ReaderMarks: Set[int] =
    Registered.map(t => if (tx.get(t).outerB != 0) tx.get(t).outerMark else tx.get(t).mark)

  // ---------------------------------------------------------------------------
  // Initial state

  action Init = all {
    clock' = 0,
    chain' = Vals.mapBy(k => Set()),
    lastCommitted' = 0,
    llog' = Set(),
    wal' = List(),
    nBackfill' = 0,
    db' = Vals.mapBy(k => false),
    dbMeta' = 0,
    durable' = 0,
    backfillFloor' = 0,
    ck' = { pc: "idle", owner: "", auto: false, history: Set(), purged: Set(), snap: 0, old: 0, wset: EmptyFn, frame: 0, seqdel: Set(), pagerOwned: false },
    nCk' = 0,
    tx' = Txs.mapBy(t => { pc: "idle", b: 0, mark: 0, c: 0, ins: 0, del: Set(), nv: false, outerB: 0, outerMark: 0, pinned: false }),
    writes' = Set(),
    handed' = Set(),
    err' = Set(),
  }

  // ---------------------------------------------------------------------------
  // Connections

  // begin_tx: pin a WAL read mark, then take a begin timestamp. A blocking
  // checkpoint holds the checkpoint lock, so no transaction begins while it
  // runs. (The window between the pin and the clock lock is in
  // MvccPassiveCheckpoint.qnt, rule R5.)
  action TxBegin(t: str): bool = all {
    ck.owner != t or ck.pc == "idle",
    tx.get(t).pc == "idle",
    clock < MaxTs,
    Mode == "blocking" implies ck.pc == "idle",
    clock' = clock + 1,
    tx' = tx.setBy(t, x => { ...x, pc: "active", b: clock + 1, pinned: true,
      mark: if (x.outerB != 0 and x.pinned) x.outerMark else wal.length() }),
    chain' = chain, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
    nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
    handed' = handed, err' = err,
  }

  action OuterBegin(t: str): bool = all {
    ck.owner != t or ck.pc == "idle",
    CheckpointOwnerReaders,
    tx.get(t).pc == "idle", tx.get(t).outerB == 0,
    clock < MaxTs,
    Mode == "blocking" implies ck.pc == "idle",
    clock' = clock + 1,
    tx' = tx.setBy(t, x => { ...x, outerB: clock + 1, outerMark: wal.length(),
                            mark: wal.length(), pinned: true }),
    chain' = chain, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
    nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
    handed' = handed, err' = err,
  }

  action OuterEnd(t: str): bool = all {
    ck.owner != t or ck.pc == "idle",
    tx.get(t).pc == "idle", tx.get(t).outerB != 0,
    tx' = tx.setBy(t, x => { ...x, outerB: 0, pinned: false }),
    clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
    wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
    handed' = handed, err' = err,
  }

  def ReadErr(t: str, got: int): Set[ErrRec] =
    if (got == Current(OracleRows(tx.get(t).b))) err
    else err.union(Set({ tx: t, b: tx.get(t).b, mark: tx.get(t).mark, got: got,
                         want: Current(OracleRows(tx.get(t).b)) }))

  // SELECT currval-like read of the backing table: Last or Rewind.
  action TxRead(t: str): bool = {
    val b = if (tx.get(t).outerB != 0) tx.get(t).outerB else tx.get(t).b
    val mark = if (tx.get(t).outerB != 0) tx.get(t).outerMark else tx.get(t).mark
    val got = Current(RowsFor(b, mark))
    val want = Current(OracleRows(b))
    all {
      ck.owner != t or ck.pc == "idle",
      tx.get(t).pc == "active" or (tx.get(t).pc == "idle" and tx.get(t).outerB != 0),
      err' = if (got == want) err else err.union(Set({ tx: t, b: b, mark: mark,
                                                      got: got, want: want })),
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, tx' = tx, writes' = writes,
      handed' = handed,
    }
  }

  // emit_disk_read_nextval: read the current value, compute the next value,
  // insert it. A CYCLE sequence also deletes every other row it sees.
  action TxNextval(t: str): bool = {
    val rows = RowsFor(tx.get(t).b, tx.get(t).mark)
    val n = NextOf(Current(rows))
    all {
      ck.owner != t or ck.pc == "idle",
      tx.get(t).pc == "active",
      n != 0,
      err' = ReadErr(t, Current(rows)),
      tx' = tx.setBy(t, x => { ...x, pc: "written", ins: n,
                               del: if (Cycle) rows.exclude(Set(n)) else Set(),
                               nv: true }),
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
      handed' = handed,
    }
  }

  // setval(v): delete every row, insert v.
  action TxSetval(t: str, v: int): bool = {
    val rows = RowsFor(tx.get(t).b, tx.get(t).mark)
    all {
      ck.owner != t or ck.pc == "idle",
      WithSetval,
      tx.get(t).pc == "active",
      tx' = tx.setBy(t, x => { ...x, pc: "written", ins: v, del: rows.exclude(Set(v)),
                               nv: false }),
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
      handed' = handed, err' = err,
    }
  }

  action TxEnd(t: str): bool = all {
    ck.owner != t or ck.pc == "idle",
    tx.get(t).pc == "active",
    tx' = tx.setBy(t, x => { ...x, pc: "idle", pinned: x.outerB != 0 and x.pinned }),
    clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
    wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, writes' = writes,
    handed' = handed, err' = err,
  }

  // check_version_conflicts: a key conflicts when another commit changed it
  // after our begin (a later begin, or an end after our begin, also from a
  // recorded sequence delete).
  def Conflict(k: int, b: int): bool =
    chain.get(k).exists(v => v.b > b or (v.e != Inf and v.e > b))

  // End the version of k that the writer sees, as a delete or as the old
  // side of an update. A row that is only in the B-tree gets a marker.
  def EndRow(k: int, t: str, c: int): Set[Version] =
    val cur = chain.get(k).filter(x => x.e == Inf and x.b != 0)
    if (cur != Set()) chain.get(k).exclude(cur).union(cur.map(x => { ...x, e: c, mat: 0 }))
    else if (PresentFor(k, tx.get(t).b, tx.get(t).mark))
      chain.get(k).union(Set({ b: 0, e: c, mat: 0 }))
    else chain.get(k)

  // SequenceCommitInnerTx: validate, take the commit timestamp, write the
  // logical log record. On a conflict the inner transaction rolls back. The
  // statement then retries with a new inner transaction, which is a new
  // TxBegin in this model. nextval always runs in its own inner transaction,
  // so a connection commits at most one write per TxBegin.
  action TxCommit(t: str, ckNow: bool): bool = {
    val auto = ckNow and not(FixRefuseCheckpointOwner and tx.get(t).outerB != 0)
    val b = tx.get(t).b
    val txKeys = tx.get(t).del.union(Set(tx.get(t).ins))
    val c = clock + 1
    val recs = tx.get(t).del.map(k => { ts: c, k: k, p: false })
                 .union(Set({ ts: c, k: tx.get(t).ins, p: true }))
    all {
      ck.owner != t or ck.pc == "idle",
      tx.get(t).pc == "written",
      ckNow implies (CheckpointOwnerReaders and Mode == "passive"
                       and ck.pc == "idle" and nCk < MaxCheckpoints),
      if (txKeys.exists(k => Conflict(k, b))) all {
        tx' = tx.setBy(t, x => { ...x, pc: "idle", pinned: x.outerB != 0 and x.pinned }),
        clock' = clock, chain' = chain, llog' = llog, writes' = writes, handed' = handed,
        lastCommitted' = lastCommitted, ck' = ck, nCk' = nCk,
      } else all {
        clock < MaxTs,
        clock' = c,
        chain' = Vals.mapBy(k =>
                   if (tx.get(t).del.contains(k)) EndRow(k, t, c)
                   else if (k == tx.get(t).ins)
                     EndRow(k, t, c).union(Set({ b: c, e: Inf, mat: 0 }))
                   else chain.get(k)),
        llog' = llog.union(recs),
        writes' = writes.union(recs.map(r => { ts: r.ts, k: r.k, p: r.p, cmp: false })),
        handed' = if (tx.get(t).nv) handed.union(Set({ ts: c, v: tx.get(t).ins }))
                  else handed,
        tx' = tx.setBy(t, x => { ...x, pc: if (auto) "idle" else "preparing", c: c,
          pinned: if (auto) x.outerB != 0 and x.pinned else x.pinned }),
        lastCommitted' = if (auto) Max(Set(lastCommitted, c)) else lastCommitted,
        ck' = if (auto) { pc: "collect", owner: t, auto: true, history: ck.history, purged: Set(),
                          snap: Min(InflightTs.union(Set(c + 1))) - 1,
                          old: durable, wset: EmptyFn, frame: 0, seqdel: Set(), pagerOwned: false } else ck,
        nCk' = if (auto) nCk + 1 else nCk,
      },
      wal' = wal, nBackfill' = nBackfill, db' = db,
      dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor, err' = err,
    }
  }

  action TxFinish(t: str): bool = all {
      ck.owner != t or ck.pc == "idle",
    tx.get(t).pc == "preparing",
    lastCommitted' = Max(Set(lastCommitted, tx.get(t).c)),
    tx' = tx.setBy(t, x => { ...x, pc: "idle", pinned: x.outerB != 0 and x.pinned }),
    clock' = clock, chain' = chain, llog' = llog, wal' = wal, nBackfill' = nBackfill,
    db' = db, dbMeta' = dbMeta, durable' = durable, backfillFloor' = backfillFloor,
    ck' = ck, nCk' = nCk, writes' = writes, handed' = handed, err' = err,
  }

  // ---------------------------------------------------------------------------
  // Checkpoint

  val SnapshotTs: int =
    if (InflightTs != Set()) Min(Set(lastCommitted, Min(InflightTs) - 1))
    else lastCommitted

  // A blocking (TRUNCATE) checkpoint waits for the checkpoint lock, so no
  // transaction is open while it runs.
  action CheckpointStart: bool = {
    nondet owner = (if (CheckpointOwnerReaders) Registered.filter(t => tx.get(t).pc == "idle").union(Set("")) else Set("")).oneOf()
    all {
      (FixRefuseCheckpointOwner and owner != "") implies not(Registered.contains(owner)),
      ck.pc == "idle",
      nCk < MaxCheckpoints,
      Mode == "blocking" implies Registered == Set(),
      ck' = { pc: "collect", owner: owner, auto: false, history: ck.history, purged: Set(), snap: SnapshotTs, old: durable, wset: EmptyFn, frame: 0,
              seqdel: Set(), pagerOwned: false },
      nCk' = nCk + 1,
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, tx' = tx, writes' = writes, handed' = handed,
      err' = err,
    }
  }

  def ChangedSince(c: Set[Version], old: int, snap: int): bool =
    c.exists(v => or {
      v.b != 0 and Resolved(v.b) and old < v.b and v.b <= snap,
      v.e != Inf and Resolved(v.e) and old < v.e and v.e <= snap,
    })

  def ExistsAt(c: Set[Version], t: int): bool =
    c.exists(v => and {
      v.b != 0, Resolved(v.b), v.b <= t,
      v.e == Inf or not(Resolved(v.e)) or t < v.e,
    })

  // Collection of every key in one step. The per-key interleaving with
  // writers is in MvccPassiveCheckpoint.qnt.
  action CheckpointCollect: bool = {
    val ch = Vals.filter(k => ChangedSince(chain.get(k), ck.old, ck.snap))
    all {
      ck.pc == "collect",
      ck' = { ...ck, pc: if (ch != Set() or ck.snap > ck.old) "beginPager" else "published",
              wset: ch.mapBy(k => ExistsAt(chain.get(k), ck.snap)) },
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk, tx' = tx, writes' = writes,
      handed' = handed, err' = err,
    }
  }

  action CheckpointBeginPager: bool = {
    val stale = ck.owner != "" and tx.get(ck.owner).pinned
                  and tx.get(ck.owner).mark != wal.length()
    all {
      ck.pc == "beginPager",
      ck' = { ...ck, pc: if (stale) "idle" else "compact", pagerOwned: not(stale) },
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk, tx' = tx, writes' = writes,
      handed' = handed, err' = err,
    }
  }

  // The B-tree inside the checkpoint's pager transaction: the last WAL frame
  // plus the rows this pass wrote.
  def CkBtree(k: int): bool =
    if (ck.wset.keys().contains(k)) ck.wset.get(k) else BtreeAt(k, wal.length())

  // SeqCompactDriver: find the watermark row with Last (ascending) or Rewind
  // (descending), then take every other row. Passive records the rows in
  // `compacted`. Blocking deletes them from the B-tree and purges their
  // version chains (purge_row_versions_during_checkpoint).
  action CheckpointCompact: bool = {
    val rows = Vals.filter(k => CkBtree(k))
    val skip = Cycle and GuardSkipCycle
    val wm = if (GuardWatermarkFollowsDirection) Current(rows)
             else if (rows == Set()) 0 else Max(rows)
    val victims = if (skip or rows == Set()) Set() else rows.exclude(Set(wm))
    val passive = Mode == "passive" and GuardDeferDeletes
    val purge = if (Mode == "passive") true else GuardBlockingPurge
    all {
      ck.pc == "compact",
      if (passive) all {
        ck' = { ...ck, pc: "compacted", seqdel: victims },
        chain' = chain, writes' = writes,
      } else all {
        ck' = { ...ck, pc: "compacted", purged: if (purge) victims.fold(Set(), (removed, k) => removed.union(
                  chain.get(k).map(v => { k: k, b: v.b, e: v.e, mat: v.mat }))) else Set(),
                wset: ck.wset.keys().union(victims).mapBy(k =>
                        if (victims.contains(k)) false else ck.wset.get(k)) },
        chain' = Vals.mapBy(k => if (purge and victims.contains(k)) Set() else chain.get(k)),
        writes' = writes.union(victims.map(k => { ts: ck.snap, k: k, p: false, cmp: true })),
      },
      clock' = clock, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
      nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk, tx' = tx, handed' = handed, err' = err,
    }
  }

  action CheckpointCommit: bool = {
    val boundary = Max(Set(ck.old, ck.snap))
    val write = ck.wset.keys() != Set() or boundary > ck.old
    all {
      ck.pc == "compacted", ck.pagerOwned,
      wal' = if (write) wal.append({ w: ck.wset, meta: boundary }) else wal,
      ck' = { ...ck, pc: "committed", frame: if (write) wal.length() + 1 else 0, pagerOwned: false,
        history: if (ck.auto and ck.owner != "" and write
                     and tx.get(ck.owner).outerB != 0 and tx.get(ck.owner).pinned)
                   ck.history.union(Set("autoOwnerCommit")) else ck.history },
      clock' = clock, lastCommitted' = lastCommitted, llog' = llog,
      chain' = if (FixStampAtPagerCommit and write) Vals.mapBy(k =>
        if (ck.wset.keys().contains(k)) chain.get(k).map(v =>
          if (Resolved(LastEvent(v)) and LastEvent(v) <= ck.snap)
            { ...v, mat: wal.length() + 1 } else v)
        else chain.get(k)) else chain,
      nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk,
      tx' = if (ck.owner == "") tx else tx.setBy(ck.owner, x => { ...x, pinned: false }),
      writes' = writes,
      handed' = handed, err' = err,
    }
  }

  // seqcompact_stamp_delete: end the live committed version; skip a row that
  // already has an end; give a B-tree-only row a marker. The code ends the
  // live version even when a commit after the snapshot wrote it.
  def StampDelete(c: Set[Version], ts: int, snap: int): Set[Version] =
    val live = c.filter(v => v.b != 0 and Resolved(v.b) and v.e == Inf)
    if (live != Set()) {
      val v = AnyVersion(live)
      if (FixStampSnapshotVersion and v.b > snap) c
      else c.exclude(Set(v)).union(Set({ ...v, e: ts, mat: 0 }))
    }
    else if (c.exists(v => v.e != Inf)) c
    else c.union(Set({ b: 0, e: ts, mat: 0 }))

  // apply_passive_publish_window_ordered: under the clock lock take a new
  // timestamp, apply the recorded deletes with it, publish the boundary.
  action CheckpointPublish: bool = all {
    ck.pc == "committed",
    ApplyCheckpointPublish("published", false),
  }

  action ApplyCheckpointPublish(nextPc: str, failed: bool): bool = {
    val passive = Mode == "passive"
    val ts = if (DeleteTsRule == "clock") clock + 1 else ck.snap
    val ended = ck.seqdel.filter(k => StampDelete(chain.get(k), ts, ck.snap) != chain.get(k))
    all {
      clock' = if (passive) clock + 1 else clock,
      chain' = Vals.mapBy(k =>
                 if (ck.seqdel.contains(k)) StampDelete(chain.get(k), ts, ck.snap)
                 else chain.get(k)),
      writes' = writes.union(ended.map(k => { ts: ts, k: k, p: false, cmp: true })),
      durable' = Max(Set(ck.old, ck.snap)),
      ck' = { ...ck, pc: nextPc,
        seqdel: if (failed) Set() else ck.seqdel,
        history: if (failed and ck.frame > 0 and ck.wset.keys() != Set())
                   ck.history.union(Set("cleanupPublished")) else ck.history },
      lastCommitted' = lastCommitted, llog' = llog, wal' = wal, nBackfill' = nBackfill,
      db' = db, dbMeta' = dbMeta, backfillFloor' = backfillFloor, nCk' = nCk, tx' = tx,
      handed' = handed, err' = err,
    }
  }

  action CheckpointBackfill: bool = {
    val limit = Min(Pinned.map(t => tx.get(t).mark).union(Set(wal.length())))
    all {
      ck.pc == "published",
      Mode == "truncateFlagOn" implies Pinned == Set(),
      // \E to \in nBackfill..limit is false when the range is empty. "to" is
      // a built-in name in Quint, so the pick is named upTo.
      nBackfill <= limit,
      nondet upTo = nBackfill.to(limit).oneOf()
      all {
        db' = Vals.mapBy(k =>
                if (FramesWith(k, nBackfill + 1, upTo) == Set()) db.get(k)
                else Frame(Max(FramesWith(k, nBackfill + 1, upTo))).w.get(k)),
        dbMeta' = if (upTo > nBackfill) Frame(upTo).meta else dbMeta,
        nBackfill' = upTo,
      },
      ck' = { ...ck, pc: "backfilled" },
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, durable' = durable, backfillFloor' = backfillFloor, nCk' = nCk,
      tx' = tx, writes' = writes, handed' = handed, err' = err,
    }
  }

  action CheckpointTruncateLog: bool = all {
    ck.pc == "backfilled",
    llog' = if (llog.forall(l => l.ts <= durable)) Set() else llog,
    ck' = { ...ck, pc: "truncated" },
    clock' = clock, chain' = chain, lastCommitted' = lastCommitted, wal' = wal,
    nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
    backfillFloor' = backfillFloor, nCk' = nCk, tx' = tx, writes' = writes,
    handed' = handed, err' = err,
  }

  pure def LastEvent(v: Version): int = if (v.e != Inf) v.e else v.b

  action CheckpointStamp: bool = all {
    ck.pc == "truncated",
    backfillFloor' = nBackfill,
    chain' = Vals.mapBy(k =>
               if (ck.wset.keys().contains(k))
                 chain.get(k).map(v =>
                   if (Resolved(LastEvent(v)) and LastEvent(v) <= ck.snap) { ...v, mat: ck.frame }
                   else v)
               else chain.get(k)),
    ck' = { ...ck, pc: "idle", seqdel: Set(), pagerOwned: false },
    clock' = clock, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
    nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable, nCk' = nCk,
    tx' = tx, writes' = writes, handed' = handed, err' = err,
  }

  action CheckpointFail(at: str): bool = all {
    CheckpointFailures or (Mode == "truncateFlagOn" and at == "published" and Pinned != Set()),
    ck.pc == at,
    if (FixCleanupPublishes and at == "committed") ApplyCheckpointPublish("idle", true)
    else all {
      ck' = { ...ck, pc: "idle", seqdel: Set(), pagerOwned: false, history:
        if (Mode == "truncateFlagOn" and at == "published" and Pinned != Set()
            and ck.frame > 0 and ck.wset.keys() != Set())
          ck.history.union(Set("truncateBusy")).union(
            if (RemovedRowNeeded) Set("purgedBeforeBusy") else Set())
        else ck.history },
      clock' = clock, chain' = chain, lastCommitted' = lastCommitted, llog' = llog,
      wal' = wal, nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, nCk' = nCk,
      tx' = if (not(ck.pagerOwned) or ck.owner == "") tx
              else tx.setBy(ck.owner, x => { ...x, pinned: false }),
      writes' = writes,
      handed' = handed, err' = err,
    },
  }

  val RemovedRowNeeded: bool = ck.purged.exists(v => Registered.exists(t => {
    val b = if (tx.get(t).outerB != 0) tx.get(t).outerB else tx.get(t).b
    val mark = if (tx.get(t).outerB != 0) tx.get(t).outerMark else tx.get(t).mark
    and {
      tx.get(t).pc == "active" or tx.get(t).outerB != 0,
      v.b != 0, Resolved(v.b), v.b < b, b < v.e,
      Current(OracleRows(b)) == v.k,
      not(BtreeAt(v.k, mark)),
      not(PresentFor(v.k, b, mark)),
    }
  }))

  // ---------------------------------------------------------------------------
  // Garbage collection: the passive rules of gc_version_chain

  val Lwm: int = if (Registered == Set()) Inf else Min(Registered.map(t =>
    if (tx.get(t).outerB == 0) tx.get(t).b
    else if (tx.get(t).pc == "idle") tx.get(t).outerB
    else Min(Set(tx.get(t).b, tx.get(t).outerB))))

  val GcFloor: int = Min(ReaderMarks.union(Set(backfillFloor, Inf)))

  def InBtreeForAllReaders(v: Version): bool = v.mat != 0 and v.mat <= GcFloor

  def Rule2(v: Version): bool = and {
    v.e != Inf, Resolved(v.e), v.e < Lwm,
    InBtreeForAllReaders(v),
  }

  def Rule3(c: Set[Version]): bool = and {
    c.size() == 1,
    val v = AnyVersion(c)
    and {
      v.e == Inf, v.b != 0, Resolved(v.b),
      InBtreeForAllReaders(v),
      Lwm == Inf,
    },
  }

  action Gc(k: int): bool = {
    val afterRule2 = chain.get(k).filter(v => not(Rule2(v)))
    val after = if (Rule3(afterRule2)) Set() else afterRule2
    all {
      after != chain.get(k),
      chain' = chain.set(k, after),
      clock' = clock, lastCommitted' = lastCommitted, llog' = llog, wal' = wal,
      nBackfill' = nBackfill, db' = db, dbMeta' = dbMeta, durable' = durable,
      backfillFloor' = backfillFloor, ck' = ck, nCk' = nCk, tx' = tx, writes' = writes,
      handed' = handed, err' = err,
    }
  }

  // ---------------------------------------------------------------------------
  // Specification

  // Each pick has a name, so `quint run --mbt` records it.
  action Next = any {
    nondet t = Txs.oneOf()
    any {
      OuterBegin(t),
      OuterEnd(t),
      TxBegin(t),
      TxRead(t),
      TxNextval(t),
      nondet v = Vals.oneOf()
      TxSetval(t, v),
      TxEnd(t),
      nondet ckNow = (if (CheckpointOwnerReaders) Bool else Set(false)).oneOf()
      TxCommit(t, ckNow),
      TxFinish(t),
    },
    nondet at = Set("collect", "beginPager", "compact", "compacted", "committed", "published", "backfilled", "truncated").oneOf()
    CheckpointFail(at),
    CheckpointStart,
    CheckpointCollect,
    CheckpointBeginPager,
    CheckpointCompact,
    CheckpointCommit,
    CheckpointPublish,
    CheckpointBackfill,
    CheckpointTruncateLog,
    CheckpointStamp,
    nondet k = Vals.oneOf()
    Gc(k),
  }

  // ---------------------------------------------------------------------------
  // Properties

  val ReadsCorrect: bool = err == Set()

  val UniqueValues: bool =
    (not(WithSetval) and not(Cycle)) implies
      handed.forall(h1 => handed.forall(h2 => h1.v == h2.v implies h1 == h2))

  // A compaction delete never ends the row that holds the current value. The
  // rows before the delete include a commit at the same timestamp: the
  // blocking mode deletes at the snapshot timestamp, after that commit.
  def RowsBefore(ts: int): Set[int] =
    val W = writes.filter(w => w.ts < ts or (w.ts == ts and not(w.cmp)))
    Vals.filter(k => PresentIn(W, k, ts + 1))

  val CompactionKeepsCurrent: bool =
    writes.forall(w => w.cmp implies w.k != Current(RowsBefore(w.ts)))

  val RecoveredMeta: int = if (wal == List()) dbMeta else Frame(wal.length()).meta

  def Recovered(k: int): bool =
    val replay = llog.filter(l => l.k == k and l.ts > RecoveredMeta)
    if (replay == Set()) BtreeAt(k, wal.length())
    else replay.fold({ ts: -1, k: k, p: false }, (a, l) => if (l.ts > a.ts) l else a).p

  // After a crash, nextval continues from the value that the committed
  // history gives. So it never hands out a committed value again (without
  // setval or CYCLE, the current value is the last value handed out).
  val RecoveryCorrect: bool =
    Current(Vals.filter(k => Recovered(k))) == Current(OracleRows(Inf))

  // Every version whose last event is at or below the boundary is stamped by
  // the time the checkpoint is idle. An unstamped one stays in memory
  // forever, and its B-tree row is never deleted.
  val NoStrandedVersions: bool =
    ck.pc == "idle" implies
      Vals.forall(k => chain.get(k).forall(v =>
        (Resolved(LastEvent(v)) and LastEvent(v) <= durable) implies v.mat != 0))

  val TypeOK: bool = and {
    ck.pc == "committed" implies ck.frame > 0,
    ck.pagerOwned == Set("compact", "compacted").contains(ck.pc),
    clock.in(0.to(MaxTs + MaxCheckpoints)),
    nBackfill.in(0.to(wal.length())),
    wal.length() <= MaxCheckpoints,
    durable <= lastCommitted,
    Vals.forall(k => chain.get(k).forall(v => and {
      v.b.in(0.to(MaxTs + MaxCheckpoints)),
      v.e.in(1.to(MaxTs + MaxCheckpoints).union(Set(Inf))),
      v.mat.in(0.to(wal.length())),
    })),
  }

  // ---------------------------------------------------------------------------
  // Reachability checks. Each one must be violated.

  val Safety: bool = and {
    TypeOK, ReadsCorrect, CompactionKeepsCurrent, UniqueValues, RecoveryCorrect, NoStrandedVersions,
  }

  val NeverCleanupPublishes: bool = not(ck.history.contains("cleanupPublished"))

  val NeverAutoCheckpointOnReader: bool = not(ck.history.contains("autoOwnerCommit"))

  val NeverTruncateBusy: bool = not(ck.history.contains("truncateBusy"))
  val NeverPurgesBeforeBusy: bool = not(ck.history.contains("purgedBeforeBusy"))

  val NeverRecordsDelete: bool = not(writes.exists(w => w.cmp))
  val NeverCompacts: bool = ck.seqdel == Set()
  val NeverDeletesInBtree: bool =
    not(1.to(wal.length()).exists(i => Frame(i).w.keys().exists(k => and {
      not(Frame(i).w.get(k)),
      not(writes.exists(w => w.k == k and not(w.p) and not(w.cmp))),
    })))
  val NeverRecoversAfterCompaction: bool = NeverDeletesInBtree or llog == Set()
}
