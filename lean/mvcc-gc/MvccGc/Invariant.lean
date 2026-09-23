import MvccGc.System

/-!
# Candidate invariant, as executable checks

The proofs use these facts. The search runs every check on every state it
reaches, so a wrong candidate fails fast before any proof work starts.
-/

namespace MvccGc

/-- The transaction that the next `begin` creates. -/
def St.nextReader (s : St) : Tx :=
  { id := s.nextTx, beginTs := s.clock, state := .active, readMark := s.walPos,
    rewritten := false }

def St.readers (s : St) : List Tx :=
  s.nextReader :: s.txs.filter (fun t => t.state = .active)

/-- A stamp, with a transaction id replaced by what its transaction state says. -/
inductive RS where
  | none
  | time (t : Nat)
  | pending (x : Nat)
  | dead (x : Nat)
  | missing (x : Nat)
  deriving DecidableEq, Repr

def resolve (txs : List Tx) : Option Stamp → RS
  | none => .none
  | some (.ts t) => .time t
  | some (.id x) =>
    match findTx txs x with
    | some t =>
      match t.state with
      | .active => .pending x
      | .preparing e => .time e
      | .committed e => .time e
      | .aborted => .dead x
      | .terminated => .dead x
    | none => .missing x

def RS.isAt : RS → Bool
  | .time _ => true
  | _ => false

def RS.after (t : Nat) : RS → Bool
  | .time c => decide (c > t)
  | _ => false

def isGarbage (v : Version) : Bool := v.beginAt.isNone && v.endAt.isNone

/-- The version belongs to the committed history of the row. -/
def settled (txs : List Tx) (v : Version) : Bool :=
  match resolve txs v.beginAt with
  | .time _ => true
  | .none => (resolve txs v.endAt).isAt
  | _ => false

/-- A committed version that no committed transaction has ended. -/
def live (txs : List Tx) (v : Version) : Bool :=
  (resolve txs v.beginAt).isAt && !(resolve txs v.endAt).isAt

def belongsTo (x : Nat) (v : Version) : Bool :=
  v.beginAt == some (.id x) || (v.beginAt.isNone && v.endAt == some (.id x))

def livePendingOf (x : Nat) (v : Version) : Bool :=
  v.beginAt == some (.id x) && v.endAt.isNone

/-- A committed change that `t` did not see is in the chain, so `t` cannot commit a write. -/
def doomed (txs : List Tx) (t : Tx) (vs : List Version) : Bool :=
  vs.any fun w =>
    settled txs w &&
      ((resolve txs w.beginAt).after t.beginTs || (resolve txs w.endAt).after t.beginTs)

def epochChangeBefore (s : St) (t : Nat) : Bool :=
  s.hist.any fun e => decide (s.ckptMax < e.1) && decide (e.1 < t)

def wroteRow (s : St) (x : Nat) : Bool := (lookupWrote s.wrote x).isSome

def stamps (v : Version) : List Stamp :=
  (match v.beginAt with
    | some st => [st]
    | none => []) ++
  (match v.endAt with
    | some st => [st]
    | none => [])

def txEnds (t : Tx) : List Nat :=
  match t.state with
  | .preparing e => [e]
  | .committed e => [e]
  | _ => []

def nodupNat (l : List Nat) : Bool := l.eraseDups.length == l.length

def enumFrom : Nat → List Version → List (Nat × Version)
  | _, [] => []
  | n, v :: rest => (n, v) :: enumFrom (n + 1) rest

def strictlyDecreasing : List Nat → Bool
  | a :: b :: rest => decide (a > b) && strictlyDecreasing (b :: rest)
  | _ => true

def checks (s : St) : List (String × Bool) :=
  let txs := s.txs
  let vs := s.chain
  let histTs := s.hist.map (·.1)
  let ivs := enumFrom 0 vs
  [ ("tx ids distinct", nodupNat (txs.map (·.id))),
    ("tx bounds", txs.all fun t =>
      decide (t.id < s.nextTx) && decide (s.ckptMax < t.beginTs) && decide (t.beginTs < s.clock) &&
      t.readMark == s.walPos &&
      (txEnds t).all fun e => decide (t.beginTs < e) && decide (e < s.clock)),
    ("timestamps distinct", nodupNat (txs.map (·.beginTs) ++ txs.flatMap txEnds)),
    ("hist sorted", strictlyDecreasing histTs && histTs.all (· < s.clock)),
    ("hist of prepared", txs.all fun t =>
      (txEnds t).all fun e =>
        match lookupWrote s.wrote t.id with
        | some w => s.hist.contains (e, w)
        | none => !histTs.contains e),
    ("hist of epoch", s.hist.all fun e =>
      decide (e.1 ≤ s.lastCommitted) || txs.any fun t => (txEnds t).contains e.1),
    ("last committed", decide (s.lastCommitted < s.clock) && decide (s.ckptMax ≤ s.lastCommitted)),
    ("tx begins after epoch hist", txs.all fun t => s.hist.all fun e =>
      decide (e.1 ≠ t.beginTs)),
    ("chain stamps", vs.all fun v => (stamps v).all fun
      | .ts t => decide (s.ckptMax < t) && decide (t < s.clock) && histTs.contains t
      | .id x =>
        match findTx txs x with
        | some t => !(t.state == .terminated) && !(t.rewritten && (txEnds t).length == 1) &&
            (match t.state with
              | .preparing e => histTs.contains e
              | .committed e => histTs.contains e
              | _ => true)
        | none => false),
    ("no stamps", vs.all fun v => v.mat == 0),
    ("btree", s.btree == valueAt s.init s.hist (s.ckptMax + 1)),
    ("wrote keys", (s.wrote.all fun p => (findTx txs p.1).isSome) && nodupNat (s.wrote.map (·.1))),
    ("wal", s.backfill == s.walPos),
    ("reads", s.readers.all fun t => s.read t == s.expected t),
    ("unique", s.readers.all fun t => (vs.filter (isVisible txs s.fin t)).length ≤ 1),
    ("evidence", s.readers.all fun t =>
      !s.btree.isSome || !(epochChangeBefore s t.beginTs || wroteRow s t.id) ||
        vs.any fun v => v.resident && isBtreeInvalidating txs s.fin t v),
    ("no resident without row", s.btree.isSome || vs.all fun v => !v.resident),
    ("live is last", ivs.all fun (i, c) => !live txs c || ivs.all fun (j, w) =>
      j == i || isGarbage w || !settled txs w || decide (j < i)),
    ("pending after settled", txs.all fun x => x.state != .active || ivs.all fun (i, n) =>
      !livePendingOf x.id n || doomed txs x vs || ivs.all fun (j, w) =>
        j == i || isGarbage w || !(settled txs w || belongsTo x.id w) || decide (j < i)),
    ("deletes see", vs.all fun v =>
      match v.beginAt, v.endAt with
      | some (.id z), some (.id x) => z == x || (resolve txs (some (.id z))).isAt
      | _, _ => true),
    ("own ends", vs.all fun v =>
      match v.beginAt with
      | some (.id x) =>
        (resolve txs v.beginAt).isAt || v.endAt.isNone || v.endAt == some (.id x)
      | _ => true),
    ("rewritten only when committed", txs.all fun t =>
      !t.rewritten || (txEnds t).length == 1 && t.state != .preparing (txEnds t).head!),
    ("active deleter saw", vs.all fun v =>
      match v.endAt with
      | some (.id x) =>
        match findTx txs x with
        | some t =>
          t.state != .active || v.beginAt.isNone || v.beginAt == some (.id x) ||
            match resolve txs v.beginAt with
            | .time b => decide (b < t.beginTs)
            | _ => false
        | none => true
      | _ => true),
    ("writer has stamp", txs.all fun t =>
      !wroteRow s t.id || t.state != .active ||
        vs.any fun v => (stamps v).contains (.id t.id)),
    ("end after begin", vs.all fun v =>
      match resolve txs v.beginAt, resolve txs v.endAt with
      | .time b, .time c => decide (b ≤ c)
      | _, _ => true),
    ("id stamp wrote", vs.all fun v => (stamps v).all fun
      | .id x => wroteRow s x
      | .ts _ => true) ]

def failedChecks (s : St) : List String :=
  (checks s).filterMap fun (name, ok) => if ok then none else some name

end MvccGc
