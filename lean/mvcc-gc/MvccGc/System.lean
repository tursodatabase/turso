import MvccGc.Rules

/-!
# One row under MVCC in Truncate checkpoint mode

The state holds one version chain, the B-tree value of the same row, and the
transactions. Each `Action` is one atomic step of the engine.

The ghost fields `init`, `hist` and `wrote` are not in the engine. They record
what snapshot isolation says each transaction must read.
-/

namespace MvccGc

structure St where
  txs : List Tx
  fin : Finalized
  chain : List Version
  btree : Option Nat
  ckptMax : Nat
  clock : Nat
  nextTx : Nat
  lastCommitted : Nat
  walPos : Nat
  backfill : Nat
  init : Option Nat
  hist : List (Nat × Option Nat)
  wrote : List (Nat × Option Nat)
  deriving DecidableEq, Repr, Hashable

def St.initial (init : Option Nat) : St where
  txs := []
  fin := []
  chain := []
  btree := init
  ckptMax := 0
  clock := 1
  nextTx := 1
  lastCommitted := 0
  walPos := 0
  backfill := 0
  init := init
  hist := []
  wrote := []

instance : Inhabited St := ⟨St.initial none⟩

def lookupWrote : List (Nat × Option Nat) → Nat → Option (Option Nat)
  | [], _ => none
  | (k, v) :: rest, x => if k = x then some v else lookupWrote rest x

def setWrote (w : List (Nat × Option Nat)) (x : Nat) (v : Option Nat) :
    List (Nat × Option Nat) :=
  (x, v) :: w.filter (fun p => p.1 ≠ x)

def dropWrote (w : List (Nat × Option Nat)) (x : Nat) : List (Nat × Option Nat) :=
  w.filter (fun p => p.1 ≠ x)

def updateTx (txs : List Tx) (x : Nat) (f : Tx → Tx) : List Tx :=
  txs.map fun t => if t.id = x then f t else t

def removeTx (txs : List Tx) (x : Nat) : List Tx :=
  txs.filter fun t => t.id ≠ x

def isOpen (t : Tx) : Bool :=
  match t.state with
  | .active => true
  | .preparing _ => true
  | _ => false

def minOpt : Option Nat → Nat → Option Nat
  | none, n => some n
  | some m, n => some (min m n)

/-- `compute_lwm`. -/
def computeLwm (txs : List Tx) : Option Nat :=
  txs.foldl (fun acc t => if isOpen t then minOpt acc t.beginTs else acc) none

/-- `compute_min_reader_mark().min(backfill_floor)`. -/
def inlineMinReaderMark (txs : List Tx) (backfill : Nat) : Option Nat :=
  some (txs.foldl (fun acc t => if isOpen t then min acc t.readMark else acc) backfill)

/-- A GC may use a low-water mark that was sampled before some transactions
ended. Such a value is never above the current one. `none` (`u64::MAX`) is
checked again under the chain lock, so it is always exact. -/
def lwmAllowed (lwm current : Option Nat) : Prop :=
  match lwm, current with
  | none, none => True
  | none, some _ => False
  | some _, none => True
  | some l, some c => l ≤ c

instance (lwm current : Option Nat) : Decidable (lwmAllowed lwm current) := by
  unfold lwmAllowed
  split <;> infer_instance

/-- The value of the row just before timestamp `s`. `hist` is newest first. -/
def valueAt (init : Option Nat) (hist : List (Nat × Option Nat)) (s : Nat) : Option Nat :=
  match hist.find? (fun e => decide (e.1 < s)) with
  | some e => e.2
  | none => init

def St.expected (s : St) (t : Tx) : Option Nat :=
  match lookupWrote s.wrote t.id with
  | some w => w
  | none => valueAt s.init s.hist t.beginTs

def St.read (s : St) (t : Tx) : Option Nat :=
  readRow s.txs s.fin t s.chain s.btree s.ckptMax

def referencedIds (vs : List Version) : List Nat :=
  vs.foldr
    (fun v acc =>
      let fromBegin := match v.beginAt with
        | some (.id x) => [x]
        | _ => []
      let fromEnd := match v.endAt with
        | some (.id x) => [x]
        | _ => []
      fromBegin ++ fromEnd ++ acc)
    []

inductive Action where
  | begin
  | write (x v : Nat)
  | delete (x : Nat)
  | prepare (x : Nat)
  | commit (x : Nat)
  | rewrite (x : Nat)
  | finish (x : Nat)
  | abort (x : Nat)
  | rollback (x : Nat)
  | remove (x : Nat)
  | gc (lwm : Option Nat)
  | checkpoint (pinnedReader backfillBehind : Bool)
  deriving DecidableEq, Repr, Hashable

inductive Outcome where
  | ok (s : St)
  | disabled
  | panic (msg : String)
  deriving Repr

def newVersion (x v : Nat) (resident : Bool) : Version :=
  { beginAt := some (.id x), endAt := none, val := v, resident, mat := 0 }

def tombstone (x v : Nat) : Version :=
  { beginAt := none, endAt := some (.id x), val := v, resident := true, mat := 0 }

def hasMissingTxRef (txs : List Tx) (vs : List Version) : Bool :=
  vs.any fun v =>
    match v.beginAt with
    | some (.id y) => (findTx txs y).isNone
    | _ => false

def St.insert (s : St) (vs : List Version) (nv : Version) : Outcome :=
  if hasMissingTxRef s.txs (nv :: vs) then .panic "resolve_begin_timestamp: tx missing from txs"
  else .ok { s with chain := insertVersion s.txs vs nv }

/-- `MvccLazyCursor::insert` of value `v`: the `Insert` opcode. -/
def stepWrite (s : St) (t : Tx) (v : Nat) : Outcome :=
  let s' := { s with wrote := setWrote s.wrote t.id (some v) }
  match mvccSide s.txs s.fin t s.chain s.ckptMax with
  | some _ =>
    match deleteFromChain s.txs s.fin t s.chain with
    | .deleted vs => s'.insert vs (newVersion t.id v false)
    | .conflict => .disabled
    | .notFound => .panic "update could not supersede a visible version"
  | none =>
    let onBtreeRow := s.btree.isSome && btreeSideValid s.txs s.fin t s.chain s.ckptMax
    s'.insert s.chain (newVersion t.id v onBtreeRow)

/-- `MvccLazyCursor::delete` on the row that the cursor shows to `t`. -/
def stepDelete (s : St) (t : Tx) : Outcome :=
  match s.read t with
  | none => .disabled
  | some shown =>
    let s' := { s with wrote := setWrote s.wrote t.id none }
    let inBtree := (mvccSide s.txs s.fin t s.chain s.ckptMax).isNone
    match deleteFromChain s.txs s.fin t s.chain with
    | .deleted vs => .ok { s' with chain := vs }
    | .conflict => .disabled
    | .notFound =>
      if inBtree then s'.insert s.chain (tombstone t.id shown)
      else .panic "delete of a shown MVCC row found no visible version"

/-- Get the end timestamp and validate, as one step. -/
def stepPrepare (s : St) (t : Tx) : Outcome :=
  let e := s.clock
  let wroteRow := lookupWrote s.wrote t.id
  if wroteRow.isSome && hasVersionConflict s.txs s.fin t e s.chain then .disabled
  else
    let hist := match wroteRow with
      | some w => (e, w) :: s.hist
      | none => s.hist
    .ok { s with
      txs := updateTx s.txs t.id (fun u => { u with state := .preparing e })
      clock := e + 1
      hist }

def stepCheckpoint (s : St) (pinnedReader backfillBehind : Bool) : Outcome :=
  if !s.txs.isEmpty then .disabled
  else
    let snapshot := s.lastCommitted
    let old := if s.ckptMax = 0 then none else some s.ckptMax
    let chosen := checkpointSelect [] s.fin s.chain snapshot old
    let newMax := max s.ckptMax snapshot
    let frame := s.walPos + 1
    let backfill := if backfillBehind then s.backfill else frame
    let floor := if pinnedReader then some (min s.walPos backfill) else some backfill
    let afterWriteSetGc :=
      if chosen.isSome then
        gcChain (stampChain [] s.fin frame snapshot s.chain) none newMax false floor true
      else s.chain
    let chain := gcChain afterWriteSetGc none newMax false (some backfill) true
    let refs := referencedIds chain
    .ok { s with
      btree := applyCheckpointWrite s.btree chosen
      ckptMax := newMax
      walPos := frame
      backfill
      chain
      fin := s.fin.filter fun p => refs.contains p.1 }

def step (s : St) : Action → Outcome
  | .begin =>
    .ok { s with
      txs := s.txs ++ [{ id := s.nextTx, beginTs := s.clock, state := .active,
                         readMark := s.walPos, rewritten := false }]
      clock := s.clock + 1
      nextTx := s.nextTx + 1 }
  | .write x v =>
    match findTx s.txs x with
    | some t => if t.state = .active then stepWrite s t v else .disabled
    | none => .disabled
  | .delete x =>
    match findTx s.txs x with
    | some t => if t.state = .active then stepDelete s t else .disabled
    | none => .disabled
  | .prepare x =>
    match findTx s.txs x with
    | some t => if t.state = .active then stepPrepare s t else .disabled
    | none => .disabled
  | .commit x =>
    match findTx s.txs x with
    | some t =>
      match t.state with
      | .preparing e =>
        .ok { s with txs := updateTx s.txs x (fun u => { u with state := .committed e }) }
      | _ => .disabled
    | none => .disabled
  | .rewrite x =>
    match findTx s.txs x with
    | some t =>
      match t.state with
      | .committed e =>
        if t.rewritten then .disabled
        else .ok { s with
          chain := s.chain.map (rewriteVersion x e)
          txs := updateTx s.txs x (fun u => { u with rewritten := true }) }
      | _ => .disabled
    | none => .disabled
  | .finish x =>
    match findTx s.txs x with
    | some t =>
      match t.state with
      | .committed e =>
        if !t.rewritten then .disabled
        else
          let fin := if (lookupWrote s.wrote x).isSome then (x, e) :: s.fin else s.fin
          .ok { s with
            txs := removeTx s.txs x
            fin
            lastCommitted := max s.lastCommitted e
            wrote := dropWrote s.wrote x }
      | _ => .disabled
    | none => .disabled
  | .abort x =>
    match findTx s.txs x with
    | some t =>
      if t.state = .active then
        .ok { s with txs := updateTx s.txs x (fun u => { u with state := .aborted }) }
      else .disabled
    | none => .disabled
  | .rollback x =>
    match findTx s.txs x with
    | some t =>
      if t.state = .aborted then
        .ok { s with
          chain := s.chain.map (rollbackVersion x)
          txs := updateTx s.txs x (fun u => { u with state := .terminated }) }
      else .disabled
    | none => .disabled
  | .remove x =>
    match findTx s.txs x with
    | some t =>
      if t.state = .terminated then
        .ok { s with txs := removeTx s.txs x, wrote := dropWrote s.wrote x }
      else .disabled
    | none => .disabled
  | .gc lwm =>
    if lwmAllowed lwm (computeLwm s.txs) then
      .ok { s with
        chain := gcChain s.chain lwm s.ckptMax false (inlineMinReaderMark s.txs s.backfill) true }
    else .disabled
  | .checkpoint pinnedReader backfillBehind => stepCheckpoint s pinnedReader backfillBehind

/-- Every open reader sees its snapshot. -/
def readsCorrect (s : St) : Bool :=
  s.txs.all fun t => t.state ≠ .active || s.read t == s.expected t

/-- The B-tree holds the value of the row at the durable boundary. -/
def btreeCorrect (s : St) : Bool :=
  s.btree == valueAt s.init s.hist (s.ckptMax + 1)

end MvccGc
