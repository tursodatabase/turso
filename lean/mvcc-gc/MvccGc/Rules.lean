/-!
# Version-chain rules

Each definition in this file copies one Rust function. The Rust names are in
the docstrings. `README.md` lists the source locations.
-/

namespace MvccGc

/-- `TxTimestampOrID`. -/
inductive Stamp where
  | ts (t : Nat)
  | id (x : Nat)
  deriving DecidableEq, Repr, Hashable

/-- `TransactionState`. -/
inductive TxState where
  | active
  | preparing (endTs : Nat)
  | committed (endTs : Nat)
  | aborted
  | terminated
  deriving DecidableEq, Repr, Hashable

/-- The fields of `Transaction` that the rules read. -/
structure Tx where
  id : Nat
  beginTs : Nat
  state : TxState
  readMark : Nat
  rewritten : Bool
  deriving DecidableEq, Repr, Hashable

/-- `RowVersion`. `mat = 0` is `WalPos::ORIGIN`. -/
structure Version where
  beginAt : Option Stamp
  endAt : Option Stamp
  val : Nat
  resident : Bool
  mat : Nat
  deriving DecidableEq, Repr, Hashable

/-- `finalized_tx_states`. Only `Committed` states are ever put in it. -/
abbrev Finalized := List (Nat × Nat)

def findTx : List Tx → Nat → Option Tx
  | [], _ => none
  | t :: rest, x => if t.id = x then some t else findTx rest x

def lookupFinal : Finalized → Nat → Option Nat
  | [], _ => none
  | (k, v) :: rest, x => if k = x then some v else lookupFinal rest x

/-- `lookup_tx_state`. -/
def lookupTxState (txs : List Tx) (fin : Finalized) (x : Nat) : Option TxState :=
  match findTx txs x with
  | some t => some t.state
  | none => (lookupFinal fin x).map TxState.committed

/-- `is_begin_visible`. -/
def isBeginVisible (txs : List Tx) (fin : Finalized) (tx : Tx) (rv : Version) : Bool :=
  match rv.beginAt with
  | some (.ts b) => decide (tx.beginTs > b)
  | some (.id x) =>
    match findTx txs x with
    | some tb =>
      match tb.state with
      | .active => decide (tx.id = tb.id) && rv.endAt.isNone
      | .preparing e => decide (tx.beginTs > e)
      | .committed c => decide (tx.beginTs > c)
      | .aborted => false
      | .terminated => false
    | none =>
      match lookupFinal fin x with
      | some c => decide (tx.beginTs > c)
      | none => false
  | none => false

/-- `is_end_visible`. -/
def isEndVisible (txs : List Tx) (fin : Finalized) (tx : Tx) (rv : Version) : Bool :=
  match rv.endAt with
  | some (.ts e) => decide (tx.beginTs < e)
  | some (.id x) =>
    match findTx txs x with
    | some te =>
      match te.state with
      | .active => decide (tx.id ≠ te.id)
      | .preparing e => decide (tx.beginTs < e)
      | .committed c => decide (tx.beginTs < c)
      | .aborted => true
      | .terminated => true
    | none =>
      match lookupFinal fin x with
      | some c => decide (tx.beginTs < c)
      | none => true
  | none => true

/-- `RowVersion::is_visible_to`. -/
def isVisible (txs : List Tx) (fin : Finalized) (tx : Tx) (rv : Version) : Bool :=
  isBeginVisible txs fin tx rv && isEndVisible txs fin tx rv

/-- `RowVersion::is_btree_invalidating_version`. -/
def isBtreeInvalidating (txs : List Tx) (fin : Finalized) (tx : Tx) (rv : Version) : Bool :=
  isVisible txs fin tx rv ||
    match rv.endAt with
    | some (.ts e) => decide (tx.beginTs > e)
    | some (.id x) =>
      if x = tx.id then true
      else
        match lookupTxState txs fin x with
        | some (.committed c) => decide (tx.beginTs > c)
        | some (.preparing e) => decide (tx.beginTs > e)
        | _ => false
    | none => false

/-- `chain_is_write_buffer_for`. -/
def chainIsWriteBuffer (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version)
    (ckptMax : Nat) : Bool :=
  match vs with
  | [] => false
  | [rv] =>
    match rv.beginAt with
    | some (.ts b) =>
      if rv.endAt.isSome || !isVisible txs fin tx rv then true
      else if b > ckptMax then true
      else if rv.mat = 0 || tx.readMark < rv.mat then true
      else false
    | _ => true
  | _ => true

/-- `btree_covers_chain_for_tx` in Truncate mode, outside a checkpoint, for a
table whose B-tree is readable. -/
def btreeCovers (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version)
    (ckptMax : Nat) : Bool :=
  !chainIsWriteBuffer txs fin tx vs ckptMax

/-- The newest version visible to `tx` (`versions.iter().rev().find(..)`). -/
def lastVisible (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version) :
    Option Version :=
  vs.reverse.find? (isVisible txs fin tx)

/-- `skipmap_row_while_uncovered`: the SkipMap side of a table read. -/
def mvccSide (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version)
    (ckptMax : Nat) : Option Nat :=
  if vs.isEmpty then none
  else if btreeCovers txs fin tx vs ckptMax then none
  else (lastVisible txs fin tx vs).map (·.val)

/-- `query_btree_version_is_valid`: the B-tree side of a table read. -/
def btreeSideValid (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version)
    (ckptMax : Nat) : Bool :=
  if vs.isEmpty then true
  else if btreeCovers txs fin tx vs ckptMax then true
  else !(vs.any (isBtreeInvalidating txs fin tx))

/-- The row that the dual cursor gives to `tx`. -/
def readRow (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version)
    (btree : Option Nat) (ckptMax : Nat) : Option Nat :=
  match mvccSide txs fin tx vs ckptMax with
  | some v => some v
  | none => if btreeSideValid txs fin tx vs ckptMax then btree else none

/-! ## Garbage collection (`gc_version_chain`) -/

/-- `lwm = none` is `u64::MAX`: no transaction is open. -/
def belowLwm (lwm : Option Nat) (e : Nat) : Bool :=
  match lwm with
  | none => true
  | some l => decide (e ≤ l)

/-- `minReaderMark = none` is `WalPos::STAGED`. -/
def matForReaders (minReaderMark : Option Nat) (rv : Version) : Bool :=
  rv.mat ≠ 0 &&
    match minReaderMark with
    | none => true
    | some m => decide (m ≥ rv.mat)

def isAbortedGarbage (rv : Version) : Bool :=
  rv.beginAt.isNone && rv.endAt.isNone

def isCurrent (rv : Version) : Bool :=
  (match rv.beginAt with
    | some (.ts _) => true
    | _ => false) && rv.endAt.isNone

def inBtree (ckptMax : Nat) (rv : Version) : Bool :=
  rv.resident ||
    match rv.beginAt with
    | some (.ts b) => decide (b ≤ ckptMax)
    | _ => false

def keepByRule2 (lwm : Option Nat) (ckptMax : Nat) (passive : Bool)
    (minReaderMark : Option Nat) (hasCurrent : Bool) (rv : Version) : Bool :=
  match rv.endAt with
  | some (.ts e) =>
    if belowLwm lwm e then
      if passive then !matForReaders minReaderMark rv
      else decide (e > ckptMax) && (inBtree ckptMax rv || !hasCurrent)
    else true
  | _ => true

def rule3 (lwm : Option Nat) (ckptMax : Nat) (passive : Bool) (minReaderMark : Option Nat)
    (vs : List Version) : List Version :=
  match vs with
  | [rv] =>
    match rv.beginAt, rv.endAt with
    | some (.ts b), none =>
      if lwm.isNone && matForReaders minReaderMark rv && (passive || decide (b ≤ ckptMax)) then []
      else vs
    | _, _ => vs
  | _ => vs

/-- `MvStore::gc_version_chain`. -/
def gcChain (vs : List Version) (lwm : Option Nat) (ckptMax : Nat) (passive : Bool)
    (minReaderMark : Option Nat) (dropCurrent : Bool) : List Version :=
  let vs1 := vs.filter (fun rv => !isAbortedGarbage rv)
  let hasCurrent := vs1.any isCurrent
  let vs2 := vs1.filter (keepByRule2 lwm ckptMax passive minReaderMark hasCurrent)
  if dropCurrent then rule3 lwm ckptMax passive minReaderMark vs2 else vs2

/-! ## Checkpoint (`maybe_get_checkpointable_versions`) -/

def resolveStamp (txs : List Tx) (fin : Finalized) : Option Stamp → Option Nat
  | some (.ts t) => some t
  | some (.id x) =>
    match lookupTxState txs fin x with
    | some (.committed c) => some c
    | _ => none
  | none => none

structure Selection where
  existsInDb : Bool
  chosen : Option Version
  deriving DecidableEq, Repr

def selectStep (txs : List Tx) (fin : Finalized) (snapshot : Nat) (old : Option Nat)
    (st : Selection) (v : Version) : Selection :=
  let b := resolveStamp txs fin v.beginAt
  let e0 := resolveStamp txs fin v.endAt
  let beginsAfterSnapshot := match b with
    | some b => decide (b > snapshot)
    | none => false
  if beginsAfterSnapshot then st
  else
    let endsAfterSnapshot := match e0 with
      | some e => decide (e > snapshot)
      | none => false
    let e := if endsAfterSnapshot then none else e0
    if b.isNone && e.isNone then st
    else
      let seenResident := st.existsInDb || v.resident
      let seenCheckpointedBegin := seenResident ||
        match old, b with
        | some o, some b => decide (b ≤ o)
        | _, _ => false
      let checkpointedEnd := match old, e with
        | some o, some e => decide (e ≤ o)
        | _, _ => false
      let existsInDb := if checkpointedEnd then false else seenCheckpointedBegin
      let uncheckpointedInsert := e.isNone &&
        match old with
        | none => true
        | some o =>
          match b with
          | some b => decide (b > o)
          | none => false
      let deleteOfExistingRow := e.isSome && existsInDb
      if uncheckpointedInsert || deleteOfExistingRow then
        { existsInDb, chosen := some { v with endAt := e.map Stamp.ts } }
      else
        { existsInDb, chosen := st.chosen }

/-- `CheckpointStateMachine::maybe_get_checkpointable_versions` for a user table. -/
def checkpointSelect (txs : List Tx) (fin : Finalized) (vs : List Version) (snapshot : Nat)
    (old : Option Nat) : Option Version :=
  (vs.foldl (selectStep txs fin snapshot old) { existsInDb := false, chosen := none }).chosen

/-- The B-tree write that the checkpoint does for the chosen version. -/
def applyCheckpointWrite (btree : Option Nat) : Option Version → Option Nat
  | none => btree
  | some v => if v.endAt.isSome then none else some v.val

/-- `MvStore::stamp_chain_materialized`. -/
def stampChain (txs : List Tx) (fin : Finalized) (frame snapshot : Nat) (vs : List Version) :
    List Version :=
  vs.map fun v =>
    let terminal :=
      if v.endAt.isSome then resolveStamp txs fin v.endAt else resolveStamp txs fin v.beginAt
    match terminal with
    | some t => if t ≤ snapshot then { v with mat := frame } else v
    | none => v

/-! ## Commit and rollback -/

/-- `RowVersion::rewrite_txid_to_timestamp`. `set_begin`/`set_end` reset the stamp. -/
def rewriteVersion (x endTs : Nat) (v : Version) : Version :=
  let v1 := if v.beginAt = some (.id x) then { v with beginAt := some (.ts endTs), mat := 0 } else v
  if v1.endAt = some (.id x) then { v1 with endAt := some (.ts endTs), mat := 0 } else v1

/-- `rollback_row_version`. -/
def rollbackVersion (x : Nat) (v : Version) : Version :=
  if v.beginAt = some (.id x) then { v with beginAt := none, endAt := none, mat := 0 }
  else if v.endAt = some (.id x) then { v with endAt := none, mat := 0 }
  else v

/-! ## Writes -/

/-- `is_write_write_conflict`. -/
def isWriteWriteConflict (txs : List Tx) (fin : Finalized) (tx : Tx) (rv : Version) : Bool :=
  match rv.endAt with
  | some (.id x) =>
    if x = tx.id then false
    else
      match lookupTxState txs fin x with
      | some .aborted => false
      | some .terminated => false
      | some _ => true
      | none => true
  | some (.ts e) => decide (e > tx.beginTs)
  | none => false

def tombstoneConflicts (txs : List Tx) (fin : Finalized) (tx : Tx) (endTs : Nat)
    (v : Version) : Bool :=
  match v.endAt with
  | some (.id o) =>
    if o = tx.id then false
    else
      match lookupTxState txs fin o with
      | some (.committed _) => true
      | some (.preparing oe) => decide (oe < endTs)
      | _ => false
  | _ => false

inductive EndCheck where
  | conflict
  | skip
  | checkBegin

def endCheck (txs : List Tx) (fin : Finalized) (tx : Tx) (v : Version) : EndCheck :=
  match v.endAt with
  | some (.ts _) => .skip
  | some (.id x) =>
    if x = tx.id then .skip
    else
      match lookupTxState txs fin x with
      | some (.committed c) => if c > tx.beginTs then .conflict else .skip
      | _ => .checkBegin
  | none => .checkBegin

def beginConflicts (txs : List Tx) (fin : Finalized) (tx : Tx) (endTs : Nat) (v : Version) :
    Bool :=
  match v.beginAt with
  | some (.id o) =>
    if o = tx.id then false
    else
      match lookupTxState txs fin o with
      | some (.committed _) => true
      | some (.preparing oe) => decide (oe < endTs)
      | some .active => false
      | some .aborted => false
      | some .terminated => false
      | none => true
  | some (.ts _) => true
  | none => false

def versionConflicts (txs : List Tx) (fin : Finalized) (tx : Tx) (endTs : Nat) (v : Version) :
    Bool :=
  (match v.endAt with
    | some (.ts e) => decide (e > tx.beginTs)
    | _ => false) ||
  if v.beginAt.isNone then tombstoneConflicts txs fin tx endTs v
  else
    match endCheck txs fin tx v with
    | .conflict => true
    | .skip => false
    | .checkBegin => beginConflicts txs fin tx endTs v

/-- `check_version_conflicts`. -/
def hasVersionConflict (txs : List Tx) (fin : Finalized) (tx : Tx) (endTs : Nat)
    (vs : List Version) : Bool :=
  vs.any (versionConflicts txs fin tx endTs)

/-- Result of `delete_from_table_or_index`. -/
inductive DeleteResult where
  | conflict
  | deleted (vs : List Version)
  | notFound
  deriving DecidableEq, Repr

/-- `delete_from_table_or_index` on a chain given newest first. -/
def deleteNewestFirst (txs : List Tx) (fin : Finalized) (tx : Tx) :
    List Version → DeleteResult
  | [] => .notFound
  | rv :: older =>
    let visible := isVisible txs fin tx rv
    if (visible || rv.beginAt.isNone) && isWriteWriteConflict txs fin tx rv then .conflict
    else if !visible then
      match deleteNewestFirst txs fin tx older with
      | .deleted vs => .deleted (rv :: vs)
      | r => r
    else .deleted ({ rv with endAt := some (.id tx.id), mat := 0 } :: older)

def deleteFromChain (txs : List Tx) (fin : Finalized) (tx : Tx) (vs : List Version) :
    DeleteResult :=
  match deleteNewestFirst txs fin tx vs.reverse with
  | .deleted vs' => .deleted vs'.reverse
  | r => r

/-- `resolve_begin_timestamp`. -/
def orderKey (txs : List Tx) (v : Version) : Nat :=
  match v.beginAt with
  | some (.ts t) => t
  | some (.id x) =>
    match findTx txs x with
    | some t => t.beginTs
    | none => 0
  | none => 0

def insertNewestFirst (txs : List Tx) (nv : Version) : List Version → List Version
  | [] => [nv]
  | v :: older => if orderKey txs v ≤ orderKey txs nv then nv :: v :: older
    else v :: insertNewestFirst txs nv older

/-- `insert_version_raw` (without the recovery-only duplicate rule). -/
def insertVersion (txs : List Tx) (vs : List Version) (nv : Version) : List Version :=
  (insertNewestFirst txs nv vs.reverse).reverse

end MvccGc
