import GroupCommit.Coordinator

/-!
# The commit protocol

This file models the group commit part of `CommitStateMachine` in
`core/mvcc/database/mod.rs`, from `BeginCommitLogicalLog` to
`FinalizeCommit`, and the cleanup that runs when a commit does not finish
(`cleanup_unfinished_commit`): the statement is dropped, or a step returns an
error.

Each transaction has one committer thread. A thread does one atomic step at a
time, and steps of different threads interleave in all orders. One atomic
step is one call on the coordinator, one change of the `txs` map, one lock
operation, or one log operation. Code between two such operations that only
changes local state is part of the step before it.

A statement can be dropped only when the Rust `step` function returned to its
caller: while it waits for I/O, while it is parked, or when it yields.
`Pc.dropPoint` lists these points.

The log is modeled at the level of records. A record is in `log` when the
write offset moved past it (`advance_logical_log_offset_after_success`).
`synced` is the length of the log prefix that an fsync made durable.

Assumptions:
* A write that was discarded before the offset moved never becomes part of
  the log.
* An fsync makes durable every record that is in the log when it starts.
* Checkpoints and the chunked parts of a commit that do not touch the group
  or the log are not modeled.
-/

namespace GroupCommit

/-- A record in the logical log. The record of a group commit has its ticket. -/
structure Rec where
  tx : Nat
  ticket : Option Nat
deriving DecidableEq, Repr, Hashable

/-- `GroupBatch`. -/
structure Batch where
  writing : Entry
  rest : List Entry
  advanced : Option Nat
deriving DecidableEq, Repr, Hashable

/-- The state of a transaction in the `txs` map. -/
inductive St where
  | none
  | live
  | aborted
  | committed
  | removed
deriving DecidableEq, Repr, Hashable

/-- The step of the committer thread of a transaction. The names after `d` are the
steps of the cleanup of a dropped commit. Some steps exist only in one variant. -/
inductive Pc where
  /-- The transaction did not start. -/
  | idle
  /-- `BeginCommitLogicalLog`. -/
  | begin
  /-- `AwaitGroupCommit`: the durable check, then `take_retry`. -/
  | await (t : Nat)
  /-- `AwaitGroupCommit`: try to take the commit lock. -/
  | awaitLock (t : Nat)
  /-- `AwaitGroupCommit` with the commit lock: `take_retry`. -/
  | awaitLocked (t : Nat)
  /-- `AwaitGroupCommit` with the commit lock: `take_work`. -/
  | awaitWork (t : Nat)
  /-- `UpgradeLogicalLogHeader`. -/
  | upgrade
  /-- `WriteLogicalLog`: `try_issue` in the fixed code, the `txs` check in the original code. -/
  | write
  /-- Original code: `WriteLogicalLog` after the `txs` check. -/
  | writeChecked
  /-- Fixed code: `WriteLogicalLog` after `try_issue`. -/
  | writeIssued
  /-- `FinishLogicalLogWrite`. -/
  | finish
  /-- `own_logical_log_record`: set `log_appended`. -/
  | own2
  /-- `own_logical_log_record`: `finish_issue`, or `clear_issued` in the original code. -/
  | own3
  /-- Original code: `own_logical_log_record`, `take_abandoned`. -/
  | own4
  /-- `SyncLogicalLog`. -/
  | syncLog
  /-- `EndCommitLogicalLog`. -/
  | endCommit
  /-- `CommitEnd`. -/
  | commitEnd
  /-- `FinalizeCommit`. -/
  | committed
  /-- `SyncGroupPrefix`. -/
  | syncPrefix (t : Nat)
  /-- `GroupPrefixSynced`. -/
  | prefixSynced (t : Nat)
  /-- The commit is complete. -/
  | done
  /-- `release_group_claim`: note the written prefix. -/
  | dR1
  /-- `release_group_claim`: give back the batch or the issued write. -/
  | dR2
  /-- Original code: `release_group_claim`, `take_abandoned`. -/
  | dR2c
  /-- `release_group_claim`: roll back the abandoned waiter `w`. -/
  | dRbOtherA (w : Nat)
  /-- `release_group_claim`: remove the abandoned waiter `w` from `txs`. -/
  | dRbOtherB (w : Nat)
  /-- Original code: `release_group_claim`, `request_retry`. -/
  | dR2d
  /-- `release_group_claim`: put the rest of the batch back. -/
  | dR2e
  /-- Original code: `release_group_claim`, `drop_pending`. -/
  | dR3
  /-- Fixed code: `cleanup_dropped_commit`, `leave`. -/
  | dLeave
  /-- `cleanup_dropped_commit`: the `log_appended` check. -/
  | dC1
  /-- Original code: `cleanup_dropped_commit`, `abandon_if_issued`. -/
  | dC2
  /-- `cleanup_dropped_commit`: roll back the transaction. -/
  | dRbA
  /-- `cleanup_dropped_commit`: remove the rolled-back transaction from `txs`. -/
  | dRbB
  /-- `cleanup_dropped_commit` of a committed transaction. -/
  | dCommitted
  /-- The cleanup is complete. -/
  | dropped
deriving DecidableEq, Repr, Hashable

def Pc.dropPoint : Pc → Bool
  | .begin | .await _ | .upgrade | .finish | .syncLog | .endCommit
  | .committed | .prefixSynced _ => true
  | _ => false

def Pc.ticket : Pc → Option Nat
  | .await t | .prefixSynced t => some t
  | _ => none

/-- Everything the model keeps for one transaction. The last four fields are
ghost fields. They record history and do not change any step. -/
structure TxRec where
  st : St := .none
  pc : Pc := .idle
  submitted : Bool := false
  failed : Bool := false
  excl : Bool := false
  held : Bool := false
  appended : Bool := false
  dropFrom : Pc := .idle
  rolledBack : Bool := false
  acked : Bool := false
  notified : Bool := false
  wasCommitted : Bool := false
deriving DecidableEq, Repr, Hashable

structure Sys where
  g : Group := {}
  lead : Option (Nat × Batch) := none
  lockHolder : Option Nat := none
  enabled : Bool := true
  log : List Rec := []
  synced : Nat := 0
  tx : Nat → TxRec := fun _ => {}

def upd (f : Nat → TxRec) (c : Nat) (r : TxRec) : Nat → TxRec :=
  fun x => if x = c then r else f x

namespace Sys

def set (s : Sys) (c : Nat) (r : TxRec) : Sys := { s with tx := upd s.tx c r }

def withG (s : Sys) (g : Group) : Sys := { s with g }
def withLock (s : Sys) (o : Option Nat) : Sys := { s with lockHolder := o }
def withLog (s : Sys) (log : List Rec) : Sys := { s with log }
def withSynced (s : Sys) (n : Nat) : Sys := { s with synced := n }
def withLead (s : Sys) (lead : Option (Nat × Batch)) : Sys := { s with lead }
def withEnabled (s : Sys) (e : Bool) : Sys := { s with enabled := e }

/-- `pager_commit_lock.is_write_locked()`. The model keeps the holder as a
ghost value. The code never reads it. -/
def locked (s : Sys) : Bool := s.lockHolder.isSome

/-- The `group_batch` of the state machine of `c`. Only the holder of the
commit lock has a batch, so the model keeps the one batch in one slot. -/
def batchOf (s : Sys) (c : Nat) : Option Batch :=
  match s.lead with
  | some (l, b) => if l = c then some b else none
  | none => none

def setBatch (s : Sys) (c : Nat) (b : Option Batch) : Sys :=
  match b with
  | some b => s.withLead (some (c, b))
  | none => if s.batchOf c = none then s else s.withLead none

def unlockIfHeld (s : Sys) (c : Nat) : Sys :=
  if (s.tx c).held then (s.set c { s.tx c with held := false }).withLock none
  else s

/-- `advance_group_or_sync`. -/
def advance (s : Sys) (c : Nat) : Sys :=
  let r := s.tx c
  match s.batchOf c with
  | some b =>
    match b.rest with
    | e :: rest => (s.setBatch c (some { b with writing := e, rest })).set c { r with pc := .upgrade }
    | [] => s.set c { r with pc := .syncLog }
  | none => s.set c { r with pc := .syncLog }

/-- `finish_abandoned_group_waiter`. The fixed code also notifies the
transactions that depend on `w`. -/
def finishAbandoned (v : Variant) (s : Sys) (w : Nat) : Sys :=
  let r := s.tx w
  if r.st = .live then
    let s := s.set w { r with st := .removed, wasCommitted := true,
                              notified := r.notified || v = .fixed }
    s.unlockIfHeld w
  else s

def writingOwner (s : Sys) (c : Nat) : Nat :=
  match s.batchOf c with
  | some b => b.writing.tx
  | none => c

/-- The step after the batch was given back in `release_group_claim`. -/
def afterRelease (v : Variant) (s : Sys) (c : Nat) : Sys :=
  let r := s.tx c
  let s := s.setBatch c none
  match v with
  | .original => s.set c { r with pc := .dR3 }
  | .fixed => s.set c { r with pc := .dLeave }

end Sys

/-- Which alternative a step takes. `main` is the usual result. `alt` is the
other result of a check whose result the model does not fix. `fail` is an
I/O error. `drop` starts the cleanup of a commit that did not finish: the
statement was dropped, or a step returned an error. After `fail`, `drop` is the
only next step of the thread. -/
inductive Choice where
  | main
  | alt
  | fail
  | drop
deriving DecidableEq, Repr, Hashable

open Sys in
/-- The next atomic step of the thread of transaction `c`, when the statement is not
dropped and the commit did not fail. For a transaction that did not start, `alt` starts
an exclusive transaction, which takes the commit lock at `BEGIN`. -/
def step (v : Variant) (s : Sys) (c : Nat) (ch : Choice) : Option Sys :=
  let r := s.tx c
  match r.pc, ch with
  | .idle, .main =>
    if r.st = .none then some (s.set c { r with st := .live, pc := .begin }) else none
  | .idle, .alt =>
    if r.st = .none ∧ !s.locked then
      some ((s.set c { r with st := .live, pc := .begin, excl := true, held := true }).withLock
        (some c))
    else none
  | .begin, .main =>
    if !r.excl ∧ s.enabled then
      some ((s.set c { r with pc := .await (s.g.nextTicket + 1) }).withG (s.g.enqueue c))
    else if !r.excl then
      if !s.locked then
        some ((s.set c { r with pc := .upgrade, held := true }).withLock (some c))
      else none
    else some (s.set c { r with pc := .upgrade })
  | .await t, .main =>
    if s.g.durableThrough ≥ t then some (s.set c { r with pc := .endCommit })
    else none
  | .await t, .alt =>
    let (g, hit) := s.g.takeRetry t
    if hit then some ((s.set c { r with pc := .begin }).withG g)
    else some (s.set c { r with pc := .awaitLock t })
  | .awaitLock t, .main =>
    if !s.locked then some ((s.set c { r with pc := .awaitLocked t }).withLock (some c))
    else some (s.set c { r with pc := .await t })
  | .awaitLocked t, .main =>
    let (g, hit) := s.g.takeRetry t
    if hit then some (((s.set c { r with pc := .begin }).withG g).withLock none)
    else some (s.set c { r with pc := .awaitWork t })
  | .awaitWork t, .main =>
    match s.g.takeWork v with
    | (g, .lead e rest) =>
      some (((s.setBatch c (some ⟨e, rest, none⟩)).set c { r with pc := .upgrade, held := true }).withG
        g)
    | (g, .syncPrefix) => some ((s.set c { r with pc := .syncPrefix t, held := true }).withG g)
    | (g, .none) => some (((s.set c { r with pc := .await t }).withG g).withLock none)
  | .upgrade, .main => some (s.set c { r with pc := .write })
  | .upgrade, .alt =>
    match v, s.batchOf c with
    | .original, some b =>
      if (s.tx b.writing.tx).st = .removed then some (s.advance c) else none
    | _, _ => none
  | .write, .main =>
    match v, s.batchOf c with
    | _, none => some (s.set c { r with pc := .finish, submitted := true })
    | .original, some b =>
      if (s.tx b.writing.tx).st ≠ .removed then some (s.set c { r with pc := .writeChecked })
      else none
    | .fixed, some b =>
      match s.g.tryIssue b.writing.tx with
      | (g, true) => some ((s.set c { r with pc := .writeIssued }).withG g)
      | (g, false) => some ((s.advance c).withG g)
  | .write, .alt =>
    match v, s.batchOf c with
    | .original, some b =>
      if (s.tx b.writing.tx).st = .removed then some (s.advance c) else none
    | _, _ => none
  | .write, .fail =>
    match s.batchOf c with
    | none => some (s.set c { r with pc := .finish, failed := true })
    | some _ => none
  | .writeChecked, .main =>
    match s.batchOf c with
    | some b =>
      some ((s.set c { r with pc := .finish, submitted := true }).withG
        { s.g with issued := some b.writing.tx })
    | none => none
  | .writeChecked, .fail => some (s.set c { r with pc := .finish, failed := true })
  | .writeIssued, .main => some (s.set c { r with pc := .finish, submitted := true })
  | .writeIssued, .fail => some (s.set c { r with pc := .finish, failed := true })
  | .finish, .main =>
    match s.batchOf c with
    | some b =>
      some ((((s.setBatch c (some { b with advanced := some b.writing.ticket })).set c
               { r with pc := .own2, submitted := false }).withLog
             (s.log ++ [⟨b.writing.tx, some b.writing.ticket⟩])).withG
             (s.g.noteWritten b.writing.ticket))
    | none =>
      some ((s.set c { r with pc := .own2, submitted := false }).withLog (s.log ++ [⟨c, none⟩]))
  | .finish, .fail => some (s.set c { r with failed := true })
  | .own2, .main =>
    let w := s.writingOwner c
    let s := if (s.tx w).st ≠ .removed then s.set w { s.tx w with appended := true } else s
    some (s.set c { s.tx c with pc := .own3 })
  | .own3, .main =>
    let w := s.writingOwner c
    match v with
    | .original =>
      let s := s.withG { s.g with issued := none }
      if w ≠ c then some (s.set c { r with pc := .own4 }) else some (s.advance c)
    | .fixed =>
      let (g, ab) := s.g.finishIssue w
      let s := s.withG g
      let s := if ab ∧ w ≠ c then s.finishAbandoned v w else s
      some (s.advance c)
  | .own4, .main =>
    let w := s.writingOwner c
    let (g, ab) := s.g.takeAbandoned w
    let s := s.withG g
    let s := if ab then s.finishAbandoned v w else s
    some (s.advance c)
  | .syncLog, .main => some ((s.set c { r with pc := .endCommit }).withSynced s.log.length)
  | .syncLog, .fail => some (s.set c { r with pc := .endCommit, failed := true })
  | .endCommit, .main =>
    let g := if (s.batchOf c).isSome then s.g.markDurable s.g.writtenThrough else s.g
    some (((s.setBatch c none).set c { r with pc := .commitEnd }).withG g)
  | .commitEnd, .main =>
    some (s.set c { r with pc := .committed, st := .committed, wasCommitted := true,
                           acked := true })
  | .committed, .main =>
    let s := s.set c { r with notified := true }
    let s := s.unlockIfHeld c
    some (s.set c { s.tx c with st := .removed, pc := .done })
  | .syncPrefix t, .main =>
    some ((s.set c { r with pc := .prefixSynced t }).withSynced s.log.length)
  | .syncPrefix t, .fail => some (s.set c { r with pc := .prefixSynced t, failed := true })
  | .prefixSynced t, .main =>
    let s := s.withG (s.g.markDurable s.g.writtenThrough)
    let s := if (s.tx c).st ≠ .removed then s.unlockIfHeld c else s.withLock none
    some (s.set c { s.tx c with pc := .await t })
  | .dR1, .main =>
    match s.batchOf c with
    | some ⟨_, _, some a⟩ => some ((s.set c { r with pc := .dR2 }).withG (s.g.noteWritten a))
    | _ => some (s.set c { r with pc := .dR2 })
  | .dR2, .main =>
    match s.batchOf c with
    | none => some (s.afterRelease v c)
    | some b =>
      let owned := b.advanced = some b.writing.ticket
      let beforeLogTx := !r.submitted ∧ (r.dropFrom = .upgrade ∨ r.dropFrom = .write)
      if beforeLogTx then
        some ((s.withG (s.g.requeue v (b.writing :: b.rest))).afterRelease v c)
      else if !owned ∧ (r.dropFrom = .write ∨ r.dropFrom = .finish) then
        match v with
        | .original =>
          let s := s.withG { s.g with issued := none }
          if b.writing.tx ≠ c then some (s.set c { r with pc := .dR2c })
          else some (s.set c { r with pc := .dR2e })
        | .fixed =>
          if b.writing.tx ≠ c then
            match s.g.releaseIssued b.writing with
            | (g, true) => some ((s.set c { r with pc := .dRbOtherA b.writing.tx }).withG g)
            | (g, false) => some ((s.set c { r with pc := .dR2e }).withG g)
          else some ((s.set c { r with pc := .dR2e }).withG { s.g with issued := none })
      else
        some ((s.withG (s.g.requeue v b.rest)).afterRelease v c)
  | .dR2c, .main =>
    match s.batchOf c with
    | some b =>
      let w := b.writing.tx
      let (g, ab) := s.g.takeAbandoned w
      let s := s.withG g
      if ab then
        if (s.tx w).st ≠ .removed then some (s.set c { r with pc := .dRbOtherA w })
        else some (s.set c { r with pc := .dR2e })
      else some (s.set c { r with pc := .dR2d })
    | none => none
  | .dRbOtherA w, .main =>
    let s := s.set w { s.tx w with st := .aborted, rolledBack := true }
    let s := s.unlockIfHeld w
    some (s.set c { s.tx c with pc := .dRbOtherB w })
  | .dRbOtherB w, .main =>
    let s := s.set w { s.tx w with st := .removed }
    some (s.set c { s.tx c with pc := .dR2e })
  | .dR2d, .main =>
    match s.batchOf c with
    | some b => some ((s.set c { r with pc := .dR2e }).withG (s.g.requestRetry b.writing.ticket))
    | none => none
  | .dR2e, .main =>
    match s.batchOf c with
    | some b => some ((s.withG (s.g.requeue v b.rest)).afterRelease v c)
    | none => none
  | .dR3, .main =>
    match r.dropFrom with
    | .await t => some ((s.set c { r with pc := .dC1 }).withG (s.g.dropPending t))
    | _ => some (s.set c { r with pc := .dC1 })
  | .dLeave, .main =>
    match s.g.leave c r.dropFrom.ticket with
    | (g, true) => some ((s.set c { r with pc := .dropped, submitted := false }).withG g)
    | (g, false) => some ((s.set c { r with pc := .dC1 }).withG g)
  | .dC1, .main =>
    let r := { r with submitted := false }
    if r.st = .live ∧ r.appended then
      some (s.set c { r with st := .committed, wasCommitted := true, pc := .dCommitted })
    else if r.st = .live then
      match v with
      | .original => some (s.set c { r with pc := .dC2 })
      | .fixed => some (s.set c { r with pc := .dRbA })
    else if r.st = .committed then some (s.set c { r with pc := .dCommitted })
    else some (s.set c { r with pc := .dropped })
  | .dC2, .main =>
    match s.g.abandonIfIssued c with
    | (g, true) => some ((s.set c { r with pc := .dropped }).withG g)
    | (g, false) => some ((s.set c { r with pc := .dRbA }).withG g)
  | .dRbA, .main =>
    let s := s.set c { r with st := .aborted, rolledBack := true }
    let s := s.unlockIfHeld c
    some (s.set c { s.tx c with pc := .dRbB })
  | .dRbB, .main => some (s.set c { r with st := .removed, pc := .dropped })
  | .dCommitted, .main =>
    let s := s.set c { r with notified := true }
    let s := s.unlockIfHeld c
    some (s.set c { s.tx c with st := .removed, pc := .dropped })
  | _, _ => none

/-- The next atomic step of the thread of transaction `c`. -/
def next (v : Variant) (s : Sys) (c : Nat) (ch : Choice) : Option Sys :=
  let r := s.tx c
  if ch = .drop then
    if r.pc.dropPoint then some (s.set c { r with dropFrom := r.pc, pc := .dR1, failed := false })
    else none
  else if r.failed then none
  else step v s c ch

/-- One step of the whole system: a step of one thread, or a change of the
`mvcc_group_commit` pragma. -/
inductive Step (v : Variant) : Sys → Sys → Prop where
  | thread (s s' : Sys) (c : Nat) (ch : Choice) : next v s c ch = some s' → Step v s s'
  | toggle (s : Sys) : Step v s (s.withEnabled (!s.enabled))

def init : Sys := {}

inductive Reachable (v : Variant) : Sys → Prop where
  | init : Reachable v init
  | step {s s' : Sys} : Reachable v s → Step v s s' → Reachable v s'

end GroupCommit
