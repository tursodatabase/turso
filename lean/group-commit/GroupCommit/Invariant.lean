import GroupCommit.Protocol

/-!
# The invariant of the fixed protocol

`Inv` holds in every reachable state of `Variant.fixed`. It is the induction
hypothesis of the safety proof. Each part is a separate definition, so that a
proof can use one part at a time.
-/

set_option synthInstance.maxSize 4096
set_option synthInstance.maxHeartbeats 400000

namespace GroupCommit

def Pc.waitTicket : Pc → Option Nat
  | .await t | .awaitLock t | .awaitLocked t | .awaitWork t | .syncPrefix t
  | .prefixSynced t => some t
  | _ => none

/-- The drop steps before `leave`, while the group can still hold the entry
of the transaction. -/
def Pc.beforeLeave : Pc → Bool
  | .dR1 | .dR2 | .dRbOtherA _ | .dRbOtherB _ | .dR2e | .dLeave => true
  | _ => false

def Pc.afterLeave : Pc → Bool
  | .dC1 | .dRbA | .dRbB | .dCommitted | .dropped => true
  | _ => false

/-- The steps where the leader holds a batch and has not given it back. -/
def Pc.isPrefixSynced : Pc → Bool
  | .prefixSynced _ => true
  | _ => false

def Pc.leading : Pc → Bool
  | .upgrade | .write | .writeIssued | .finish | .own2 | .own3 | .syncLog | .endCommit => true
  | _ => false

def Pc.holdsBatch : Pc → Bool
  | .dR1 | .dR2 | .dRbOtherA _ | .dRbOtherB _ | .dR2e => true
  | p => p.leading

/-- The ticket that a transaction waits on, or still owns while it is dropped. -/
def TxRec.ticket (r : TxRec) : Option Nat :=
  match r.pc.waitTicket with
  | some t => some t
  | none => if r.pc.beforeLeave then r.dropFrom.ticket else none

def TxRec.holdsLock (r : TxRec) : Bool :=
  match r.pc with
  | .awaitLocked _ | .awaitWork _ => true
  | _ => r.held

/-- The leader issued the write of the batch record, and the offset did not move. -/
def TxRec.issuing (r : TxRec) : Bool :=
  match r.pc with
  | .writeIssued | .finish => true
  | .dR1 | .dR2 => r.dropFrom == .finish
  | _ => false

/-- The record the leader writes is not issued yet. -/
def TxRec.writingUnissued (r : TxRec) : Bool :=
  match r.pc with
  | .upgrade | .write => true
  | .dR1 | .dR2 => r.dropFrom == .upgrade
  | _ => false

namespace Sys

def unissued (s : Sys) : List Entry :=
  match s.lead with
  | some (l, b) => if (s.tx l).writingUnissued then b.writing :: b.rest else b.rest
  | none => []

def issuedEntry (s : Sys) : Option Entry :=
  match s.lead with
  | some (l, b) => if (s.tx l).issuing then some b.writing else none
  | none => none

/-- The entry that owns `GroupState::issued`: issued, or in the log and not
yet finished. -/
def issuedSlot (s : Sys) : Option Entry :=
  match s.lead with
  | some (l, b) =>
    if (s.tx l).issuing || (s.tx l).pc == .own2 || (s.tx l).pc == .own3 then some b.writing
    else none
  | none => none

/-- All entries whose records are still to be written, in the order of writing. -/
def queue (s : Sys) : List Entry := s.issuedEntry.toList ++ s.unissued ++ s.g.pending

def InLog (s : Sys) (c : Nat) : Prop := ∃ r ∈ s.log, r.tx = c

def InSynced (s : Sys) (c : Nat) : Prop := ∃ r ∈ s.log.take s.synced, r.tx = c

instance (s : Sys) (c : Nat) : Decidable (s.InLog c) := by unfold InLog; infer_instance
instance (s : Sys) (c : Nat) : Decidable (s.InSynced c) := by unfold InSynced; infer_instance

end Sys

def Pc.rollingBackOther : Pc → Option Nat
  | .dRbOtherA w => some w
  | _ => none

def Pc.removingOther : Pc → Option Nat
  | .dRbOtherB w => some w
  | _ => none

/-- The state of the transaction matches the step of its thread. -/
def StatusOf (r : TxRec) : Prop :=
  match r.pc with
  | .idle => r.st = .none ∧ r.held = false ∧ r.excl = false ∧ r.submitted = false
  | .begin => r.st = .live ∧ r.held = r.excl ∧ r.submitted = false
  | .await _ | .awaitLock _ | .awaitLocked _ | .awaitWork _ =>
    r.st = .live ∧ r.held = false ∧ r.excl = false ∧ r.submitted = false
  | .upgrade | .write | .own2 | .own3 => r.st = .live ∧ r.held = true ∧ r.submitted = false
  | .writeIssued | .finish | .syncLog => r.st = .live ∧ r.held = true
  | .endCommit | .commitEnd => r.st = .live
  | .committed => r.st = .committed
  | .syncPrefix _ | .prefixSynced _ =>
    r.st = .live ∧ r.held = true ∧ r.excl = false ∧ r.submitted = false
  | .done => r.st = .removed ∧ r.held = false
  | .dR1 | .dR2 | .dLeave | .dC1 =>
    (r.st = .live ∧ r.dropFrom ≠ .committed) ∨ (r.st = .committed ∧ r.dropFrom = .committed)
  | .dRbOtherA _ | .dRbOtherB _ | .dR2e => r.st = .live ∧ r.dropFrom = .finish
  | .dRbA => r.st = .live ∧ r.appended = false
  | .dRbB => r.st = .aborted ∧ r.held = false ∧ r.rolledBack = true
  | .dCommitted => r.st = .committed
  | .dropped => r.held = false ∧ r.st ≠ .none ∧ r.st ≠ .committed
  | .writeChecked | .own4 | .dR2c | .dR2d | .dR3 | .dC2 => False

instance (r : TxRec) : Decidable (StatusOf r) := by
  unfold StatusOf; split <;> infer_instance

def StatusInv (s : Sys) (c : Nat) : Prop := StatusOf (s.tx c)

instance (s : Sys) (c : Nat) : Decidable (StatusInv s c) := by
  unfold StatusInv; infer_instance

/-- Facts about one transaction and the log. -/
def LogInv (s : Sys) (c : Nat) : Prop :=
  ((s.tx c).appended = true → s.InLog c) ∧
  (s.InLog c → (s.tx c).st = .live →
    (s.tx c).appended = true ∨ s.g.issued = some c ∨
    ((s.tx c).pc = .own2 ∧ s.batchOf c = none)) ∧
  (s.InLog c → (s.tx c).st ≠ .none ∧ (s.tx c).rolledBack = false) ∧
  ((s.tx c).wasCommitted = true → s.InLog c) ∧
  ((s.tx c).acked = true → s.InSynced c) ∧
  ((s.tx c).st = .removed → (s.tx c).wasCommitted = true → (s.tx c).notified = true) ∧
  ((s.tx c).rolledBack = true →
    ((s.tx c).st = .aborted ∨ (s.tx c).st = .removed) ∧ (s.tx c).wasCommitted = false) ∧
  ((s.tx c).wasCommitted = true → (s.tx c).st = .committed ∨ (s.tx c).st = .removed) ∧
  (((s.tx c).pc = .endCommit ∧ (s.tx c).failed = false) ∨ (s.tx c).pc = .commitEnd →
    s.InSynced c) ∧
  ((s.tx c).pc = .syncLog → s.InLog c) ∧
  (((s.tx c).pc = .own2 ∨ (s.tx c).pc = .own3) → s.batchOf c = none → s.InLog c) ∧
  ((s.tx c).pc = .begin → ¬ s.InLog c) ∧
  (((s.tx c).pc = .upgrade ∨ (s.tx c).pc = .write ∨ (s.tx c).pc = .finish) →
    s.batchOf c = none → ¬ s.InLog c) ∧
  ((((s.tx c).pc = .endCommit ∧ (s.batchOf c).isSome) ∨ (s.tx c).pc.isPrefixSynced) →
    (s.tx c).failed = false → s.synced = s.log.length) ∧
  (∀ t ∈ (s.tx c).pc.waitTicket.toList,
    (t ≤ s.g.writtenThrough → ⟨c, some t⟩ ∈ s.log) ∧
    (t ≤ s.g.durableThrough → ⟨c, some t⟩ ∈ s.log.take s.synced)) ∧
  (((s.tx c).pc = .writeIssued ∨ (s.tx c).pc.rollingBackOther.isSome ∨
      (s.tx c).pc.removingOther.isSome) → (s.batchOf c).isSome)

instance (s : Sys) (c : Nat) : Decidable (LogInv s c) := by
  unfold LogInv; infer_instance

/-- Where the entry of a waiting transaction is. -/
def TicketInv (s : Sys) (c : Nat) : Prop :=
  ∀ t ∈ (s.tx c).ticket.toList,
    1 ≤ t ∧ t ≤ s.g.nextTicket ∧
    (⟨c, some t⟩ ∈ s.log ∨ ⟨t, c⟩ ∈ s.queue ∨ t ∈ s.g.retry) ∧
    (t ∈ s.g.retry → ¬ s.InLog c) ∧
    ((s.tx c).pc = .awaitWork t → t ∉ s.g.retry)

instance (s : Sys) (c : Nat) : Decidable (TicketInv s c) := by
  unfold TicketInv; infer_instance

/-- Facts about one transaction and the coordinator. -/
def GroupTxInv (s : Sys) (c : Nat) : Prop :=
  ((s.tx c).holdsLock = true ↔ s.lockHolder = some c) ∧
  ((s.tx c).pc.afterLeave = true → c ∉ s.g.abandoned →
    s.g.issued ≠ some c ∧ c ∉ s.g.taken ∧ ∀ e ∈ s.g.pending, e.tx ≠ c) ∧
  ((s.tx c).pc = .dropped → (s.tx c).st = .live →
    c ∈ s.g.abandoned ∨
    ∃ l ∈ (s.lead.map (·.1)).toList, (s.tx l).pc = .dRbOtherA c) ∧
  ((s.tx c).pc = .dropped → (s.tx c).st = .aborted →
    ∃ l ∈ (s.lead.map (·.1)).toList, (s.tx l).pc = .dRbOtherB c)

instance (s : Sys) (c : Nat) : Decidable (GroupTxInv s c) := by
  unfold GroupTxInv; infer_instance

/-- Facts about the thread that holds the batch. -/
def LeadOf (s : Sys) (l : Nat) (b : Batch) : Prop :=
  (s.tx l).held = true ∧ (s.tx l).pc.holdsBatch = true ∧
  (((s.tx l).pc = .syncLog ∨ (s.tx l).pc = .endCommit) → b.rest = []) ∧
  (((s.tx l).pc = .dR1 ∨ (s.tx l).pc = .dR2) →
    ((s.tx l).dropFrom = .upgrade ∧ (s.tx l).submitted = false) ∨
    (s.tx l).dropFrom = .finish ∨
    (((s.tx l).dropFrom = .syncLog ∨ (s.tx l).dropFrom = .endCommit) ∧ b.rest = [])) ∧
  (∀ a ∈ b.advanced.toList, a ≤ s.g.writtenThrough ∧ a ≤ b.writing.ticket) ∧
  (((s.tx l).issuing ∨ (s.tx l).writingUnissued) → b.advanced ≠ some b.writing.ticket) ∧
  (((s.tx l).pc = .own2 ∨ (s.tx l).pc = .own3) → b.advanced = some b.writing.ticket ∧
    ⟨b.writing.tx, some b.writing.ticket⟩ ∈ s.log) ∧
  ((s.tx l).pc.leading → (s.tx l).pc ≠ .syncLog → (s.tx l).pc ≠ .endCommit →
    s.InLog l ∨ (∃ e ∈ s.unissued, e.tx = l) ∨ s.issuedSlot.map (·.tx) = some l) ∧
  (((s.tx l).pc.leading ∨ (s.tx l).pc = .dR1 ∨ (s.tx l).pc = .dR2) → s.g.retry = []) ∧
  (∀ w ∈ (s.tx l).pc.rollingBackOther.toList,
    w ≠ l ∧ ¬ s.InLog w ∧ (s.tx w).st = .live ∧ (s.tx w).pc = .dropped ∧
    w ∉ s.g.abandoned) ∧
  (∀ w ∈ (s.tx l).pc.removingOther.toList,
    w ≠ l ∧ ¬ s.InLog w ∧ (s.tx w).st = .aborted ∧ (s.tx w).pc = .dropped) ∧
  ((s.tx l).pc = .own3 → (s.tx b.writing.tx).appended = true)

instance (s : Sys) (l : Nat) (b : Batch) : Decidable (LeadOf s l b) := by
  unfold LeadOf; infer_instance

def LeadInv (s : Sys) : Prop :=
  match s.lead with
  | none => s.g.taken = [] ∧ s.g.withdrawn = []
  | some (l, b) => LeadOf s l b

instance (s : Sys) : Decidable (LeadInv s) := by
  unfold LeadInv; split <;> infer_instance

/-- Facts about the entries that are still to be written. -/
def QueueInv (s : Sys) : Prop :=
  s.queue.Pairwise (fun a b => a.ticket < b.ticket) ∧
  (s.queue.map (·.tx)).Nodup ∧
  (∀ e ∈ s.queue, 1 ≤ e.ticket ∧ e.ticket ≤ s.g.nextTicket ∧ ¬ s.InLog e.tx ∧
    ((s.tx e.tx).st = .live ∨ e.tx ∈ s.g.withdrawn) ∧ e.ticket ∉ s.g.retry) ∧
  (∀ e ∈ s.g.pending,
    (s.tx e.tx).ticket = some e.ticket ∨
    ((s.tx e.tx).pc = .dLeave ∧ (s.tx e.tx).dropFrom.ticket = none ∧ (s.tx e.tx).held = true)) ∧
  (∀ e ∈ s.unissued,
    (s.tx e.tx).ticket = some e.ticket ∨ e.tx ∈ s.g.withdrawn ∨ s.lead.map (·.1) = some e.tx) ∧
  (∀ e ∈ s.unissued, e.tx ∈ s.g.taken ∨ e.tx ∈ s.g.withdrawn) ∧
  (∀ x ∈ s.g.taken ++ s.g.withdrawn, x ∈ s.unissued.map (·.tx)) ∧
  (∀ x ∈ s.g.taken, x ∉ s.g.withdrawn ∧ (s.tx x).pc.afterLeave = false) ∧
  (∀ x ∈ s.g.withdrawn, (s.tx x).pc.afterLeave = true ∧ x ∉ s.g.abandoned)

instance (s : Sys) : Decidable (QueueInv s) := by unfold QueueInv; infer_instance

/-- Facts about `issued` and `abandoned`. -/
def IssueInv (s : Sys) : Prop :=
  s.g.issued = s.issuedSlot.map (·.tx) ∧
  (∀ w ∈ s.g.issued.toList, (s.tx w).st = .live ∧ (s.tx w).rolledBack = false) ∧
  (∀ e ∈ s.issuedSlot.toList, ∀ l ∈ (s.lead.map (·.1)).toList, e.tx ≠ l →
    (s.tx e.tx).ticket = some e.ticket ∨ e.tx ∈ s.g.abandoned) ∧
  (∀ x ∈ s.g.abandoned, s.g.issued = some x ∧ (s.tx x).pc = .dropped ∧
    (s.tx x).st = .live ∧ (s.tx x).held = false)

instance (s : Sys) : Decidable (IssueInv s) := by unfold IssueInv; infer_instance

/-- Facts about retry holes and the watermarks. -/
def MarkInv (s : Sys) : Prop :=
  (∀ h ∈ s.g.retry, 1 ≤ h ∧ h ≤ s.g.nextTicket) ∧
  s.g.writtenThrough ≤ s.g.nextTicket ∧
  s.g.durableThrough ≤ s.g.nextTicket ∧
  s.synced ≤ s.log.length ∧
  (s.log.map (·.tx)).Nodup

instance (s : Sys) : Decidable (MarkInv s) := by unfold MarkInv; infer_instance

/-- The watermarks stay below the entries that are still to be written. -/
def OrderInv (s : Sys) : Prop :=
  (∀ e ∈ s.queue, s.g.writtenThrough < e.ticket) ∧
  (∀ e ∈ s.issuedSlot.toList, s.g.durableThrough < e.ticket) ∧
  s.g.durableThrough ≤ s.g.writtenThrough

instance (s : Sys) : Decidable (OrderInv s) := by unfold OrderInv; infer_instance

/-- Every retry hole has a waiter that can still take it or remove it. -/
def HoleOwners (s : Sys) : Prop :=
  ∀ h ∈ s.g.retry, ∃ c, (s.tx c).ticket = some h

/-- Two transactions never own the same ticket. -/
def TicketsUnique (s : Sys) : Prop :=
  ∀ c d t, (s.tx c).ticket = some t → (s.tx d).ticket = some t → c = d

structure Inv (s : Sys) : Prop where
  status : ∀ c, StatusInv s c
  log : ∀ c, LogInv s c
  ticket : ∀ c, TicketInv s c
  groupTx : ∀ c, GroupTxInv s c
  lead : LeadInv s
  queue : QueueInv s
  issue : IssueInv s
  mark : MarkInv s
  order : OrderInv s
  holes : HoleOwners s
  unique : TicketsUnique s

end GroupCommit
