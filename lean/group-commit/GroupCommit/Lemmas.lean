import GroupCommit.SimpAttr

/-!
# Basic lemmas

Rewrite rules for state updates and for the coordinator calls.
-/

namespace GroupCommit

@[simp, sys_simps] theorem upd_apply (f : Nat → TxRec) (c : Nat) (r : TxRec) (x : Nat) :
    upd f c r x = if x = c then r else f x := rfl

@[simp, sys_simps] theorem mem_setInsert {x y : Nat} {l : List Nat} :
    x ∈ setInsert y l ↔ x = y ∨ x ∈ l := by
  unfold setInsert; split <;> simp_all

@[simp, sys_simps] theorem mem_setErase {x y : Nat} {l : List Nat} :
    x ∈ setErase y l ↔ x ≠ y ∧ x ∈ l := by
  simp [setErase, List.mem_filter, and_comm]

@[simp, sys_simps] theorem mem_eraseAll {x : Nat} {xs l : List Nat} :
    x ∈ eraseAll xs l ↔ x ∉ xs ∧ x ∈ l := by
  simp [eraseAll, List.mem_filter, and_comm]

namespace Sys

@[simp, sys_simps] theorem set_tx (s : Sys) (c : Nat) (r : TxRec) (d : Nat) :
    (s.set c r).tx d = if d = c then r else s.tx d := rfl
@[simp, sys_simps] theorem set_g (s : Sys) (c : Nat) (r : TxRec) : (s.set c r).g = s.g := rfl
@[simp, sys_simps] theorem set_lead (s : Sys) (c : Nat) (r : TxRec) : (s.set c r).lead = s.lead := rfl
@[simp, sys_simps] theorem set_lockHolder (s : Sys) (c : Nat) (r : TxRec) :
    (s.set c r).lockHolder = s.lockHolder := rfl
@[simp, sys_simps] theorem set_enabled (s : Sys) (c : Nat) (r : TxRec) : (s.set c r).enabled = s.enabled := rfl
@[simp, sys_simps] theorem set_log (s : Sys) (c : Nat) (r : TxRec) : (s.set c r).log = s.log := rfl
@[simp, sys_simps] theorem set_synced (s : Sys) (c : Nat) (r : TxRec) : (s.set c r).synced = s.synced := rfl

@[simp, sys_simps] theorem locked_eq (s : Sys) : s.locked = s.lockHolder.isSome := rfl

theorem batchOf_eq (s : Sys) (c : Nat) :
    s.batchOf c = match s.lead with
      | some (l, b) => if l = c then some b else none
      | none => none := rfl

@[simp, sys_simps] theorem batchOf_set (s : Sys) (c : Nat) (r : TxRec) (d : Nat) :
    (s.set c r).batchOf d = s.batchOf d := rfl

end Sys

namespace Group

@[simp, sys_simps] theorem enqueue_pending (g : Group) (tx : Nat) :
    (g.enqueue tx).pending = g.pending ++ [⟨g.nextTicket + 1, tx⟩] := rfl
@[simp, sys_simps] theorem enqueue_nextTicket (g : Group) (tx : Nat) :
    (g.enqueue tx).nextTicket = g.nextTicket + 1 := rfl
@[simp, sys_simps] theorem enqueue_retry (g : Group) (tx : Nat) : (g.enqueue tx).retry = g.retry := rfl
@[simp, sys_simps] theorem enqueue_issued (g : Group) (tx : Nat) : (g.enqueue tx).issued = g.issued := rfl
@[simp, sys_simps] theorem enqueue_abandoned (g : Group) (tx : Nat) :
    (g.enqueue tx).abandoned = g.abandoned := rfl
@[simp, sys_simps] theorem enqueue_taken (g : Group) (tx : Nat) : (g.enqueue tx).taken = g.taken := rfl
@[simp, sys_simps] theorem enqueue_withdrawn (g : Group) (tx : Nat) :
    (g.enqueue tx).withdrawn = g.withdrawn := rfl
@[simp, sys_simps] theorem enqueue_writtenThrough (g : Group) (tx : Nat) :
    (g.enqueue tx).writtenThrough = g.writtenThrough := rfl
@[simp, sys_simps] theorem enqueue_durableThrough (g : Group) (tx : Nat) :
    (g.enqueue tx).durableThrough = g.durableThrough := rfl

theorem densePrefixCap_le (ticket written : Nat) (retry : List Nat) :
    densePrefixCap ticket written retry ≤ min ticket written := by
  unfold densePrefixCap
  dsimp only
  split
  · rename_i hole h
    have hm := List.min?_mem h
    simp [List.mem_filter] at hm
    omega
  · exact Nat.le_refl _

theorem markDurable_le (g : Group) :
    (g.markDurable g.writtenThrough).durableThrough ≤ max g.durableThrough g.writtenThrough := by
  have := densePrefixCap_le g.writtenThrough g.writtenThrough g.retry
  simp only [markDurable]
  omega

end Group

end GroupCommit

namespace GroupCommit
open Sys

theorem Sys.unissued_eq (s : Sys) : s.unissued = match s.lead with
    | some (l, b) => if (s.tx l).writingUnissued then b.writing :: b.rest else b.rest
    | none => [] := rfl

theorem Sys.issuedEntry_eq (s : Sys) : s.issuedEntry = match s.lead with
    | some (l, b) => if (s.tx l).issuing then some b.writing else none
    | none => none := rfl

theorem Sys.issuedSlot_eq (s : Sys) : s.issuedSlot = match s.lead with
    | some (l, b) =>
      if (s.tx l).issuing || (s.tx l).pc == .own2 || (s.tx l).pc == .own3 then some b.writing
      else none
    | none => none := rfl

/-- The thread `c` does not hold the batch. -/
def NotHolder (s : Sys) (c : Nat) : Prop := ∀ l b, s.lead = some (l, b) → l ≠ c

theorem notHolder_of_pc {s : Sys} {c : Nat} (hl : LeadInv s)
    (hpc : (s.tx c).pc.holdsBatch = false) : NotHolder s c := by
  intro l b hlead hlc
  subst hlc
  simp only [LeadInv, hlead, LeadOf] at hl
  rw [hl.2.1] at hpc
  contradiction

theorem unissued_set {s : Sys} {c : Nat} (r : TxRec) (h : NotHolder s c) :
    (s.set c r).unissued = s.unissued := by
  simp only [Sys.unissued_eq, set_lead]
  split
  · rename_i l b hlead
    simp [set_tx, h l b hlead]
  · rfl

theorem issuedEntry_set {s : Sys} {c : Nat} (r : TxRec) (h : NotHolder s c) :
    (s.set c r).issuedEntry = s.issuedEntry := by
  simp only [Sys.issuedEntry_eq, set_lead]
  split
  · rename_i l b hlead
    simp [set_tx, h l b hlead]
  · rfl

theorem issuedSlot_set {s : Sys} {c : Nat} (r : TxRec) (h : NotHolder s c) :
    (s.set c r).issuedSlot = s.issuedSlot := by
  simp only [Sys.issuedSlot_eq, set_lead]
  split
  · rename_i l b hlead
    simp [set_tx, h l b hlead]
  · rfl

theorem queue_set {s : Sys} {c : Nat} (r : TxRec) (h : NotHolder s c) :
    (s.set c r).queue = s.queue := by
  simp [Sys.queue, unissued_set r h, issuedEntry_set r h]

theorem batchOf_notHolder {s : Sys} {c : Nat} (h : NotHolder s c) : s.batchOf c = none := by
  simp only [batchOf_eq]
  split
  · rename_i l b hlead
    simp [h l b hlead]
  · rfl

@[simp, sys_simps] theorem Sys.unissued_with_g (s : Sys) (g : Group) : { s with g }.unissued = s.unissued := rfl
@[simp, sys_simps] theorem Sys.issuedEntry_with_g (s : Sys) (g : Group) :
    { s with g }.issuedEntry = s.issuedEntry := rfl
@[simp, sys_simps] theorem Sys.issuedSlot_with_g (s : Sys) (g : Group) :
    { s with g }.issuedSlot = s.issuedSlot := rfl
@[simp, sys_simps] theorem Sys.queue_with_g (s : Sys) (g : Group) :
    { s with g }.queue = s.issuedEntry.toList ++ s.unissued ++ g.pending := rfl
@[simp, sys_simps] theorem Sys.batchOf_with_g (s : Sys) (g : Group) (c : Nat) :
    { s with g }.batchOf c = s.batchOf c := rfl

end GroupCommit

namespace GroupCommit
open Sys

@[simp, sys_simps] theorem Sys.inLog_set (s : Sys) (c : Nat) (r : TxRec) (d : Nat) :
    (s.set c r).InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.inSynced_set (s : Sys) (c : Nat) (r : TxRec) (d : Nat) :
    (s.set c r).InSynced d = s.InSynced d := rfl
@[simp, sys_simps] theorem Sys.inLog_with_g (s : Sys) (g : Group) (d : Nat) :
    { s with g }.InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.inSynced_with_g (s : Sys) (g : Group) (d : Nat) :
    { s with g }.InSynced d = s.InSynced d := rfl
@[simp, sys_simps] theorem Sys.inLog_with_lockHolder (s : Sys) (o : Option Nat) (d : Nat) :
    { s with lockHolder := o }.InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.inSynced_with_lockHolder (s : Sys) (o : Option Nat) (d : Nat) :
    { s with lockHolder := o }.InSynced d = s.InSynced d := rfl
@[simp, sys_simps] theorem Sys.inLog_with_lead (s : Sys) (o : Option (Nat × Batch)) (d : Nat) :
    { s with lead := o }.InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.inSynced_with_lead (s : Sys) (o : Option (Nat × Batch)) (d : Nat) :
    { s with lead := o }.InSynced d = s.InSynced d := rfl

theorem Sys.inLog_append (s : Sys) (x : Rec) (d : Nat) :
    { s with log := s.log ++ [x] }.InLog d ↔ s.InLog d ∨ x.tx = d := by
  simp [InLog, or_and_right, exists_or]

theorem Sys.inSynced_append (s : Sys) (x : Rec) (d : Nat) (h : s.synced ≤ s.log.length) :
    { s with log := s.log ++ [x] }.InSynced d ↔ s.InSynced d := by
  simp [InSynced, List.take_append_of_le_length h]

theorem Sys.inSynced_all (s : Sys) (d : Nat) :
    { s with synced := s.log.length }.InSynced d ↔ s.InLog d := by
  simp [InSynced, InLog, List.take_length]

theorem Sys.inSynced_inLog {s : Sys} {d : Nat} (h : s.InSynced d) : s.InLog d := by
  obtain ⟨r, hr, hrd⟩ := h
  exact ⟨r, List.mem_of_mem_take hr, hrd⟩

theorem notHolder_iff {s : Sys} {c : Nat} : NotHolder s c ↔ s.lead.map (·.1) ≠ some c := by
  constructor
  · intro h hm
    cases hl : s.lead with
    | none => simp [hl] at hm
    | some p =>
      obtain ⟨l, b⟩ := p
      simp [hl] at hm
      exact h l b hl hm
  · intro h l b hl hlc
    apply h
    simp [hl, hlc]

end GroupCommit

namespace GroupCommit

/-- Facts about a step kind of a waiting thread. -/
structure WaitFacts (p : Pc) : Prop where
  afterLeave : p.afterLeave = false
  beforeLeave : p.beforeLeave = false
  holdsBatch : p.holdsBatch = false
  leading : p.leading = false
  ne_dLeave : p ≠ .dLeave
  ne_dropped : p ≠ .dropped
  ne_endCommit : p ≠ .endCommit
  ne_commitEnd : p ≠ .commitEnd
  ne_syncLog : p ≠ .syncLog
  ne_own2 : p ≠ .own2
  ne_own3 : p ≠ .own3
  ne_begin : p ≠ .begin
  ne_upgrade : p ≠ .upgrade
  ne_write : p ≠ .write
  ne_finish : p ≠ .finish
  ne_dR1 : p ≠ .dR1
  ne_dR2 : p ≠ .dR2
  rollingBackOther : p.rollingBackOther = none
  removingOther : p.removingOther = none

theorem waitFacts {p : Pc} {t : Nat} (h : p.waitTicket = some t) : WaitFacts p := by
  cases p <;> simp_all [Pc.waitTicket] <;> constructor <;>
    simp [Pc.afterLeave, Pc.beforeLeave, Pc.holdsBatch, Pc.leading, Pc.rollingBackOther,
      Pc.removingOther]

theorem TxRec.ticket_of_wait {r : TxRec} {t : Nat} (h : r.pc.waitTicket = some t) :
    r.ticket = some t := by
  simp [TxRec.ticket, h]

end GroupCommit

namespace GroupCommit
open Sys

@[simp, sys_simps] theorem Sys.withLock_tx (s : Sys) (o : Option Nat) : (s.withLock o).tx = s.tx := rfl
@[simp, sys_simps] theorem Sys.withLock_g (s : Sys) (o : Option Nat) : (s.withLock o).g = s.g := rfl
@[simp, sys_simps] theorem Sys.withLock_lead (s : Sys) (o : Option Nat) : (s.withLock o).lead = s.lead := rfl
@[simp, sys_simps] theorem Sys.withLock_lockHolder (s : Sys) (o : Option Nat) :
    (s.withLock o).lockHolder = o := rfl
@[simp, sys_simps] theorem Sys.withLock_log (s : Sys) (o : Option Nat) : (s.withLock o).log = s.log := rfl
@[simp, sys_simps] theorem Sys.withLock_synced (s : Sys) (o : Option Nat) :
    (s.withLock o).synced = s.synced := rfl
@[simp, sys_simps] theorem Sys.withLock_enabled (s : Sys) (o : Option Nat) :
    (s.withLock o).enabled = s.enabled := rfl
@[simp, sys_simps] theorem Sys.withLock_unissued (s : Sys) (o : Option Nat) :
    (s.withLock o).unissued = s.unissued := rfl
@[simp, sys_simps] theorem Sys.withLock_issuedEntry (s : Sys) (o : Option Nat) :
    (s.withLock o).issuedEntry = s.issuedEntry := rfl
@[simp, sys_simps] theorem Sys.withLock_issuedSlot (s : Sys) (o : Option Nat) :
    (s.withLock o).issuedSlot = s.issuedSlot := rfl
@[simp, sys_simps] theorem Sys.withLock_queue (s : Sys) (o : Option Nat) : (s.withLock o).queue = s.queue := rfl
@[simp, sys_simps] theorem Sys.withLock_batchOf (s : Sys) (o : Option Nat) (c : Nat) :
    (s.withLock o).batchOf c = s.batchOf c := rfl
@[simp, sys_simps] theorem Sys.withLock_inLog (s : Sys) (o : Option Nat) (d : Nat) :
    (s.withLock o).InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.withLock_inSynced (s : Sys) (o : Option Nat) (d : Nat) :
    (s.withLock o).InSynced d = s.InSynced d := rfl

@[simp, sys_simps] theorem Sys.withG_tx (s : Sys) (g : Group) : (s.withG g).tx = s.tx := rfl
@[simp, sys_simps] theorem Sys.withG_g (s : Sys) (g : Group) : (s.withG g).g = g := rfl
@[simp, sys_simps] theorem Sys.withG_lead (s : Sys) (g : Group) : (s.withG g).lead = s.lead := rfl
@[simp, sys_simps] theorem Sys.withG_lockHolder (s : Sys) (g : Group) :
    (s.withG g).lockHolder = s.lockHolder := rfl
@[simp, sys_simps] theorem Sys.withG_log (s : Sys) (g : Group) : (s.withG g).log = s.log := rfl
@[simp, sys_simps] theorem Sys.withG_synced (s : Sys) (g : Group) : (s.withG g).synced = s.synced := rfl
@[simp, sys_simps] theorem Sys.withG_enabled (s : Sys) (g : Group) : (s.withG g).enabled = s.enabled := rfl
@[simp, sys_simps] theorem Sys.withG_unissued (s : Sys) (g : Group) : (s.withG g).unissued = s.unissued := rfl
@[simp, sys_simps] theorem Sys.withG_issuedEntry (s : Sys) (g : Group) :
    (s.withG g).issuedEntry = s.issuedEntry := rfl
@[simp, sys_simps] theorem Sys.withG_issuedSlot (s : Sys) (g : Group) :
    (s.withG g).issuedSlot = s.issuedSlot := rfl
@[simp, sys_simps] theorem Sys.withG_queue (s : Sys) (g : Group) :
    (s.withG g).queue = s.issuedEntry.toList ++ s.unissued ++ g.pending := rfl
@[simp, sys_simps] theorem Sys.withG_batchOf (s : Sys) (g : Group) (c : Nat) :
    (s.withG g).batchOf c = s.batchOf c := rfl
@[simp, sys_simps] theorem Sys.withG_inLog (s : Sys) (g : Group) (d : Nat) :
    (s.withG g).InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.withG_inSynced (s : Sys) (g : Group) (d : Nat) :
    (s.withG g).InSynced d = s.InSynced d := rfl

@[simp, sys_simps] theorem Sys.withSynced_tx (s : Sys) (n : Nat) : (s.withSynced n).tx = s.tx := rfl
@[simp, sys_simps] theorem Sys.withSynced_g (s : Sys) (n : Nat) : (s.withSynced n).g = s.g := rfl
@[simp, sys_simps] theorem Sys.withSynced_lead (s : Sys) (n : Nat) : (s.withSynced n).lead = s.lead := rfl
@[simp, sys_simps] theorem Sys.withSynced_lockHolder (s : Sys) (n : Nat) :
    (s.withSynced n).lockHolder = s.lockHolder := rfl
@[simp, sys_simps] theorem Sys.withSynced_log (s : Sys) (n : Nat) : (s.withSynced n).log = s.log := rfl
@[simp, sys_simps] theorem Sys.withSynced_synced (s : Sys) (n : Nat) : (s.withSynced n).synced = n := rfl
@[simp, sys_simps] theorem Sys.withSynced_unissued (s : Sys) (n : Nat) :
    (s.withSynced n).unissued = s.unissued := rfl
@[simp, sys_simps] theorem Sys.withSynced_issuedEntry (s : Sys) (n : Nat) :
    (s.withSynced n).issuedEntry = s.issuedEntry := rfl
@[simp, sys_simps] theorem Sys.withSynced_issuedSlot (s : Sys) (n : Nat) :
    (s.withSynced n).issuedSlot = s.issuedSlot := rfl
@[simp, sys_simps] theorem Sys.withSynced_queue (s : Sys) (n : Nat) :
    (s.withSynced n).queue = s.queue := rfl
@[simp, sys_simps] theorem Sys.withSynced_batchOf (s : Sys) (n : Nat) (c : Nat) :
    (s.withSynced n).batchOf c = s.batchOf c := rfl
@[simp, sys_simps] theorem Sys.withSynced_inLog (s : Sys) (n : Nat) (d : Nat) :
    (s.withSynced n).InLog d = s.InLog d := rfl

@[simp, sys_simps] theorem Sys.withLog_tx (s : Sys) (l : List Rec) : (s.withLog l).tx = s.tx := rfl
@[simp, sys_simps] theorem Sys.withLog_g (s : Sys) (l : List Rec) : (s.withLog l).g = s.g := rfl
@[simp, sys_simps] theorem Sys.withLog_lead (s : Sys) (l : List Rec) : (s.withLog l).lead = s.lead := rfl
@[simp, sys_simps] theorem Sys.withLog_lockHolder (s : Sys) (l : List Rec) :
    (s.withLog l).lockHolder = s.lockHolder := rfl
@[simp, sys_simps] theorem Sys.withLog_log (s : Sys) (l : List Rec) : (s.withLog l).log = l := rfl
@[simp, sys_simps] theorem Sys.withLog_synced (s : Sys) (l : List Rec) : (s.withLog l).synced = s.synced := rfl
@[simp, sys_simps] theorem Sys.withLog_unissued (s : Sys) (l : List Rec) :
    (s.withLog l).unissued = s.unissued := rfl
@[simp, sys_simps] theorem Sys.withLog_issuedEntry (s : Sys) (l : List Rec) :
    (s.withLog l).issuedEntry = s.issuedEntry := rfl
@[simp, sys_simps] theorem Sys.withLog_issuedSlot (s : Sys) (l : List Rec) :
    (s.withLog l).issuedSlot = s.issuedSlot := rfl
@[simp, sys_simps] theorem Sys.withLog_queue (s : Sys) (l : List Rec) : (s.withLog l).queue = s.queue := rfl
@[simp, sys_simps] theorem Sys.withLog_batchOf (s : Sys) (l : List Rec) (c : Nat) :
    (s.withLog l).batchOf c = s.batchOf c := rfl

@[simp, sys_simps] theorem Sys.withLead_tx (s : Sys) (o : Option (Nat × Batch)) : (s.withLead o).tx = s.tx := rfl
@[simp, sys_simps] theorem Sys.withLead_g (s : Sys) (o : Option (Nat × Batch)) : (s.withLead o).g = s.g := rfl
@[simp, sys_simps] theorem Sys.withLead_lead (s : Sys) (o : Option (Nat × Batch)) : (s.withLead o).lead = o := rfl
@[simp, sys_simps] theorem Sys.withLead_lockHolder (s : Sys) (o : Option (Nat × Batch)) :
    (s.withLead o).lockHolder = s.lockHolder := rfl
@[simp, sys_simps] theorem Sys.withLead_log (s : Sys) (o : Option (Nat × Batch)) : (s.withLead o).log = s.log := rfl
@[simp, sys_simps] theorem Sys.withLead_synced (s : Sys) (o : Option (Nat × Batch)) :
    (s.withLead o).synced = s.synced := rfl
@[simp, sys_simps] theorem Sys.withLead_inLog (s : Sys) (o : Option (Nat × Batch)) (d : Nat) :
    (s.withLead o).InLog d = s.InLog d := rfl
@[simp, sys_simps] theorem Sys.withLead_inSynced (s : Sys) (o : Option (Nat × Batch)) (d : Nat) :
    (s.withLead o).InSynced d = s.InSynced d := rfl

@[simp, sys_simps] theorem Sys.set_withLock (s : Sys) (c : Nat) (r : TxRec) (o : Option Nat) :
    (s.withLock o).set c r = (s.set c r).withLock o := rfl
@[simp, sys_simps] theorem Sys.set_withG (s : Sys) (c : Nat) (r : TxRec) (g : Group) :
    (s.withG g).set c r = (s.set c r).withG g := rfl

end GroupCommit

namespace GroupCommit
open Sys

@[simp, sys_simps] theorem upd_upd (f : Nat → TxRec) (c : Nat) (r r' : TxRec) :
    upd (upd f c r) c r' = upd f c r' := by
  funext x; simp only [upd]; split <;> rfl

@[simp, sys_simps] theorem Sys.set_set (s : Sys) (c : Nat) (r r' : TxRec) :
    (s.set c r).set c r' = s.set c r' := by
  simp only [Sys.set, upd_upd]

end GroupCommit
