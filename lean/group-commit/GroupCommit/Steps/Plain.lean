import GroupCommit.Facts

/-!
# Steps of a thread that does not hold the batch

These steps change only the record of the thread, and maybe the commit lock.
-/

namespace GroupCommit
open Sys

theorem frame_unref_nolock {s : Sys} {c : Nat} {r' : TxRec} (hs : Inv s)
    (hu : Unreferenced s c) (hr' : r'.ticket = none) : Others (s.set c r') c :=
  frame_unref (o := s.lockHolder) hs hu hr' (fun _ _ => Iff.rfl)

theorem frame_set_nolock {s : Sys} {c : Nat} {r' : TxRec} (hs : Inv s)
    (hnh : NotHolder s c) (hobs : (s.tx c).obs = r'.obs) (hab : c ∉ s.g.abandoned) :
    Others (s.set c r') c :=
  frame_set (o := s.lockHolder) hs hnh hobs hab (fun _ _ => Iff.rfl)

theorem not_abandoned_of_pc {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc ≠ .dropped) :
    c ∉ s.g.abandoned := fun h => hpc (hs.issue.2.2.2 c h).2.1

theorem lockHolder_ne_of_not_holds {s : Sys} {c : Nat} (hs : Inv s)
    (h : (s.tx c).holdsLock = false) : s.lockHolder ≠ some c := by
  intro h'; rw [← holdsLock_iff hs] at h'; simp [h] at h'

/-- A transaction starts. -/
theorem step_start {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .idle)
    (hnone : (s.tx c).st = .none) :
    Inv (s.set c { s.tx c with st := .live, pc := .begin }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlk := lockHolder_ne_of_not_holds (c := c) hs (by simp [TxRec.holdsLock, hpc, hc])
  have hlc := hs.log c
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv, InLog, InSynced] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hc, hlk]

/-- An exclusive transaction starts and takes the commit lock. -/
theorem step_start_exclusive {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .idle)
    (hnone : (s.tx c).st = .none) (hfree : s.lockHolder = none) :
    Inv ((s.set c { s.tx c with st := .live, pc := .begin, excl := true, held := true }).withLock
      (some c)) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  refine inv_of_parts (frame_unref hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave]) (fun d hd => by simp [hfree, Ne.symm hd])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv, InLog, InSynced] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave]


theorem status_of {s : Sys} {c : Nat} (hs : Inv s) : StatusOf (s.tx c) := hs.status c

/-- `BeginCommitLogicalLog` without group commit: take the commit lock. -/
theorem step_begin_direct {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .begin)
    (hfree : s.lockHolder = none) :
    Inv ((s.set c { s.tx c with pc := .upgrade, held := true }).withLock (some c)) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  have hnb : s.batchOf c = none := batchOf_notHolder hu.notHolder
  refine inv_of_parts (frame_unref hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave]) (fun d hd => by simp [hfree, Ne.symm hd])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv, InLog, InSynced] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    simp only [Sys.batchOf_eq] at hnb ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave]

/-- `BeginCommitLogicalLog` of an exclusive transaction, which holds the lock. -/
theorem step_begin_exclusive {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .begin)
    (hex : (s.tx c).excl = true) :
    Inv (s.set c { s.tx c with pc := .upgrade }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnb : s.batchOf c = none := batchOf_notHolder hu.notHolder
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc, hex]
  · simp only [LogInv, InLog, InSynced] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    simp only [Sys.batchOf_eq] at hnb ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp only [GroupTxInv, TxRec.holdsLock, hpc] at hgc
    simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hgc.1]

/-- A waiter sees that its ticket is durable. -/
theorem step_ack {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .await t)
    (hdur : t ≤ s.g.durableThrough) :
    Inv (s.set c { s.tx c with pc := .endCommit }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hw : (s.tx c).pc.waitTicket = some t := by simp [hpc, Pc.waitTicket]
  have hu := unref_of_acked hs hw hdur
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hsync : ⟨c, some t⟩ ∈ s.log.take s.synced :=
    ((hs.log c).2.2.2.2.2.2.2.2.2.2.2.2.2.2.1 t (by simp [hw])).2 hdur
  have hnb : s.batchOf c = none := batchOf_notHolder hu.notHolder
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv, InLog, InSynced] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    simp only [Sys.batchOf_eq] at hnb ⊢
    have hin : ∃ r ∈ s.log.take s.synced, r.tx = c := ⟨_, hsync, rfl⟩
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp only [GroupTxInv, TxRec.holdsLock, hpc] at hgc
    have hlk : s.lockHolder ≠ some c := by intro h; rw [← hgc.1] at h; simp [hc] at h
    simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hc, hlk]

/-- A step inside the waiting states that keeps the ticket. -/
theorem step_wait_move {s : Sys} {c t : Nat} {p : Pc} {hd f : Bool} {o : Option Nat} (hs : Inv s)
    (hw : (s.tx c).pc.waitTicket = some t) (hp : p.waitTicket = some t)
    (hstat : StatusOf { s.tx c with pc := p, held := hd, failed := f })
    (hwork : p = .awaitWork t → t ∉ s.g.retry)
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d))
    (hholds : ({ s.tx c with pc := p, held := hd, failed := f } : TxRec).holdsLock = true ↔
      o = some c)
    (hps : p.isPrefixSynced = true → f = false → s.synced = s.log.length) :
    Inv ((s.set c { s.tx c with pc := p, held := hd, failed := f }).withLock o) := by
  have fo := waitFacts hw
  have fp := waitFacts hp
  have hnh := notHolder_of_wait hs hw
  have hp' : ({ s.tx c with pc := p, held := hd, failed := f } : TxRec).pc.waitTicket = some t := hp
  have hobs : (s.tx c).obs = ({ s.tx c with pc := p, held := hd, failed := f } : TxRec).obs := by
    have e1 : ((s.tx c).pc == Pc.dLeave) = false := by simpa using fo.ne_dLeave
    have e2 : (p == Pc.dLeave) = false := by simpa using fp.ne_dLeave
    have e3 : ((s.tx c).pc == Pc.dropped) = false := by simpa using fo.ne_dropped
    have e4 : (p == Pc.dropped) = false := by simpa using fp.ne_dropped
    simp only [TxRec.obs, TxRec.ticket_of_wait hw, TxRec.ticket_of_wait hp']
    simp [fo.afterLeave, fp.afterLeave, e1, e2, e3, e4]
  have hlc := hs.log c
  have htc := hs.ticket c
  refine inv_of_parts (frame_set hs hnh hobs (not_abandoned_of_pc hs fo.ne_dropped) hlock)
    ?_ ?_ ?_ ?_
  · simpa [StatusInv] using hstat
  · simp only [LogInv, sys_simps, ite_true] at hlc ⊢
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, _, _, _, _, _, _, h15⟩ := hlc
    refine ⟨h1, ?_, h3, h4, h5, h6, h7, h8, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro hl hl'
      rcases h2 hl hl' with h | h | h
      · exact Or.inl h
      · exact Or.inr (Or.inl h)
      · exact absurd h.1 fo.ne_own2
    · intro h; rcases h with h | h
      · exact absurd h.1 fp.ne_endCommit
      · exact absurd h fp.ne_commitEnd
    · intro h; exact absurd h fp.ne_syncLog
    · intro h; rcases h with h | h
      · exact absurd h fp.ne_own2
      · exact absurd h fp.ne_own3
    · intro h; exact absurd h fp.ne_begin
    · intro h; rcases h with h | h | h
      · exact absurd h fp.ne_upgrade
      · exact absurd h fp.ne_write
      · exact absurd h fp.ne_finish
    · intro h; rcases h with h | h
      · exact absurd h.1 fp.ne_endCommit
      · exact hps h
    · rw [hw] at h15
      refine ⟨by rw [hp]; exact h15.1, ?_⟩
      intro h
      rcases h with h | h | h
      · rw [h] at hp; simp [Pc.waitTicket] at hp
      · rw [fp.rollingBackOther] at h; cases h
      · rw [fp.removingOther] at h; cases h
  · simp only [TicketInv, sys_simps, ite_true, TxRec.ticket_of_wait hp', queue_set _ hnh]
    simp only [TicketInv, TxRec.ticket_of_wait hw] at htc
    intro t' ht'
    simp at ht'; subst ht'
    obtain ⟨h1, h2, h3, h4, _⟩ := htc t' (by simp)
    exact ⟨h1, h2, h3, h4, hwork⟩
  · have hgc := hs.groupTx c
    simp only [GroupTxInv] at hgc ⊢
    simp only [sys_simps, ite_true]
    refine ⟨hholds, ?_, ?_, ?_⟩
    · intro h; rw [fp.afterLeave] at h; contradiction
    · intro h; exact absurd h fp.ne_dropped
    · intro h; exact absurd h fp.ne_dropped


/-- `CommitEnd`: the transaction becomes Committed. -/
theorem step_commit_end {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .commitEnd) :
    Inv (s.set c { s.tx c with pc := .committed, st := .committed, wasCommitted := true,
                               acked := true }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hsync : s.InSynced c := (hs.log c).2.2.2.2.2.2.2.2.1 (Or.inr hpc)
  have hin : s.InLog c := inSynced_inLog hsync
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf]
  · simp only [LogInv] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp only [GroupTxInv, TxRec.holdsLock, hpc] at hgc
    simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hgc.1]

theorem unlockIfHeld_eq (s : Sys) (c : Nat) :
    s.unlockIfHeld c =
      if (s.tx c).held then (s.set c { s.tx c with held := false }).withLock none else s := rfl

/-- `FinalizeCommit` of a thread that holds the commit lock. -/
theorem step_finalize_held {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .committed)
    (hh : (s.tx c).held = true) :
    Inv ((s.set c { s.tx c with notified := true, held := false, st := .removed, pc := .done }).withLock
      none) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  have hgc := hs.groupTx c
  simp only [GroupTxInv, TxRec.holdsLock, hpc] at hgc
  have hlk : s.lockHolder = some c := hgc.1.1 hh
  refine inv_of_parts (frame_unref hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave]) (fun d hd => by simp [hlk, Ne.symm hd])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf]
  · simp only [LogInv] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave]

/-- `FinalizeCommit` of a waiter, which does not hold the commit lock. -/
theorem step_finalize_free {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .committed)
    (hh : (s.tx c).held = false) :
    Inv (s.set c { s.tx c with notified := true, st := .removed, pc := .done }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hlc := hs.log c
  have hgc := hs.groupTx c
  simp only [GroupTxInv, TxRec.holdsLock, hpc] at hgc
  have hlk : s.lockHolder ≠ some c := by intro h; rw [← hgc.1] at h; simp [hh] at h
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hh]
  · simp only [LogInv] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hh, hlk]


theorem status_wait {s : Sys} {c t : Nat} (hs : Inv s) (hw : (s.tx c).pc.waitTicket = some t) :
    (s.tx c).st = .live ∧ (s.tx c).excl = false := by
  have hc := status_of (c := c) hs
  cases hp : (s.tx c).pc <;> simp_all [StatusOf, Pc.waitTicket]

/-- `take_retry` did not find the ticket. -/
theorem step_await_miss {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .await t) :
    Inv (s.set c { s.tx c with pc := .awaitLock t }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  have := step_wait_move (c := c) (t := t) (p := .awaitLock t) (hd := (s.tx c).held) (f := (s.tx c).failed)
    (o := s.lockHolder) hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun _ _ => Iff.rfl) (by simp [TxRec.holdsLock, hgc])
    (by simp [Pc.isPrefixSynced])
  exact this

/-- The waiter takes the free commit lock. -/
theorem step_lock_ok {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitLock t)
    (hfree : s.lockHolder = none) :
    Inv ((s.set c { s.tx c with pc := .awaitLocked t }).withLock (some c)) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  exact step_wait_move (c := c) (t := t) (p := .awaitLocked t) (hd := (s.tx c).held) (f := (s.tx c).failed)
    hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun d hd => by simp [hfree, Ne.symm hd])
    (by simp [TxRec.holdsLock]) (by simp [Pc.isPrefixSynced])

/-- The commit lock is busy, so the waiter parks. -/
theorem step_lock_busy {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitLock t) :
    Inv (s.set c { s.tx c with pc := .await t }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  exact step_wait_move (c := c) (t := t) (p := .await t) (hd := (s.tx c).held) (f := (s.tx c).failed)
    (o := s.lockHolder) hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun _ _ => Iff.rfl) (by simp [TxRec.holdsLock, hgc])
    (by simp [Pc.isPrefixSynced])

/-- `take_retry` under the lock did not find the ticket. -/
theorem step_locked_miss {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitLocked t)
    (hmiss : t ∉ s.g.retry) :
    Inv (s.set c { s.tx c with pc := .awaitWork t }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  exact step_wait_move (c := c) (t := t) (p := .awaitWork t) (hd := (s.tx c).held) (f := (s.tx c).failed)
    (o := s.lockHolder) hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (fun _ => hmiss) (fun _ _ => Iff.rfl)
    (by simp only [TxRec.holdsLock]; exact ⟨fun _ => hgc.1 trivial, fun _ => trivial⟩)
    (by simp [Pc.isPrefixSynced])

/-- `take_work` found only written records: sync them. -/
theorem step_work_sync {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitWork t) :
    Inv (s.set c { s.tx c with pc := .syncPrefix t, held := true }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  exact step_wait_move (c := c) (t := t) (p := .syncPrefix t) (hd := true) (f := (s.tx c).failed)
    (o := s.lockHolder) hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun _ _ => Iff.rfl)
    (by simp only [TxRec.holdsLock]; exact ⟨fun _ => hgc.1 trivial, fun _ => trivial⟩)
    (by simp [Pc.isPrefixSynced])

/-- `take_work` found nothing: release the lock and park. -/
theorem step_work_none {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitWork t) :
    Inv ((s.set c { s.tx c with pc := .await t }).withLock none) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  have hlk : s.lockHolder = some c := hgc.1 trivial
  exact step_wait_move (c := c) (t := t) (p := .await t) (hd := (s.tx c).held) (f := (s.tx c).failed)
    hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun d hd => by simp [hlk, Ne.symm hd])
    (by simp [TxRec.holdsLock, hc]) (by simp [Pc.isPrefixSynced])

/-- `SyncGroupPrefix`: the fsync covers the whole log. -/
theorem step_sync_prefix {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .syncPrefix t) :
    Inv ((s.set c { s.tx c with pc := .prefixSynced t }).withSynced s.log.length) := by
  have hs1 := inv_raise_synced hs
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  have := step_wait_move (s := s.withSynced s.log.length) (c := c) (t := t) (p := .prefixSynced t)
    (hd := (s.tx c).held) (f := (s.tx c).failed) (o := s.lockHolder) hs1
    (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun _ _ => Iff.rfl) (by simp [TxRec.holdsLock, hgc])
    (by simp)
  exact this

/-- `SyncGroupPrefix`: the fsync fails. -/
theorem step_sync_prefix_fail {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .syncPrefix t) :
    Inv (s.set c { s.tx c with pc := .prefixSynced t, failed := true }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  exact step_wait_move (c := c) (t := t) (p := .prefixSynced t) (hd := (s.tx c).held) (f := true)
    (o := s.lockHolder) hs (by simp [hpc, Pc.waitTicket]) (by simp [Pc.waitTicket])
    (by simp [StatusOf, hc]) (by simp) (fun _ _ => Iff.rfl) (by simp [TxRec.holdsLock, hgc])
    (by simp)


/-- `EndCommitLogicalLog` of a thread without a batch. -/
theorem step_end_commit_nobatch {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .endCommit)
    (hf : (s.tx c).failed = false) (hnb : s.batchOf c = none) :
    Inv (s.set c { s.tx c with pc := .commitEnd }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hnh := notHolder_of_batchOf hnb
  have hu := unref_of_notHolder (c := c) hs hnh
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
  have hlc := hs.log c
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  have hsync : s.InSynced c := (hs.log c).2.2.2.2.2.2.2.2.1 (Or.inl ⟨hpc, hf⟩)
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv] at hlc ⊢
    simp only [sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hgc]

/-- `SyncLogicalLog` of a direct commit fails. -/
theorem step_sync_log_fail_nobatch {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .syncLog)
    (hnb : s.batchOf c = none) :
    Inv (s.set c { s.tx c with pc := .endCommit, failed := true }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hnh := notHolder_of_batchOf hnb
  have hu := unref_of_notHolder (c := c) hs hnh
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
  have hlc := hs.log c
  have hgc := (hs.groupTx c).1
  simp only [TxRec.holdsLock, hpc] at hgc
  have hin : s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.1 hpc
  refine inv_of_parts (frame_unref_nolock hs hu (by simp [TxRec.ticket, Pc.waitTicket,
    Pc.beforeLeave])) ?_ ?_ ?_ ?_
  · simp [StatusInv, StatusOf, hc]
  · simp only [LogInv] at hlc ⊢
    simp only [sys_simps, ite_true, hpc, hnb] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp [TicketInv, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · simp [GroupTxInv, TxRec.holdsLock, Pc.afterLeave, hgc]

theorem dropPoint_facts {p : Pc} (h : p.dropPoint = true) :
    p.afterLeave = false ∧ p ≠ .dLeave ∧ p ≠ .dropped ∧ p.beforeLeave = false ∧
    p.waitTicket = p.ticket ∧ (p ≠ .awaitLocked (p.waitTicket.getD 0)) ∧
    (p ≠ .awaitWork (p.waitTicket.getD 0)) := by
  cases p <;> simp_all [Pc.dropPoint, Pc.afterLeave, Pc.beforeLeave, Pc.waitTicket, Pc.ticket]

/-- A statement is dropped: the cleanup starts. The thread does not hold the batch. -/
theorem step_drop_nobatch {s : Sys} {c : Nat} (hs : Inv s) (hdp : (s.tx c).pc.dropPoint = true)
    (hnb : s.batchOf c = none) :
    Inv (s.set c { s.tx c with dropFrom := (s.tx c).pc, pc := .dR1 }) := by
  have hc := status_of (c := c) hs
  obtain ⟨ha, hl, hd, hb, hwt, _, _⟩ := dropPoint_facts hdp
  have hnh := notHolder_of_batchOf hnb
  have hto : (s.tx c).ticket = (s.tx c).pc.ticket := by
    cases hp : (s.tx c).pc <;>
      simp_all [TxRec.ticket, Pc.dropPoint, Pc.waitTicket, Pc.beforeLeave, Pc.ticket]
  have htn : ({ s.tx c with dropFrom := (s.tx c).pc, pc := .dR1 } : TxRec).ticket =
      (s.tx c).pc.ticket := by
    simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  have hobs : (s.tx c).obs = ({ s.tx c with dropFrom := (s.tx c).pc, pc := .dR1 } : TxRec).obs := by
    have e1 : ((s.tx c).pc == Pc.dLeave) = false := by simpa using hl
    have e3 : ((s.tx c).pc == Pc.dropped) = false := by simpa using hd
    simp only [TxRec.obs, hto, htn]
    simp [ha, e1, e3, show Pc.dR1.afterLeave = false from rfl]
  have hlc := hs.log c
  have htc := hs.ticket c
  have hgc := hs.groupTx c
  refine inv_of_parts (frame_set_nolock hs hnh hobs (not_abandoned_of_pc hs hd)) ?_ ?_ ?_ ?_
  · simp only [StatusInv, StatusOf, sys_simps, ite_true]
    cases hp : (s.tx c).pc <;> simp_all [StatusOf, Pc.dropPoint]
  · simp only [LogInv, sys_simps, ite_true] at hlc ⊢
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, _, _, _, _, _, _, _⟩ := hlc
    refine ⟨h1, ?_, h3, h4, h5, h6, h7, h8, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro hl hl'
      rcases h2 hl hl' with h | h | h
      · exact Or.inl h
      · exact Or.inr (Or.inl h)
      · exact absurd h.1 (by cases hp : (s.tx c).pc <;> simp_all [Pc.dropPoint])
    all_goals simp [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [TicketInv, sys_simps, ite_true, queue_set _ hnh] at htc ⊢
    rw [htn, ← hto]
    intro t hmem
    obtain ⟨h1, h2, h3, h4, _⟩ := htc t hmem
    exact ⟨h1, h2, h3, h4, by simp⟩
  · simp only [GroupTxInv, sys_simps, ite_true] at hgc ⊢
    refine ⟨?_, ?_, ?_, ?_⟩
    · rw [← hgc.1]
      cases hp : (s.tx c).pc <;> simp_all [TxRec.holdsLock, Pc.dropPoint]
    all_goals simp [Pc.afterLeave]

end GroupCommit
