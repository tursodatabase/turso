import GroupCommit.Steps.Retry

/-!
# The steps of a commit that writes its own record without a batch

This is the path of an exclusive transaction, and of every transaction when
group commit is off.
-/

namespace GroupCommit
open Sys

theorem unref_direct {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .upgrade ∨ (s.tx c).pc = .write ∨ (s.tx c).pc = .finish ∨
      (s.tx c).pc = .own2 ∨ (s.tx c).pc = .own3 ∨ (s.tx c).pc = .syncLog) :
    Unreferenced s c := by
  apply unref_of_notHolder hs (notHolder_of_batchOf hnb)
  · rcases hpc with h | h | h | h | h | h <;> simp [TxRec.ticket, h, Pc.waitTicket, Pc.beforeLeave]
  · rcases hpc with h | h | h | h | h | h <;> simp [h, Pc.afterLeave]
  · rcases hpc with h | h | h | h | h | h <;> simp [h]

/-- A step of a direct commit that changes only its own record. -/
theorem step_direct_local {s : Sys} {c : Nat} (r' : TxRec) (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .upgrade ∨ (s.tx c).pc = .write ∨ (s.tx c).pc = .finish ∨
      (s.tx c).pc = .own2 ∨ (s.tx c).pc = .own3 ∨ (s.tx c).pc = .syncLog)
    (hr'tk : r'.ticket = none) (hst : StatusOf r')
    (hlog : LogInv (s.set c r') c)
    (hheld : r'.held = (s.tx c).held)
    (hr'pc : r'.pc.afterLeave = false ∧ r'.pc ≠ .dropped ∧ r'.pc ≠ .awaitLocked 0 ∧
      (∀ t, r'.pc ≠ .awaitLocked t) ∧ (∀ t, r'.pc ≠ .awaitWork t)) :
    Inv (s.set c r') := by
  have hu := unref_direct hs hnb hpc
  refine inv_of_parts (frame_unref_nolock hs hu hr'tk) (by simpa [StatusInv] using hst) hlog
    (by simp [TicketInv, hr'tk]) ?_
  have hgc := hs.groupTx c
  have hold : (s.tx c).holdsLock = (s.tx c).held := by
    rcases hpc with h | h | h | h | h | h <;> simp [TxRec.holdsLock, h]
  have hnew : r'.holdsLock = r'.held := by
    simp only [TxRec.holdsLock]
    split
    · rename_i t h; exact absurd h (hr'pc.2.2.2.1 t)
    · rename_i t h; exact absurd h (hr'pc.2.2.2.2 t)
    · rfl
  simp only [GroupTxInv, sys_simps, ite_true] at hgc ⊢
  refine ⟨?_, ?_, ?_, ?_⟩
  · rw [hnew, hheld, ← hold]; exact hgc.1
  · intro h; rw [hr'pc.1] at h; cases h
  · intro h; exact absurd h hr'pc.2.1
  · intro h; exact absurd h hr'pc.2.1


theorem direct_status {s : Sys} {c : Nat} (hs : Inv s)
    (hpc : (s.tx c).pc = .upgrade ∨ (s.tx c).pc = .write ∨ (s.tx c).pc = .finish ∨
      (s.tx c).pc = .own2 ∨ (s.tx c).pc = .own3 ∨ (s.tx c).pc = .syncLog) :
    (s.tx c).st = .live ∧ (s.tx c).held = true := by
  have hc := status_of (c := c) hs
  rcases hpc with h | h | h | h | h | h <;> simp only [StatusOf, h] at hc <;> simp [hc]

/-- `UpgradeLogicalLogHeader` of a direct commit. -/
theorem step_direct_upgrade {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .upgrade) : Inv (s.set c { s.tx c with pc := .write }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inl hpc)
  have hsub := submitted_false hs (Or.inl hpc)
  have hlc := hs.log c
  refine step_direct_local _ hs hnb (Or.inl hpc) (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld, hsub]) ?_ rfl (by simp [Pc.afterLeave])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- `WriteLogicalLog` of a direct commit: `log_tx` is issued. -/
theorem step_direct_write {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .write) :
    Inv (s.set c { s.tx c with pc := .finish, submitted := true }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inl hpc))
  have hlc := hs.log c
  refine step_direct_local _ hs hnb (Or.inr (Or.inl hpc))
    (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld]) ?_ rfl (by simp [Pc.afterLeave])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- `log_tx` of a direct commit returns an error. -/
theorem step_direct_write_fail {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .write) :
    Inv (s.set c { s.tx c with pc := .finish, failed := true }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inl hpc))
  have hlc := hs.log c
  refine step_direct_local _ hs hnb (Or.inr (Or.inl hpc))
    (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld]) ?_ rfl (by simp [Pc.afterLeave])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- The write of a direct commit fails. -/
theorem step_direct_io_fail {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .finish) :
    Inv (s.set c { s.tx c with failed := true }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inr (Or.inl hpc)))
  have hlc := hs.log c
  refine step_direct_local _ hs hnb (Or.inr (Or.inr (Or.inl hpc)))
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld, hpc]) ?_ rfl (by simp [Pc.afterLeave, hpc])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- `own2` of a direct commit: mark its own record as appended. -/
theorem step_direct_own2 {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .own2) :
    Inv (s.set c { s.tx c with appended := true, pc := .own3 }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inr (Or.inr (Or.inl hpc))))
  have hsub := submitted_false hs (Or.inr (Or.inr (Or.inl hpc)))
  have hlc := hs.log c
  have hin : s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.2.1 (Or.inl hpc) hnb
  refine step_direct_local _ hs hnb (Or.inr (Or.inr (Or.inr (Or.inl hpc))))
    (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld, hsub]) ?_ rfl (by simp [Pc.afterLeave])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  simp only [InLog, InSynced] at hin hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

theorem group_eq_of_finishIssue {g : Group} {c : Nat} (hi : g.issued = none) (ha : g.abandoned = []) :
    (g.finishIssue c).1 = g ∧ (g.finishIssue c).2 = false := by
  cases g; simp_all [Group.finishIssue, setErase]

/-- `own3` of a direct commit: nothing is issued, so `finish_issue` changes nothing. -/
theorem step_direct_own3 {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .own3) :
    Inv (s.set c { s.tx c with pc := .syncLog }) := by
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl hpc)))))
  have hlc := hs.log c
  have hin : s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.2.1 (Or.inr hpc) hnb
  refine step_direct_local _ hs hnb (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl hpc)))))
    (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst, hheld]) ?_ rfl (by simp [Pc.afterLeave])
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  simp only [InLog, InSynced] at hin hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- `SyncLogicalLog` of a direct commit. -/
theorem step_direct_sync {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .syncLog) :
    Inv ((s.set c { s.tx c with pc := .endCommit }).withSynced s.log.length) := by
  have hs1 := inv_raise_synced hs
  obtain ⟨hst, hheld⟩ := direct_status hs (Or.inr (Or.inr (Or.inr (Or.inr (Or.inr hpc)))))
  have hin : s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.1 hpc
  have hlc := hs1.log c
  have := step_direct_local (s := s.withSynced s.log.length) (c := c)
    { s.tx c with pc := .endCommit } hs1 (by simpa using hnb)
    (by simp [hpc]) (by simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave])
    (by simp [StatusOf, hst]) ?_ (by simp) (by simp [Pc.afterLeave])
  · exact this
  simp only [LogInv, sys_simps, ite_true, hpc, hnb] at hlc ⊢
  simp only [InLog, InSynced, sys_simps, List.take_length] at hin hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

end GroupCommit
