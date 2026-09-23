import GroupCommit.Steps.Direct

/-!
# Steps of the leader that change only its own record
-/

namespace GroupCommit
open Sys

/-- A step of the leader that keeps what the queue and `issued` read from its
record keeps the parts of the invariant about other transactions. -/
theorem frame_holder {s : Sys} {c : Nat} {b : Batch} {r' : TxRec} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hobs : (s.tx c).obs = r'.obs)
    (hiss : r'.issuing = (s.tx c).issuing) (hwu : r'.writingUnissued = (s.tx c).writingUnissued)
    (hsl : (r'.issuing || r'.pc == .own2 || r'.pc == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3))
    (hrb : (s.tx c).pc.rollingBackOther = none ∧ (s.tx c).pc.removingOther = none)
    (hab : c ∉ s.g.abandoned)
    (hlead : LeadOf (s.set c r') c b) : Others (s.set c r') c := by
  obtain ⟨hst, hlog, htk, hgtx, _, hq, hiss0, hmark, hord, hholes, huniq⟩ := hs
  have hc : (s.set c r').tx c = r' := by simp
  have hunis : (s.set c r').unissued = s.unissued := by
    simp only [Sys.unissued_eq, set_lead, hl, hc, hwu]
  have hient : (s.set c r').issuedEntry = s.issuedEntry := by
    simp only [Sys.issuedEntry_eq, set_lead, hl, hc, hiss]
  have hslot : (s.set c r').issuedSlot = s.issuedSlot := by
    simp only [Sys.issuedSlot_eq, set_lead, hl, hc, hsl]
  have hqueue : (s.set c r').queue = s.queue := by
    simp only [Sys.queue, hunis, hient, set_g]
  have hlc : ∀ l, l ∈ (s.lead.map (·.1)).toList → l = c := by
    intro l h; rw [hl] at h; simpa using h
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d hd; simpa [StatusInv, hd] using hst d
  · intro d hd; simpa [LogInv, hd] using hlog d
  · intro d hd; simpa [TicketInv, hd, hqueue] using htk d
  · intro d hd
    have hg := hgtx d
    simp only [GroupTxInv, set_tx, hd, ite_false, set_g, set_lead, set_lockHolder] at hg ⊢
    refine ⟨hg.1, hg.2.1, ?_, ?_⟩
    · intro h1 h2
      rcases hg.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
      · exact Or.inl h
      · have := hlc l hl'; subst this
        rw [hl2] at hrb; simp [Pc.rollingBackOther] at hrb
    · intro h1 h2
      obtain ⟨l, hl', hl2⟩ := hg.2.2.2 h1 h2
      have := hlc l hl'; subst this
      rw [hl2] at hrb; simp [Pc.removingOther] at hrb
  · simp only [LeadInv, set_lead, hl]; exact hlead
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hq
    simp only [QueueInv, hqueue, hunis, set_g, set_lead, inLog_set]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨h1, h2, h3, h4, h5⟩ := q3 e he
      refine ⟨h1, h2, h3, ?_, h5⟩
      by_cases hec : e.tx = c
      · simp only [set_tx, hec, ite_true]; rw [← obs_st hobs]; simpa [hec] using h4
      · simpa [hec] using h4
    · intro e he
      rcases q4 e he with h | h
      · left
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; rw [← obs_ticket hobs]; simpa [hec] using h
        · simpa [hec] using h
      · right
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; exact (obs_pendingLeaver hobs).1 (by simpa [hec] using h)
        · simpa [hec] using h
    · intro e he
      rcases q5 e he with h | h | h
      · left
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; rw [← obs_ticket hobs]; simpa [hec] using h
        · simpa [hec] using h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
    · intro x hx
      refine ⟨(q8 x hx).1, ?_⟩
      by_cases hxc : x = c
      · simp only [set_tx, hxc, ite_true]; rw [← obs_afterLeave hobs]; simpa [hxc] using (q8 x hx).2
      · simpa [hxc] using (q8 x hx).2
    · intro x hx
      refine ⟨?_, (q9 x hx).2⟩
      by_cases hxc : x = c
      · simp only [set_tx, hxc, ite_true]; rw [← obs_afterLeave hobs]; simpa [hxc] using (q9 x hx).1
      · simpa [hxc] using (q9 x hx).1
  · obtain ⟨i1, i2, i3, i4⟩ := hiss0
    simp only [IssueInv, hslot, set_g, set_lead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      obtain ⟨h1, h2⟩ := i2 w hw
      by_cases hwc : w = c
      · simp only [set_tx, hwc, ite_true]
        subst hwc; exact ⟨(obs_st hobs) ▸ h1, (obs_rolledBack hobs) ▸ h2⟩
      · simp only [set_tx, hwc, ite_false]; exact ⟨h1, h2⟩
    · intro e he l hl' hel
      have hlc' := hlc l hl'; subst hlc'
      rcases i3 e he l hl' hel with h | h
      · left; simpa [hel] using h
      · exact Or.inr h
    · intro x hxa
      obtain ⟨h1, h2, h3, h4⟩ := i4 x hxa
      refine ⟨h1, ?_⟩
      by_cases hxc : x = c
      · exact absurd (hxc ▸ hxa) hab
      · simp only [set_tx, hxc, ite_false]; exact ⟨h2, h3, h4⟩
  · simpa [MarkInv] using hmark
  · simpa [OrderInv, hqueue, hslot] using hord
  · intro h hh
    obtain ⟨d, hd⟩ := hholes h hh
    refine ⟨d, ?_⟩
    by_cases hdc : d = c
    · subst hdc; simp; rw [← obs_ticket hobs]; exact hd
    · simpa [hdc] using hd
  · intro d e t hd he
    have hd' : (s.tx d).ticket = some t := by
      by_cases hdc : d = c
      · subst hdc; simp at hd; rw [obs_ticket hobs]; exact hd
      · simpa [hdc] using hd
    have he' : (s.tx e).ticket = some t := by
      by_cases hec : e = c
      · subst hec; simp at he; rw [obs_ticket hobs]; exact he
      · simpa [hec] using he
    exact huniq d e t hd' he'

end GroupCommit

namespace GroupCommit
open Sys

theorem holder_queue_eq {s : Sys} {c : Nat} {b : Batch} {r' : TxRec}
    (hl : s.lead = some (c, b))
    (hiss : r'.issuing = (s.tx c).issuing) (hwu : r'.writingUnissued = (s.tx c).writingUnissued) :
    (s.set c r').queue = s.queue := by
  have hc : (s.set c r').tx c = r' := by simp
  have hunis : (s.set c r').unissued = s.unissued := by
    simp only [Sys.unissued_eq, set_lead, hl, hc, hwu]
  have hient : (s.set c r').issuedEntry = s.issuedEntry := by
    simp only [Sys.issuedEntry_eq, set_lead, hl, hc, hiss]
  simp only [Sys.queue, hunis, hient, set_g]

theorem leadOf_of_lead {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b)) :
    LeadOf s c b := by
  have h := hs.lead
  simp only [LeadInv, hl] at h
  exact h

theorem holder_holdsLock {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b)) :
    (s.tx c).holdsLock = (s.tx c).held := by
  have h := (leadOf_of_lead hs hl).2.1
  simp only [TxRec.holdsLock]
  split
  · rename_i t hp; rw [hp] at h; simp [Pc.holdsBatch, Pc.leading] at h
  · rename_i t hp; rw [hp] at h; simp [Pc.holdsBatch, Pc.leading] at h
  · rfl

theorem holder_not_dropped {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b)) :
    (s.tx c).pc ≠ .dropped := by
  intro h
  have := (leadOf_of_lead hs hl).2.1
  rw [h] at this; simp [Pc.holdsBatch, Pc.leading] at this

/-- A step of the leader that changes only its own record. -/
theorem step_holder_local {s : Sys} {c : Nat} {b : Batch} (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b)) (hobs : (s.tx c).obs = r'.obs)
    (hiss : r'.issuing = (s.tx c).issuing) (hwu : r'.writingUnissued = (s.tx c).writingUnissued)
    (hsl : (r'.issuing || r'.pc == .own2 || r'.pc == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3))
    (hrb : (s.tx c).pc.rollingBackOther = none ∧ (s.tx c).pc.removingOther = none)
    (hst : StatusOf r') (hlog : LogInv (s.set c r') c) (hlead : LeadOf (s.set c r') c b)
    (hheld : r'.held = (s.tx c).held)
    (hpc : ∀ t, r'.pc ≠ .awaitLocked t ∧ r'.pc ≠ .awaitWork t)
    (hafter : r'.pc.afterLeave = false) (hdrop : r'.pc ≠ .dropped) :
    Inv (s.set c r') := by
  have hab : c ∉ s.g.abandoned := not_abandoned_of_pc hs (holder_not_dropped hs hl)
  have hqueue := holder_queue_eq (r' := r') hl hiss hwu
  refine inv_of_parts (frame_holder hs hl hobs hiss hwu hsl hrb hab hlead)
    (by simpa [StatusInv] using hst) hlog ?_ ?_
  · have htc := hs.ticket c
    simp only [TicketInv, set_tx, ite_true, hqueue, set_g, set_log, inLog_set] at htc ⊢
    rw [← obs_ticket hobs]
    intro t ht
    obtain ⟨h1, h2, h3, h4, _⟩ := htc t ht
    exact ⟨h1, h2, h3, h4, fun h => absurd h (hpc t).2⟩
  · have hgc := hs.groupTx c
    have hold := holder_holdsLock hs hl
    have hnew : r'.holdsLock = r'.held := by
      simp only [TxRec.holdsLock]
      split
      · rename_i t h; exact absurd h (hpc t).1
      · rename_i t h; exact absurd h (hpc t).2
      · rfl
    simp only [GroupTxInv, set_tx, ite_true, set_lockHolder, set_g, set_lead] at hgc ⊢
    refine ⟨?_, ?_, ?_, ?_⟩
    · rw [hnew, hheld, ← hold]; exact hgc.1
    · intro h; rw [hafter] at h; cases h
    · intro h; exact absurd h hdrop
    · intro h; exact absurd h hdrop

end GroupCommit

namespace GroupCommit
open Sys

theorem holder_unissued_eq {s : Sys} {c : Nat} {b : Batch} {r' : TxRec}
    (hl : s.lead = some (c, b)) (hwu : r'.writingUnissued = (s.tx c).writingUnissued) :
    (s.set c r').unissued = s.unissued := by
  have hc : (s.set c r').tx c = r' := by simp
  simp only [Sys.unissued_eq, set_lead, hl, hc, hwu]

theorem holder_issuedSlot_eq {s : Sys} {c : Nat} {b : Batch} {r' : TxRec}
    (hl : s.lead = some (c, b))
    (hsl : (r'.issuing || r'.pc == .own2 || r'.pc == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3)) :
    (s.set c r').issuedSlot = s.issuedSlot := by
  have hc : (s.set c r').tx c = r' := by simp
  simp only [Sys.issuedSlot_eq, set_lead, hl, hc, hsl]

theorem holder_status {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b)) :
    (s.tx c).held = true := (leadOf_of_lead hs hl).1

theorem Pc.beq_eq (a b : Pc) : (a == b) = decide (a = b) := rfl

/-- `UpgradeLogicalLogHeader` of the leader. -/
theorem step_holder_upgrade {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .upgrade) :
    Inv (s.set c { s.tx c with pc := .write }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlc := hs.log c
  have hlo := leadOf_of_lead hs hl
  have hwu : ({ s.tx c with pc := .write } : TxRec).writingUnissued = (s.tx c).writingUnissued := by
    simp [TxRec.writingUnissued, hpc]
  have hsl : (({ s.tx c with pc := .write } : TxRec).issuing || Pc.write == .own2 || Pc.write == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3) := by
    simp [TxRec.issuing, hpc, Pc.beq_eq]
  refine step_holder_local _ hs hl (by simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket,
    Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]) (by simp [TxRec.issuing, hpc]) hwu hsl
    (by simp [hpc, Pc.rollingBackOther, Pc.removingOther]) (by simp [StatusOf, hc]) ?_ ?_ rfl
    (by simp) (by simp [Pc.afterLeave]) (by simp)
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    have hb : s.batchOf c = some b := by simp [Sys.batchOf_eq, hl]
    simp only [hb] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [LeadOf, sys_simps, ite_true, holder_unissued_eq hl hwu, holder_issuedSlot_eq hl hsl,
      hpc] at hlo ⊢
    simp only [TxRec.issuing, TxRec.writingUnissued, hpc, Pc.holdsBatch, Pc.leading,
      Pc.rollingBackOther, Pc.removingOther] at hlo ⊢
    grind

end GroupCommit

namespace GroupCommit
open Sys

theorem batchOf_holder {s : Sys} {c : Nat} {b : Batch} (hl : s.lead = some (c, b)) :
    s.batchOf c = some b := by simp [Sys.batchOf_eq, hl]

/-- A local step of the leader from one step kind to another, where both kinds
are in the same group for the queue. -/
theorem step_holder_move {s : Sys} {c : Nat} {b : Batch} (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b))
    (hkind : ((s.tx c).pc = .writeIssued ∨ (s.tx c).pc = .finish) ∧ r'.pc = .finish ∨
      (s.tx c).pc = .syncLog ∧ r'.pc = .endCommit)
    (hr' : r'.st = (s.tx c).st ∧ r'.held = (s.tx c).held ∧ r'.dropFrom = (s.tx c).dropFrom ∧
      r'.rolledBack = (s.tx c).rolledBack ∧ r'.appended = (s.tx c).appended)
    (hlog : LogInv (s.set c r') c) :
    Inv (s.set c r') := by
  have hc := hs.status c
  have hlo := leadOf_of_lead hs hl
  obtain ⟨e1, e2, e3, e4, e5⟩ := hr'
  have hwu : r'.writingUnissued = (s.tx c).writingUnissued := by
    rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [TxRec.writingUnissued, h, h']
  have hiss : r'.issuing = (s.tx c).issuing := by
    rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [TxRec.issuing, h, h']
  have hsl : (r'.issuing || r'.pc == .own2 || r'.pc == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3) := by
    rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [TxRec.issuing, h, h', Pc.beq_eq]
  refine step_holder_local r' hs hl ?_ hiss hwu hsl ?_ ?_ hlog ?_ e2 ?_ ?_ ?_
  · rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;>
      simp [TxRec.obs, TxRec.ticket, h, h', e1, e2, e3, e4, e5, Pc.waitTicket, Pc.beforeLeave,
        Pc.afterLeave, Pc.beq_eq]
  · rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [h, Pc.rollingBackOther, Pc.removingOther]
  · rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp only [StatusInv, StatusOf, h] at hc <;>
      simp [StatusOf, h', e1, e2, hc]
  · simp only [LeadOf, sys_simps, ite_true, holder_unissued_eq hl hwu, holder_issuedSlot_eq hl hsl,
      hiss, hwu, e2, e3] at hlo ⊢
    rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;>
      simp only [h, h', Pc.holdsBatch, Pc.leading, Pc.rollingBackOther, Pc.removingOther] at hlo ⊢ <;>
      grind
  · intro t; rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [h']
  · rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [h', Pc.afterLeave]
  · rcases hkind with ⟨h | h, h'⟩ | ⟨h, h'⟩ <;> simp [h']

/-- The log write of the leader was submitted. -/
theorem step_holder_write_issued {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .writeIssued) :
    Inv (s.set c { s.tx c with pc := .finish, submitted := true }) := by
  have hlc := hs.log c
  refine step_holder_move _ hs hl (Or.inl ⟨Or.inl hpc, rfl⟩) (by simp) ?_
  simp only [LogInv, sys_simps, ite_true, hpc, batchOf_holder hl] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- The leader could not submit its log write. -/
theorem step_holder_write_issued_fail {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .writeIssued) :
    Inv (s.set c { s.tx c with pc := .finish, failed := true }) := by
  have hlc := hs.log c
  refine step_holder_move _ hs hl (Or.inl ⟨Or.inl hpc, rfl⟩) (by simp) ?_
  simp only [LogInv, sys_simps, ite_true, hpc, batchOf_holder hl] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- The log write of the leader failed. -/
theorem step_holder_io_fail {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .finish) :
    Inv (s.set c { s.tx c with failed := true }) := by
  have hlc := hs.log c
  refine step_holder_move _ hs hl (Or.inl ⟨Or.inr hpc, hpc⟩) (by simp) ?_
  simp only [LogInv, sys_simps, ite_true, hpc, batchOf_holder hl] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- The fsync of the leader failed. -/
theorem step_holder_sync_fail {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .syncLog) :
    Inv (s.set c { s.tx c with pc := .endCommit, failed := true }) := by
  have hlc := hs.log c
  refine step_holder_move _ hs hl (Or.inr ⟨hpc, rfl⟩) (by simp) ?_
  simp only [LogInv, sys_simps, ite_true, hpc, batchOf_holder hl] at hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

/-- `SyncLogicalLog` of the leader. -/
theorem step_holder_sync {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .syncLog) :
    Inv ((s.set c { s.tx c with pc := .endCommit }).withSynced s.log.length) := by
  have hs1 := inv_raise_synced hs
  have hin : s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.1 hpc
  have hlc := hs1.log c
  have := step_holder_move (s := s.withSynced s.log.length) (c := c) (b := b)
    { s.tx c with pc := .endCommit } hs1 (by simpa using hl) (Or.inr ⟨by simpa using hpc, rfl⟩)
    (by simp) ?_
  · exact this
  simp only [LogInv, sys_simps, ite_true, hpc, batchOf_holder hl] at hlc ⊢
  simp only [InLog, InSynced, sys_simps, List.take_length] at hin hlc ⊢
  grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]

end GroupCommit

namespace GroupCommit
open Sys

theorem holder_drop_pc {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hdp : (s.tx c).pc.dropPoint = true) :
    (s.tx c).pc = .upgrade ∨ (s.tx c).pc = .finish ∨ (s.tx c).pc = .syncLog ∨
      (s.tx c).pc = .endCommit := by
  have h := (leadOf_of_lead hs hl).2.1
  cases hp : (s.tx c).pc <;> simp_all [Pc.dropPoint, Pc.holdsBatch, Pc.leading]

/-- The statement of the leader is dropped. -/
theorem step_holder_drop {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hdp : (s.tx c).pc.dropPoint = true) :
    Inv (s.set c { s.tx c with dropFrom := (s.tx c).pc, pc := .dR1, failed := false }) := by
  have hc := hs.status c
  have hlc := hs.log c
  have hlo := leadOf_of_lead hs hl
  have hp := holder_drop_pc hs hl hdp
  have hwu : ({ s.tx c with dropFrom := (s.tx c).pc, pc := .dR1, failed := false } : TxRec).writingUnissued =
      (s.tx c).writingUnissued := by
    rcases hp with h | h | h | h <;> simp [TxRec.writingUnissued, h]
  have hiss : ({ s.tx c with dropFrom := (s.tx c).pc, pc := .dR1, failed := false } : TxRec).issuing =
      (s.tx c).issuing := by
    rcases hp with h | h | h | h <;> simp [TxRec.issuing, h]
  have hsl : (({ s.tx c with dropFrom := (s.tx c).pc, pc := .dR1, failed := false } : TxRec).issuing ||
      Pc.dR1 == .own2 || Pc.dR1 == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3) := by
    rcases hp with h | h | h | h <;> simp [TxRec.issuing, h, Pc.beq_eq]
  refine step_holder_local _ hs hl ?_ hiss hwu hsl ?_ ?_ ?_ ?_ rfl (by simp) (by simp [Pc.afterLeave])
    (by simp)
  · rcases hp with h | h | h | h <;>
      simp [TxRec.obs, TxRec.ticket, h, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq,
        Pc.ticket]
  · rcases hp with h | h | h | h <;> simp [h, Pc.rollingBackOther, Pc.removingOther]
  · simp only [StatusInv] at hc
    rcases hp with h | h | h | h <;> simp only [StatusOf, h] at hc <;> simp [StatusOf, hc, h]
  · simp only [LogInv, sys_simps, ite_true, batchOf_holder hl] at hlc ⊢
    rcases hp with h | h | h | h <;> simp only [h] at hlc ⊢ <;> grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · have hsub : (s.tx c).pc = .upgrade → (s.tx c).submitted = false := fun h =>
      submitted_false hs (Or.inl h)
    simp only [LeadOf, sys_simps, ite_true, holder_unissued_eq hl hwu, holder_issuedSlot_eq hl hsl,
      hiss, hwu] at hlo ⊢
    rcases hp with h | h | h | h <;>
      simp only [h, Pc.holdsBatch, Pc.leading, Pc.rollingBackOther, Pc.removingOther] at hlo hsub ⊢ <;>
      grind

theorem noteWritten_same {g : Group} {a : Nat} (hr : g.retry = []) (ha : a ≤ g.writtenThrough) :
    g.noteWritten a = g := by
  unfold Group.noteWritten
  rw [ite_eq_right (by simp [hr]), Nat.max_eq_left ha]

/-- `release_group_claim` notes the written prefix again. -/
theorem step_holder_dR1 {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR1) :
    Inv (s.set c { s.tx c with pc := .dR2 }) := by
  have hc := hs.status c
  have hlc := hs.log c
  have hlo := leadOf_of_lead hs hl
  have hwu : ({ s.tx c with pc := .dR2 } : TxRec).writingUnissued = (s.tx c).writingUnissued := by
    simp [TxRec.writingUnissued, hpc]
  have hiss : ({ s.tx c with pc := .dR2 } : TxRec).issuing = (s.tx c).issuing := by
    simp [TxRec.issuing, hpc]
  have hsl : (({ s.tx c with pc := .dR2 } : TxRec).issuing || Pc.dR2 == .own2 || Pc.dR2 == .own3) =
      ((s.tx c).issuing || (s.tx c).pc == .own2 || (s.tx c).pc == .own3) := by
    simp [TxRec.issuing, hpc, Pc.beq_eq]
  refine step_holder_local _ hs hl ?_ hiss hwu hsl ?_ ?_ ?_ ?_ rfl (by simp) (by simp [Pc.afterLeave])
    (by simp)
  · simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
  · simp [hpc, Pc.rollingBackOther, Pc.removingOther]
  · simp only [StatusInv, StatusOf, hpc] at hc; simp [StatusOf, hc]
  · simp only [LogInv, sys_simps, ite_true, batchOf_holder hl, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [LeadOf, sys_simps, ite_true, holder_unissued_eq hl hwu, holder_issuedSlot_eq hl hsl,
      hiss, hwu, hpc, Pc.holdsBatch, Pc.leading, Pc.rollingBackOther, Pc.removingOther] at hlo ⊢
    grind

theorem retry_nil_of_dR1 {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR1) : s.g.retry = [] :=
  (leadOf_of_lead hs hl).2.2.2.2.2.2.2.2.1 (Or.inr (Or.inl hpc))

theorem advanced_le {s : Sys} {c : Nat} {b : Batch} {a : Nat} (hs : Inv s)
    (hl : s.lead = some (c, b)) (ha : b.advanced = some a) : a ≤ s.g.writtenThrough :=
  ((leadOf_of_lead hs hl).2.2.2.2.1 a (by simp [ha])).1

end GroupCommit
