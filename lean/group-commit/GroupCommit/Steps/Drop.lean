import GroupCommit.Steps.Durable

/-!
# The cleanup of a dropped commit: `release_group_claim` and `cleanup_dropped_commit`
-/

namespace GroupCommit
open Sys

/-- A general frame lemma for a step of a transaction that does not hold the batch. -/
theorem frame_gen {s : Sys} {c : Nat} {r' : TxRec} {o : Option Nat} (hs : Inv s)
    (hnh : NotHolder s c) (hnd : (s.tx c).pc ≠ .dropped)
    (htk : r'.ticket = (s.tx c).ticket) (hal : r'.pc.afterLeave = (s.tx c).pc.afterLeave)
    (hpl : (s.tx c).pc = .dLeave ∧ (s.tx c).dropFrom.ticket = none ∧ (s.tx c).held = true →
      r'.pc = .dLeave ∧ r'.dropFrom.ticket = none ∧ r'.held = true)
    (hq3 : ∀ e ∈ s.queue, e.tx = c → r'.st = .live ∨ c ∈ s.g.withdrawn)
    (hi2 : s.g.issued = some c →
      r'.st = .live ∧ r'.rolledBack = false ∧ r'.appended = (s.tx c).appended)
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d)) :
    Others ((s.set c r').withLock o) c := by
  have hab : c ∉ s.g.abandoned := fun h => hnd (hs.issue.2.2.2 c h).2.1
  have hunis : ((s.set c r').withLock o).unissued = s.unissued := by
    simp only [withLock_unissued]; exact unissued_set r' hnh
  have hslot : ((s.set c r').withLock o).issuedSlot = s.issuedSlot := by
    simp only [withLock_issuedSlot]; exact issuedSlot_set r' hnh
  have hqueue : ((s.set c r').withLock o).queue = s.queue := by
    simp only [withLock_queue]; exact queue_set r' hnh
  have htx : ∀ d, d ≠ c → ((s.set c r').withLock o).tx d = s.tx d := by intro d hd; simp [hd]
  have htxc : ((s.set c r').withLock o).tx c = r' := by simp
  have hg : ((s.set c r').withLock o).g = s.g := rfl
  have hl : ((s.set c r').withLock o).lead = s.lead := rfl
  have hlog' : ((s.set c r').withLock o).log = s.log := rfl
  have hsync : ((s.set c r').withLock o).synced = s.synced := rfl
  have hlk : ((s.set c r').withLock o).lockHolder = o := rfl
  have hinlog : ∀ d, ((s.set c r').withLock o).InLog d ↔ s.InLog d := fun _ => Iff.rfl
  have hinsync : ∀ d, ((s.set c r').withLock o).InSynced d ↔ s.InSynced d := fun _ => Iff.rfl
  have hbatch : ∀ d, ((s.set c r').withLock o).batchOf d = s.batchOf d := fun _ => rfl
  have hi1 := hs.issue.1
  have hnt : ∀ l b, s.lead = some (l, b) → l ≠ c := hnh
  obtain ⟨hst, hlog, htk0, hgtx, hlead, hq, hiss, hmark, hord, hholes, huniq⟩ := hs
  generalize (s.set c r').withLock o = s' at *
  have hlc : ∀ l, l ∈ (s.lead.map (·.1)).toList → l ≠ c := by
    intro l h
    simp only [Option.mem_toList, Option.map_eq_some_iff] at h
    obtain ⟨⟨l', b⟩, hb, rfl⟩ := h
    exact hnt _ _ hb
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d hd; rw [StatusInv, htx d hd]; exact hst d
  · intro d hd
    have := hlog d
    simp only [LogInv, htx d hd, hg, hinlog, hinsync, hbatch, hlog', hsync] at this ⊢
    exact this
  · intro d hd
    have := htk0 d
    simp only [TicketInv, htx d hd, hqueue, hg, hinlog, hlog'] at this ⊢
    exact this
  · intro d hd
    have h := hgtx d
    simp only [GroupTxInv, htx d hd, hg, hl, hlk] at h ⊢
    refine ⟨by rw [h.1]; exact (hlock d hd).symm, h.2.1, ?_, ?_⟩
    · intro h1 h2
      rcases h.2.2.1 h1 h2 with h' | ⟨l, hl', hl2⟩
      · exact Or.inl h'
      · exact Or.inr ⟨l, hl', by rw [htx l (hlc l hl')]; exact hl2⟩
    · intro h1 h2
      obtain ⟨l, hl', hl2⟩ := h.2.2.2 h1 h2
      exact ⟨l, hl', by rw [htx l (hlc l hl')]; exact hl2⟩
  · simp only [LeadInv, hl] at hlead ⊢
    split
    · rename_i hn; rw [hn] at hlead; simpa [hg] using hlead
    · rename_i l b hlb
      rw [hlb] at hlead
      have hlc' : l ≠ c := hnt l b hlb
      simp only [LeadOf, htx l hlc', hg, hunis, hslot, hinlog, hlog'] at hlead ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlead
      refine ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, ?_, ?_, ?_⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4, a5⟩ := h10 w hw
        have hwc : w ≠ c := by intro h; subst h; exact hnd a4
        rw [htx w hwc]; exact ⟨a1, a2, a3, a4, a5⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4⟩ := h11 w hw
        have hwc : w ≠ c := by intro h; subst h; exact hnd a4
        rw [htx w hwc]; exact ⟨a1, a2, a3, a4⟩
      · intro hp
        by_cases hwc : b.writing.tx = c
        · have hic : s.g.issued = some c := by
            rw [hi1, Sys.issuedSlot_eq, hlb]; simp [hp, hwc]
          rw [hwc, htxc, (hi2 hic).2.2]; rw [hwc] at h12; exact h12 hp
        · rw [htx _ hwc]; exact h12 hp
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hq
    simp only [QueueInv, hqueue, hunis, hg, hl, hinlog]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      refine ⟨a1, a2, a3, ?_, a5⟩
      by_cases hec : e.tx = c
      · rw [hec, htxc]; exact hq3 e he hec
      · rw [htx _ hec]; exact a4
    · intro e he
      by_cases hec : e.tx = c
      · rw [hec, htxc]
        rcases q4 e he with h | h
        · left; rw [htk, ← hec]; exact h
        · right; rw [hec] at h; exact hpl h
      · rw [htx _ hec]; exact q4 e he
    · intro e he
      by_cases hec : e.tx = c
      · rcases q5 e he with h | h | h
        · left; rw [hec, htxc, htk, ← hec]; exact h
        · exact Or.inr (Or.inl h)
        · exact Or.inr (Or.inr h)
      · rw [htx _ hec]; exact q5 e he
    · intro x hx
      by_cases hxc : x = c
      · subst hxc; rw [htxc, hal]; exact q8 x hx
      · rw [htx _ hxc]; exact q8 x hx
    · intro x hx
      by_cases hxc : x = c
      · subst hxc; rw [htxc, hal]; exact q9 x hx
      · rw [htx _ hxc]; exact q9 x hx
  · obtain ⟨i1, i2, i3, i4⟩ := hiss
    simp only [IssueInv, hslot, hg, hl]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      by_cases hwc : w = c
      · subst hwc; rw [htxc]; simp at hw; exact ⟨(hi2 hw).1, (hi2 hw).2.1⟩
      · rw [htx _ hwc]; exact i2 w hw
    · intro e he l hl' hel
      rcases i3 e he l hl' hel with h | h
      · left
        by_cases hec : e.tx = c
        · rw [hec, htxc, htk, ← hec]; exact h
        · rw [htx _ hec]; exact h
      · exact Or.inr h
    · intro x hx
      have hxc : x ≠ c := fun h => hab (h ▸ hx)
      rw [htx _ hxc]; exact i4 x hx
  · simpa only [MarkInv, hg, hlog', hsync] using hmark
  · simpa only [OrderInv, hqueue, hslot, hg] using hord
  · intro h hh
    obtain ⟨d, hd⟩ := hholes h (by simpa [hg] using hh)
    refine ⟨d, ?_⟩
    by_cases hdc : d = c
    · subst hdc; rw [htxc, htk]; exact hd
    · rw [htx _ hdc]; exact hd
  · intro d e t hd he
    have hd' : (s.tx d).ticket = some t := by
      by_cases hdc : d = c
      · subst hdc; rw [htxc, htk] at hd; exact hd
      · rw [htx _ hdc] at hd; exact hd
    have he' : (s.tx e).ticket = some t := by
      by_cases hec : e = c
      · subst hec; rw [htxc, htk] at he; exact he
      · rw [htx _ hec] at he; exact he
    exact huniq d e t hd' he'

end GroupCommit

namespace GroupCommit
open Sys

/-- A step of a transaction that does not hold the batch and changes only its own
record and maybe the commit lock. -/
theorem step_nh_local {s : Sys} {c : Nat} (r' : TxRec) {o : Option Nat} (hs : Inv s)
    (hnh : NotHolder s c) (hnd : (s.tx c).pc ≠ .dropped)
    (htk : r'.ticket = (s.tx c).ticket) (hal : r'.pc.afterLeave = (s.tx c).pc.afterLeave)
    (hpl : (s.tx c).pc = .dLeave ∧ (s.tx c).dropFrom.ticket = none ∧ (s.tx c).held = true →
      r'.pc = .dLeave ∧ r'.dropFrom.ticket = none ∧ r'.held = true)
    (hq3 : ∀ e ∈ s.queue, e.tx = c → r'.st = .live ∨ c ∈ s.g.withdrawn)
    (hi2 : s.g.issued = some c →
      r'.st = .live ∧ r'.rolledBack = false ∧ r'.appended = (s.tx c).appended)
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d))
    (hwork : ∀ t, r'.pc ≠ .awaitWork t)
    (hst : StatusOf r') (hlog : LogInv ((s.set c r').withLock o) c)
    (hgtx : GroupTxInv ((s.set c r').withLock o) c) :
    Inv ((s.set c r').withLock o) := by
  refine inv_of_parts (frame_gen hs hnh hnd htk hal hpl hq3 hi2 hlock)
    (by simpa [StatusInv] using hst) hlog ?_ hgtx
  have htc := hs.ticket c
  simp only [TicketInv, sys_simps, ite_true, queue_set _ hnh] at htc ⊢
  rw [htk]
  intro t ht
  obtain ⟨h1, h2, h3, h4, _⟩ := htc t ht
  exact ⟨h1, h2, h3, h4, fun h => absurd h (hwork t)⟩

theorem not_in_log_of_dC1 {s : Sys} {c : Nat} (hs : Inv s)
    (hpc : (s.tx c).pc = .dC1 ∨ (s.tx c).pc = .dRbA) (hlive : (s.tx c).st = .live)
    (happ : (s.tx c).appended = false) : ¬ s.InLog c := by
  intro hin
  have hab : c ∉ s.g.abandoned := fun ha => by
    have := (hs.issue.2.2.2 c ha).2.1; rcases hpc with h | h <;> rw [h] at this <;> cases this
  have hal : (s.tx c).pc.afterLeave = true := by rcases hpc with h | h <;> simp [h, Pc.afterLeave]
  rcases (hs.log c).2.1 hin hlive with h | h | h
  · rw [happ] at h; cases h
  · exact ((hs.groupTx c).2.1 hal hab).1 h
  · rcases hpc with h' | h' <;> rw [h'] at h <;> cases h.1

/-- A commit that left the group is in the queue only as a withdrawn record. -/
theorem queued_leaver_withdrawn {s : Sys} {c : Nat} (hs : Inv s)
    (hal : (s.tx c).pc.afterLeave = true) (hab : c ∉ s.g.abandoned) :
    ∀ e ∈ s.queue, e.tx = c → c ∈ s.g.withdrawn := by
  intro e he hec
  obtain ⟨hi, htk, hpend⟩ := (hs.groupTx c).2.1 hal hab
  simp only [Sys.queue, List.mem_append, Option.mem_toList] at he
  rcases he with (he | he) | he
  · exfalso
    apply hi
    rw [hs.issue.1]
    cases hl : s.lead with
    | none => simp [Sys.issuedEntry_eq, hl] at he
    | some p =>
      obtain ⟨l, b⟩ := p
      simp only [Sys.issuedEntry_eq, hl] at he
      simp only [Sys.issuedSlot_eq, hl]
      split at he
      · rename_i h; simp [h]; simp at he; rw [he, hec]
      · simp at he
  · rcases hs.queue.2.2.2.2.2.1 e he with h | h
    · exact absurd (hec ▸ h) htk
    · exact hec ▸ h
  · exact absurd hec (hpend e he)

end GroupCommit

namespace GroupCommit
open Sys

theorem q3_same {s : Sys} {c : Nat} (hs : Inv s) :
    ∀ e ∈ s.queue, e.tx = c → (s.tx c).st = .live ∨ c ∈ s.g.withdrawn := by
  intro e he hec
  have := (hs.queue.2.2.1 e he).2.2.2.1
  rw [hec] at this; exact this

theorem i2_same {s : Sys} {c : Nat} (hs : Inv s) (h : s.g.issued = some c) :
    (s.tx c).st = .live ∧ (s.tx c).rolledBack = false := hs.issue.2.1 c (by simp [h])

theorem not_issued_of_leaver {s : Sys} {c : Nat} (hs : Inv s)
    (hal : (s.tx c).pc.afterLeave = true) (hnd : (s.tx c).pc ≠ .dropped) : s.g.issued ≠ some c :=
  ((hs.groupTx c).2.1 hal (not_abandoned_of_pc hs hnd)).1

/-- `GroupTxInv` for a commit that already left the group and moves on. -/
theorem groupTx_leaver {s : Sys} {c : Nat} {r' : TxRec} {o : Option Nat} (hs : Inv s)
    (hal : (s.tx c).pc.afterLeave = true) (hlk : r'.holdsLock = true ↔ o = some c)
    (hdrop : r'.pc = .dropped → r'.st ≠ .live ∧ r'.st ≠ .aborted) :
    GroupTxInv ((s.set c r').withLock o) c := by
  have h2 := (hs.groupTx c).2.1 hal
  simp only [GroupTxInv, sys_simps, ite_true]
  refine ⟨hlk, fun _ => h2, ?_, ?_⟩
  · intro h1 h3; exact absurd h3 (hdrop h1).1
  · intro h1 h3; exact absurd h3 (hdrop h1).2

/-- `release_group_claim` of a commit without a batch: `dR1`. -/
theorem step_dR1_nobatch {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .dR1) : Inv (s.set c { s.tx c with pc := .dR2 }) := by
  have hc := hs.status c
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have := step_nh_local (o := s.lockHolder) { s.tx c with pc := .dR2 } hs
    (notHolder_of_batchOf hnb) (by simp [hpc])
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc]) (fun e he hec => by simpa using q3_same hs e he hec)
    (fun h => by simp [i2_same hs h]) (fun _ _ => Iff.rfl) (by simp)
    (by simp only [StatusInv, StatusOf, hpc] at hc; simp [StatusOf, hc]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [GroupTxInv, sys_simps, ite_true, TxRec.holdsLock, hpc] at hgc ⊢
    simp [hgc.1, Pc.afterLeave]

/-- `release_group_claim` of a commit without a batch: `dR2` goes to `cleanup_dropped_commit`. -/
theorem step_dR2_nobatch {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .dR2) : Inv (s.set c { s.tx c with pc := .dLeave }) := by
  have hc := hs.status c
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have := step_nh_local (o := s.lockHolder) { s.tx c with pc := .dLeave } hs
    (notHolder_of_batchOf hnb) (by simp [hpc])
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc]) (fun e he hec => by simpa using q3_same hs e he hec)
    (fun h => by simp [i2_same hs h]) (fun _ _ => Iff.rfl) (by simp)
    (by simp only [StatusInv, StatusOf, hpc] at hc; simp [StatusOf, hc]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [GroupTxInv, sys_simps, ite_true, TxRec.holdsLock, hpc] at hgc ⊢
    simp [hgc.1, Pc.afterLeave]

/-- `cleanup_dropped_commit`: the record was appended, so the transaction commits. -/
theorem step_dC1_commit {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dC1)
    (hlive : (s.tx c).st = .live) (happ : (s.tx c).appended = true) :
    Inv (s.set c
      { s.tx c with submitted := false, st := .committed, wasCommitted := true, pc := .dCommitted }) := by
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have hin : s.InLog c := hlc.1 happ
  have hni := not_issued_of_leaver (c := c) hs (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hrb := rolledBack_false_of_live hs hlive
  have := step_nh_local (o := s.lockHolder)
    { s.tx c with submitted := false, st := .committed, wasCommitted := true, pc := .dCommitted }
    hs hnh (by simp [hpc])
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
    (fun e he hec => absurd (hec ▸ hin) (hs.queue.2.2.1 e he).2.2.1)
    (fun h => absurd h hni) (fun _ _ => Iff.rfl) (by simp) (by simp [StatusOf]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    simp only [InLog, InSynced] at hin hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    rw [← hgc.1]; simp [TxRec.holdsLock, hpc]

/-- `cleanup_dropped_commit`: the record was not appended, so the transaction rolls back. -/
theorem step_dC1_rollback {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dC1)
    (hlive : (s.tx c).st = .live) (happ : (s.tx c).appended = false) :
    Inv (s.set c { s.tx c with submitted := false, pc := .dRbA }) := by
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have := step_nh_local (o := s.lockHolder) { s.tx c with submitted := false, pc := .dRbA } hs hnh
    (by simp [hpc])
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc]) (fun e he hec => by simpa using q3_same hs e he hec)
    (fun h => by simp [i2_same hs h]) (fun _ _ => Iff.rfl) (by simp)
    (by simp [StatusOf, hlive, happ]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    rw [← hgc.1]; simp [TxRec.holdsLock, hpc]

/-- `cleanup_dropped_commit` of a transaction that committed before the drop. -/
theorem step_dC1_committed {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dC1)
    (hst : (s.tx c).st = .committed) :
    Inv (s.set c { s.tx c with submitted := false, pc := .dCommitted }) := by
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have := step_nh_local (o := s.lockHolder) { s.tx c with submitted := false, pc := .dCommitted } hs
    hnh (by simp [hpc])
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc]) (fun e he hec => by simpa using q3_same hs e he hec)
    (fun h => by simp [i2_same hs h]) (fun _ _ => Iff.rfl) (by simp)
    (by simp [StatusOf, hst]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    rw [← hgc.1]; simp [TxRec.holdsLock, hpc]

theorem lock_other {s : Sys} {c : Nat} (hs : Inv s) (hh : (s.tx c).held = true)
    (hpc : ∀ t, (s.tx c).pc ≠ .awaitLocked t ∧ (s.tx c).pc ≠ .awaitWork t) :
    ∀ d, d ≠ c → ((none : Option Nat) = some d ↔ s.lockHolder = some d) := by
  intro d hd
  have hhc : (s.tx c).holdsLock = true := by
    simp only [TxRec.holdsLock]
    split
    · rename_i t h; exact absurd h (hpc t).1
    · rename_i t h; exact absurd h (hpc t).2
    · exact hh
  rw [(hs.groupTx c).1.1 hhc]
  simp [Ne.symm hd]

/-- The rollback of a dropped commit: `rollback_tx_inner`, which also releases the
commit lock. The new state is given by its lock. -/
theorem step_dRbA {s : Sys} {c : Nat} {o : Option Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dRbA)
    (ho : o = if (s.tx c).held then none else s.lockHolder) :
    Inv ((s.set c
      { s.tx c with st := .aborted, rolledBack := true, held := false, pc := .dRbB }).withLock o) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have hnd : (s.tx c).pc ≠ .dropped := by simp [hpc]
  have hnotin := not_in_log_of_dC1 hs (Or.inr hpc) hc.1 hc.2
  have hni := not_issued_of_leaver hs (by simp [hpc, Pc.afterLeave]) hnd
  have hwc : (s.tx c).wasCommitted = false := by
    cases h : (s.tx c).wasCommitted
    · rfl
    · have := hlc.2.2.2.2.2.2.2.1 h; rw [hc.1] at this; simp at this
  have hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d) := by
    intro d hd
    rw [ho]
    split
    · rename_i hh; exact lock_other hs hh (fun t => by simp [hpc]) d hd
    · exact Iff.rfl
  have hlkc : o ≠ some c := by
    rw [ho]; split
    · simp
    · rename_i hh
      intro h
      have := (hgc.1).2 h
      simp [TxRec.holdsLock, hpc, hh] at this
  refine step_nh_local _ hs hnh hnd
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
    (fun e he hec => Or.inr (queued_leaver_withdrawn hs (by simp [hpc, Pc.afterLeave])
      (not_abandoned_of_pc hs hnd) e he hec))
    (fun h => absurd h hni) hlock (by simp) (by simp [StatusOf]) ?_ ?_
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    simp only [InLog] at hnotin hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    simp [TxRec.holdsLock, hlkc]

/-- The rollback ends: the transaction leaves `txs`. -/
theorem step_dRbB {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dRbB) :
    Inv (s.set c { s.tx c with st := .removed, pc := .dropped }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have hnd : (s.tx c).pc ≠ .dropped := by simp [hpc]
  have hnotin : ¬ s.InLog c := fun h => by
    have := (hlc.2.2.1 h).2; rw [hc.2.2] at this; cases this
  have hni := not_issued_of_leaver hs (by simp [hpc, Pc.afterLeave]) hnd
  have hwc : (s.tx c).wasCommitted = false := by
    cases h : (s.tx c).wasCommitted
    · rfl
    · have := hlc.2.2.2.2.2.2.2.1 h; rw [hc.1] at this; simp at this
  have := step_nh_local (o := s.lockHolder) { s.tx c with st := .removed, pc := .dropped } hs hnh hnd
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
    (fun e he hec => Or.inr (queued_leaver_withdrawn hs (by simp [hpc, Pc.afterLeave])
      (not_abandoned_of_pc hs hnd) e he hec))
    (fun h => absurd h hni) (fun _ _ => Iff.rfl) (by simp) (by simp [StatusOf, hc]) ?_ ?_
  · exact this
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    simp only [InLog] at hnotin hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    rw [← hgc.1]; simp [TxRec.holdsLock, hpc]

/-- `cleanup_dropped_commit` of a committed transaction: notify the dependents, release
the commit lock and leave `txs`. The new state is given by its lock. -/
theorem step_dCommitted {s : Sys} {c : Nat} {o : Option Nat} (hs : Inv s)
    (hpc : (s.tx c).pc = .dCommitted)
    (ho : o = if (s.tx c).held then none else s.lockHolder) :
    Inv ((s.set c
      { s.tx c with notified := true, held := false, st := .removed, pc := .dropped }).withLock o) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlc := hs.log c
  have hgc := hs.groupTx c
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have hnd : (s.tx c).pc ≠ .dropped := by simp [hpc]
  have hni := not_issued_of_leaver hs (by simp [hpc, Pc.afterLeave]) hnd
  have hrb : (s.tx c).rolledBack = false := by
    cases h : (s.tx c).rolledBack
    · rfl
    · have := (hlc.2.2.2.2.2.2.1 h).1; rw [hc] at this; simp at this
  have hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d) := by
    intro d hd
    rw [ho]
    split
    · rename_i hh; exact lock_other hs hh (fun t => by simp [hpc]) d hd
    · exact Iff.rfl
  have hlkc : o ≠ some c := by
    rw [ho]; split
    · simp
    · rename_i hh
      intro h
      have := (hgc.1).2 h
      simp [TxRec.holdsLock, hpc, hh] at this
  refine step_nh_local _ hs hnh hnd
    (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]) (by simp [hpc, Pc.afterLeave])
    (by simp [hpc])
    (fun e he hec => Or.inr (queued_leaver_withdrawn hs (by simp [hpc, Pc.afterLeave])
      (not_abandoned_of_pc hs hnd) e he hec))
    (fun h => absurd h hni) hlock (by simp) (by simp [StatusOf]) ?_ ?_
  · simp only [LogInv, sys_simps, ite_true, hpc] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · refine groupTx_leaver hs (by simp [hpc, Pc.afterLeave]) ?_ (by simp)
    simp [TxRec.holdsLock, hlkc]

end GroupCommit
