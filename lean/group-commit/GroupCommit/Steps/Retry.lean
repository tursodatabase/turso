import GroupCommit.Steps.TakeLead

/-!
# `take_retry` finds the ticket: the waiter rebuilds its record
-/

namespace GroupCommit
open Sys

theorem step_take_retry_aux {s : Sys} {c t : Nat} {o : Option Nat} (r' : TxRec) (hs : Inv s)
    (hw : (s.tx c).pc.waitTicket = some t)
    (hnsp : (s.tx c).pc ≠ .syncPrefix t ∧ (s.tx c).pc ≠ .prefixSynced t)
    (hhit : t ∈ s.g.retry)
    (hr' : r' = { s.tx c with pc := .begin })
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d))
    (hlockc : o ≠ some c) :
    Inv (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o) := by
  have fw := waitFacts hw
  obtain ⟨hst, hex⟩ := status_wait hs hw
  have hc := status_of (c := c) hs
  have hheld : (s.tx c).held = false := by
    cases hp : (s.tx c).pc <;> simp_all [StatusOf, Pc.waitTicket]
  have hsub : (s.tx c).submitted = false := by
    cases hp : (s.tx c).pc <;> simp_all [StatusOf, Pc.waitTicket]
  have hnh := notHolder_of_wait hs hw
  have hct : (s.tx c).ticket = some t := TxRec.ticket_of_wait hw
  have htc := hs.ticket c
  simp only [TicketInv, hct] at htc
  obtain ⟨t1, t2, t3, t4, t5⟩ := htc t (by simp)
  have hnotlog : ¬ s.InLog c := t4 hhit
  have hnq : ∀ e ∈ s.queue, e.tx ≠ c := by
    intro e he hec
    obtain ⟨_, _, _, _, h5⟩ := hs.queue.2.2.1 e he
    have hown : (s.tx c).ticket = some e.ticket ∨ ((s.tx c).pc = .dLeave) ∨
        e.tx ∈ s.g.withdrawn ∨ s.lead.map (·.1) = some e.tx ∨ e ∈ s.issuedEntry.toList := by
      simp only [Sys.queue, List.mem_append, Option.mem_toList] at he
      rcases he with (he | he) | he
      · exact Or.inr (Or.inr (Or.inr (Or.inr (by simp [he]))))
      · rcases hs.queue.2.2.2.2.1 e he with h | h | h
        · exact Or.inl (hec ▸ h)
        · exact Or.inr (Or.inr (Or.inl h))
        · exact Or.inr (Or.inr (Or.inr (Or.inl h)))
      · rcases hs.queue.2.2.2.1 e he with h | h
        · exact Or.inl (hec ▸ h)
        · exact Or.inr (Or.inl (hec ▸ h.1))
    rcases hown with h | h | h | h | h
    · rw [hct] at h; cases h; exact h5 hhit
    · exact fw.ne_dLeave h
    · have := (hs.queue.2.2.2.2.2.2.2.2 e.tx h).1
      rw [hec, fw.afterLeave] at this; cases this
    · rw [hec] at h
      simp only [Option.map_eq_some_iff] at h
      obtain ⟨⟨l, b⟩, hlb, hlc⟩ := h
      exact absurd hlc (hnh l b hlb)
    · cases hl : s.lead with
      | none => simp [Sys.issuedEntry_eq, hl] at h
      | some p =>
        obtain ⟨l, b⟩ := p
        have hlc : l ≠ c := hnh l b hl
        have hslot : e ∈ s.issuedSlot.toList := by
          simp only [Sys.issuedEntry_eq, hl, Option.mem_toList] at h
          simp only [Sys.issuedSlot_eq, hl, Option.mem_toList]
          split at h
          · rename_i hi; simp [hi] at h ⊢; exact h
          · simp at h
        rcases hs.issue.2.2.1 e hslot l (by simp [hl]) (by rw [hec]; exact Ne.symm hlc) with h' | h'
        · rw [hec, hct] at h'; cases h'; exact h5 hhit
        · rw [hec] at h'
          exact fw.ne_dropped (hs.issue.2.2.2 c h').2.1
  have hnotiss : s.g.issued ≠ some c := by
    intro hi
    rw [hs.issue.1] at hi
    cases hl : s.lead with
    | none => simp [Sys.issuedSlot_eq, hl] at hi
    | some p =>
      obtain ⟨l, b⟩ := p
      have hlc : l ≠ c := hnh l b hl
      have hlo := hs.lead
      simp only [LeadInv, hl, LeadOf] at hlo
      simp only [Sys.issuedSlot_eq, hl] at hi
      split at hi
      · rename_i hcond
        simp at hi
        have hlead : (s.tx l).pc.leading ∨ (s.tx l).pc = .dR1 ∨ (s.tx l).pc = .dR2 := by
          simp only [Bool.or_eq_true, beq_iff_eq] at hcond
          rcases hcond with (h | h) | h
          · cases hp : (s.tx l).pc <;> simp_all [TxRec.issuing, Pc.leading]
          · left; rw [h]; rfl
          · left; rw [h]; rfl
        have := hlo.2.2.2.2.2.2.2.2.1 hlead
        rw [this] at hhit; cases hhit
      · simp at hi
  have hab : c ∉ s.g.abandoned := not_abandoned_of_pc hs fw.ne_dropped
  have htk : c ∉ s.g.taken := by
    intro h
    have := hs.queue.2.2.2.2.2.2.1 c (by simp [h])
    simp only [List.mem_map] at this
    obtain ⟨e, he, hec⟩ := this
    exact hnq e (mem_unissued_queue he) hec
  have huniq : ∀ d, (s.tx d).ticket = some t → d = c := fun d hd => hs.unique d c t hd hct
  have hr'pc : r'.pc = .begin := by rw [hr']
  have hr'held : r'.held = false := by rw [hr']; exact hheld
  have hr'st : r'.st = .live := by rw [hr']; exact hst
  have hr'excl : r'.excl = false := by rw [hr']; exact hex
  have hr'sub : r'.submitted = false := by rw [hr']; exact hsub
  have hr'tk : r'.ticket = none := by simp [TxRec.ticket, hr'pc, Pc.waitTicket, Pc.beforeLeave]
  have htx : ∀ d, (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).tx d =
      if d = c then r' else s.tx d := by intro d; simp
  have hg : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).g =
      { s.g with retry := setErase t s.g.retry } := by simp
  have hlead : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).lead =
      s.lead := by simp
  have hlog : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).log =
      s.log := by simp
  have hsync : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).synced =
      s.synced := by simp
  have hlock' : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).lockHolder =
      o := by simp
  have huni : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).unissued =
      s.unissued := by simp [unissued_set _ hnh]
  have hent : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).issuedEntry =
      s.issuedEntry := by simp [issuedEntry_set _ hnh]
  have hslot : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).issuedSlot =
      s.issuedSlot := by simp [issuedSlot_set _ hnh]
  have hqueue : (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).queue =
      s.queue := by simp [Sys.queue, issuedEntry_set _ hnh, unissued_set _ hnh]
  have hbatch : ∀ d, (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o).batchOf d =
      s.batchOf d := by intro d; simp
  generalize (((s.set c r').withG { s.g with retry := setErase t s.g.retry }).withLock o) = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by intro d; simp [Sys.InSynced, hlog, hsync]
  have hmem : ∀ x, x ∈ s'.g.retry ↔ x ≠ t ∧ x ∈ s.g.retry := by intro x; simp [hg]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; simp [StatusInv, htxc, StatusOf, hr'pc, hr'st, hr'held, hr'excl, hr'sub]
    · simpa [StatusInv, htxd d hd] using hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync] at hld ⊢
      rw [hr'] at ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hlog, hsync] at hld ⊢
      exact hld
  · intro d
    have htd := hs.ticket d
    by_cases hd : d = c
    · subst hd; simp [TicketInv, htxc, hr'tk]
    · simp only [TicketInv, htxd d hd, hqueue, hg, hlog, hinlog] at htd ⊢
      intro u hu
      obtain ⟨h1, h2, h3, h4, h5⟩ := htd u hu
      have hdu : (s.tx d).ticket = some u := by simpa using hu
      have hut : u ≠ t := by intro h; subst h; exact hd (huniq d hdu)
      refine ⟨h1, h2, ?_, ?_, ?_⟩
      · rcases h3 with h | h | h
        · exact Or.inl h
        · exact Or.inr (Or.inl h)
        · exact Or.inr (Or.inr (by simp [hut, h]))
      · intro h; exact h4 (by simp at h; exact h.2)
      · intro h; have := h5 h; simp [this]
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock', hg, hlead]
      simp [TxRec.holdsLock, hr'pc, hr'held, hlockc, Pc.afterLeave]
    · simp only [GroupTxInv, htxd d hd, hlock', hg, hlead] at hgd ⊢
      have hl2 : ∀ l ∈ (s.lead.map (·.1)).toList, s'.tx l = s.tx l :=
        fun l hl => htxd l (lead_ne_of_notHolder hnh hl)
      refine ⟨?_, hgd.2.1, ?_, ?_⟩
      · rw [hgd.1]; exact (hlock d hd).symm
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl, h3⟩
        · exact Or.inl h
        · exact Or.inr ⟨l, hl, by rw [hl2 l hl]; exact h3⟩
      · intro h1 h2
        obtain ⟨l, hl, h3⟩ := hgd.2.2.2 h1 h2
        exact ⟨l, hl, by rw [hl2 l hl]; exact h3⟩
  · have hlo := hs.lead
    simp only [LeadInv, hlead] at hlo ⊢
    split
    · rename_i hl; rw [hl] at hlo; simpa [hg] using hlo
    · rename_i l b hl
      rw [hl] at hlo
      have hlc : l ≠ c := hnh l b hl
      simp only [LeadOf, htxd l hlc, hg, hinlog, huni, hslot, hlog] at hlo ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
      refine ⟨h1, h2, h3, h4, h5, h6, h7, h8, ?_, ?_, ?_, ?_⟩
      · intro hp; have := h9 hp; rw [this] at hhit; cases hhit
      · intro w hw
        obtain ⟨a1, a2, a3, a4, a5⟩ := h10 w hw
        have hwc : w ≠ c := by intro h; subst h; exact fw.ne_dropped a4
        rw [htxd w hwc]; exact ⟨a1, a2, a3, a4, a5⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4⟩ := h11 w hw
        have hwc : w ≠ c := by intro h; subst h; exact fw.ne_dropped a4
        rw [htxd w hwc]; exact ⟨a1, a2, a3, a4⟩
      · intro hp
        rw [htxd _ (writing_ne_of_own3 hs hl hp hnotiss)]; exact h12 hp
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    have hpend : ∀ e ∈ s.g.pending, e.tx ≠ c := fun e he => hnq e (mem_pending_queue he)
    have hun : ∀ e ∈ s.unissued, e.tx ≠ c := fun e he => hnq e (mem_unissued_queue he)
    simp only [QueueInv, hqueue, huni, hg, hlead, hinlog]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨h1, h2, h3, h4, h5⟩ := q3 e he
      refine ⟨h1, h2, h3, by rw [htxd _ (hnq e he)]; exact h4, by simp [h5]⟩
    · intro e he; rw [htxd _ (hpend e he)]; exact q4 e he
    · intro e he; rw [htxd _ (hun e he)]; exact q5 e he
    · intro x hx
      have hxc : x ≠ c := fun h => htk (h ▸ hx)
      rw [htxd _ hxc]; exact q8 x hx
    · intro x hx
      have hxc : x ≠ c := by
        intro h; subst h; have := (q9 x hx).1; rw [fw.afterLeave] at this; cases this
      rw [htxd _ hxc]; exact q9 x hx
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot, hg, hlead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hwi
      have hwc : w ≠ c := by intro h; rw [h] at hwi; apply hnotiss; simpa using hwi
      rw [htxd w hwc]; exact i2 w hwi
    · intro e he l hl hel
      rcases i3 e he l hl hel with h | h
      · left
        have hec : e.tx ≠ c := by
          intro hec'
          apply hnotiss
          rw [i1]; simp only [Option.mem_toList] at he; rw [he]; simp [hec']
        rw [htxd _ hec]; exact h
      · exact Or.inr h
    · intro x hx
      have hxc : x ≠ c := fun h => hab (h ▸ hx)
      rw [htxd x hxc]; exact i4 x hx
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hsync, hlog]
    refine ⟨fun h hh => m1 h (by simp at hh; exact hh.2), m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hqueue, hslot, hg]
    exact ⟨o1, o2, o3⟩
  · intro h hh
    rw [hmem] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh.2
    have hdc : d ≠ c := by intro h'; subst h'; rw [hct] at hd; cases hd; exact hh.1 rfl
    exact ⟨d, by rw [htxd d hdc]; exact hd⟩
  · intro d e u hd he
    have hd' : (s.tx d).ticket = some u := by
      by_cases hdc : d = c
      · subst hdc; rw [htxc, hr'tk] at hd; cases hd
      · rwa [htxd d hdc] at hd
    have he' : (s.tx e).ticket = some u := by
      by_cases hec : e = c
      · subst hec; rw [htxc, hr'tk] at he; cases he
      · rwa [htxd e hec] at he
    exact hs.unique d e u hd' he'

end GroupCommit
