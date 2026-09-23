import GroupCommit.Steps.Enqueue

/-!
# `take_work` gives a batch to the waiter that holds the commit lock
-/

namespace GroupCommit
open Sys

/-- The holder of the commit lock at `awaitWork` has no batch, so nobody has one. -/
theorem lead_none_of_awaitWork {s : Sys} {c t : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .awaitWork t) :
    s.lead = none := by
  cases hl : s.lead with
  | none => rfl
  | some p =>
    obtain ⟨l, b⟩ := p
    exfalso
    have hlo := hs.lead
    simp only [LeadInv, hl, LeadOf] at hlo
    have hheld := hlo.1
    have hlk : s.lockHolder = some l := (hs.groupTx l).1.1 (by simp [TxRec.holdsLock, hheld]; split <;> simp_all)
    have hck : s.lockHolder = some c := (hs.groupTx c).1.1 (by simp [TxRec.holdsLock, hpc])
    rw [hlk] at hck
    cases hck
    have := hlo.2.1
    rw [hpc] at this
    simp [Pc.holdsBatch, Pc.leading] at this

theorem ticket_some_not_afterLeave {r : TxRec} {t : Nat} (h : r.ticket = some t) :
    r.pc.afterLeave = false := by
  simp only [TxRec.ticket] at h
  cases hp : r.pc <;> simp_all [Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave]

theorem step_take_lead_aux {s : Sys} {c t : Nat} (r' : TxRec) (hs : Inv s)
    (hpc : (s.tx c).pc = .awaitWork t) (hretry : s.g.retry = []) {e : Entry} {rest : List Entry}
    (hpend : s.g.pending = e :: rest)
    (hr' : r' = { s.tx c with pc := .upgrade, held := true }) :
    Inv (((s.withLead (some (c, ⟨e, rest, none⟩))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc] at hc
  have hln := lead_none_of_awaitWork hs hpc
  have hlo := hs.lead
  simp only [LeadInv, hln] at hlo
  obtain ⟨htk0, hwd0⟩ := hlo
  obtain ⟨i1, i2, i3, i4⟩ := hs.issue
  have hslot0 : s.issuedSlot = none := by simp [Sys.issuedSlot_eq, hln]
  have hent0 : s.issuedEntry = none := by simp [Sys.issuedEntry_eq, hln]
  have huni0 : s.unissued = [] := by simp [Sys.unissued_eq, hln]
  have hiss0 : s.g.issued = none := by rw [i1, hslot0]; rfl
  have hab0 : s.g.abandoned = [] := by
    cases h : s.g.abandoned with
    | nil => rfl
    | cons x xs => have := (i4 x (by simp [h])).1; rw [hiss0] at this; cases this
  have hq0 : s.queue = e :: rest := by simp [Sys.queue, hent0, huni0, hpend]
  have hck : s.lockHolder = some c := (hs.groupTx c).1.1 (by simp [TxRec.holdsLock, hpc])
  have hr'pc : r'.pc = .upgrade := by rw [hr']
  have hr'held : r'.held = true := by rw [hr']
  have hr'st : r'.st = .live := by rw [hr']; exact hc.1
  have hr'sub : r'.submitted = false := by rw [hr']; exact hc.2.2.2
  generalize hb : (⟨e, rest, none⟩ : Batch) = b0
  have hbw : b0.writing = e := by rw [← hb]
  have hbr : b0.rest = rest := by rw [← hb]
  have hba : b0.advanced = none := by rw [← hb]
  have htx : ∀ d, (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).tx d =
      if d = c then r' else s.tx d := by
    intro d; simp
  have hg : (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).g =
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) } := by simp
  have hlead : (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).lead =
      some (c, b0) := by simp
  have hlog : (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).log = s.log := by simp
  have hsync : (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).synced =
      s.synced := by simp
  have hlock : (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }).lockHolder =
      s.lockHolder := by simp
  generalize (((s.withLead (some (c, b0))).set c r').withG
      { s.g with pending := [], taken := s.g.taken ++ (e :: rest).map (·.tx) }) = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have huni : s'.unissued = e :: rest := by
    simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hr'pc, hbw, hbr]
  have hent : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hslot : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hqueue : s'.queue = s.queue := by
    rw [hq0]; simp [Sys.queue, hent, huni, hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = if d = c then some b0 else none := by
    intro d
    simp only [Sys.batchOf_eq, hlead]
    by_cases hd : d = c
    · subst hd; simp
    · simp [hd, Ne.symm hd]
  have hbatch0 : ∀ d, s.batchOf d = none := by intro d; simp [Sys.batchOf_eq, hln]
  have hholds : ∀ d, (s.tx d).holdsLock = true ↔ s.lockHolder = some d := fun d => (hs.groupTx d).1
  have hpendown : ∀ x ∈ s.g.pending, (s.tx x.tx).ticket = some x.ticket := by
    intro x hx
    rcases hs.queue.2.2.2.1 x hx with h | ⟨h1, _, h3⟩
    · exact h
    · exfalso
      have := (hholds x.tx).1 (by simp [TxRec.holdsLock, h1, h3])
      rw [hck] at this
      cases this
      rw [hpc] at h1; cases h1
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; simp [StatusInv, htxc, StatusOf, hr'pc, hr'st, hr'held, hr'sub]
    · simpa [StatusInv, htxd d hd] using hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync, ite_true] at hld ⊢
      rw [hbatch0] at hld
      simp only [hpc, hr'pc] at hld ⊢
      rw [hr'] at ⊢
      simp only [hiss0] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hlog, hsync, hd, ite_false] at hld ⊢
      rw [hbatch0] at hld
      simpa using hld
  · intro d
    have htd := hs.ticket d
    by_cases hd : d = c
    · subst hd; simp [TicketInv, htxc, TxRec.ticket, hr'pc, Pc.waitTicket, Pc.beforeLeave]
    · simp only [TicketInv, htxd d hd, hqueue, hg, hlog, hinlog] at htd ⊢
      exact htd
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock, hg] at hgd ⊢
      simp [TxRec.holdsLock, hr'pc, hr'held, hck, Pc.afterLeave]
    · simp only [GroupTxInv, htxd d hd, hlock, hg, hlead] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        refine ⟨a1, ?_, by simp⟩
        simp only [htk0, List.nil_append, List.mem_map, not_exists, not_and]
        intro x hx hxd
        exact a3 x (by rw [hpend]; exact hx) hxd
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl, _⟩
        · exact Or.inl h
        · simp [hln] at hl
      · intro h1 h2
        obtain ⟨l, hl, _⟩ := hgd.2.2.2 h1 h2
        simp [hln] at hl
  · simp only [LeadInv, hlead, LeadOf, htxc, hr'pc, hr'held, hg, hba, huni, hslot, hretry]
    have hcloc : s.InLog c ∨ ⟨t, c⟩ ∈ s.g.pending := by
      have htc := hs.ticket c
      simp only [TicketInv, TxRec.ticket, hpc, Pc.waitTicket] at htc
      obtain ⟨_, _, h3, _, _⟩ := htc t (by simp)
      rcases h3 with h | h | h
      · exact Or.inl ⟨_, h, rfl⟩
      · rw [hq0] at h; rw [hpend]; exact Or.inr h
      · rw [hretry] at h; cases h
    refine ⟨trivial, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, by simp, by simp, by simp,
      ?_, by simp, by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
    intro _ _ _
    rcases hcloc with h | h
    · exact Or.inl ((hinlog c).2 h)
    · right; left
      rw [hpend] at h
      exact ⟨_, h, rfl⟩
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, huni, hg, hlead, hinlog, hwd0, htk0, List.nil_append]
    refine ⟨q1, q2, ?_, by simp, ?_, ?_, ?_, ?_, by simp⟩
    · intro x hx
      obtain ⟨h1, h2, h3, h4, h5⟩ := q3 x hx
      refine ⟨h1, h2, h3, ?_, h5⟩
      by_cases hxc : x.tx = c
      · simp [htx, hxc, hr'st]
      · simpa [htxd _ hxc, hwd0] using h4
    · intro x hx
      rw [← hpend] at hx
      by_cases hxc : x.tx = c
      · right; right; simp [hxc]
      · left; rw [htxd _ hxc]; exact hpendown x hx
    · intro x hx; left
      simp only [List.mem_map]; exact ⟨x, hx, rfl⟩
    · intro x hx
      simp only [List.mem_append, List.not_mem_nil, or_false] at hx
      exact hx
    · intro x hx
      refine ⟨by simp, ?_⟩
      simp only [List.mem_map] at hx
      obtain ⟨y, hy, rfl⟩ := hx
      by_cases hyc : y.tx = c
      · simp [htx, hyc, hr'pc, Pc.afterLeave]
      · rw [htxd _ hyc]
        rw [← hpend] at hy
        exact ticket_some_not_afterLeave (hpendown y hy)
  · simp only [IssueInv, hslot, hg, hlead, hiss0, hab0]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hqueue, hslot, hg]
    exact ⟨o1, by simp, o3⟩
  · intro h hh
    simp [hg, hretry] at hh
  · intro d e u hd he
    have hd' : (s.tx d).ticket = some u := by
      by_cases hdc : d = c
      · subst hdc; simp [htxc, TxRec.ticket, hr'pc, Pc.waitTicket, Pc.beforeLeave] at hd
      · rwa [htxd d hdc] at hd
    have he' : (s.tx e).ticket = some u := by
      by_cases hec : e = c
      · subst hec; simp [htxc, TxRec.ticket, hr'pc, Pc.waitTicket, Pc.beforeLeave] at he
      · rwa [htxd e hec] at he
    exact hs.unique d e u hd' he'

end GroupCommit
