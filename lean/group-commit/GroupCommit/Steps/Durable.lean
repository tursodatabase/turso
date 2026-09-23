import GroupCommit.Steps.Own

/-!
# `mark_durable`: `EndCommitLogicalLog` of the leader and `GroupPrefixSynced`
-/

namespace GroupCommit
open Sys

theorem markDurable_noHoles {g : Group} (hr : g.retry = []) (hle : g.durableThrough ≤ g.writtenThrough) :
    g.markDurable g.writtenThrough = { g with durableThrough := g.writtenThrough } := by
  simp only [Group.markDurable, Group.densePrefixCap, hr, List.filter_nil, List.min?_nil,
    Nat.min_self, Nat.max_eq_right hle]

/-- `EndCommitLogicalLog` of the leader: the whole written prefix is durable, and the
batch is given back. The new state is given by its parts. -/
theorem step_end_commit_batch_gen {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .endCommit) (hf : (s.tx c).failed = false)
    {s' : Sys}
    (hg : s'.g = { s.g with durableThrough := s.g.writtenThrough }) (hlead : s'.lead = none)
    (hlog : s'.log = s.log) (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .commitEnd })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  have hrest : b.rest = [] := hlo.2.2.1 (Or.inr hpc)
  have hretry : s.g.retry = [] := hlo.2.2.2.2.2.2.2.2.1 (Or.inl (by simp [hpc, Pc.leading]))
  have hunis : s.unissued = [] := by
    simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hpc, hrest]
  have hent : s.issuedEntry = none := by simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hpc]
  have hslot : s.issuedSlot = none := by simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hpc]
  have hiss : s.g.issued = none := by rw [hs.issue.1, hslot]; rfl
  have hab := abandoned_nil hs hiss
  have hq : s.queue = s.g.pending := by simp [Sys.queue, hent, hunis]
  have htw : s.g.taken = [] ∧ s.g.withdrawn = [] := by
    have q7 := hs.queue.2.2.2.2.2.2.1
    rw [hunis] at q7
    constructor
    · apply List.eq_nil_iff_forall_not_mem.2; intro x hx; have := q7 x (by simp [hx]); simp at this
    · apply List.eq_nil_iff_forall_not_mem.2; intro x hx; have := q7 x (by simp [hx]); simp at this
  have hlc := hs.log c
  have hsyn : s.synced = s.log.length :=
    hlc.2.2.2.2.2.2.2.2.2.2.2.2.2.1 (Or.inl ⟨hpc, by simp [batchOf_holder hl]⟩) hf
  have hcsync : s.InSynced c := hlc.2.2.2.2.2.2.2.2.1 (Or.inl ⟨hpc, hf⟩)
  have htake : s.log.take s.synced = s.log := by rw [hsyn, List.take_length]
  have htx : ∀ d, s'.tx d = if d = c then { s.tx c with pc := .commitEnd } else s.tx d := by
    intro d; by_cases hd : d = c
    · subst hd; simp [htxc]
    · simp [hd, htxd d hd]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd]
  have huni : s'.unissued = [] := by simp [Sys.unissued_eq, hlead]
  have hent' : s'.issuedEntry = none := by simp [Sys.issuedEntry_eq, hlead]
  have hslot' : s'.issuedSlot = none := by simp [Sys.issuedSlot_eq, hlead]
  have hqueue : s'.queue = s.queue := by rw [hq]; simp [Sys.queue, hent', huni, hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = none := by intro d; simp [Sys.batchOf_eq, hlead]
  have hbatch0 : ∀ d, d ≠ c → s.batchOf d = none := by
    intro d hd; simp [Sys.batchOf_eq, hl, Ne.symm hd]
  have hnodrop : ∀ d, (s.tx d).pc = .dropped → (s.tx d).st = .live ∨ (s.tx d).st = .aborted →
      False := by
    intro d h1 h2
    have hg' := (hs.groupTx d).2.2
    rcases h2 with h2 | h2
    · rcases hg'.1 h1 h2 with h | ⟨l, hl', hl2⟩
      · rw [hab] at h; cases h
      · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
    · obtain ⟨l, hl', hl2⟩ := hg'.2 h1 h2
      rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync] at hld ⊢
      simp only [hpc, batchOf_holder hl, hf] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hlog, hsync, hbatch0 d hd,
        htake] at hld ⊢
      obtain ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, _, l15, l16⟩ := hld
      refine ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, fun _ _ => hsyn, ?_, l16⟩
      intro t ht
      exact ⟨(l15 t ht).1, (l15 t ht).1⟩
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hqueue, hg, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw
    by_cases hd : d = c
    · subst hd; rw [htxc] at hw; cases hw
    · rw [htxd d hd] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock, hg] at hgd ⊢
      have hold := holder_holdsLock hs hl
      rw [hold] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · simp only [GroupTxInv, htxd d hd, hlock, hg, hlead] at hgd ⊢
      refine ⟨hgd.1, hgd.2.1, fun h1 h2 => (hnodrop d h1 (Or.inl h2)).elim,
        fun h1 h2 => (hnodrop d h1 (Or.inr h2)).elim⟩
  · simp only [LeadInv, hlead, hg]
    exact htw
  · obtain ⟨q1, q2, q3, q4, _, _, _, _, _⟩ := hs.queue
    simp only [QueueInv, hqueue, huni, hg, hinlog, hlead, htw.1, htw.2]
    refine ⟨q1, q2, ?_, ?_, by simp, by simp, by simp, by simp, by simp⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      exact ⟨a1, a2, a3, by rw [obs_st (hobsd e.tx), ← htw.2]; exact a4, a5⟩
    · intro e he
      rcases q4 e he with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd e.tx)).2 h
  · simp only [IssueInv, hslot', hg, hiss, hab, hlead]
    simp
  · obtain ⟨m1, m2, _, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hsync, hlog]
    exact ⟨m1, m2, m2, m4, m5⟩
  · obtain ⟨o1, _, _⟩ := hs.order
    simp only [OrderInv, hqueue, hslot', hg]
    exact ⟨o1, by simp, Nat.le_refl _⟩
  · intro h hh
    simp only [hg, hretry] at hh
    cases hh
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

theorem markDurable_eq (g : Group) (x : Nat) :
    g.markDurable x =
      { g with durableThrough := max g.durableThrough (Group.densePrefixCap x g.writtenThrough g.retry) } :=
  rfl

/-- The holder of the commit lock at a step without a batch: nobody holds a batch. -/
theorem lead_none_of_lock {s : Sys} {c : Nat} (hs : Inv s) (hh : (s.tx c).holdsLock = true)
    (hb : (s.tx c).pc.holdsBatch = false) : s.lead = none := by
  cases hl : s.lead with
  | none => rfl
  | some p =>
    obtain ⟨l, b⟩ := p
    exfalso
    have := lock_unique hs hh (holder_holdsLock_true hs hl)
    subst this
    have h2 := (leadOf_of_lead hs hl).2.1
    rw [hb] at h2; cases h2

/-- `GroupPrefixSynced`: the prefix up to the first retry hole is durable, and the thread
gives back the commit lock. The new state is given by its parts. -/
theorem step_prefix_synced_gen {s : Sys} {c t : Nat} (hs : Inv s)
    (hpc : (s.tx c).pc = .prefixSynced t) (hf : (s.tx c).failed = false) {s' : Sys}
    (hg : s'.g = s.g.markDurable s.g.writtenThrough) (hlead : s'.lead = s.lead)
    (hlog : s'.log = s.log) (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = none)
    (htxc : s'.tx c = { s.tx c with held := false, pc := .await t })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hhc : (s.tx c).holdsLock = true := by simp [TxRec.holdsLock, hpc, hc.2.1]
  have hlk : s.lockHolder = some c := (hs.groupTx c).1.1 hhc
  have hln : s.lead = none := lead_none_of_lock hs hhc (by simp [hpc, Pc.holdsBatch, Pc.leading])
  have htw : s.g.taken = [] ∧ s.g.withdrawn = [] := by
    have := hs.lead; simp only [LeadInv, hln] at this; exact this
  have hslot : s.issuedSlot = none := by simp [Sys.issuedSlot_eq, hln]
  have hiss : s.g.issued = none := by rw [hs.issue.1, hslot]; rfl
  have hab := abandoned_nil hs hiss
  have hlc := hs.log c
  have hsyn : s.synced = s.log.length :=
    hlc.2.2.2.2.2.2.2.2.2.2.2.2.2.1 (Or.inr (by simp [hpc, Pc.isPrefixSynced])) hf
  have htake : s.log.take s.synced = s.log := by rw [hsyn, List.take_length]
  have hcap := Group.densePrefixCap_le s.g.writtenThrough s.g.writtenThrough s.g.retry
  have hdle : s.g.durableThrough ≤ s.g.writtenThrough := hs.order.2.2
  have hg'dt : s'.g.durableThrough ≤ s.g.writtenThrough := by
    rw [hg, markDurable_eq]; simp only; omega
  have hg'dt0 : s.g.durableThrough ≤ s'.g.durableThrough := by
    rw [hg, markDurable_eq]; simp only; omega
  have hg' : s'.g.writtenThrough = s.g.writtenThrough ∧ s'.g.retry = s.g.retry ∧
      s'.g.pending = s.g.pending ∧ s'.g.issued = s.g.issued ∧ s'.g.abandoned = s.g.abandoned ∧
      s'.g.taken = s.g.taken ∧ s'.g.withdrawn = s.g.withdrawn ∧
      s'.g.nextTicket = s.g.nextTicket := by
    rw [hg, markDurable_eq]; simp
  obtain ⟨g1, g2, g3, g4, g5, g6, g7, g8⟩ := hg'
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd]
  have huni : s'.unissued = s.unissued := by simp [Sys.unissued_eq, hlead, hln]
  have hent' : s'.issuedEntry = s.issuedEntry := by simp [Sys.issuedEntry_eq, hlead, hln]
  have hslot' : s'.issuedSlot = none := by simp [Sys.issuedSlot_eq, hlead, hln]
  have hqueue : s'.queue = s.queue := by simp [Sys.queue, hent', huni, g3]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  have hholds : ∀ d, d ≠ c → (s.tx d).holdsLock = false := by
    intro d hd
    cases h : (s.tx d).holdsLock
    · rfl
    · exact absurd (lock_unique hs h hhc) hd
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    have hlog' : ∀ u, u ≤ s'.g.durableThrough → u ≤ s.g.writtenThrough := fun u h => by omega
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, g1, g4, htake] at hld ⊢
      simp only [hpc, Pc.waitTicket, Pc.isPrefixSynced] at hld ⊢
      obtain ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, _, l15, _⟩ := hld
      refine ⟨l1, by simpa using l2, l3, l4, l5, l6, l7, l8, by simp, by simp, by simp, by simp,
        by simp, by simp, ?_, by simp [Pc.rollingBackOther, Pc.removingOther]⟩
      intro u hu
      exact ⟨(l15 u hu).1, fun h => (l15 u hu).1 (hlog' u h)⟩
    · simp only [LogInv, htxd d hd, hinlog, hinsync, hbatch, hlog, hsync, g1, g4, htake] at hld ⊢
      obtain ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, _, l15, l16⟩ := hld
      refine ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, fun _ _ => hsyn, ?_, l16⟩
      intro u hu
      exact ⟨(l15 u hu).1, fun h => (l15 u hu).1 (hlog' u h)⟩
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hqueue, g2, g8, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro u hu
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd u hu
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw
    by_cases hd : d = c
    · subst hd; rw [htxc] at hw; cases hw
    · rw [htxd d hd] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock]
      simp [TxRec.holdsLock, Pc.afterLeave]
    · simp only [GroupTxInv, htxd d hd, hlock, g4, g5, g6, g3, hlead, hln] at hgd ⊢
      refine ⟨by simp [hholds d hd], hgd.2.1, ?_, ?_⟩
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | h
        · rw [hab] at h; cases h
        · simp at h
      · intro h1 h2
        obtain ⟨_, h, _⟩ := hgd.2.2.2 h1 h2
        simp at h
  · simp only [LeadInv, hlead, hln, g6, g7]
    exact htw
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, huni, g2, g3, g5, g6, g7, g8, hinlog, hlead]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      exact ⟨a1, a2, a3, by rw [obs_st (hobsd e.tx)]; exact a4, a5⟩
    · intro e he
      rcases q4 e he with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd e.tx)).2 h
    · intro e he
      rcases q5 e he with h | h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
    · intro x hx
      exact ⟨(q8 x hx).1, by rw [obs_afterLeave (hobsd x)]; exact (q8 x hx).2⟩
    · intro x hx
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact (q9 x hx).1, (q9 x hx).2⟩
  · simp only [IssueInv, hslot', g4, g5, hiss, hab, hlead]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, g1, g2, g8, hsync, hlog]
    exact ⟨m1, m2, by omega, m4, m5⟩
  · obtain ⟨o1, _, _⟩ := hs.order
    simp only [OrderInv, hqueue, hslot', g1]
    exact ⟨o1, by simp, hg'dt⟩
  · intro h hh
    rw [g2] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    exact ⟨d, by rw [obs_ticket (hobsd d)]; exact hd⟩
  · intro d e u hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e u hd he

end GroupCommit

namespace GroupCommit
open Sys

theorem lead_none_of_direct {s : Sys} {c : Nat} (hs : Inv s) (hh : (s.tx c).held = true)
    (hnb : s.batchOf c = none) : s.lead = none := by
  cases hl : s.lead with
  | none => rfl
  | some p =>
    obtain ⟨l, b⟩ := p
    exfalso
    have hhc : (s.tx c).holdsLock = true := by
      simp only [TxRec.holdsLock]
      split
      · rfl
      · rfl
      · exact hh
    have := lock_unique hs hhc (holder_holdsLock_true hs hl)
    subst this
    simp [Sys.batchOf_eq, hl] at hnb

/-- `FinishLogicalLogWrite` of a direct commit: its own record is in the log. -/
theorem step_direct_finish_gen {s : Sys} {c : Nat} (hs : Inv s) (hnb : s.batchOf c = none)
    (hpc : (s.tx c).pc = .finish) {s' : Sys}
    (hg : s'.g = s.g) (hlead : s'.lead = s.lead) (hlog : s'.log = s.log ++ [⟨c, none⟩])
    (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .own2, submitted := false })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hln : s.lead = none := lead_none_of_direct hs hc.2 hnb
  have hhc : (s.tx c).holdsLock = true := by simp [TxRec.holdsLock, hpc, hc.2]
  have hlc := hs.log c
  have hnotin : ¬ s.InLog c := hlc.2.2.2.2.2.2.2.2.2.2.2.2.1 (Or.inr (Or.inr hpc)) hnb
  have hrb : (s.tx c).rolledBack = false := rolledBack_false_of_live hs hc.1
  have hu := unref_direct hs hnb (Or.inr (Or.inr (Or.inl hpc)))
  have hsyn : s.synced ≤ s.log.length := hs.mark.2.2.2.1
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd]
  have huni : s'.unissued = s.unissued := by simp [Sys.unissued_eq, hlead, hln]
  have hent : s'.issuedEntry = s.issuedEntry := by simp [Sys.issuedEntry_eq, hlead, hln]
  have hslot : s'.issuedSlot = s.issuedSlot := by simp [Sys.issuedSlot_eq, hlead, hln]
  have hqueue : s'.queue = s.queue := by simp [Sys.queue, hent, huni, hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d ∨ d = c := by
    intro d; simp only [Sys.InLog, hlog, List.mem_append, List.mem_singleton]
    constructor
    · rintro ⟨r, hr | hr, hrd⟩
      · exact Or.inl ⟨r, hr, hrd⟩
      · subst hr; exact Or.inr hrd.symm
    · rintro (⟨r, hr, hrd⟩ | hd)
      · exact ⟨r, Or.inl hr, hrd⟩
      · exact ⟨⟨c, none⟩, Or.inr rfl, hd.symm⟩
  have htake : s'.log.take s'.synced = s.log.take s.synced := by
    rw [hlog, hsync, List.take_append_of_le_length hsyn]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by intro d; simp only [Sys.InSynced, htake]
  have hbatch : ∀ d, s'.batchOf d = none := by intro d; simp [Sys.batchOf_eq, hlead, hln]
  have hbatch0 : ∀ d, s.batchOf d = none := by intro d; simp [Sys.batchOf_eq, hln]
  have hnps : ∀ d, d ≠ c → (s.tx d).pc.isPrefixSynced = false := by
    intro d hd
    cases hp : (s.tx d).pc with
    | prefixSynced t =>
      exfalso
      have hst := hs.status d
      simp only [StatusInv, StatusOf, hp] at hst
      have : (s.tx d).holdsLock = true := by simp [TxRec.holdsLock, hp, hst.2.1]
      exact hd (lock_unique hs this hhc)
    | _ => rfl
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync] at hld ⊢
      simp only [hpc, hbatch0] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hbatch0, hlog, hsync,
        hd, or_false, hnps d hd] at hld ⊢
      obtain ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, _, l15, l16⟩ := hld
      refine ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, by simp, ?_, l16⟩
      intro t ht
      exact ⟨fun h => List.mem_append_left _ ((l15 t ht).1 h),
        fun h => by rw [List.take_append_of_le_length hsyn]; exact (l15 t ht).2 h⟩
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hqueue, hg, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    have hdc : d ≠ c := by
      intro h; subst h
      simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave] at ht
    refine ⟨h1, h2, ?_, ?_, ?_⟩
    · rcases h3 with h | h | h
      · exact Or.inl (List.mem_append_left _ h)
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
    · intro hr; rintro (h | h)
      · exact h4 hr h
      · exact hdc h
    · intro hw; rw [htxd d hdc] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, hg, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, htxd d hd, hlock, hg, hlead]; simpa [GroupTxInv, hln] using hgd
  · have := hs.lead
    simp only [LeadInv, hlead, hln, hg] at this ⊢
    exact this
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, huni, hg, hinlog, hlead]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      refine ⟨a1, a2, ?_, by rw [obs_st (hobsd e.tx)]; exact a4, a5⟩
      rintro (h | h)
      · exact a3 h
      · exact hu.notQueued e he h
    · intro e he
      rcases q4 e he with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd e.tx)).2 h
    · intro e he
      rcases q5 e he with h | h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
    · intro x hx
      exact ⟨(q8 x hx).1, by rw [obs_afterLeave (hobsd x)]; exact (q8 x hx).2⟩
    · intro x hx
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact (q9 x hx).1, (q9 x hx).2⟩
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot, hg, hlead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw; rw [obs_st (hobsd w), obs_rolledBack (hobsd w)]; exact i2 w hw
    · intro e he; simp [Sys.issuedSlot_eq, hln] at he
    · intro x hx
      obtain ⟨a1, a2, a3, a4⟩ := i4 x hx
      have hxc : x ≠ c := fun h => hu.notAbandoned (h ▸ hx)
      rw [htxd x hxc]; exact ⟨a1, a2, a3, a4⟩
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hsync, hlog]
    refine ⟨m1, m2, m3, by simp; omega, ?_⟩
    simp only [List.map_append, List.map_cons, List.map_nil]
    refine List.nodup_append.2 ⟨m5, by simp, ?_⟩
    intro a ha b hb
    simp at hb; subst hb
    intro h; subst h
    simp only [List.mem_map] at ha
    obtain ⟨r, hr, hrt⟩ := ha
    exact hnotin ⟨r, hr, hrt⟩
  · simpa only [OrderInv, hqueue, hslot, hg] using hs.order
  · intro h hh
    rw [hg] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    exact ⟨d, by rw [obs_ticket (hobsd d)]; exact hd⟩
  · intro d e u hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e u hd he

end GroupCommit
