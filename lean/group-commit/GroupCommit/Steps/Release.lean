import GroupCommit.Steps.Drop

/-!
# `release_group_claim` of a dropped leader
-/

namespace GroupCommit
open Sys

/-- The dropped leader at `dR2` or `dR2e` has a live transaction and no ticket. -/
theorem released_holder_facts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR2 ∨ (s.tx c).pc = .dR2e) :
    (s.tx c).st = .live ∧ (s.tx c).dropFrom ≠ .committed ∧ (s.tx c).dropFrom.ticket = none ∧
      (s.tx c).held = true := by
  have hc := hs.status c
  have hlo := leadOf_of_lead hs hl
  rcases hpc with h | h
  · simp only [StatusInv, StatusOf, h] at hc
    have h4 := hlo.2.2.2.1 (Or.inr h)
    have hdf : (s.tx c).dropFrom = .upgrade ∨ (s.tx c).dropFrom = .finish ∨
        (s.tx c).dropFrom = .syncLog ∨ (s.tx c).dropFrom = .endCommit := by
      rcases h4 with ⟨h', _⟩ | h' | ⟨h' | h', _⟩ <;> simp [h']
    have hnc : (s.tx c).dropFrom ≠ .committed := by rcases hdf with h' | h' | h' | h' <;> simp [h']
    refine ⟨?_, hnc, ?_, hlo.1⟩
    · rcases hc with ⟨h1, _⟩ | ⟨_, h2⟩
      · exact h1
      · exact absurd h2 hnc
    · rcases hdf with h' | h' | h' | h' <;> simp [h', Pc.ticket]
  · simp only [StatusInv, StatusOf, h] at hc
    exact ⟨hc.1, by simp [hc.2], by simp [hc.2, Pc.ticket], hlo.1⟩

theorem requeue_fields (g : Group) (L : List Entry) :
    (g.requeue .fixed L).pending = L.filter (fun e => !g.withdrawn.contains e.tx) ++ g.pending ∧
    (g.requeue .fixed L).taken = eraseAll (L.map (·.tx)) g.taken ∧
    (g.requeue .fixed L).withdrawn = eraseAll (L.map (·.tx)) g.withdrawn ∧
    (g.requeue .fixed L).retry = g.retry ∧ (g.requeue .fixed L).issued = g.issued ∧
    (g.requeue .fixed L).abandoned = g.abandoned ∧
    (g.requeue .fixed L).writtenThrough = g.writtenThrough ∧
    (g.requeue .fixed L).durableThrough = g.durableThrough ∧
    (g.requeue .fixed L).nextTicket = g.nextTicket := by
  simp [Group.requeue]

/-- `release_group_claim` gives the records that are still to be written back to the
queue, and the leader goes to `cleanup_dropped_commit`. The new state is given by its parts. -/
theorem step_release_gen {s : Sys} {c : Nat} {b : Batch} {L : List Entry} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR2 ∨ (s.tx c).pc = .dR2e)
    (hunis : s.unissued = L) (hent : s.issuedEntry = none) (hslot : s.issuedSlot = none)
    {s' : Sys} (hg : s'.g = s.g.requeue .fixed L) (hlead : s'.lead = none)
    (hlog : s'.log = s.log) (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dLeave })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  obtain ⟨hlive, hnc, hdft, hheld⟩ := released_holder_facts hs hl hpc
  have hlo := leadOf_of_lead hs hl
  have hiss : s.g.issued = none := by rw [hs.issue.1, hslot]; rfl
  have hab := abandoned_nil hs hiss
  have hq : s.queue = L ++ s.g.pending := by simp [Sys.queue, hent, hunis]
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  rw [hunis] at q5 q6 q7
  obtain ⟨r1, r2, r3, r4, r5, r6, r7, r8, r9⟩ := requeue_fields s.g L
  have htk' : s'.g.taken = [] := by
    rw [hg, r2]; apply List.eq_nil_iff_forall_not_mem.2
    intro x hx; rw [mem_eraseAll] at hx
    have := q7 x (by simp [hx.2]); exact hx.1 (by simpa using this)
  have hwd' : s'.g.withdrawn = [] := by
    rw [hg, r3]; apply List.eq_nil_iff_forall_not_mem.2
    intro x hx; rw [mem_eraseAll] at hx
    have := q7 x (by simp [hx.2]); exact hx.1 (by simpa using this)
  have hnd := hq ▸ q2
  simp only [List.map_append, List.nodup_append] at hnd
  have hpendw : ∀ e ∈ s.g.pending, e.tx ∉ s.g.withdrawn := by
    intro e he hw
    have := q7 e.tx (by simp [hw])
    exact hnd.2.2 _ this _ (List.mem_map_of_mem he) rfl
  have hctk : (s.tx c).ticket = none := by
    rcases hpc with h | h <;> simp [TxRec.ticket, h, Pc.waitTicket, Pc.beforeLeave, hdft]
  have hpendc : ∀ e ∈ s.g.pending, e.tx ≠ c := by
    intro e he hec
    rcases q4 e he with h | h
    · rw [hec, hctk] at h; cases h
    · rw [hec] at h; rcases hpc with h' | h' <;> rw [h'] at h <;> cases h.1
  have hobsd : ∀ d, d ≠ c → (s'.tx d).obs = (s.tx d).obs := by
    intro d hd; rw [htxd d hd]
  have hqueue : s'.queue = L.filter (fun e => !s.g.withdrawn.contains e.tx) ++ s.g.pending := by
    simp [Sys.queue, Sys.issuedEntry_eq, Sys.unissued_eq, hlead, hg, r1]
  have hqsub : ∀ e ∈ s'.queue, e ∈ s.queue := by
    intro e he; rw [hqueue] at he; rw [hq]
    simp only [List.mem_append, List.mem_filter] at he ⊢
    rcases he with h | h
    · exact Or.inl h.1
    · exact Or.inr h
  have hqsub' : s'.queue.Sublist s.queue := by
    rw [hqueue, hq]; exact List.Sublist.append (List.filter_sublist) (List.Sublist.refl _)
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = none := by intro d; simp [Sys.batchOf_eq, hlead]
  have hbatch0 : ∀ d, d ≠ c → s.batchOf d = none := by
    intro d hd; simp [Sys.batchOf_eq, hl, Ne.symm hd]
  have hslot' : s'.issuedSlot = none := by simp [Sys.issuedSlot_eq, hlead]
  have hnotarget : ∀ d, (s.tx c).pc ≠ .dRbOtherA d ∧ (s.tx c).pc ≠ .dRbOtherB d := by
    intro d; rcases hpc with h | h <;> simp [h]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, hlive, hnc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync, r5, r7, r8] at hld ⊢
      rcases hpc with h | h <;> simp only [h, batchOf_holder hl] at hld ⊢ <;>
        grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hlog, hsync, r5, r7, r8,
        hbatch0 d hd] at hld ⊢
      exact hld
  · intro d
    have htd := hs.ticket d
    by_cases hd : d = c
    · subst hd; rw [TicketInv, htxc]
      simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave, hdft]
    · simp only [TicketInv, htxd d hd, hg, r4, r9, hinlog, hlog] at htd ⊢
      intro t ht
      obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
      refine ⟨h1, h2, ?_, h4, h5⟩
      rcases h3 with h | h | h
      · exact Or.inl h
      · right; left
        rw [hqueue]
        rw [hq] at h
        simp only [List.mem_append, List.mem_filter] at h ⊢
        rcases h with h | h
        · left; refine ⟨h, ?_⟩
          simp only [Bool.not_eq_true', List.contains_eq_mem, decide_eq_false_iff_not]
          intro hw
          have := (q9 d hw).1
          rw [ticket_none_of_afterLeave this] at ht; simp at ht
        · right; exact h
      · exact Or.inr (Or.inr h)
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, hg, r5, r6, r2, r1]
      rw [GroupTxInv] at hgd
      have hold := holder_holdsLock hs hl
      rw [hold] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, htxd d hd, hlock, hg, r5, r6, r2, r1, hlead]
      rw [GroupTxInv] at hgd
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, _, a3⟩ := hgd.2.1 h1 h2
        refine ⟨a1, ?_, ?_⟩
        · rw [← r2, ← hg, htk']; simp
        · intro e he
          simp only [List.mem_append, List.mem_filter] at he
          rcases he with ⟨he, hw⟩ | he
          · intro hed
            rcases q6 e he with h | h
            · have := (q8 e.tx h).2; rw [hed, h1] at this; cases this
            · simp [h] at hw
          · exact a3 e he
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · rw [hab] at h; cases h
        · rw [hl] at hl'; simp at hl'; subst hl'; exact absurd hl2 (hnotarget d).1
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; exact absurd hl2 (hnotarget d).2
  · simp only [LeadInv, hlead]
    exact ⟨htk', hwd'⟩
  · simp only [QueueInv, htk', hwd', hinlog, hlead, Sys.unissued_eq]
    refine ⟨List.Pairwise.sublist hqsub' q1, List.Nodup.sublist (hqsub'.map _) q2, ?_, ?_, by simp,
      by simp, by simp, by simp, by simp⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e (hqsub e he)
      refine ⟨a1, by rw [hg, r9]; exact a2, a3, ?_, by rw [hg, r4]; exact a5⟩
      left
      have hst' : (s'.tx e.tx).st = (s.tx e.tx).st := by
        by_cases hec : e.tx = c
        · rw [hec, htxc]
        · rw [htxd _ hec]
      rw [hst']
      rcases a4 with h | h
      · exact h
      · exfalso
        rw [hqueue] at he
        simp only [List.mem_append, List.mem_filter] at he
        rcases he with ⟨_, hw⟩ | he
        · simp [h] at hw
        · exact hpendw e he h
    · intro e he
      rw [hg, r1] at he
      simp only [List.mem_append, List.mem_filter] at he
      rcases he with ⟨he, hw⟩ | he
      · by_cases hec : e.tx = c
        · right; rw [hec, htxc]; exact ⟨rfl, hdft, hheld⟩
        · rcases q5 e he with h | h | h
          · left; rw [htxd _ hec]; exact h
          · simp [h] at hw
          · rw [hl] at h; simp at h; exact absurd h.symm hec
      · left
        have hec := hpendc e he
        rw [htxd _ hec]
        rcases q4 e he with h | h
        · exact h
        · exfalso
          have hh : (s.tx e.tx).holdsLock = true := by simp [TxRec.holdsLock, h.1, h.2.2]
          exact hec (lock_unique hs hh (holder_holdsLock_true hs hl))
  · simp only [IssueInv, hslot', hg, r5, r6, hiss, hab, hlead]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, r4, r7, r8, r9, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, _, o3⟩ := hs.order
    simp only [OrderInv, hslot', hg, r7, r8]
    exact ⟨fun e he => o1 e (hqsub e he), by simp, o3⟩
  · intro h hh
    rw [hg, r4] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    refine ⟨d, ?_⟩
    by_cases hdc : d = c
    · subst hdc; rw [hctk] at hd; cases hd
    · rw [htxd d hdc]; exact hd
  · intro d e t hd he
    have ht : ∀ x, (s'.tx x).ticket = (s.tx x).ticket := by
      intro x
      by_cases hxc : x = c
      · subst hxc; rw [htxc, hctk]
        simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave, hdft]
      · rw [htxd x hxc]
    rw [ht] at hd he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

/-- Facts about a dropped leader whose write was issued and did not complete. -/
structure GiveUpFacts (s : Sys) (c : Nat) (b : Batch) : Prop where
  unissued : s.unissued = b.rest
  issuedEntry : s.issuedEntry = some b.writing
  issuedSlot : s.issuedSlot = some b.writing
  issued : s.g.issued = some b.writing.tx
  queue : s.queue = b.writing :: (b.rest ++ s.g.pending)
  retry : s.g.retry = []
  notLog : ¬ s.InLog b.writing.tx
  written : s.g.writtenThrough < b.writing.ticket
  durable : s.g.durableThrough < b.writing.ticket
  ticketPos : 1 ≤ b.writing.ticket
  ticketLe : b.writing.ticket ≤ s.g.nextTicket
  abandoned : ∀ x ∈ s.g.abandoned, x = b.writing.tx
  live : (s.tx c).st = .live
  heldC : (s.tx c).held = true

theorem giveUpFacts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .dR2) (hdf : (s.tx c).dropFrom = .finish) : GiveUpFacts s c b := by
  have hunis : s.unissued = b.rest := by
    simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hpc, hdf]
  have hent : s.issuedEntry = some b.writing := by
    simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hpc, hdf]
  have hslot : s.issuedSlot = some b.writing := by
    simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hpc, hdf]
  have hiss : s.g.issued = some b.writing.tx := by rw [hs.issue.1, hslot]; rfl
  have hq : s.queue = b.writing :: (b.rest ++ s.g.pending) := by simp [Sys.queue, hent, hunis]
  have hw := hs.queue.2.2.1 b.writing (by simp [hq])
  have hlo := leadOf_of_lead hs hl
  obtain ⟨hlive, _, _, hheld⟩ := released_holder_facts hs hl (Or.inl hpc)
  refine ⟨hunis, hent, hslot, hiss, hq, hlo.2.2.2.2.2.2.2.2.1 (Or.inr (Or.inr hpc)), hw.2.2.1,
    hs.order.1 _ (by simp [hq]), hs.order.2.1 _ (by simp [hslot]), hw.1, hw.2.1, ?_, hlive, hheld⟩
  intro x hx
  have := (hs.issue.2.2.2 x hx).1
  rw [hiss] at this; exact (Option.some.inj this).symm

theorem lowerTo_below {t m : Nat} (h : m < t) : Group.lowerTo t m = m := by
  simp only [Group.lowerTo]; split <;> omega

/-- `release_group_claim` gives up the issued write of the batch record. If the record is
the leader's own, or its owner still waits, the leader goes on to `dR2e`. In the second case
the owner must retry. The new state is given by its parts. -/
theorem step_give_up_gen {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR2) (hdf : (s.tx c).dropFrom = .finish)
    {s' : Sys}
    (hcase : (b.writing.tx ≠ c ∧ b.writing.tx ∉ s.g.abandoned ∧
        s'.g = ({ s.g with issued := none } : Group).requestRetry b.writing.ticket) ∨
      (b.writing.tx = c ∧ s'.g = { s.g with issued := none }))
    (hlead : s'.lead = s.lead) (hlog : s'.log = s.log) (hsync : s'.synced = s.synced)
    (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dR2e })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have xf := giveUpFacts hs hl hpc hdf
  have hlo := leadOf_of_lead hs hl
  have hcab : c ∉ s.g.abandoned := not_abandoned_of_pc hs (by simp [hpc])
  have hab : s.g.abandoned = [] := by
    apply List.eq_nil_iff_forall_not_mem.2
    intro x hx
    have := xf.abandoned x hx
    rcases hcase with ⟨_, hna, _⟩ | ⟨hwc, _⟩
    · exact hna (this ▸ hx)
    · exact hcab (hwc ▸ this ▸ hx)
  have hg' : s'.g.issued = none ∧ s'.g.pending = s.g.pending ∧ s'.g.taken = s.g.taken ∧
      s'.g.withdrawn = s.g.withdrawn ∧ s'.g.abandoned = s.g.abandoned ∧
      s'.g.writtenThrough = s.g.writtenThrough ∧ s'.g.durableThrough = s.g.durableThrough ∧
      s'.g.nextTicket = s.g.nextTicket := by
    rcases hcase with ⟨_, _, hg⟩ | ⟨_, hg⟩ <;> rw [hg] <;>
      simp [Group.requestRetry, lowerTo_below xf.written, lowerTo_below xf.durable]
  obtain ⟨g1, g2, g3, g4, g5, g6, g7, g8⟩ := hg'
  have hretry' : ∀ h, h ∈ s'.g.retry ↔ b.writing.tx ≠ c ∧ h = b.writing.ticket := by
    intro h
    rcases hcase with ⟨hne, _, hg⟩ | ⟨hwc, hg⟩ <;> rw [hg] <;>
      simp [Group.requestRetry, xf.retry, *]
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  rw [xf.unissued] at q5 q6 q7
  have hctk : (s.tx c).ticket = none := by
    simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, hdf, Pc.ticket]
  have hwt : b.writing.tx ≠ c → (s.tx b.writing.tx).ticket = some b.writing.ticket := by
    intro hne
    rcases hs.issue.2.2.1 b.writing (by simp [xf.issuedSlot]) c (by simp [hl]) hne with h | h
    · exact h
    · rw [hab] at h; cases h
  have hsorted := List.pairwise_cons.1 (xf.queue ▸ q1)
  have hnd := xf.queue ▸ q2
  simp only [List.map_cons, List.nodup_cons, List.map_append, List.mem_append, List.mem_map] at hnd
  have hqueue : s'.queue = b.rest ++ s.g.pending := by
    have hu : s'.unissued = b.rest := by
      simp [Sys.unissued_eq, hlead, hl, htxc, TxRec.writingUnissued]
    have he : s'.issuedEntry = none := by
      simp [Sys.issuedEntry_eq, hlead, hl, htxc, TxRec.issuing]
    simp [Sys.queue, hu, he, g2]
  have hunis' : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, hl, htxc, TxRec.writingUnissued]
  have hslot' : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, hl, htxc, TxRec.issuing]
  have hqsub : ∀ e ∈ s'.queue, e ∈ s.queue := by
    intro e he; rw [hqueue] at he; rw [xf.queue]; simp_all
  have hqgt : ∀ e ∈ s'.queue, b.writing.ticket < e.ticket := by
    intro e he; rw [hqueue] at he; exact hsorted.1 e he
  have hqne : ∀ e ∈ s'.queue, e.tx ≠ b.writing.tx := by
    intro e he hew; rw [hqueue] at he
    rcases List.mem_append.1 he with h | h
    · exact hnd.1 (Or.inl ⟨e, h, hew⟩)
    · exact hnd.1 (Or.inr ⟨e, h, hew⟩)
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd]
  have hholdc : (s.tx c).holdsLock = true := by simp [TxRec.holdsLock, hpc, xf.heldC]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, xf.live, hdf]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      simp only [hpc, xf.issued, batchOf_holder hl] at hld ⊢
      have hwc : b.writing.tx = d → ¬ s.InLog d := fun h => h ▸ xf.notLog
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      simp only [xf.issued] at hld
      have hwd : b.writing.tx = d → ¬ s.InLog d := fun h => h ▸ xf.notLog
      grind
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, g8, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    have hdt : (s.tx d).ticket = some t := by simpa using ht
    refine ⟨h1, h2, ?_, ?_, ?_⟩
    · rcases h3 with h | h | h
      · exact Or.inl h
      · rw [xf.queue] at h
        rcases List.mem_cons.1 h with h | h
        · right; right
          have hdw : d = b.writing.tx := by rw [← h]
          have hdc : d ≠ c := by intro hdc; rw [hdc, hctk] at hdt; cases hdt
          refine (hretry' t).2 ⟨hdw ▸ hdc, ?_⟩
          rw [← h]
        · right; left; rw [hqueue]; exact h
      · rw [xf.retry] at h; cases h
    · intro hr
      obtain ⟨hne, rfl⟩ := (hretry' t).1 hr
      have := hs.unique d b.writing.tx _ hdt (hwt hne)
      subst this
      exact xf.notLog
    · intro hw hr
      obtain ⟨hne, rfl⟩ := (hretry' t).1 hr
      have := hs.unique d b.writing.tx _ hdt (hwt hne)
      subst this
      rw [htxd _ hne] at hw
      have hh : (s.tx b.writing.tx).holdsLock = true := by simp [TxRec.holdsLock, hw]
      exact hne (lock_unique hs hh hholdc)
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, g1, g3, g2, g5, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, htxd d hd, hlock, g1, g3, g2, g5, hlead]
      rw [GroupTxInv] at hgd
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨_, a2, a3⟩ := hgd.2.1 h1 h2
        exact ⟨by simp, a2, a3⟩
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · rw [hab] at h; cases h
        · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, hl, LeadOf, htxc, hunis', hslot', hinlog, hlog, g6]
    obtain ⟨h1, _, _, _, h5, _, _, _, _, _, _, _⟩ := hlo
    refine ⟨h1, by simp [Pc.holdsBatch], by simp, by simp, h5, by simp [TxRec.issuing,
      TxRec.writingUnissued], by simp, by simp [Pc.leading], by simp [Pc.leading],
      by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
  · simp only [QueueInv, hqueue, hunis', g2, g3, g4, g5, g8, hinlog, hlead]
    refine ⟨hsorted.2, ?_, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · simpa using hnd.2
    · intro e he
      have he' : e ∈ s'.queue := by rw [hqueue]; exact he
      obtain ⟨a1, a2, a3, a4, _⟩ := q3 e (hqsub e he')
      refine ⟨a1, a2, a3, by rw [obs_st (hobsd e.tx)]; exact a4, ?_⟩
      intro hr
      obtain ⟨_, heq⟩ := (hretry' _).1 hr
      have := hqgt e he'; omega
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
  · simp only [IssueInv, hslot', g1, g5, hab]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, g6, g7, g8, hsync, hlog]
    refine ⟨?_, m2, m3, m4, m5⟩
    intro h hh
    obtain ⟨_, rfl⟩ := (hretry' h).1 hh
    exact ⟨xf.ticketPos, xf.ticketLe⟩
  · obtain ⟨o1, _, o3⟩ := hs.order
    simp only [OrderInv, hslot', g6, g7]
    exact ⟨fun e he => o1 e (hqsub e he), by simp, o3⟩
  · intro h hh
    obtain ⟨hne, rfl⟩ := (hretry' h).1 hh
    exact ⟨b.writing.tx, by rw [obs_ticket (hobsd _)]; exact hwt hne⟩
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

/-- `release_issued` finds that the owner of the record left: the leader takes over its
rollback. The new state is given by its parts. -/
theorem step_abandon_gen {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dR2) (hdf : (s.tx c).dropFrom = .finish)
    (hwa : b.writing.tx ∈ s.g.abandoned) {s' : Sys}
    (hg : s'.g = { s.g with issued := none, abandoned := setErase b.writing.tx s.g.abandoned })
    (hlead : s'.lead = s.lead) (hlog : s'.log = s.log) (hsync : s'.synced = s.synced)
    (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dRbOtherA b.writing.tx })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have xf := giveUpFacts hs hl hpc hdf
  have hlo := leadOf_of_lead hs hl
  obtain ⟨_, hwpc, hwlive, hwheld⟩ := hs.issue.2.2.2 _ hwa
  have hne : b.writing.tx ≠ c := by intro h; rw [h, hpc] at hwpc; cases hwpc
  have hab' : s'.g.abandoned = [] := by
    rw [hg]; simp only
    apply List.eq_nil_iff_forall_not_mem.2
    intro x hx; rw [mem_setErase] at hx; exact hx.1 (xf.abandoned x hx.2)
  have hg' : s'.g.issued = none ∧ s'.g.pending = s.g.pending ∧ s'.g.taken = s.g.taken ∧
      s'.g.withdrawn = s.g.withdrawn ∧ s'.g.retry = s.g.retry ∧
      s'.g.writtenThrough = s.g.writtenThrough ∧ s'.g.durableThrough = s.g.durableThrough ∧
      s'.g.nextTicket = s.g.nextTicket := by
    rw [hg]; simp
  obtain ⟨g1, g2, g3, g4, g5, g6, g7, g8⟩ := hg'
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  rw [xf.unissued] at q5 q6 q7
  have hsorted := List.pairwise_cons.1 (xf.queue ▸ q1)
  have hnd := xf.queue ▸ q2
  simp only [List.map_cons, List.nodup_cons, List.map_append, List.mem_append, List.mem_map] at hnd
  have hunis' : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, hl, htxc, TxRec.writingUnissued]
  have hent' : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, hl, htxc, TxRec.issuing]
  have hslot' : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, hl, htxc, TxRec.issuing]
  have hqueue : s'.queue = b.rest ++ s.g.pending := by simp [Sys.queue, hunis', hent', g2]
  have hqsub : ∀ e ∈ s'.queue, e ∈ s.queue := by
    intro e he; rw [hqueue] at he; rw [xf.queue]; simp_all
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd]
  have hwafter : (s.tx b.writing.tx).pc.afterLeave = true := by simp [hwpc, Pc.afterLeave]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, xf.live, hdf]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      simp only [hpc, xf.issued, batchOf_holder hl] at hld ⊢
      have hwc : b.writing.tx = d → False := fun h => hne h
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      simp only [xf.issued] at hld
      have hwd : b.writing.tx = d → ¬ s.InLog d := fun h => h ▸ xf.notLog
      grind
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, g8, g5, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    have hdt : (s.tx d).ticket = some t := by simpa using ht
    refine ⟨h1, h2, ?_, h4, ?_⟩
    · rcases h3 with h | h | h
      · exact Or.inl h
      · rw [xf.queue] at h
        rcases List.mem_cons.1 h with h | h
        · exfalso
          have hdw : d = b.writing.tx := by rw [← h]
          rw [hdw, ticket_none_of_afterLeave hwafter] at hdt; cases hdt
        · right; left; rw [hqueue]; exact h
      · exact Or.inr (Or.inr h)
    · intro hw
      by_cases hd : d = c
      · subst hd; rw [htxc] at hw; cases hw
      · rw [htxd d hd] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, g1, g3, g2, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, htxd d hd, hlock, g1, g3, g2, hab', hlead]
      rw [GroupTxInv] at hgd
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 _
        by_cases hdw : d = b.writing.tx
        · subst hdw
          refine ⟨by simp, (fun h => by have := (q8 _ h).2; rw [h1] at this; cases this), ?_⟩
          intro e he hed
          rcases q4 e he with h | h
          · rw [hed, ticket_none_of_afterLeave h1] at h; cases h
          · rw [hed, hwpc] at h; cases h.1
        · obtain ⟨_, a2, a3⟩ := hgd.2.1 h1 (fun h => hdw (xf.abandoned d h))
          exact ⟨by simp, a2, a3⟩
      · intro h1 h2
        by_cases hdw : d = b.writing.tx
        · right; refine ⟨c, by simp [hl], ?_⟩; rw [htxc, hdw]
        · rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
          · exact absurd (xf.abandoned d h) hdw
          · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, hl, LeadOf, htxc, hunis', hslot', hinlog, hlog, g6, g5, hab']
    obtain ⟨h1, _, _, _, h5, _, _, _, _, _, _, _⟩ := hlo
    refine ⟨h1, by simp [Pc.holdsBatch], by simp, by simp, h5, by simp [TxRec.issuing,
      TxRec.writingUnissued], by simp, by simp [Pc.leading], by simp [Pc.leading], ?_,
      by simp [Pc.removingOther], by simp⟩
    intro w hw
    simp [Pc.rollingBackOther] at hw; subst hw
    refine ⟨hne, xf.notLog, ?_, ?_, by simp⟩
    · rw [htxd _ hne]; exact hwlive
    · rw [htxd _ hne]; exact hwpc
  · simp only [QueueInv, hqueue, hunis', g2, g3, g4, g5, g8, hab', hinlog, hlead]
    refine ⟨hsorted.2, by simpa using hnd.2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      have he' : e ∈ s'.queue := by rw [hqueue]; exact he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e (hqsub e he')
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
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact (q9 x hx).1, by simp⟩
  · simp only [IssueInv, hslot', g1, hab']
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, g5, g6, g7, g8, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, _, o3⟩ := hs.order
    simp only [OrderInv, hslot', g6, g7]
    exact ⟨fun e he => o1 e (hqsub e he), by simp, o3⟩
  · intro h hh
    rw [g5, xf.retry] at hh; cases hh
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

/-- Facts about a dropped leader that rolls back an abandoned waiter. -/
structure OtherFacts (s : Sys) (c : Nat) (b : Batch) : Prop where
  unissued : s.unissued = b.rest
  issuedEntry : s.issuedEntry = none
  issuedSlot : s.issuedSlot = none
  issued : s.g.issued = none
  abandoned : s.g.abandoned = []
  queue : s.queue = b.rest ++ s.g.pending
  live : (s.tx c).st = .live
  finish : (s.tx c).dropFrom = .finish

theorem otherFacts {s : Sys} {c w : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .dRbOtherA w ∨ (s.tx c).pc = .dRbOtherB w) : OtherFacts s c b := by
  have hunis : s.unissued = b.rest := by
    rcases hpc with h | h <;> simp [Sys.unissued_eq, hl, TxRec.writingUnissued, h]
  have hent : s.issuedEntry = none := by
    rcases hpc with h | h <;> simp [Sys.issuedEntry_eq, hl, TxRec.issuing, h]
  have hslot : s.issuedSlot = none := by
    rcases hpc with h | h <;> simp [Sys.issuedSlot_eq, hl, TxRec.issuing, h]
  have hiss : s.g.issued = none := by rw [hs.issue.1, hslot]; rfl
  have hc := hs.status c
  have hst : (s.tx c).st = .live ∧ (s.tx c).dropFrom = .finish := by
    rcases hpc with h | h <;> simp only [StatusInv, StatusOf, h] at hc <;> exact hc
  exact ⟨hunis, hent, hslot, hiss, abandoned_nil hs hiss, by simp [Sys.queue, hent, hunis],
    hst.1, hst.2⟩

/-- A waiter that is in the queue after it was dropped was withdrawn. -/
theorem dropped_in_queue_withdrawn {s : Sys} {c w : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hunis : s.unissued = b.rest) (hent : s.issuedEntry = none)
    (hwc : w ≠ c) (hwpc : (s.tx w).pc = .dropped) :
    ∀ e ∈ s.queue, e.tx = w → w ∈ s.g.withdrawn := by
  intro e he hew
  simp only [Sys.queue, hent, hunis, Option.toList_none, List.nil_append, List.mem_append] at he
  rcases he with he | he
  · rcases hs.queue.2.2.2.2.1 e (by rw [hunis]; exact he) with h | h | h
    · rw [hew, ticket_none_of_afterLeave (by simp [hwpc, Pc.afterLeave])] at h; cases h
    · rw [hew] at h; exact h
    · rw [hl] at h; simp at h; exact absurd (hew ▸ h.symm) hwc
  · rcases hs.queue.2.2.2.1 e he with h | h
    · rw [hew, ticket_none_of_afterLeave (by simp [hwpc, Pc.afterLeave])] at h; cases h
    · rw [hew, hwpc] at h; cases h.1

/-- The leader rolls back the transaction of an abandoned waiter (`rollback_tx_inner`,
first part). The new state is given by its parts. -/
theorem step_rb_other_a_gen {s : Sys} {c w : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dRbOtherA w) {s' : Sys}
    (hg : s'.g = s.g) (hlead : s'.lead = s.lead) (hlog : s'.log = s.log)
    (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dRbOtherB w })
    (htxw : s'.tx w = { s.tx w with st := .aborted, rolledBack := true })
    (htxd : ∀ d, d ≠ c → d ≠ w → s'.tx d = s.tx d) :
    Inv s' := by
  have xf := otherFacts hs hl (Or.inl hpc)
  have hlo := leadOf_of_lead hs hl
  obtain ⟨hwc, hwlog, hwlive, hwpc, hwab⟩ := hlo.2.2.2.2.2.2.2.2.2.1 w (by simp [hpc, Pc.rollingBackOther])
  have hwst := hs.status w
  simp only [StatusInv, StatusOf, hwpc] at hwst
  have hwlc := hs.log w
  have hwwc : (s.tx w).wasCommitted = false := by
    cases h : (s.tx w).wasCommitted
    · rfl
    · have := hwlc.2.2.2.2.2.2.2.1 h; rw [hwlive] at this; simp at this
  have hwapp : (s.tx w).appended = false := by
    cases h : (s.tx w).appended
    · rfl
    · exact absurd (hwlc.1 h) hwlog
  have hwq := dropped_in_queue_withdrawn hs hl xf.unissued xf.issuedEntry hwc hwpc
  have htx : ∀ d, d ≠ w → (s'.tx d).obs = (s.tx d).obs := by
    intro d hdw; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd hdw]
  have hticket : ∀ d, (s'.tx d).ticket = (s.tx d).ticket := by
    intro d; by_cases hdw : d = w
    · subst hdw; rw [htxw]; simp [TxRec.ticket]
    · exact obs_ticket (htx d hdw)
  have hafter : ∀ d, (s'.tx d).pc.afterLeave = (s.tx d).pc.afterLeave := by
    intro d; by_cases hdw : d = w
    · subst hdw; rw [htxw]
    · exact obs_afterLeave (htx d hdw)
  have hpcd : ∀ d, d ≠ c → (s'.tx d).pc = (s.tx d).pc := by
    intro d hd; by_cases hdw : d = w
    · subst hdw; rw [htxw]
    · rw [htxd d hd hdw]
  have hunis' : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, hl, htxc, TxRec.writingUnissued]
  have hent' : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, hl, htxc, TxRec.issuing]
  have hslot' : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, hl, htxc, TxRec.issuing]
  have hqueue : s'.queue = s.queue := by rw [xf.queue]; simp [Sys.queue, hunis', hent', hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, xf.live, xf.finish]
    · by_cases hdw : d = w
      · subst hdw; rw [StatusInv, htxw]; simp [StatusOf, hwpc, hwst.1]
      · rw [StatusInv, htxd d hd hdw]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
      simp only [hpc] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · by_cases hdw : d = w
      · subst hdw
        simp only [LogInv, htxw, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
        simp only [hwpc] at hld ⊢
        grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
      · simp only [LogInv, htxd d hd hdw, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
        exact hld
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hticket, hqueue, hg, hinlog, hlog] at htd ⊢
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw'
    by_cases hd : d = c
    · subst hd; rw [htxc] at hw'; cases hw'
    · rw [hpcd d hd] at hw'; exact h5 hw'
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, hg, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, hlock, hg, hlead]
      rw [GroupTxInv] at hgd
      have hhl : (s'.tx d).holdsLock = (s.tx d).holdsLock := by
        by_cases hdw : d = w
        · subst hdw; rw [htxw]; simp [TxRec.holdsLock, hwpc]
        · rw [htxd d hd hdw]
      rw [hhl, hafter, hpcd d hd]
      refine ⟨hgd.1, hgd.2.1, ?_, ?_⟩
      · intro h1 h2
        by_cases hdw : d = w
        · subst hdw; rw [htxw] at h2; cases h2
        · rw [htxd d hd hdw] at h2
          rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
          · rw [xf.abandoned] at h; cases h
          · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2; exact absurd rfl hdw
      · intro h1 h2
        by_cases hdw : d = w
        · subst hdw; exact ⟨c, by simp [hl], by rw [htxc]⟩
        · rw [htxd d hd hdw] at h2
          obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
          rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, hl, LeadOf, htxc, hunis', hslot', hinlog, hlog, hg]
    obtain ⟨h1, _, _, _, h5, _, _, _, _, _, _, _⟩ := hlo
    refine ⟨h1, by simp [Pc.holdsBatch], by simp, by simp, h5, by simp [TxRec.issuing,
      TxRec.writingUnissued], by simp, by simp [Pc.leading], by simp [Pc.leading],
      by simp [Pc.rollingBackOther], ?_, by simp⟩
    intro x hx
    simp [Pc.removingOther] at hx; subst hx
    exact ⟨hwc, hwlog, by rw [htxw], by rw [htxw]; exact hwpc⟩
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, hunis', hg, hinlog, hlead]
    rw [xf.unissued] at q5 q6 q7
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      refine ⟨a1, a2, a3, ?_, a5⟩
      by_cases hew : e.tx = w
      · right; rw [hew]; exact hwq e he hew
      · rw [obs_st (htx _ hew)]; exact a4
    · intro e he
      rw [hticket]
      rcases q4 e he with h | h
      · exact Or.inl h
      · right
        have hew : e.tx ≠ w := by intro h'; rw [h', hwpc] at h; cases h.1
        exact (obs_pendingLeaver (htx _ hew)).2 h
    · intro e he
      rw [hticket]; exact q5 e he
    · intro x hx
      exact ⟨(q8 x hx).1, by rw [hafter]; exact (q8 x hx).2⟩
    · intro x hx
      exact ⟨by rw [hafter]; exact (q9 x hx).1, (q9 x hx).2⟩
  · simp only [IssueInv, hslot', hg, xf.issued, xf.abandoned]
    simp
  · simpa only [MarkInv, hg, hlog, hsync] using hs.mark
  · simpa only [OrderInv, hqueue, hslot', hg, xf.issuedSlot] using hs.order
  · intro h hh
    rw [hg] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    exact ⟨d, by rw [hticket]; exact hd⟩
  · intro d e t hd he
    rw [hticket] at hd he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

/-- The rollback of the abandoned waiter ends: its transaction leaves `txs`. The new state
is given by its parts. -/
theorem step_rb_other_b_gen {s : Sys} {c w : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .dRbOtherB w) {s' : Sys}
    (hg : s'.g = s.g) (hlead : s'.lead = s.lead) (hlog : s'.log = s.log)
    (hsync : s'.synced = s.synced) (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dR2e })
    (htxw : s'.tx w = { s.tx w with st := .removed })
    (htxd : ∀ d, d ≠ c → d ≠ w → s'.tx d = s.tx d) :
    Inv s' := by
  have xf := otherFacts hs hl (Or.inr hpc)
  have hlo := leadOf_of_lead hs hl
  obtain ⟨hwc, hwlog, hwab, hwpc⟩ :=
    hlo.2.2.2.2.2.2.2.2.2.2.1 w (by simp [hpc, Pc.removingOther])
  have hwst := hs.status w
  simp only [StatusInv, StatusOf, hwpc] at hwst
  have hwlc := hs.log w
  have hwwc : (s.tx w).wasCommitted = false := by
    cases h : (s.tx w).wasCommitted
    · rfl
    · have := hwlc.2.2.2.2.2.2.2.1 h; rw [hwab] at this; simp at this
  have hwq := dropped_in_queue_withdrawn hs hl xf.unissued xf.issuedEntry hwc hwpc
  have htx : ∀ d, d ≠ w → (s'.tx d).obs = (s.tx d).obs := by
    intro d hdw; by_cases hd : d = c
    · subst hd; rw [htxc]
      simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
    · rw [htxd d hd hdw]
  have hticket : ∀ d, (s'.tx d).ticket = (s.tx d).ticket := by
    intro d; by_cases hdw : d = w
    · subst hdw; rw [htxw]; simp [TxRec.ticket]
    · exact obs_ticket (htx d hdw)
  have hafter : ∀ d, (s'.tx d).pc.afterLeave = (s.tx d).pc.afterLeave := by
    intro d; by_cases hdw : d = w
    · subst hdw; rw [htxw]
    · exact obs_afterLeave (htx d hdw)
  have hpcd : ∀ d, d ≠ c → (s'.tx d).pc = (s.tx d).pc := by
    intro d hd; by_cases hdw : d = w
    · subst hdw; rw [htxw]
    · rw [htxd d hd hdw]
  have hunis' : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, hl, htxc, TxRec.writingUnissued]
  have hent' : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, hl, htxc, TxRec.issuing]
  have hslot' : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, hl, htxc, TxRec.issuing]
  have hqueue : s'.queue = s.queue := by rw [xf.queue]; simp [Sys.queue, hunis', hent', hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, xf.live, xf.finish]
    · by_cases hdw : d = w
      · subst hdw; rw [StatusInv, htxw]; simp [StatusOf, hwpc, hwst.1]
      · rw [StatusInv, htxd d hd hdw]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
      simp only [hpc] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · by_cases hdw : d = w
      · subst hdw
        simp only [LogInv, htxw, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
        simp only [hwpc] at hld ⊢
        grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
      · simp only [LogInv, htxd d hd hdw, hinlog, hinsync, hbatch, hlog, hsync, hg] at hld ⊢
        exact hld
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hticket, hqueue, hg, hinlog, hlog] at htd ⊢
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw'
    by_cases hd : d = c
    · subst hd; rw [htxc] at hw'; cases hw'
    · rw [hpcd d hd] at hw'; exact h5 hw'
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, hg, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · rw [GroupTxInv, hlock, hg, hlead]
      rw [GroupTxInv] at hgd
      have hhl : (s'.tx d).holdsLock = (s.tx d).holdsLock := by
        by_cases hdw : d = w
        · subst hdw; rw [htxw]; simp [TxRec.holdsLock, hwpc]
        · rw [htxd d hd hdw]
      rw [hhl, hafter, hpcd d hd]
      refine ⟨hgd.1, hgd.2.1, ?_, ?_⟩
      · intro h1 h2
        by_cases hdw : d = w
        · subst hdw; rw [htxw] at h2; cases h2
        · rw [htxd d hd hdw] at h2
          rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
          · rw [xf.abandoned] at h; cases h
          · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        by_cases hdw : d = w
        · subst hdw; rw [htxw] at h2; cases h2
        · rw [htxd d hd hdw] at h2
          obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
          rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2; exact absurd rfl hdw
  · simp only [LeadInv, hlead, hl, LeadOf, htxc, hunis', hslot', hinlog, hlog, hg]
    obtain ⟨h1, _, _, _, h5, _, _, _, _, _, _, _⟩ := hlo
    exact ⟨h1, by simp [Pc.holdsBatch], by simp, by simp, h5, by simp [TxRec.issuing,
      TxRec.writingUnissued], by simp, by simp [Pc.leading], by simp [Pc.leading],
      by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, hunis', hg, hinlog, hlead]
    rw [xf.unissued] at q5 q6 q7
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      refine ⟨a1, a2, a3, ?_, a5⟩
      by_cases hew : e.tx = w
      · right; rw [hew]; exact hwq e he hew
      · rw [obs_st (htx _ hew)]; exact a4
    · intro e he
      rw [hticket]
      rcases q4 e he with h | h
      · exact Or.inl h
      · right
        have hew : e.tx ≠ w := by intro h'; rw [h', hwpc] at h; cases h.1
        exact (obs_pendingLeaver (htx _ hew)).2 h
    · intro e he
      rw [hticket]; exact q5 e he
    · intro x hx
      exact ⟨(q8 x hx).1, by rw [hafter]; exact (q8 x hx).2⟩
    · intro x hx
      exact ⟨by rw [hafter]; exact (q9 x hx).1, (q9 x hx).2⟩
  · simp only [IssueInv, hslot', hg, xf.issued, xf.abandoned]
    simp
  · simpa only [MarkInv, hg, hlog, hsync] using hs.mark
  · simpa only [OrderInv, hqueue, hslot', hg, xf.issuedSlot] using hs.order
  · intro h hh
    rw [hg] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    exact ⟨d, by rw [hticket]; exact hd⟩
  · intro d e t hd he
    rw [hticket] at hd he
    exact hs.unique d e t hd he

end GroupCommit
