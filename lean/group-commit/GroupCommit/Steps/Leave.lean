import GroupCommit.Steps.Release

/-!
# `cleanup_dropped_commit`: `leave`
-/

namespace GroupCommit
open Sys

/-- The record of a commit that is not the leader is issued only if it is the record of
the batch: it is not in the pending queue and not among the records still to be written. -/
theorem issued_not_queued {s : Sys} {c : Nat} (hs : Inv s) (hnh : NotHolder s c)
    (hiss : s.g.issued = some c) :
    (∀ e ∈ s.g.pending, e.tx ≠ c) ∧ (∀ e ∈ s.unissued, e.tx ≠ c) ∧ s.g.retry = [] ∧
      ∃ l b, s.lead = some (l, b) ∧ b.writing.tx = c ∧ s.issuedSlot = some b.writing := by
  have hi1 := hs.issue.1
  rw [hiss] at hi1
  cases hl : s.lead with
  | none => simp [Sys.issuedSlot_eq, hl] at hi1
  | some p =>
    obtain ⟨l, b⟩ := p
    have hlo := leadOf_of_lead hs hl
    have hlc : l ≠ c := hnh l b hl
    simp only [Sys.issuedSlot_eq, hl] at hi1
    split at hi1
    · rename_i hcond
      simp at hi1
      have hslot : s.issuedSlot = some b.writing := by simp [Sys.issuedSlot_eq, hl, hcond]
      simp only [Bool.or_eq_true, beq_iff_eq] at hcond
      have hlead : (s.tx l).pc.leading ∨ (s.tx l).pc = .dR1 ∨ (s.tx l).pc = .dR2 := by
        rcases hcond with (h | h) | h
        · cases hp : (s.tx l).pc <;> simp_all [TxRec.issuing, Pc.leading]
        · left; rw [h]; rfl
        · left; rw [h]; rfl
      have hretry := hlo.2.2.2.2.2.2.2.2.1 hlead
      have hq : ∀ e ∈ s.queue, e.tx ≠ c ∨ s.issuedEntry = some e := by
        intro e he
        by_cases hec : e.tx = c
        · rcases hcond with (h | h) | h
          · have hent : s.issuedEntry = some b.writing := by simp [Sys.issuedEntry_eq, hl, h]
            have hq2 := hs.queue.2.1
            simp only [Sys.queue, hent, Option.toList_some, List.cons_append, List.map_cons,
              List.nodup_cons, List.mem_map] at hq2
            simp only [Sys.queue, hent, Option.toList_some, List.cons_append, List.mem_cons] at he
            rcases he with he | he
            · right; rw [hent, he]
            · exact absurd ⟨e, he, by rw [hec, ← hi1]⟩ hq2.1
          · have hin := (hlo.2.2.2.2.2.2.1 (Or.inl h)).2
            have hin' : s.InLog e.tx := ⟨_, hin, by simp [hi1, hec]⟩
            exact absurd hin' (hs.queue.2.2.1 e he).2.2.1
          · have hin := (hlo.2.2.2.2.2.2.1 (Or.inr h)).2
            have hin' : s.InLog e.tx := ⟨_, hin, by simp [hi1, hec]⟩
            exact absurd hin' (hs.queue.2.2.1 e he).2.2.1
        · exact Or.inl hec
      have hent_nd : ∀ e, s.issuedEntry = some e →
          (∀ x ∈ s.unissued ++ s.g.pending, x.tx ≠ e.tx) := by
        intro e hent x hx hxe
        have hq2 := hs.queue.2.1
        simp only [Sys.queue, hent, Option.toList_some, List.cons_append, List.map_cons,
          List.nodup_cons, List.mem_map] at hq2
        exact hq2.1 ⟨x, by simpa using hx, hxe⟩
      refine ⟨?_, ?_, hretry, l, b, rfl, hi1.symm, hslot⟩
      · intro e he hec
        rcases hq e (mem_pending_queue he) with h | h
        · exact h hec
        · exact hent_nd e h e (by simp [he]) rfl
      · intro e he hec
        rcases hq e (mem_unissued_queue he) with h | h
        · exact h hec
        · exact hent_nd e h e (by simp [he]) rfl
    · simp at hi1

end GroupCommit

namespace GroupCommit
open Sys

/-- A commit at `dLeave` whose record is issued does not hold the commit lock. -/
theorem leaver_not_held {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dLeave)
    (hiss : s.g.issued = some c) : (s.tx c).held = false := by
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  obtain ⟨_, _, _, l, b, hl, _, _⟩ := issued_not_queued hs hnh hiss
  cases h : (s.tx c).held
  · rfl
  · exfalso
    have hh : (s.tx c).holdsLock = true := by simp [TxRec.holdsLock, hpc, h]
    exact hnh l b hl (lock_unique hs (holder_holdsLock_true hs hl) hh)

/-- `leave` finds that the leader issued the record: the commit is abandoned to the leader.
The new state is given by its parts. -/
theorem step_leave_abandon_gen {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dLeave)
    (hiss : s.g.issued = some c) {s' : Sys}
    (hg : s'.g = { s.g with abandoned := setInsert c s.g.abandoned })
    (hlead : s'.lead = s.lead) (hlog : s'.log = s.log) (hsync : s'.synced = s.synced)
    (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dropped, submitted := false })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  obtain ⟨hnp, hnu, hretry, l, b, hl, hwc, hslot⟩ := issued_not_queued hs hnh hiss
  have hheld := leaver_not_held hs hpc hiss
  obtain ⟨hlive, hrb⟩ := i2_same hs hiss
  have hlc : l ≠ c := hnh l b hl
  have hab : ∀ x ∈ s.g.abandoned, x = c := by
    intro x hx; have := (hs.issue.2.2.2 x hx).1; rw [hiss] at this; exact (Option.some.inj this).symm
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hntk : c ∉ s.g.taken := by
    intro h
    have := q7 c (by simp [h])
    simp only [List.mem_map] at this
    obtain ⟨e, he, hec⟩ := this
    exact hnu e he hec
  have hnwd : c ∉ s.g.withdrawn := by
    intro h; have := (q9 c h).1; rw [hpc] at this; simp [Pc.afterLeave] at this
  have hg' : s'.g.issued = s.g.issued ∧ s'.g.pending = s.g.pending ∧ s'.g.taken = s.g.taken ∧
      s'.g.withdrawn = s.g.withdrawn ∧ s'.g.retry = s.g.retry ∧
      s'.g.writtenThrough = s.g.writtenThrough ∧ s'.g.durableThrough = s.g.durableThrough ∧
      s'.g.nextTicket = s.g.nextTicket ∧ (∀ x, x ∈ s'.g.abandoned ↔ x = c ∨ x ∈ s.g.abandoned) := by
    rw [hg]; simp
  obtain ⟨g1, g2, g3, g4, g5, g6, g7, g8, g9⟩ := hg'
  have hobs : ∀ d, d ≠ c → (s'.tx d).obs = (s.tx d).obs := by intro d hd; rw [htxd d hd]
  have hunis : s'.unissued = s.unissued := by
    simp only [Sys.unissued_eq, hlead, hl]; rw [htxd l hlc]
  have hent : s'.issuedEntry = s.issuedEntry := by
    simp only [Sys.issuedEntry_eq, hlead, hl]; rw [htxd l hlc]
  have hslot' : s'.issuedSlot = s.issuedSlot := by
    simp only [Sys.issuedSlot_eq, hlead, hl]; rw [htxd l hlc]
  have hqueue : s'.queue = s.queue := by simp [Sys.queue, hunis, hent, g2]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]; simp [StatusOf, hheld, hlive]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      simp only [hpc] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hinlog, hinsync, hbatch, hlog, hsync, g1, g6, g7] at hld ⊢
      exact hld
  · intro d
    by_cases hd : d = c
    · subst hd; rw [TicketInv, htxc]; simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
    · have htd := hs.ticket d
      simp only [TicketInv, htxd d hd, hqueue, g5, g8, hinlog, hlog] at htd ⊢
      exact htd
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc, hheld] at hgd
      refine ⟨by simp [TxRec.holdsLock, hheld, hgd.1], ?_, ?_, ?_⟩
      · intro _ h; exact absurd ((g9 d).2 (Or.inl rfl)) h
      · intro _ _; exact Or.inl ((g9 d).2 (Or.inl rfl))
      · intro _ h; simp [hlive] at h
    · rw [GroupTxInv, htxd d hd, hlock, g1, g3, g2, hlead]
      rw [GroupTxInv] at hgd
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        exact hgd.2.1 h1 (fun h => h2 ((g9 d).2 (Or.inr h)))
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l', hl', hl2⟩
        · exact Or.inl ((g9 d).2 (Or.inr h))
        · refine Or.inr ⟨l', hl', ?_⟩
          rw [hl] at hl'; simp at hl'; subst hl'; rw [htxd _ hlc]; exact hl2
      · intro h1 h2
        obtain ⟨l', hl', hl2⟩ := hgd.2.2.2 h1 h2
        refine ⟨l', hl', ?_⟩
        rw [hl] at hl'; simp at hl'; subst hl'; rw [htxd _ hlc]; exact hl2
  · have hlo := leadOf_of_lead hs hl
    simp only [LeadInv, hlead, hl, LeadOf, htxd l hlc, hunis, hslot', hinlog, hlog, g5, g6] at hlo ⊢
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
    refine ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, ?_, ?_, ?_⟩
    · intro w hw
      obtain ⟨a1, a2, a3, a4, a5⟩ := h10 w hw
      have hwc' : w ≠ c := by intro h; subst h; rw [hpc] at a4; cases a4
      rw [htxd w hwc']
      refine ⟨a1, a2, a3, a4, ?_⟩
      intro h; rcases (g9 w).1 h with h | h
      · exact hwc' h
      · exact a5 h
    · intro w hw
      obtain ⟨a1, a2, a3, a4⟩ := h11 w hw
      have hwc' : w ≠ c := by intro h; subst h; rw [hpc] at a4; cases a4
      rw [htxd w hwc']; exact ⟨a1, a2, a3, a4⟩
    · intro hp
      by_cases hw : b.writing.tx = c
      · rw [hw, htxc]; have := h12 hp; rw [hw] at this; exact this
      · rw [htxd _ hw]; exact h12 hp
  · simp only [QueueInv, hqueue, hunis, g2, g3, g4, g5, g8, hinlog, hlead]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      refine ⟨a1, a2, a3, ?_, a5⟩
      by_cases hec : e.tx = c
      · rw [hec, htxc]; left; exact hlive
      · rw [htxd _ hec]; exact a4
    · intro e he
      rw [htxd _ (hnp e he)]; exact q4 e he
    · intro e he
      rw [htxd _ (hnu e he)]; exact q5 e he
    · intro x hx
      have hxc : x ≠ c := fun h => hntk (h ▸ hx)
      rw [htxd _ hxc]; exact q8 x hx
    · intro x hx
      have hxc : x ≠ c := fun h => hnwd (h ▸ hx)
      rw [htxd _ hxc]
      refine ⟨(q9 x hx).1, ?_⟩
      intro h; rcases (g9 x).1 h with h | h
      · exact hxc h
      · exact (q9 x hx).2 h
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot', g1, hlead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      by_cases hwc' : w = c
      · subst hwc'; rw [htxc]; exact ⟨hlive, hrb⟩
      · rw [htxd _ hwc']; exact i2 w hw
    · intro e he l' hl' hel
      by_cases hec : e.tx = c
      · right; exact (g9 _).2 (Or.inl hec)
      · rcases i3 e he l' hl' hel with h | h
        · left; rw [htxd _ hec]; exact h
        · right; exact (g9 _).2 (Or.inr h)
    · intro x hx
      rcases (g9 x).1 hx with h | h
      · subst h; rw [htxc]; exact ⟨hiss, rfl, hlive, hheld⟩
      · have := hab x h; subst this; rw [htxc]; exact ⟨hiss, rfl, hlive, hheld⟩
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, g5, g6, g7, g8, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · simpa only [OrderInv, hqueue, hslot', g6, g7] using hs.order
  · intro h hh
    rw [g5, hretry] at hh; cases hh
  · intro d e t hd he
    have ht : ∀ x, (s'.tx x).ticket = some t → (s.tx x).ticket = some t := by
      intro x hx
      by_cases hxc : x = c
      · subst hxc; rw [htxc] at hx; simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave] at hx
      · rw [htxd x hxc] at hx; exact hx
    exact hs.unique d e t (ht d hd) (ht e he)

end GroupCommit

namespace GroupCommit
open Sys

theorem leave_fields {g : Group} {tx : Nat} {t : Option Nat} (h : g.issued ≠ some tx) :
    (g.leave tx t).2 = false ∧
    (∀ e, e ∈ (g.leave tx t).1.pending ↔ e ∈ g.pending ∧ e.tx ≠ tx) ∧
    (g.leave tx t).1.pending.Sublist g.pending ∧
    (∀ x, x ∈ (g.leave tx t).1.retry ↔ x ∈ g.retry ∧ t ≠ some x) ∧
    (∀ x, x ∈ (g.leave tx t).1.taken ↔ x ∈ g.taken ∧ x ≠ tx) ∧
    (∀ x, x ∈ (g.leave tx t).1.withdrawn ↔ x ∈ g.withdrawn ∨ (x = tx ∧ tx ∈ g.taken)) ∧
    (g.leave tx t).1.issued = g.issued ∧ (g.leave tx t).1.abandoned = g.abandoned ∧
    (g.leave tx t).1.writtenThrough = g.writtenThrough ∧
    (g.leave tx t).1.durableThrough = g.durableThrough ∧
    (g.leave tx t).1.nextTicket = g.nextTicket := by
  unfold Group.leave
  simp only [h, ↓reduceIte]
  have hsub : (g.pending.filter (·.tx != tx)).Sublist g.pending := List.filter_sublist
  by_cases hc : g.taken.contains tx = true
  · simp only [hc, ↓reduceIte]
    have hc' : tx ∈ g.taken := by simpa using hc
    cases t <;> refine ⟨by simp, ?_, hsub, ?_, ?_, ?_, by simp, by simp, by simp, by simp, by simp⟩ <;>
      intro x <;> simp [List.mem_filter, hc'] <;> grind
  · simp only [hc, Bool.false_eq_true, ↓reduceIte]
    have hc' : tx ∉ g.taken := by simpa using hc
    cases t <;> refine ⟨by simp, ?_, hsub, ?_, ?_, ?_, by simp, by simp, by simp, by simp, by simp⟩ <;>
      intro x <;> simp [List.mem_filter, hc'] <;> grind

end GroupCommit

namespace GroupCommit
open Sys

/-- `leave` removes the commit from the group: its retry hole, its pending records, and
its taken record, which becomes withdrawn. The new state is given by its parts. -/
theorem step_leave_gen {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .dLeave)
    (hiss : s.g.issued ≠ some c) {s' : Sys}
    (hg : s'.g = (s.g.leave c (s.tx c).dropFrom.ticket).1)
    (hlead : s'.lead = s.lead) (hlog : s'.log = s.log) (hsync : s'.synced = s.synced)
    (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := .dC1 })
    (htxd : ∀ d, d ≠ c → s'.tx d = s.tx d) :
    Inv s' := by
  have hnh := notHolder_of_pc (c := c) hs.lead (by simp [hpc, Pc.holdsBatch, Pc.leading])
  obtain ⟨_, lp, lsub, lr, lt, lw, li, la, lwt, ldt, lnt⟩ :=
    leave_fields (t := (s.tx c).dropFrom.ticket) hiss
  rw [← hg] at lp lsub lr lt lw li la lwt ldt lnt
  have hab : c ∉ s.g.abandoned := not_abandoned_of_pc hs (by simp [hpc])
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hnwd : c ∉ s.g.withdrawn := by
    intro h; have := (q9 c h).1; rw [hpc] at this; simp [Pc.afterLeave] at this
  have hctk : (s.tx c).ticket = (s.tx c).dropFrom.ticket := by
    simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave]
  have htxl : ∀ l b, s.lead = some (l, b) → s'.tx l = s.tx l := fun l b hl => htxd l (hnh l b hl)
  have hunis : s'.unissued = s.unissued := by
    simp only [Sys.unissued_eq, hlead]
    split
    · rename_i l b hl; rw [htxl l b hl]
    · rfl
  have hent : s'.issuedEntry = s.issuedEntry := by
    simp only [Sys.issuedEntry_eq, hlead]
    split
    · rename_i l b hl; rw [htxl l b hl]
    · rfl
  have hslot : s'.issuedSlot = s.issuedSlot := by
    simp only [Sys.issuedSlot_eq, hlead]
    split
    · rename_i l b hl; rw [htxl l b hl]
    · rfl
  have hqueue : s'.queue = s.issuedEntry.toList ++ s.unissued ++ s'.g.pending := by
    simp [Sys.queue, hunis, hent]
  have hqsub : s'.queue.Sublist s.queue := by
    rw [hqueue]; exact List.Sublist.append (List.Sublist.refl _) lsub
  have hqmem : ∀ e ∈ s.queue, e.tx ≠ c → e ∈ s'.queue := by
    intro e he hec
    rw [hqueue]
    simp only [Sys.queue, List.mem_append] at he ⊢
    rcases he with he | he
    · exact Or.inl he
    · exact Or.inr ((lp e).2 ⟨he, hec⟩)
  have hpendc : ∀ e ∈ s'.g.pending, e.tx ≠ c := fun e he => ((lp e).1 he).2
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead]
  have hunc : ∀ e ∈ s.unissued, e.tx = c → c ∈ s.g.taken := by
    intro e he hec
    rcases q6 e he with h | h
    · rw [hec] at h; exact h
    · rw [hec] at h; exact absurd h hnwd
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd
      have hc := hs.status d
      simp only [StatusInv, StatusOf, hpc] at hc
      rw [StatusInv, htxc]; simpa [StatusOf] using hc
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hinlog, hinsync, hbatch, hlog, hsync, li, lwt, ldt] at hld ⊢
      simp only [hpc] at hld ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hinlog, hinsync, hbatch, hlog, hsync, li, lwt, ldt] at hld ⊢
      exact hld
  · intro d
    by_cases hd : d = c
    · subst hd; rw [TicketInv, htxc]; simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
    · have htd := hs.ticket d
      simp only [TicketInv, htxd d hd, lnt, hinlog, hlog] at htd ⊢
      intro t ht
      obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
      have hdt : (s.tx d).ticket = some t := by simpa using ht
      have hnc : (s.tx c).dropFrom.ticket ≠ some t := by
        intro h; rw [← hctk] at h; exact hd (hs.unique d c t hdt h)
      refine ⟨h1, h2, ?_, fun h => h4 ((lr t).1 h).1, fun h h' => h5 h ((lr t).1 h').1⟩
      rcases h3 with h | h | h
      · exact Or.inl h
      · right; left; exact hqmem _ h hd
      · right; right; exact (lr t).2 ⟨h, hnc⟩
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      rw [GroupTxInv, htxc, hlock, li, la, hlead]
      rw [GroupTxInv] at hgd
      simp only [TxRec.holdsLock, hpc] at hgd
      refine ⟨by simp [TxRec.holdsLock, hgd.1], ?_, by simp, by simp⟩
      intro _ _
      exact ⟨hiss, fun h => ((lt d).1 h).2 rfl, fun e he => hpendc e he⟩
    · rw [GroupTxInv, htxd d hd, hlock, li, la, hlead]
      rw [GroupTxInv] at hgd
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        exact ⟨a1, fun h => a2 ((lt d).1 h).1, fun e he => a3 e ((lp e).1 he).1⟩
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · exact Or.inl h
        · refine Or.inr ⟨l, hl', ?_⟩
          simp only [Option.mem_toList, Option.map_eq_some_iff] at hl'
          obtain ⟨⟨l', b⟩, hb, rfl⟩ := hl'
          rw [htxl _ _ hb]; exact hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        refine ⟨l, hl', ?_⟩
        simp only [Option.mem_toList, Option.map_eq_some_iff] at hl'
        obtain ⟨⟨l', b⟩, hb, rfl⟩ := hl'
        rw [htxl _ _ hb]; exact hl2
  · have hlo := hs.lead
    simp only [LeadInv, hlead] at hlo ⊢
    split
    · rename_i hn
      rw [hn] at hlo
      obtain ⟨ht0, hw0⟩ := hlo
      constructor
      · apply List.eq_nil_iff_forall_not_mem.2; intro x hx; rw [ht0] at lt; simpa using (lt x).1 hx
      · apply List.eq_nil_iff_forall_not_mem.2; intro x hx
        rcases (lw x).1 hx with h | ⟨_, h⟩
        · rw [hw0] at h; cases h
        · rw [ht0] at h; cases h
    · rename_i l b hl
      rw [hl] at hlo
      have hlc : l ≠ c := hnh l b hl
      simp only [LeadOf, htxl l b hl, hunis, hslot, hinlog, hlog, lwt] at hlo ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
      refine ⟨h1, h2, h3, h4, h5, h6, h7, h8, ?_, ?_, ?_, ?_⟩
      · intro hp
        apply List.eq_nil_iff_forall_not_mem.2; intro x hx
        rw [h9 hp] at lr; simpa using (lr x).1 hx
      · intro w hw
        obtain ⟨a1, a2, a3, a4, a5⟩ := h10 w hw
        have hwc : w ≠ c := by intro h; subst h; rw [hpc] at a4; cases a4
        rw [htxd w hwc, la]; exact ⟨a1, a2, a3, a4, a5⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4⟩ := h11 w hw
        have hwc : w ≠ c := by intro h; subst h; rw [hpc] at a4; cases a4
        rw [htxd w hwc]; exact ⟨a1, a2, a3, a4⟩
      · intro hp
        by_cases hw : b.writing.tx = c
        · exfalso; exact hiss (by rw [hs.issue.1, Sys.issuedSlot_eq, hl]; simp [hp, hw])
        · rw [htxd _ hw]; exact h12 hp
  · simp only [QueueInv, hunis, lnt, la, hinlog, hlead]
    refine ⟨List.Pairwise.sublist hqsub q1, List.Nodup.sublist (hqsub.map _) q2, ?_, ?_, ?_, ?_,
      ?_, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e (hqsub.subset he)
      refine ⟨a1, a2, a3, ?_, fun h => a5 ((lr _).1 h).1⟩
      by_cases hec : e.tx = c
      · rw [hec, htxc]; rw [hec] at a4
        rcases a4 with h | h
        · exact Or.inl h
        · exact absurd h hnwd
      · rw [htxd _ hec]
        rcases a4 with h | h
        · exact Or.inl h
        · exact Or.inr ((lw _).2 (Or.inl h))
    · intro e he
      have ⟨he', hec⟩ := (lp e).1 he
      rw [htxd _ hec]; exact q4 e he'
    · intro e he
      by_cases hec : e.tx = c
      · right; left; exact (lw _).2 (Or.inr ⟨hec, hunc e he hec⟩)
      · rw [htxd _ hec]
        rcases q5 e he with h | h | h
        · exact Or.inl h
        · exact Or.inr (Or.inl ((lw _).2 (Or.inl h)))
        · exact Or.inr (Or.inr h)
    · intro e he
      by_cases hec : e.tx = c
      · right; exact (lw _).2 (Or.inr ⟨hec, hunc e he hec⟩)
      · rcases q6 e he with h | h
        · left; exact (lt _).2 ⟨h, hec⟩
        · right; exact (lw _).2 (Or.inl h)
    · intro x hx
      rcases List.mem_append.1 hx with h | h
      · exact q7 x (by simp [((lt x).1 h).1])
      · rcases (lw x).1 h with h | ⟨hxc, h⟩
        · exact q7 x (by simp [h])
        · exact q7 x (by rw [hxc]; simp [h])
    · intro x hx
      obtain ⟨hx', hxc⟩ := (lt x).1 hx
      rw [htxd _ hxc]
      refine ⟨?_, (q8 x hx').2⟩
      intro h
      rcases (lw x).1 h with h | ⟨h, _⟩
      · exact (q8 x hx').1 h
      · exact hxc h
    · intro x hx
      rcases (lw x).1 hx with h | ⟨h, _⟩
      · have hxc : x ≠ c := fun h' => hnwd (h' ▸ h)
        rw [htxd _ hxc]; exact q9 x h
      · subst h; rw [htxc]; exact ⟨by simp [Pc.afterLeave], hab⟩
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot, li, la, hlead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      have hwc : w ≠ c := by intro h; subst h; simp at hw; exact hiss hw
      rw [htxd _ hwc]; exact i2 w hw
    · intro e he l hl' hel
      have hec : e.tx ≠ c := by
        intro h; apply hiss; rw [i1]; simp only [Option.mem_toList] at he; rw [he]; simp [h]
      rw [htxd _ hec]; exact i3 e he l hl' hel
    · intro x hx
      have hxc : x ≠ c := fun h => hab (h ▸ hx)
      rw [htxd _ hxc]; exact i4 x hx
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, lwt, ldt, lnt, hsync, hlog]
    exact ⟨fun h hh => m1 h ((lr h).1 hh).1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hslot, lwt, ldt]
    exact ⟨fun e he => o1 e (hqsub.subset he), o2, o3⟩
  · intro h hh
    obtain ⟨hh', hnc⟩ := (lr h).1 hh
    obtain ⟨d, hd⟩ := hs.holes h hh'
    have hdc : d ≠ c := by intro h'; subst h'; rw [hctk] at hd; exact hnc hd
    exact ⟨d, by rw [htxd _ hdc]; exact hd⟩
  · intro d e t hd he
    have ht : ∀ x, (s'.tx x).ticket = some t → (s.tx x).ticket = some t := by
      intro x hx
      by_cases hxc : x = c
      · subst hxc; rw [htxc] at hx; simp [TxRec.ticket, Pc.waitTicket, Pc.beforeLeave] at hx
      · rw [htxd x hxc] at hx; exact hx
    exact hs.unique d e t (ht d hd) (ht e he)

end GroupCommit
