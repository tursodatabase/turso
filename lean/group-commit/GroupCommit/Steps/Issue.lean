import GroupCommit.Steps.Holder

/-!
# `WriteLogicalLog` of the leader: `try_issue`
-/

namespace GroupCommit
open Sys

theorem abandoned_nil {s : Sys} (hs : Inv s) (h : s.g.issued = none) : s.g.abandoned = [] := by
  cases ha : s.g.abandoned with
  | nil => rfl
  | cons x xs =>
    have := (hs.issue.2.2.2 x (by simp [ha])).1
    rw [h] at this; cases this

theorem rolledBack_false_of_live {s : Sys} {d : Nat} (hs : Inv s) (h : (s.tx d).st = .live) :
    (s.tx d).rolledBack = false := by
  have := (hs.log d).2.2.2.2.2.2.1
  cases hr : (s.tx d).rolledBack
  · rfl
  · have := (this hr).1; rw [h] at this; simp at this

/-- Facts about the batch of a leader at `WriteLogicalLog`. -/
structure WriteFacts (s : Sys) (c : Nat) (b : Batch) : Prop where
  unissued : s.unissued = b.writing :: b.rest
  issuedEntry : s.issuedEntry = none
  issuedSlot : s.issuedSlot = none
  issued : s.g.issued = none
  abandoned : s.g.abandoned = []
  queue : s.queue = b.writing :: (b.rest ++ s.g.pending)
  restNe : ∀ e ∈ b.rest, e.tx ≠ b.writing.tx
  pendingNe : ∀ e ∈ s.g.pending, e.tx ≠ b.writing.tx
  notLog : ¬ s.InLog b.writing.tx
  retry : s.g.retry = []

theorem writeFacts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .write ∨ (s.tx c).pc = .upgrade) : WriteFacts s c b := by
  have hunis : s.unissued = b.writing :: b.rest := by
    rcases hpc with h | h <;> simp [Sys.unissued_eq, hl, TxRec.writingUnissued, h]
  have hent : s.issuedEntry = none := by
    rcases hpc with h | h <;> simp [Sys.issuedEntry_eq, hl, TxRec.issuing, h]
  have hslot : s.issuedSlot = none := by
    rcases hpc with h | h <;> simp [Sys.issuedSlot_eq, hl, TxRec.issuing, h]
  have hiss : s.g.issued = none := by rw [hs.issue.1, hslot]; rfl
  have hq : s.queue = b.writing :: (b.rest ++ s.g.pending) := by simp [Sys.queue, hent, hunis]
  have hnd := hs.queue.2.1
  rw [hq] at hnd
  simp only [List.map_cons, List.nodup_cons, List.map_append, List.mem_append, List.mem_map] at hnd
  refine ⟨hunis, hent, hslot, hiss, abandoned_nil hs hiss, hq, ?_, ?_, ?_, ?_⟩
  · intro e he h; exact hnd.1 (Or.inl ⟨e, he, h⟩)
  · intro e he h; exact hnd.1 (Or.inr ⟨e, he, h⟩)
  · exact (hs.queue.2.2.1 b.writing (by simp [hq])).2.2.1
  · have := (leadOf_of_lead hs hl).2.2.2.2.2.2.2.2.1
    rcases hpc with h | h <;> exact this (Or.inl (by simp [h, Pc.leading]))

/-- `try_issue` finds that the record is still wanted, and the leader issues `log_tx`. -/
theorem step_issue_aux {s : Sys} {c : Nat} {b : Batch} (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .write)
    (hnw : b.writing.tx ∉ s.g.withdrawn)
    (hr' : r' = { s.tx c with pc := .writeIssued }) :
    Inv ((s.set c r').withG
      { s.g with taken := setErase b.writing.tx s.g.taken, issued := some b.writing.tx }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  have wf := writeFacts hs hl (Or.inl hpc)
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hwq : b.writing ∈ s.queue := by simp [wf.queue]
  have hwtaken : b.writing.tx ∈ s.g.taken := by
    rcases q6 b.writing (by simp [wf.unissued]) with h | h
    · exact h
    · exact absurd h hnw
  have hwlive : (s.tx b.writing.tx).st = .live := by
    rcases (q3 b.writing hwq).2.2.2.1 with h | h
    · exact h
    · exact absurd h hnw
  have hwrb := rolledBack_false_of_live hs hwlive
  have hwafter : (s.tx b.writing.tx).pc.afterLeave = false := (q8 _ hwtaken).2
  have hr'pc : r'.pc = .writeIssued := by rw [hr']
  have hobs : (s.tx c).obs = r'.obs := by
    rw [hr']; simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave,
      Pc.beq_eq]
  generalize hgdef :
    { s.g with taken := setErase b.writing.tx s.g.taken, issued := some b.writing.tx } = g'
  have hg'iss : g'.issued = some b.writing.tx := by rw [← hgdef]
  have hg'tk : g'.taken = setErase b.writing.tx s.g.taken := by rw [← hgdef]
  have hg'wd : g'.withdrawn = s.g.withdrawn := by rw [← hgdef]
  have hg'pend : g'.pending = s.g.pending := by rw [← hgdef]
  have hg'retry : g'.retry = s.g.retry := by rw [← hgdef]
  have hg'ab : g'.abandoned = s.g.abandoned := by rw [← hgdef]
  have hg'wt : g'.writtenThrough = s.g.writtenThrough := by rw [← hgdef]
  have hg'dt : g'.durableThrough = s.g.durableThrough := by rw [← hgdef]
  have hg'nt : g'.nextTicket = s.g.nextTicket := by rw [← hgdef]
  have htx : ∀ d, ((s.set c r').withG g').tx d = if d = c then r' else s.tx d := by intro d; simp
  have hg : ((s.set c r').withG g').g = g' := by simp
  have hlead : ((s.set c r').withG g').lead = some (c, b) := by simp [hl]
  have hlog : ((s.set c r').withG g').log = s.log := by simp
  have hsync : ((s.set c r').withG g').synced = s.synced := by simp
  have hlock : ((s.set c r').withG g').lockHolder = s.lockHolder := by simp
  generalize ((s.set c r').withG g') = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc, hobs]
    · rw [htxd d hd]
  have huni : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hr'pc]
  have hent : s'.issuedEntry = some b.writing := by
    simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hslot : s'.issuedSlot = some b.writing := by
    simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hqueue : s'.queue = s.queue := by
    rw [wf.queue]; simp [Sys.queue, hent, huni, hg, hg'pend]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead, hl]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc, hr']; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync, hg'iss, hg'wt, hg'dt] at hld ⊢
      simp only [hpc, wf.issued, batchOf_holder hl] at hld ⊢
      rw [hr']
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      simp only [wf.issued] at hld
      grind
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hqueue, hg, hg'retry, hg'nt, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw
    by_cases hd : d = c
    · subst hd; rw [htxc, hr'pc] at hw; cases hw
    · rw [htxd d hd] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock, hg] at hgd ⊢
      have hold := holder_holdsLock hs hl
      rw [hold] at hgd
      rw [hr']
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · simp only [GroupTxInv, htxd d hd, hlock, hg, hlead, hg'iss, hg'tk, hg'pend, hg'ab] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        refine ⟨?_, fun h => a2 (mem_setErase.1 h).2, a3⟩
        intro h; simp only [Option.some.injEq] at h; subst h
        rw [hwafter] at h1; cases h1
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · exact Or.inl h
        · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, LeadOf, htxc, hr'pc, hg, huni, hslot, hg'wt, hg'retry, hinlog, hlog]
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
    simp only [hpc, TxRec.issuing, TxRec.writingUnissued, Pc.leading, Pc.holdsBatch,
      wf.unissued, wf.issuedSlot] at h1 h2 h6 h8 h9
    refine ⟨by rw [hr']; exact h1, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, h5,
      fun _ => h6 (by simp), by simp, ?_, fun _ => h9 (by simp), by simp [Pc.rollingBackOther],
      by simp [Pc.removingOther], by simp⟩
    intro _ _ _
    rcases h8 (by simp) (by simp) (by simp) with h | ⟨e, he, hec⟩ | h
    · exact Or.inl h
    · simp only [List.mem_cons] at he
      rcases he with he | he
      · right; right; subst he; simp [hec]
      · right; left; exact ⟨e, he, hec⟩
    · simp at h
  · simp only [QueueInv, hqueue, huni, hg, hg'pend, hg'wd, hg'tk, hg'retry, hg'nt, hinlog, hlead]
    refine ⟨q1, q2, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      exact ⟨a1, a2, a3, by rw [obs_st (hobsd e.tx)]; exact a4, a5⟩
    · intro e he
      rcases q4 e he with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd e.tx)).2 h
    · intro e he
      rcases q5 e (by simp [wf.unissued, he]) with h | h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr (by simpa [hl] using h))
    · intro e he
      rcases q6 e (by simp [wf.unissued, he]) with h | h
      · left; exact mem_setErase.2 ⟨wf.restNe e he, h⟩
      · exact Or.inr h
    · intro x hx
      simp only [List.mem_append, mem_setErase] at hx
      rcases hx with ⟨hxw, hx⟩ | hx
      · have := q7 x (by simp [hx])
        rw [wf.unissued] at this
        simp only [List.map_cons, List.mem_cons] at this
        rcases this with h | h
        · exact absurd h hxw
        · exact h
      · have := q7 x (by simp [hx])
        rw [wf.unissued] at this
        simp only [List.map_cons, List.mem_cons] at this
        rcases this with h | h
        · subst h; exact absurd hx hnw
        · exact h
    · intro x hx
      have := q8 x (mem_setErase.1 hx).2
      exact ⟨this.1, by rw [obs_afterLeave (hobsd x)]; exact this.2⟩
    · intro x hx
      have := q9 x hx
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact this.1, by rw [hg'ab]; exact this.2⟩
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot, hg, hg'iss, hg'ab, hlead, wf.abandoned]
    refine ⟨rfl, ?_, ?_, by simp⟩
    · intro w hw
      simp at hw; subst hw
      rw [obs_st (hobsd _), obs_rolledBack (hobsd _)]
      exact ⟨hwlive, hwrb⟩
    · intro e he l hl' hel
      simp at he hl'; subst he; subst hl'
      rcases q5 b.writing (by simp [wf.unissued]) with h | h | h
      · left; rw [obs_ticket (hobsd _)]; exact h
      · exact absurd h hnw
      · simp [hl] at h; exact absurd h.symm hel
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hg'retry, hg'nt, hg'wt, hg'dt, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hqueue, hslot, hg, hg'wt, hg'dt]
    refine ⟨o1, ?_, o3⟩
    intro e he
    simp at he; subst he
    have := o1 _ hwq
    omega
  · intro h hh
    simp only [hg, hg'retry] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    exact ⟨d, by rw [obs_ticket (hobsd d)]; exact hd⟩
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

theorem ticket_none_of_afterLeave {r : TxRec} (h : r.pc.afterLeave = true) : r.ticket = none := by
  simp only [TxRec.ticket]
  cases hp : r.pc <;> simp_all [Pc.afterLeave, Pc.waitTicket, Pc.beforeLeave]

/-- Facts about a record that `try_issue` skips because its commit left the group. -/
structure SkipFacts (s : Sys) (c : Nat) (b : Batch) : Prop where
  afterLeave : (s.tx b.writing.tx).pc.afterLeave = true
  notAbandoned : b.writing.tx ∉ s.g.abandoned
  ne : b.writing.tx ≠ c
  notTaken : b.writing.tx ∉ s.g.taken
  ticket : (s.tx b.writing.tx).ticket = none

theorem skipFacts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hpc : (s.tx c).pc = .write)
    (hw : b.writing.tx ∈ s.g.withdrawn) : SkipFacts s c b := by
  have h9 := hs.queue.2.2.2.2.2.2.2.2 _ hw
  have hne : b.writing.tx ≠ c := by
    intro h; have := h9.1; rw [h, hpc] at this; simp [Pc.afterLeave] at this
  exact ⟨h9.1, h9.2, hne, fun h => (hs.queue.2.2.2.2.2.2.2.1 _ h).1 hw,
    ticket_none_of_afterLeave h9.1⟩

/-- `try_issue` skips the record of a commit that left, and the leader goes on
with the next record of the batch. -/
theorem step_skip_next_aux {s : Sys} {c : Nat} {b : Batch} {e : Entry} {rest : List Entry}
    (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .write)
    (hw : b.writing.tx ∈ s.g.withdrawn) (hrest : b.rest = e :: rest)
    (hr' : r' = { s.tx c with pc := .upgrade }) :
    Inv (((s.withLead (some (c, { b with writing := e, rest := rest }))).set c r').withG
      { s.g with taken := setErase b.writing.tx s.g.taken,
                 withdrawn := setErase b.writing.tx s.g.withdrawn }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  have wf := writeFacts hs hl (Or.inl hpc)
  have sf := skipFacts hs hpc hw
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hr'pc : r'.pc = .upgrade := by rw [hr']
  have hobs : (s.tx c).obs = r'.obs := by
    rw [hr']; simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave,
      Pc.beq_eq]
  have hwe : b.writing.ticket < e.ticket := by
    have := List.pairwise_cons.1 (wf.queue ▸ q1)
    exact this.1 e (by simp [hrest])
  generalize hbdef : ({ b with writing := e, rest := rest } : Batch) = b'
  have hb'w : b'.writing = e := by rw [← hbdef]
  have hb'r : b'.rest = rest := by rw [← hbdef]
  have hb'a : b'.advanced = b.advanced := by rw [← hbdef]
  generalize hgdef : { s.g with taken := setErase b.writing.tx s.g.taken,
                                withdrawn := setErase b.writing.tx s.g.withdrawn } = g'
  have hg'iss : g'.issued = s.g.issued := by rw [← hgdef]
  have hg'tk : g'.taken = setErase b.writing.tx s.g.taken := by rw [← hgdef]
  have hg'wd : g'.withdrawn = setErase b.writing.tx s.g.withdrawn := by rw [← hgdef]
  have hg'pend : g'.pending = s.g.pending := by rw [← hgdef]
  have hg'retry : g'.retry = s.g.retry := by rw [← hgdef]
  have hg'ab : g'.abandoned = s.g.abandoned := by rw [← hgdef]
  have hg'wt : g'.writtenThrough = s.g.writtenThrough := by rw [← hgdef]
  have hg'dt : g'.durableThrough = s.g.durableThrough := by rw [← hgdef]
  have hg'nt : g'.nextTicket = s.g.nextTicket := by rw [← hgdef]
  have htx : ∀ d, (((s.withLead (some (c, b'))).set c r').withG g').tx d =
      if d = c then r' else s.tx d := by intro d; simp
  have hg : (((s.withLead (some (c, b'))).set c r').withG g').g = g' := by simp
  have hlead : (((s.withLead (some (c, b'))).set c r').withG g').lead = some (c, b') := by simp
  have hlog : (((s.withLead (some (c, b'))).set c r').withG g').log = s.log := by simp
  have hsync : (((s.withLead (some (c, b'))).set c r').withG g').synced = s.synced := by simp
  have hlock : (((s.withLead (some (c, b'))).set c r').withG g').lockHolder = s.lockHolder := by
    simp
  generalize (((s.withLead (some (c, b'))).set c r').withG g') = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc, hobs]
    · rw [htxd d hd]
  have huni : s'.unissued = e :: rest := by
    simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hr'pc, hb'w, hb'r]
  have hent : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hslot : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hqueue : s'.queue = e :: (rest ++ s.g.pending) := by
    simp [Sys.queue, hent, huni, hg, hg'pend]
  have hqsub : ∀ x ∈ s'.queue, x ∈ s.queue := by
    intro x hx; rw [hqueue] at hx; rw [wf.queue, hrest]; simp_all
  have hqne : ∀ x ∈ s'.queue, x.tx ≠ b.writing.tx := by
    intro x hx; rw [hqueue] at hx
    simp only [List.mem_cons, List.mem_append] at hx
    rcases hx with h | h | h
    · subst h; exact wf.restNe _ (by simp [hrest])
    · exact wf.restNe _ (by simp [hrest, h])
    · exact wf.pendingNe _ h
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, d ≠ c → s'.batchOf d = s.batchOf d := by
    intro d hd; simp [Sys.batchOf_eq, hlead, hl, Ne.symm hd]
  have hbatchc : s'.batchOf c = some b' := by simp [Sys.batchOf_eq, hlead]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc, hr']; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatchc, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      simp only [hpc, batchOf_holder hl] at hld ⊢
      rw [hr']
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch d hd, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      exact hld
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hg, hg'retry, hg'nt, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, ?_, h4, ?_⟩
    · rcases h3 with h | h | h
      · exact Or.inl h
      · right; left
        have hdw : d ≠ b.writing.tx := by
          intro hdw; subst hdw; rw [sf.ticket] at ht; simp at ht
        rw [wf.queue, hrest] at h
        rw [hqueue]
        simp only [List.mem_cons, List.mem_append] at h ⊢
        rcases h with h | h
        · exact absurd (congrArg Entry.tx h) (by simpa using hdw)
        · simpa [or_assoc] using h
      · exact Or.inr (Or.inr h)
    · intro hw'
      by_cases hd : d = c
      · subst hd; rw [htxc, hr'pc] at hw'; cases hw'
      · rw [htxd d hd] at hw'; exact h5 hw'
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock, hg] at hgd ⊢
      have hold := holder_holdsLock hs hl
      rw [hold] at hgd
      rw [hr']
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · simp only [GroupTxInv, htxd d hd, hlock, hg, hlead, hg'iss, hg'tk, hg'pend, hg'ab] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        exact ⟨a1, fun h => a2 (mem_setErase.1 h).2, a3⟩
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · exact Or.inl h
        · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, LeadOf, htxc, hr'pc, hg, huni, hslot, hg'wt, hg'retry, hinlog, hlog,
      hb'w, hb'r, hb'a]
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
    simp only [hpc, TxRec.issuing, TxRec.writingUnissued, Pc.leading, Pc.holdsBatch,
      wf.unissued, wf.issuedSlot] at h1 h2 h6 h8 h9
    refine ⟨by rw [hr']; exact h1, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, ?_,
      ?_, by simp, ?_, fun _ => h9 (by simp), by simp [Pc.rollingBackOther],
      by simp [Pc.removingOther], by simp⟩
    · intro a ha
      obtain ⟨a1, a2⟩ := h5 a ha
      exact ⟨a1, by omega⟩
    · intro _ ha
      have := (h5 e.ticket (by simp [ha])).2
      omega
    · intro _ _ _
      rcases h8 (by simp) (by simp) (by simp) with h | ⟨x, hx, hxc⟩ | h
      · exact Or.inl h
      · simp only [List.mem_cons] at hx
        rcases hx with hx | hx
        · subst hx; exact absurd hxc sf.ne
        · right; left; rw [hrest] at hx; exact ⟨x, hx, hxc⟩
      · simp at h
  · simp only [QueueInv, hg, hg'pend, hg'wd, hg'tk, hg'retry, hg'nt, hinlog, hlead, huni]
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · rw [hqueue]
      have := List.pairwise_cons.1 (wf.queue ▸ q1)
      rw [hrest] at this
      exact this.2
    · rw [hqueue]
      have := wf.queue ▸ q2
      rw [hrest] at this
      simp only [List.map_cons, List.nodup_cons] at this
      simpa using this.2
    · intro x hx
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 x (hqsub x hx)
      refine ⟨a1, a2, a3, ?_, a5⟩
      rw [obs_st (hobsd x.tx)]
      rcases a4 with h | h
      · exact Or.inl h
      · exact Or.inr (mem_setErase.2 ⟨hqne x hx, h⟩)
    · intro x hx
      rcases q4 x hx with h | h
      · left; rw [obs_ticket (hobsd x.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd x.tx)).2 h
    · intro x hx
      have hx' : x ∈ s.unissued := by rw [wf.unissued, hrest]; simp [hx]
      have hxw : x.tx ≠ b.writing.tx := wf.restNe x (by rw [hrest]; exact hx)
      rcases q5 x hx' with h | h | h
      · left; rw [obs_ticket (hobsd x.tx)]; exact h
      · exact Or.inr (Or.inl (mem_setErase.2 ⟨hxw, h⟩))
      · exact Or.inr (Or.inr (by simpa [hl] using h))
    · intro x hx
      have hx' : x ∈ s.unissued := by rw [wf.unissued, hrest]; simp [hx]
      have hxw : x.tx ≠ b.writing.tx := wf.restNe x (by rw [hrest]; exact hx)
      rcases q6 x hx' with h | h
      · left; exact mem_setErase.2 ⟨hxw, h⟩
      · right; exact mem_setErase.2 ⟨hxw, h⟩
    · intro x hx
      simp only [List.mem_append, mem_setErase] at hx
      have hxw : x ≠ b.writing.tx := by rcases hx with ⟨h, _⟩ | ⟨h, _⟩ <;> exact h
      have hx2 : x ∈ s.g.taken ++ s.g.withdrawn := by
        rcases hx with ⟨_, h⟩ | ⟨_, h⟩ <;> simp [h]
      have := q7 x hx2
      rw [wf.unissued, hrest] at this
      simp only [List.map_cons, List.mem_cons] at this
      rcases this with h | h
      · exact absurd h hxw
      · simp only [List.map_cons, List.mem_cons]; exact h
    · intro x hx
      have := q8 x (mem_setErase.1 hx).2
      exact ⟨fun h => this.1 (mem_setErase.1 h).2, by rw [obs_afterLeave (hobsd x)]; exact this.2⟩
    · intro x hx
      have := q9 x (mem_setErase.1 hx).2
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact this.1, by rw [hg'ab]; exact this.2⟩
  · simp only [IssueInv, hslot, hg, hg'iss, hg'ab, hlead, wf.abandoned, wf.issued]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hg'retry, hg'nt, hg'wt, hg'dt, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hslot, hg, hg'wt, hg'dt]
    exact ⟨fun x hx => o1 x (hqsub x hx), by simp, o3⟩
  · intro h hh
    simp only [hg, hg'retry, wf.retry] at hh
    cases hh
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

/-- `try_issue` skips the last record of the batch, and the leader goes to the fsync. -/
theorem step_skip_last_aux {s : Sys} {c : Nat} {b : Batch} (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .write)
    (hw : b.writing.tx ∈ s.g.withdrawn) (hrest : b.rest = [])
    (hr' : r' = { s.tx c with pc := .syncLog }) :
    Inv ((s.set c r').withG
      { s.g with taken := setErase b.writing.tx s.g.taken,
                 withdrawn := setErase b.writing.tx s.g.withdrawn }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  have wf := writeFacts hs hl (Or.inl hpc)
  have sf := skipFacts hs hpc hw
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hr'pc : r'.pc = .syncLog := by rw [hr']
  have hobs : (s.tx c).obs = r'.obs := by
    rw [hr']; simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave,
      Pc.beq_eq]
  have hcin : s.InLog c := by
    have h8 := hlo.2.2.2.2.2.2.2.1
    simp only [hpc, Pc.leading, wf.unissued, wf.issuedSlot, hrest] at h8
    rcases h8 (by simp) (by simp) (by simp) with h | ⟨x, hx, hxc⟩ | h
    · exact h
    · simp at hx; subst hx; exact absurd hxc sf.ne
    · simp at h
  generalize hgdef : { s.g with taken := setErase b.writing.tx s.g.taken,
                                withdrawn := setErase b.writing.tx s.g.withdrawn } = g'
  have hg'iss : g'.issued = s.g.issued := by rw [← hgdef]
  have hg'tk : g'.taken = setErase b.writing.tx s.g.taken := by rw [← hgdef]
  have hg'wd : g'.withdrawn = setErase b.writing.tx s.g.withdrawn := by rw [← hgdef]
  have hg'pend : g'.pending = s.g.pending := by rw [← hgdef]
  have hg'retry : g'.retry = s.g.retry := by rw [← hgdef]
  have hg'ab : g'.abandoned = s.g.abandoned := by rw [← hgdef]
  have hg'wt : g'.writtenThrough = s.g.writtenThrough := by rw [← hgdef]
  have hg'dt : g'.durableThrough = s.g.durableThrough := by rw [← hgdef]
  have hg'nt : g'.nextTicket = s.g.nextTicket := by rw [← hgdef]
  have htx : ∀ d, ((s.set c r').withG g').tx d = if d = c then r' else s.tx d := by intro d; simp
  have hg : ((s.set c r').withG g').g = g' := by simp
  have hlead : ((s.set c r').withG g').lead = some (c, b) := by simp [hl]
  have hlog : ((s.set c r').withG g').log = s.log := by simp
  have hsync : ((s.set c r').withG g').synced = s.synced := by simp
  have hlock : ((s.set c r').withG g').lockHolder = s.lockHolder := by simp
  generalize ((s.set c r').withG g') = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc, hobs]
    · rw [htxd d hd]
  have huni : s'.unissued = [] := by
    simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hr'pc, hrest]
  have hent : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hslot : s'.issuedSlot = none := by
    simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hqueue : s'.queue = s.g.pending := by
    simp [Sys.queue, hent, huni, hg, hg'pend]
  have hqsub : ∀ x ∈ s'.queue, x ∈ s.queue := by
    intro x hx; rw [hqueue] at hx; rw [wf.queue, hrest]; simp_all
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, s'.batchOf d = s.batchOf d := by intro d; simp [Sys.batchOf_eq, hlead, hl]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc, hr']; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatch, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      simp only [hpc, batchOf_holder hl] at hld ⊢
      rw [hr']
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch d, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      exact hld
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hg, hg'retry, hg'nt, hinlog, hlog] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, ?_, h4, ?_⟩
    · rcases h3 with h | h | h
      · exact Or.inl h
      · right; left
        have hdw : d ≠ b.writing.tx := by
          intro hdw; subst hdw; rw [sf.ticket] at ht; simp at ht
        rw [wf.queue, hrest] at h
        rw [hqueue]
        simp only [List.mem_cons, List.nil_append] at h
        rcases h with h | h
        · exact absurd (congrArg Entry.tx h) (by simpa using hdw)
        · exact h
      · exact Or.inr (Or.inr h)
    · intro hw'
      by_cases hd : d = c
      · subst hd; rw [htxc, hr'pc] at hw'; cases hw'
      · rw [htxd d hd] at hw'; exact h5 hw'
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, htxc, hlock, hg] at hgd ⊢
      have hold := holder_holdsLock hs hl
      rw [hold] at hgd
      rw [hr']
      simp [TxRec.holdsLock, Pc.afterLeave, hgd.1]
    · simp only [GroupTxInv, htxd d hd, hlock, hg, hlead, hg'iss, hg'tk, hg'pend, hg'ab] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        exact ⟨a1, fun h => a2 (mem_setErase.1 h).2, a3⟩
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
        · exact Or.inl h
        · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
        rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, LeadOf, htxc, hr'pc, hg, huni, hslot, hg'wt, hg'retry, hinlog, hlog]
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlo
    simp only [hpc, TxRec.issuing, TxRec.writingUnissued, Pc.leading, Pc.holdsBatch,
      wf.unissued, wf.issuedSlot] at h1 h2 h6 h8 h9
    refine ⟨by rw [hr']; exact h1, by simp [Pc.holdsBatch, Pc.leading], fun _ => hrest, by simp,
      h5, by simp [TxRec.issuing, TxRec.writingUnissued, hr'pc], by simp, by simp,
      fun _ => h9 (by simp), by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
  · simp only [QueueInv, hg, hg'pend, hg'wd, hg'tk, hg'retry, hg'nt, hinlog, hlead, huni]
    refine ⟨?_, ?_, ?_, ?_, by simp, by simp, ?_, ?_, ?_⟩
    · rw [hqueue]
      have := wf.queue ▸ q1
      rw [hrest] at this
      exact (List.pairwise_cons.1 this).2
    · rw [hqueue]
      have := wf.queue ▸ q2
      rw [hrest] at this
      simp only [List.map_cons, List.nodup_cons, List.nil_append] at this
      exact this.2
    · intro x hx
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 x (hqsub x hx)
      refine ⟨a1, a2, a3, ?_, a5⟩
      rw [obs_st (hobsd x.tx)]
      rcases a4 with h | h
      · exact Or.inl h
      · rw [hqueue] at hx
        exact Or.inr (mem_setErase.2 ⟨wf.pendingNe x hx, h⟩)
    · intro x hx
      rcases q4 x hx with h | h
      · left; rw [obs_ticket (hobsd x.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd x.tx)).2 h
    · intro x hx
      simp only [List.mem_append, mem_setErase] at hx
      have hxw : x ≠ b.writing.tx := by rcases hx with ⟨h, _⟩ | ⟨h, _⟩ <;> exact h
      have hx2 : x ∈ s.g.taken ++ s.g.withdrawn := by
        rcases hx with ⟨_, h⟩ | ⟨_, h⟩ <;> simp [h]
      have := q7 x hx2
      rw [wf.unissued, hrest] at this
      simp at this
      exact absurd this hxw
    · intro x hx
      have := q8 x (mem_setErase.1 hx).2
      exact ⟨fun h => this.1 (mem_setErase.1 h).2, by rw [obs_afterLeave (hobsd x)]; exact this.2⟩
    · intro x hx
      have := q9 x (mem_setErase.1 hx).2
      exact ⟨by rw [obs_afterLeave (hobsd x)]; exact this.1, by rw [hg'ab]; exact this.2⟩
  · simp only [IssueInv, hslot, hg, hg'iss, hg'ab, hlead, wf.abandoned, wf.issued]
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hg'retry, hg'nt, hg'wt, hg'dt, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hslot, hg, hg'wt, hg'dt]
    exact ⟨fun x hx => o1 x (hqsub x hx), by simp, o3⟩
  · intro h hh
    simp only [hg, hg'retry, wf.retry] at hh
    cases hh
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit
