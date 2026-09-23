import GroupCommit.Steps.Plain

/-!
# `BeginCommitLogicalLog` with group commit: `enqueue`
-/

namespace GroupCommit
open Sys

theorem ticket_le_next {s : Sys} (hs : Inv s) {d t : Nat} (h : (s.tx d).ticket = some t) :
    t ≤ s.g.nextTicket := by
  have := hs.ticket d
  simp only [TicketInv, h] at this
  exact (this t (by simp)).2.1

theorem step_enqueue_aux {s : Sys} {c : Nat} (r' : TxRec) (hs : Inv s) (hpc : (s.tx c).pc = .begin)
    (hex : (s.tx c).excl = false)
    (hr' : r' = { s.tx c with pc := .await (s.g.nextTicket + 1) }) :
    Inv ((s.set c r').withG (s.g.enqueue c)) := by
  have hc := status_of (c := c) hs
  simp only [StatusOf, hpc, hex] at hc
  have hu := unref_of_plain (c := c) hs (by simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave])
    (by simp [hpc, Pc.holdsBatch, Pc.leading]) (by simp [hpc, Pc.afterLeave]) (by simp [hpc])
  have hnh := hu.notHolder
  have hnotlog : ¬ s.InLog c := (hs.log c).2.2.2.2.2.2.2.2.2.2.2.1 hpc
  obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
  obtain ⟨o1, o2, o3⟩ := hs.order
  have hr'pc : r'.pc = .await (s.g.nextTicket + 1) := by rw [hr']
  have hr'st : r'.st = .live := by rw [hr']; exact hc.1
  have hr'held : r'.held = false := by rw [hr']; exact hc.2.1
  have hr'sub : r'.submitted = false := by rw [hr']; exact hc.2.2
  have hr'excl : r'.excl = false := by rw [hr']; exact hex
  have hr'tk : r'.ticket = some (s.g.nextTicket + 1) := by
    simp [TxRec.ticket, hr'pc, Pc.waitTicket]
  have hqueue : ((s.set c r').withG (s.g.enqueue c)).queue =
      s.queue ++ [⟨s.g.nextTicket + 1, c⟩] := by
    simp [Sys.queue, issuedEntry_set _ hnh, unissued_set _ hnh]
  have hqle : ∀ e ∈ s.queue, e.ticket ≤ s.g.nextTicket := fun e he => (hs.queue.2.2.1 e he).2.1
  have hretry : ∀ h ∈ s.g.retry, h ≤ s.g.nextTicket := fun h hh => (m1 h hh).2
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; simp [StatusInv, StatusOf, hr'pc, hr'st, hr'held, hr'excl, hr'sub]
    · simpa [StatusInv, hd] using hs.status d
  · intro d
    by_cases hd : d = c
    · subst hd
      have hlc := hs.log d
      simp only [LogInv] at hlc ⊢
      simp only [sys_simps, ite_true, Group.enqueue_issued, Group.enqueue_writtenThrough,
        Group.enqueue_durableThrough, hr'pc, hpc] at hlc ⊢
      have hw : s.g.writtenThrough < s.g.nextTicket + 1 := by omega
      have hdur : s.g.durableThrough < s.g.nextTicket + 1 := by omega
      rw [hr'] at ⊢
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · have hld := hs.log d
      simp only [LogInv] at hld ⊢
      simpa [hd] using hld
  · intro d
    by_cases hd : d = c
    · subst hd
      simp only [TicketInv, sys_simps, ite_true, hr'tk, hqueue]
      intro t ht
      simp at ht; subst ht
      refine ⟨by omega, by simp, by simp, ?_, ?_⟩
      · intro h; exact absurd (hretry _ h) (by omega)
      · intro h; rw [hr'pc] at h; simp at h
    · have htd := hs.ticket d
      simp only [TicketInv, sys_simps, hd, ite_false, hqueue] at htd ⊢
      intro t ht
      obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
      refine ⟨h1, by omega, ?_, h4, h5⟩
      rcases h3 with h | h | h
      · exact Or.inl h
      · exact Or.inr (Or.inl (List.mem_append_left _ h))
      · exact Or.inr (Or.inr h)
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, TxRec.holdsLock, hpc, hc.2] at hgd
      have hlk : s.lockHolder ≠ some d := fun h => by simpa using hgd.1.2 h
      simp [GroupTxInv, TxRec.holdsLock, hr'pc, hr'held, Pc.afterLeave, hlk]
    · simp only [GroupTxInv, sys_simps, hd, ite_false, Group.enqueue_issued, Group.enqueue_taken,
        Group.enqueue_abandoned, Group.enqueue_pending] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2
        refine ⟨a1, a2, ?_⟩
        intro e he
        simp only [List.mem_append, List.mem_singleton] at he
        rcases he with he | he
        · exact a3 e he
        · subst he; exact Ne.symm hd
      · intro h1 h2
        rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl, hl2⟩
        · exact Or.inl h
        · exact Or.inr ⟨l, hl, by simpa [lead_ne_of_notHolder hnh hl] using hl2⟩
      · intro h1 h2
        obtain ⟨l, hl, hl2⟩ := hgd.2.2.2 h1 h2
        exact ⟨l, hl, by simpa [lead_ne_of_notHolder hnh hl] using hl2⟩
  · have hlead := hs.lead
    simp only [LeadInv, sys_simps] at hlead ⊢
    split
    · rename_i hl; rw [hl] at hlead; simpa using hlead
    · rename_i l b hl
      rw [hl] at hlead
      have hlc : l ≠ c := hnh l b hl
      simp only [LeadOf, sys_simps, hlc, ite_false, Group.enqueue_writtenThrough,
        Group.enqueue_retry, Group.enqueue_abandoned, unissued_set _ hnh,
        issuedSlot_set _ hnh] at hlead ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlead
      refine ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, ?_, ?_, ?_⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4, a5⟩ := h10 w hw
        have hwc : w ≠ c := by intro h; subst h; rw [hpc] at a4; simp at a4
        simp only [hwc, ite_false]; exact ⟨a1, a2, a3, a4, a5⟩
      · intro w hw
        obtain ⟨a1, a2, a3, a4⟩ := h11 w hw
        have hwc : w ≠ c := by intro h; subst h; rw [hpc] at a4; simp at a4
        simp only [hwc, ite_false]; exact ⟨a1, a2, a3, a4⟩
      · intro hp
        simp only [writing_ne_of_own3 hs hl hp hu.notIssued, ite_false]
        exact h12 hp
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    have hpend : ∀ e ∈ s.g.pending, e.tx ≠ c := fun e he => hu.notQueued e (mem_pending_queue he)
    have hun : ∀ e ∈ s.unissued, e.tx ≠ c := fun e he => hu.notQueued e (mem_unissued_queue he)
    simp only [QueueInv, hqueue, sys_simps, unissued_set _ hnh, Group.enqueue_pending,
      Group.enqueue_nextTicket, Group.enqueue_retry, Group.enqueue_withdrawn, Group.enqueue_taken,
      Group.enqueue_abandoned]
    refine ⟨?_, ?_, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · refine List.pairwise_append.2 ⟨q1, by simp, ?_⟩
      intro a ha b hb
      simp at hb; subst hb
      have := hqle a ha; simp; omega
    · simp only [List.map_append, List.map_cons, List.map_nil]
      refine List.nodup_append.2 ⟨q2, by simp, ?_⟩
      intro a ha b hb
      simp at hb; subst hb
      simp only [List.mem_map] at ha
      obtain ⟨e, he, rfl⟩ := ha
      exact hu.notQueued e he
    · intro e he
      simp only [List.mem_append, List.mem_singleton] at he
      rcases he with he | he
      · obtain ⟨h1, h2, h3, h4, h5⟩ := q3 e he
        have hec : e.tx ≠ c := hu.notQueued e he
        refine ⟨h1, by omega, h3, by simpa [hec] using h4, h5⟩
      · subst he
        refine ⟨by simp, by simp, hnotlog, by simp [hr'st], ?_⟩
        intro h; have h2 := hretry _ h; simp only at h2; omega
    · intro e he
      simp only [List.mem_append, List.mem_singleton] at he
      rcases he with he | he
      · simpa [hpend e he] using q4 e he
      · subst he; simp [hr'tk]
    · intro e he
      simpa [hun e he] using q5 e he
    · intro x hx
      have hxc : x ≠ c := fun h => hu.notTaken (h ▸ hx)
      simpa [hxc] using q8 x hx
    · intro x hx
      have hxc : x ≠ c := fun h => hu.notWithdrawn (h ▸ hx)
      simpa [hxc] using q9 x hx
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, sys_simps, issuedSlot_set _ hnh, Group.enqueue_issued,
      Group.enqueue_abandoned]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      have hwc : w ≠ c := by intro h; subst h; simp at hw; exact hu.notIssued hw
      simpa [hwc] using i2 w hw
    · intro e he l hl hel
      have hec : e.tx ≠ c := by
        intro h
        have : s.g.issued = some c := by
          rw [i1]; simp only [Option.mem_toList] at he; rw [he]; simp [h]
        exact hu.notIssued this
      simpa [hec] using i3 e he l hl hel
    · intro x hx
      have hxc : x ≠ c := fun h => hu.notAbandoned (h ▸ hx)
      simpa [hxc] using i4 x hx
  · simp only [MarkInv, sys_simps, Group.enqueue_retry, Group.enqueue_nextTicket,
      Group.enqueue_writtenThrough, Group.enqueue_durableThrough]
    refine ⟨fun h hh => ⟨(m1 h hh).1, by have := (m1 h hh).2; omega⟩, by omega, by omega, m4, m5⟩
  · simp only [OrderInv, hqueue, sys_simps, issuedSlot_set _ hnh, Group.enqueue_writtenThrough,
      Group.enqueue_durableThrough]
    refine ⟨?_, o2, o3⟩
    intro e he
    simp only [List.mem_append, List.mem_singleton] at he
    rcases he with he | he
    · exact o1 e he
    · subst he; simp; omega
  · intro h hh
    simp only [sys_simps, Group.enqueue_retry] at hh
    obtain ⟨d, hd⟩ := hs.holes h hh
    have hdc : d ≠ c := by
      intro h'; subst h'; simp [TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave] at hd
    exact ⟨d, by simpa [hdc] using hd⟩
  · intro d e t hd he
    simp only [sys_simps] at hd he
    by_cases hdc : d = c <;> by_cases hec : e = c
    · rw [hdc, hec]
    · simp only [hdc, ite_true, hr'tk] at hd
      simp only [hec, ite_false] at he
      have := ticket_le_next hs he
      simp at hd; omega
    · simp only [hec, ite_true, hr'tk] at he
      simp only [hdc, ite_false] at hd
      have := ticket_le_next hs hd
      simp at he; omega
    · simp only [hdc, hec, ite_false] at hd he
      exact hs.unique d e t hd he

end GroupCommit

namespace GroupCommit

theorem step_enqueue {s : Sys} {c : Nat} (hs : Inv s) (hpc : (s.tx c).pc = .begin)
    (hex : (s.tx c).excl = false) :
    Inv ((s.set c { s.tx c with pc := .await (s.g.nextTicket + 1) }).withG (s.g.enqueue c)) :=
  step_enqueue_aux _ hs hpc hex rfl

end GroupCommit
