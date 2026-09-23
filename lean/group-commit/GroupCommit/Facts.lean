import GroupCommit.Frame

/-!
# Consequences of the invariant
-/

namespace GroupCommit
open Sys

theorem not_in_queue {s : Sys} {c : Nat} (hq : QueueInv s) (hiss : IssueInv s)
    (hnh : NotHolder s c) (ht : (s.tx c).ticket = none)
    (hleave : (s.tx c).pc.afterLeave = false) (hnl : (s.tx c).pc ≠ .dLeave) :
    ∀ e ∈ s.queue, e.tx ≠ c := by
  obtain ⟨_, _, _, hpend, hunis, _, _, _, hwd⟩ := hq
  obtain ⟨_, _, hent, habd⟩ := hiss
  intro e he hec
  subst hec
  simp only [Sys.queue, List.mem_append, Option.mem_toList] at he
  rcases he with (he | he) | he
  · cases hl : s.lead with
    | none => simp [Sys.issuedEntry_eq, hl] at he
    | some p =>
      obtain ⟨l, b⟩ := p
      have hlne : l ≠ e.tx := hnh l b hl
      have hslot : e ∈ s.issuedSlot.toList := by
        simp only [Sys.issuedEntry_eq, hl] at he
        simp only [Sys.issuedSlot_eq, hl]
        split at he
        · rename_i hi; simp [hi] at he ⊢; exact he.symm
        · simp at he
      have := hent e hslot l (by simp [hl]) (Ne.symm hlne)
      rcases this with h | h
      · simp [ht] at h
      · have := (habd e.tx h).2.1
        simp [this, Pc.afterLeave] at hleave
  · rcases hunis e he with h | h | h
    · simp [ht] at h
    · have := (hwd e.tx h).1
      simp [hleave] at this
    · cases hl : s.lead with
      | none => simp [hl] at h
      | some p => obtain ⟨l, b⟩ := p; simp [hl] at h; exact hnh l b hl h
  · rcases hpend e he with h | h
    · simp [ht] at h
    · exact hnl h.1

theorem mem_unissued_queue {s : Sys} {e : Entry} (he : e ∈ s.unissued) : e ∈ s.queue := by
  simp [Sys.queue, he]

theorem mem_pending_queue {s : Sys} {e : Entry} (he : e ∈ s.g.pending) : e ∈ s.queue := by
  simp [Sys.queue, he]

/-- A thread that is not the leader, owns no ticket, and did not leave the
group is not referenced by any other part of the invariant. -/
theorem unref_of_notHolder {s : Sys} {c : Nat} (hs : Inv s) (hnh : NotHolder s c)
    (ht : (s.tx c).ticket = none) (ha : (s.tx c).pc.afterLeave = false)
    (hl : (s.tx c).pc ≠ .dLeave) : Unreferenced s c := by
  have hnq := not_in_queue hs.queue hs.issue hnh ht ha hl
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  obtain ⟨i1, i2, i3, i4⟩ := hs.issue
  refine ⟨hnh, hnq, ?_, ?_, ?_, ?_, ?_, by simp [ht]⟩
  · intro hi
    rw [i1] at hi
    cases hl' : s.lead with
    | none => simp [Sys.issuedSlot_eq, hl'] at hi
    | some p =>
      obtain ⟨l, b⟩ := p
      simp only [Option.map_eq_some_iff] at hi
      obtain ⟨e, he, hec⟩ := hi
      have hlc : l ≠ c := hnh l b hl'
      rcases i3 e (by simp [he]) l (by simp [hl']) (by rw [hec]; exact Ne.symm hlc) with h | h
      · rw [hec, ht] at h; simp at h
      · rw [hec] at h
        have := (i4 c h).2.1
        rw [this] at ha; simp [Pc.afterLeave] at ha
  · intro h
    have := (i4 c h).2.1
    rw [this] at ha; simp [Pc.afterLeave] at ha
  · intro h
    have := q7 c (by simp [h])
    simp only [List.mem_map] at this
    obtain ⟨e, he, hec⟩ := this
    exact hnq e (mem_unissued_queue he) hec
  · intro h
    have := (q9 c h).1
    rw [this] at ha; simp at ha
  · intro l b hlead
    have hlo := hs.lead
    simp only [LeadInv, hlead, LeadOf] at hlo
    obtain ⟨_, _, _, _, _, _, _, _, _, h10, h11, _⟩ := hlo
    constructor
    · intro hp
      have := (h10 c (by simp [hp, Pc.rollingBackOther])).2.2.2.1
      rw [this] at ha; simp [Pc.afterLeave] at ha
    · intro hp
      have := (h11 c (by simp [hp, Pc.removingOther])).2.2.2
      rw [this] at ha; simp [Pc.afterLeave] at ha

theorem unref_of_plain {s : Sys} {c : Nat} (hs : Inv s) (ht : (s.tx c).ticket = none)
    (hb : (s.tx c).pc.holdsBatch = false) (ha : (s.tx c).pc.afterLeave = false)
    (hl : (s.tx c).pc ≠ .dLeave) : Unreferenced s c :=
  unref_of_notHolder hs (notHolder_of_pc hs.lead hb) ht ha hl

theorem holdsLock_iff {s : Sys} (hs : Inv s) (c : Nat) :
    (s.tx c).holdsLock = true ↔ s.lockHolder = some c := (hs.groupTx c).1

end GroupCommit

namespace GroupCommit
open Sys

theorem mem_take_mono {α : Type} {l : List α} {x : α} {n m : Nat} (h : x ∈ l.take n)
    (hnm : n ≤ m) : x ∈ l.take m := by
  have : l.take n = (l.take m).take n := by
    rw [List.take_take, Nat.min_eq_left hnm]
  rw [this] at h
  exact List.mem_of_mem_take h

/-- An fsync that covers the whole log keeps the invariant. -/
theorem inv_raise_synced {s : Sys} (hs : Inv s) : Inv (s.withSynced s.log.length) := by
  obtain ⟨hst, hlog, htk, hgtx, hlead, hq, hiss, hmark, hord, hholes, huniq⟩ := hs
  have hle : s.synced ≤ s.log.length := hmark.2.2.2.1
  have hmono : ∀ d, s.InSynced d → (s.withSynced s.log.length).InSynced d := by
    intro d ⟨r, hr, hrd⟩
    exact ⟨r, mem_take_mono hr hle, hrd⟩
  refine ⟨hst, ?_, htk, hgtx, hlead, hq, hiss, ?_, hord, hholes, huniq⟩
  · intro d
    have h := hlog d
    simp only [LogInv] at h ⊢
    obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12, h13, _, h15, h16⟩ := h
    refine ⟨h1, h2, h3, h4, fun ha => hmono d (h5 ha), h6, h7, h8, fun hp => hmono d (h9 hp),
      h10, h11, h12, h13, by simp, ?_, h16⟩
    intro t ht
    obtain ⟨hw, hdur⟩ := h15 t ht
    exact ⟨hw, fun hd => mem_take_mono (hdur hd) hle⟩
  · obtain ⟨m1, m2, m3, _, m5⟩ := hmark
    exact ⟨m1, m2, m3, Nat.le_refl _, m5⟩

end GroupCommit

namespace GroupCommit
open Sys

theorem ticket_of_wait {r : TxRec} {t : Nat} (h : r.pc.waitTicket = some t) : r.ticket = some t := by
  simp [TxRec.ticket, h]

theorem notHolder_of_wait {s : Sys} {c t : Nat} (hs : Inv s) (h : (s.tx c).pc.waitTicket = some t) :
    NotHolder s c := by
  apply notHolder_of_pc hs.lead
  cases hp : (s.tx c).pc <;> simp_all [Pc.waitTicket, Pc.holdsBatch, Pc.leading]

/-- A waiter whose ticket is durable is not referenced by other parts. -/
theorem unref_of_acked {s : Sys} {c t : Nat} (hs : Inv s)
    (hw : (s.tx c).pc.waitTicket = some t) (hdur : t ≤ s.g.durableThrough) :
    Unreferenced s c := by
  have hlog : s.InLog c := by
    have h := (hs.log c).2.2.2.2.2.2.2.2.2.2.2.2.2.2.1 t (by simp [hw])
    exact ⟨_, List.mem_of_mem_take (h.2 hdur), rfl⟩
  have hnh := notHolder_of_wait hs hw
  have hnq : ∀ e ∈ s.queue, e.tx ≠ c := fun e he hec => (hs.queue.2.2.1 e he).2.2.1 (hec ▸ hlog)
  have hnd : (s.tx c).pc ≠ .dropped := by intro h; rw [h] at hw; simp [Pc.waitTicket] at hw
  have hna : (s.tx c).pc.afterLeave = false := by
    cases hp : (s.tx c).pc <;> simp_all [Pc.waitTicket, Pc.afterLeave]
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  obtain ⟨i1, i2, i3, i4⟩ := hs.issue
  refine ⟨hnh, hnq, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro hi
    rw [i1] at hi
    cases hl' : s.lead with
    | none => simp [Sys.issuedSlot_eq, hl'] at hi
    | some p =>
      obtain ⟨l, b⟩ := p
      simp only [Option.map_eq_some_iff] at hi
      obtain ⟨e, he, hec⟩ := hi
      have hlc : l ≠ c := hnh l b hl'
      rcases i3 e (by simp [he]) l (by simp [hl']) (by rw [hec]; exact Ne.symm hlc) with h | h
      · rw [hec, ticket_of_wait hw] at h
        have hte : t = e.ticket := Option.some.inj h
        have hord := hs.order.2.1 e (by simp [he])
        omega
      · rw [hec] at h
        exact hnd (i4 c h).2.1
  · intro h; exact hnd (i4 c h).2.1
  · intro h
    have := q7 c (by simp [h])
    simp only [List.mem_map] at this
    obtain ⟨e, he, hec⟩ := this
    exact hnq e (mem_unissued_queue he) hec
  · intro h
    have := (q9 c h).1
    rw [hna] at this; simp at this
  · intro l b hlead
    have hlo := hs.lead
    simp only [LeadInv, hlead, LeadOf] at hlo
    obtain ⟨_, _, _, _, _, _, _, _, _, h10, h11, _⟩ := hlo
    constructor
    · intro hp
      exact hnd (h10 c (by simp [hp, Pc.rollingBackOther])).2.2.2.1
    · intro hp
      exact hnd (h11 c (by simp [hp, Pc.removingOther])).2.2.2
  · intro t' ht' hr
    rw [ticket_of_wait hw] at ht'
    simp at ht'; subst ht'
    have := hs.ticket c
    simp only [TicketInv, ticket_of_wait hw] at this
    exact (this t (by simp)).2.2.2.1 hr hlog

end GroupCommit

namespace GroupCommit
open Sys

theorem notHolder_of_batchOf {s : Sys} {c : Nat} (h : s.batchOf c = none) : NotHolder s c := by
  intro l b hl hlc
  subst hlc
  simp [batchOf_eq, hl] at h

end GroupCommit

namespace GroupCommit
open Sys

/-- At `own3`, the record of the batch belongs to the issued transaction. -/
theorem writing_ne_of_own3 {s : Sys} {c l : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (l, b)) (hp : (s.tx l).pc = .own3) (hni : s.g.issued ≠ some c) :
    b.writing.tx ≠ c := by
  intro h
  apply hni
  rw [hs.issue.1, Sys.issuedSlot_eq, hl]
  simp [hp, h]

end GroupCommit

namespace GroupCommit
open Sys

/-- The log write of a transaction is not submitted before `WriteLogicalLog`
and after `FinishLogicalLogWrite`. -/
theorem submitted_false {s : Sys} {c : Nat} (hs : Inv s)
    (hpc : (s.tx c).pc = .upgrade ∨ (s.tx c).pc = .write ∨ (s.tx c).pc = .own2 ∨
      (s.tx c).pc = .own3) :
    (s.tx c).submitted = false := by
  have hc := hs.status c
  rcases hpc with h | h | h | h <;> simp only [StatusInv, StatusOf, h] at hc <;> exact hc.2.2

end GroupCommit
