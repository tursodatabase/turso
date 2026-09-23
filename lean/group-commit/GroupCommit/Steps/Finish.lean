import GroupCommit.Steps.Issue

/-!
# `FinishLogicalLogWrite` of the leader: the record is in the log
-/

namespace GroupCommit
open Sys

theorem lock_unique {s : Sys} {c d : Nat} (hs : Inv s) (hc : (s.tx c).holdsLock = true)
    (hd : (s.tx d).holdsLock = true) : c = d := by
  have h1 := (hs.groupTx c).1.1 hc
  have h2 := (hs.groupTx d).1.1 hd
  rw [h1] at h2; exact Option.some.inj h2

theorem holder_holdsLock_true {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) : (s.tx c).holdsLock = true := by
  rw [holder_holdsLock hs hl]; exact holder_status hs hl

/-- While a leader holds the batch, no other thread is at `GroupPrefixSynced`. -/
theorem not_prefixSynced_of_holder {s : Sys} {c d : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hd : d ≠ c) : (s.tx d).pc.isPrefixSynced = false := by
  cases hp : (s.tx d).pc with
  | prefixSynced t =>
    exfalso
    have hst := hs.status d
    simp only [StatusInv, StatusOf, hp] at hst
    have : (s.tx d).holdsLock = true := by simp [TxRec.holdsLock, hp, hst.2.1]
    exact hd (lock_unique hs this (holder_holdsLock_true hs hl))
  | _ => rfl

/-- The step kinds of a transaction whose record the leader issued. -/
theorem issued_owner_pc {s : Sys} {c : Nat} {b : Batch} (hs : Inv s)
    (hl : s.lead = some (c, b)) (hslot : s.issuedSlot = some b.writing) (hne : b.writing.tx ≠ c) :
    (s.tx b.writing.tx).pc ≠ .begin ∧ (s.tx b.writing.tx).pc ≠ .upgrade ∧
      (s.tx b.writing.tx).pc ≠ .write ∧ (s.tx b.writing.tx).pc ≠ .finish := by
  rcases hs.issue.2.2.1 b.writing (by simp [hslot]) c (by simp [hl]) hne with h | h
  · simp only [TxRec.ticket] at h
    cases hp : (s.tx b.writing.tx).pc <;> simp_all [Pc.waitTicket, Pc.beforeLeave]
  · have := (hs.issue.2.2.2 _ h).2.1
    simp [this]

/-- Facts about the batch of a leader whose log write is issued. -/
structure IssuedFacts (s : Sys) (c : Nat) (b : Batch) : Prop where
  unissued : s.unissued = b.rest
  issuedEntry : s.issuedEntry = some b.writing
  issuedSlot : s.issuedSlot = some b.writing
  issued : s.g.issued = some b.writing.tx
  queue : s.queue = b.writing :: (b.rest ++ s.g.pending)
  restNe : ∀ e ∈ b.rest, e.tx ≠ b.writing.tx
  pendingNe : ∀ e ∈ s.g.pending, e.tx ≠ b.writing.tx
  notLog : ¬ s.InLog b.writing.tx
  retry : s.g.retry = []
  live : (s.tx b.writing.tx).st = .live
  notRolledBack : (s.tx b.writing.tx).rolledBack = false
  written : s.g.writtenThrough < b.writing.ticket
  durable : s.g.durableThrough < b.writing.ticket
  ticketLe : b.writing.ticket ≤ s.g.nextTicket

theorem issuedFacts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .finish) : IssuedFacts s c b := by
  have hunis : s.unissued = b.rest := by simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hpc]
  have hent : s.issuedEntry = some b.writing := by
    simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hpc]
  have hslot : s.issuedSlot = some b.writing := by
    simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hpc]
  have hiss : s.g.issued = some b.writing.tx := by rw [hs.issue.1, hslot]; rfl
  have hq : s.queue = b.writing :: (b.rest ++ s.g.pending) := by simp [Sys.queue, hent, hunis]
  have hnd := hs.queue.2.1
  rw [hq] at hnd
  simp only [List.map_cons, List.nodup_cons, List.map_append, List.mem_append, List.mem_map] at hnd
  have hw := hs.queue.2.2.1 b.writing (by simp [hq])
  have hi2 := hs.issue.2.1 b.writing.tx (by simp [hiss])
  refine ⟨hunis, hent, hslot, hiss, hq, ?_, ?_, hw.2.2.1, ?_, hi2.1, hi2.2, ?_, ?_, hw.2.1⟩
  · intro e he h; exact hnd.1 (Or.inl ⟨e, he, h⟩)
  · intro e he h; exact hnd.1 (Or.inr ⟨e, he, h⟩)
  · exact (leadOf_of_lead hs hl).2.2.2.2.2.2.2.2.1 (Or.inl (by simp [hpc, Pc.leading]))
  · exact hs.order.1 b.writing (by simp [hq])
  · exact hs.order.2.1 b.writing (by simp [hslot])

theorem noteWritten_up {g : Group} {t : Nat} (hr : g.retry = []) (ht : g.writtenThrough < t) :
    g.noteWritten t = { g with writtenThrough := t } := by
  unfold Group.noteWritten
  rw [ite_eq_right (by simp [hr]), Nat.max_eq_right (Nat.le_of_lt ht)]

/-- `FinishLogicalLogWrite` of the leader: the offset moves past the record of the batch. -/
theorem step_finish_aux {s : Sys} {c : Nat} {b : Batch} (r' : TxRec) (hs : Inv s)
    (hl : s.lead = some (c, b)) (hpc : (s.tx c).pc = .finish)
    (hr' : r' = { s.tx c with pc := .own2, submitted := false }) :
    Inv ((((s.withLead (some (c, { b with advanced := some b.writing.ticket }))).set c r').withLog
      (s.log ++ [⟨b.writing.tx, some b.writing.ticket⟩])).withG
      { s.g with writtenThrough := b.writing.ticket }) := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  have xf := issuedFacts hs hl hpc
  have hsyn : s.synced ≤ s.log.length := hs.mark.2.2.2.1
  have hsynced : ∀ d, ((s.tx d).pc = .endCommit ∧ (s.batchOf d).isSome) ∨
      (s.tx d).pc.isPrefixSynced → False := by
    intro d h
    by_cases hd : d = c
    · subst hd; rcases h with ⟨h, _⟩ | h <;> simp [hpc, Pc.isPrefixSynced] at h
    · rcases h with ⟨_, h⟩ | h
      · simp [Sys.batchOf_eq, hl, Ne.symm hd] at h
      · rw [not_prefixSynced_of_holder hs hl hd] at h; cases h
  have hwpc : b.writing.tx ≠ c → (s.tx b.writing.tx).pc ≠ .begin ∧
      (s.tx b.writing.tx).pc ≠ .upgrade ∧ (s.tx b.writing.tx).pc ≠ .write ∧
      (s.tx b.writing.tx).pc ≠ .finish := issued_owner_pc hs hl xf.issuedSlot
  obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
  have hr'pc : r'.pc = .own2 := by rw [hr']
  have hobs : (s.tx c).obs = r'.obs := by
    rw [hr']; simp [TxRec.obs, TxRec.ticket, hpc, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave,
      Pc.beq_eq]
  generalize hbdef : ({ b with advanced := some b.writing.ticket } : Batch) = b'
  have hb'w : b'.writing = b.writing := by rw [← hbdef]
  have hb'r : b'.rest = b.rest := by rw [← hbdef]
  have hb'a : b'.advanced = some b.writing.ticket := by rw [← hbdef]
  generalize hgdef : { s.g with writtenThrough := b.writing.ticket } = g'
  have hg'iss : g'.issued = s.g.issued := by rw [← hgdef]
  have hg'tk : g'.taken = s.g.taken := by rw [← hgdef]
  have hg'wd : g'.withdrawn = s.g.withdrawn := by rw [← hgdef]
  have hg'pend : g'.pending = s.g.pending := by rw [← hgdef]
  have hg'retry : g'.retry = s.g.retry := by rw [← hgdef]
  have hg'ab : g'.abandoned = s.g.abandoned := by rw [← hgdef]
  have hg'wt : g'.writtenThrough = b.writing.ticket := by rw [← hgdef]
  have hg'dt : g'.durableThrough = s.g.durableThrough := by rw [← hgdef]
  have hg'nt : g'.nextTicket = s.g.nextTicket := by rw [← hgdef]
  generalize hxdef : (⟨b.writing.tx, some b.writing.ticket⟩ : Rec) = x
  have hxtx : x.tx = b.writing.tx := by rw [← hxdef]
  have htx : ∀ d, ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').tx d =
      if d = c then r' else s.tx d := by intro d; simp
  have hg : ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').g = g' := by
    simp
  have hlead : ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').lead =
      some (c, b') := by simp
  have hlog : ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').log =
      s.log ++ [x] := by simp
  have hsync : ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').synced =
      s.synced := by simp
  have hlock :
      ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g').lockHolder =
      s.lockHolder := by simp
  generalize ((((s.withLead (some (c, b'))).set c r').withLog (s.log ++ [x])).withG g') = s' at *
  have htxc : s'.tx c = r' := by simp [htx]
  have htxd : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [htx, hd]
  have hobsd : ∀ d, (s'.tx d).obs = (s.tx d).obs := by
    intro d; by_cases hd : d = c
    · subst hd; rw [htxc, hobs]
    · rw [htxd d hd]
  have huni : s'.unissued = b.rest := by
    simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hr'pc, hb'r]
  have hent : s'.issuedEntry = none := by
    simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, hr'pc]
  have hslot : s'.issuedSlot = some b.writing := by
    simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, hr'pc, hb'w]
  have hqueue : s'.queue = b.rest ++ s.g.pending := by
    simp [Sys.queue, hent, huni, hg, hg'pend]
  have hqsub : ∀ e ∈ s'.queue, e ∈ s.queue := by
    intro e he; rw [hqueue] at he; rw [xf.queue]; simp_all
  have hqne : ∀ e ∈ s'.queue, e.tx ≠ b.writing.tx := by
    intro e he; rw [hqueue] at he
    simp only [List.mem_append] at he
    rcases he with h | h
    · exact xf.restNe e h
    · exact xf.pendingNe e h
  have hqgt : ∀ e ∈ s'.queue, b.writing.ticket < e.ticket := by
    intro e he
    have := List.pairwise_cons.1 (xf.queue ▸ q1)
    rw [hqueue] at he
    exact this.1 e he
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d ∨ d = b.writing.tx := by
    intro d; simp only [Sys.InLog, hlog, List.mem_append, List.mem_singleton]
    constructor
    · rintro ⟨r, hr | hr, hrd⟩
      · exact Or.inl ⟨r, hr, hrd⟩
      · subst hr; exact Or.inr (hxtx ▸ hrd.symm)
    · rintro (⟨r, hr, hrd⟩ | hd)
      · exact ⟨r, Or.inl hr, hrd⟩
      · exact ⟨x, Or.inr rfl, by rw [hxtx, hd]⟩
  have htake : s'.log.take s'.synced = s.log.take s.synced := by
    rw [hlog, hsync, List.take_append_of_le_length hsyn]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp only [Sys.InSynced, htake]
  have hbatch : ∀ d, d ≠ c → s'.batchOf d = s.batchOf d := by
    intro d hd; simp [Sys.batchOf_eq, hlead, hl, Ne.symm hd]
  have hbatchc : s'.batchOf c = some b' := by simp [Sys.batchOf_eq, hlead]
  have hmemlog : ∀ y, y ∈ s.log → y ∈ s'.log := by intro y hy; rw [hlog]; simp [hy]
  have hxlog : x ∈ s'.log := by rw [hlog]; simp
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc, hr']; simp [StatusOf, hc]
    · rw [StatusInv, htxd d hd]; exact hs.status d
  · intro d
    have hld := hs.log d
    have hsy := hsynced d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatchc, hlog, hsync, hg'iss, hg'wt,
        hg'dt] at hld ⊢
      simp only [hpc, batchOf_holder hl, xf.issued] at hld hsy ⊢
      rw [hr']
      grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · have htd := hs.ticket d
      simp only [LogInv, htxd d hd, hg, hinlog, hinsync, hbatch d hd, hlog, hsync, hg'iss, hg'wt,
        hg'dt, xf.issued] at hld ⊢
      obtain ⟨l1, l2, l3, l4, l5, l6, l7, l8, l9, l10, l11, l12, l13, l14, l15, l16⟩ := hld
      refine ⟨fun h => Or.inl (l1 h), ?_, ?_, fun h => Or.inl (l4 h), l5, l6, l7, l8, l9,
        fun h => Or.inl (l10 h), fun h1 h2 => Or.inl (l11 h1 h2), ?_, ?_, ?_, ?_, l16⟩
      · rintro (h | h) hst
        · exact l2 h hst
        · right; left; rw [h]
      · rintro (h | h)
        · exact l3 h
        · subst h; exact ⟨by rw [xf.live]; simp, xf.notRolledBack⟩
      · intro hb
        rintro (h | h)
        · exact l12 hb h
        · subst h; exact (hwpc hd).1 hb
      · intro hp hb
        rintro (h | h)
        · exact l13 hp hb h
        · subst h
          rcases hp with hp | hp | hp
          · exact (hwpc hd).2.1 hp
          · exact (hwpc hd).2.2.1 hp
          · exact (hwpc hd).2.2.2 hp
      · intro h; exact absurd h (fun h' => hsy (by simpa using h'))
      · intro t ht
        obtain ⟨w1, w2⟩ := l15 t ht
        refine ⟨?_, fun h => by rw [List.take_append_of_le_length hsyn]; exact w2 h⟩
        intro hle
        by_cases hlt : t ≤ s.g.writtenThrough
        · exact List.mem_append_left _ (w1 hlt)
        · have htk : (s.tx d).ticket = some t := by
            simp only [TxRec.ticket]
            simp only [Option.mem_toList] at ht
            simp [ht]
          simp only [TicketInv, htk] at htd
          obtain ⟨_, _, h3, _, _⟩ := htd t (by simp)
          rcases h3 with h | h | h
          · exact List.mem_append_left _ h
          · rw [xf.queue] at h
            simp only [List.mem_cons] at h
            rcases h with h | h
            · rw [← hxdef, ← h]; simp
            · have := (List.pairwise_cons.1 (xf.queue ▸ q1)).1 _ h
              simp only at this; omega
          · rw [xf.retry] at h; cases h
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hg, hg'retry, hg'nt, xf.retry] at htd ⊢
    rw [obs_ticket (hobsd d)]
    intro t ht
    obtain ⟨h1, h2, h3, _, _⟩ := htd t ht
    refine ⟨h1, h2, ?_, by simp, by simp⟩
    rcases h3 with h | h | h
    · exact Or.inl (hmemlog _ h)
    · rw [xf.queue] at h
      simp only [List.mem_cons] at h
      rcases h with h | h
      · left; rw [← hxdef, ← h] at hxlog; exact hxlog
      · right; left; rw [hqueue]; exact h
    · simp at h
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
      refine ⟨hgd.1, hgd.2.1, ?_, ?_⟩
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
    simp only [hpc, Pc.leading, xf.unissued, xf.issuedSlot] at h1 h8 h9
    refine ⟨by rw [hr']; exact h1, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, ?_,
      by simp [TxRec.issuing, TxRec.writingUnissued, hr'pc], fun _ => ⟨trivial, by rw [← hxdef]; simp⟩,
      ?_, fun _ => h9 (by simp), by simp [Pc.rollingBackOther], by simp [Pc.removingOther],
      by simp⟩
    · intro a ha; simp at ha; subst ha; simp
    · intro _ _ _
      rcases h8 (by simp) (by simp) (by simp) with h | h | h
      · exact Or.inl (Or.inl h)
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
  · simp only [QueueInv, hqueue, hg, hg'pend, hg'wd, hg'tk, hg'retry, hg'nt, hinlog, hlead, huni]
    refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
    · have := List.pairwise_cons.1 (xf.queue ▸ q1)
      exact this.2
    · have := xf.queue ▸ q2
      simp only [List.map_cons, List.nodup_cons] at this
      exact this.2
    · intro e he
      have he' : e ∈ s'.queue := by rw [hqueue]; exact he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e (hqsub e he')
      refine ⟨a1, a2, ?_, by rw [obs_st (hobsd e.tx)]; exact a4, a5⟩
      rintro (h | h)
      · exact a3 h
      · exact hqne e he' h
    · intro e he
      rcases q4 e he with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · right; exact (obs_pendingLeaver (hobsd e.tx)).2 h
    · intro e he
      rcases q5 e (by rw [xf.unissued]; exact he) with h | h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr (by simpa [hl] using h))
    · intro e he; exact q6 e (by rw [xf.unissued]; exact he)
    · intro y hy; have := q7 y hy; rw [xf.unissued] at this; exact this
    · intro y hy
      have := q8 y hy
      exact ⟨this.1, by rw [obs_afterLeave (hobsd y)]; exact this.2⟩
    · intro y hy
      have := q9 y hy
      exact ⟨by rw [obs_afterLeave (hobsd y)]; exact this.1, by rw [hg'ab]; exact this.2⟩
  · obtain ⟨i1, i2, i3, i4⟩ := hs.issue
    simp only [IssueInv, hslot, hg, hg'iss, hg'ab, hlead]
    refine ⟨by rw [xf.issued]; rfl, ?_, ?_, ?_⟩
    · intro w hw
      rw [obs_st (hobsd w), obs_rolledBack (hobsd w)]
      exact i2 w hw
    · intro e he l hl' hel
      rcases i3 e (by rw [xf.issuedSlot]; exact he) l (by simpa [hl] using hl') hel with h | h
      · left; rw [obs_ticket (hobsd e.tx)]; exact h
      · exact Or.inr h
    · intro y hy
      obtain ⟨a1, a2, a3, a4⟩ := i4 y hy
      have hyc : y ≠ c := by intro h; subst h; rw [hpc] at a2; cases a2
      rw [htxd y hyc]
      exact ⟨a1, a2, a3, a4⟩
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hg'retry, hg'nt, hg'wt, hg'dt, hsync, hlog]
    refine ⟨m1, xf.ticketLe, m3, by simp; omega, ?_⟩
    simp only [List.map_append, List.map_cons, List.map_nil]
    refine List.nodup_append.2 ⟨m5, by simp, ?_⟩
    intro a ha b hb
    simp at hb; subst hb
    rw [hxtx]
    intro h; subst h
    simp only [List.mem_map] at ha
    obtain ⟨r, hr, hrt⟩ := ha
    exact xf.notLog ⟨r, hr, hrt⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hslot, hg, hg'wt, hg'dt]
    refine ⟨hqgt, fun e he => by simp at he; subst he; exact xf.durable, by
      have := xf.durable; omega⟩
  · intro h hh
    simp only [hg, hg'retry, xf.retry] at hh
    cases hh
  · intro d e t hd he
    rw [obs_ticket (hobsd d)] at hd
    rw [obs_ticket (hobsd e)] at he
    exact hs.unique d e t hd he

end GroupCommit
