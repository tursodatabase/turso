import GroupCommit.Steps.Finish

/-!
# `own_logical_log_record` and `EndCommitLogicalLog` of the leader
-/

namespace GroupCommit
open Sys

/-- Setting `log_appended` on a transaction whose record is in the log keeps the invariant. -/
theorem inv_set_appended {s : Sys} {w : Nat} (hs : Inv s) (hin : s.InLog w) :
    Inv (s.set w { s.tx w with appended := true }) := by
  have hwpc : (s.tx w).pc ≠ .dRbA := by
    intro hp
    have hst := hs.status w
    simp only [StatusInv, StatusOf, hp] at hst
    rcases (hs.log w).2.1 hin hst.1 with h | h | h
    · rw [hst.2] at h; cases h
    · have hab : w ∉ s.g.abandoned := fun ha => by
        have := (hs.issue.2.2.2 w ha).2.1; rw [hp] at this; cases this
      exact ((hs.groupTx w).2.1 (by simp [hp, Pc.afterLeave]) hab).1 h
    · rw [hp] at h; cases h.1
  generalize hr : ({ s.tx w with appended := true } : TxRec) = r'
  have e : r'.st = (s.tx w).st ∧ r'.pc = (s.tx w).pc ∧ r'.submitted = (s.tx w).submitted ∧
      r'.failed = (s.tx w).failed ∧ r'.excl = (s.tx w).excl ∧ r'.held = (s.tx w).held ∧
      r'.dropFrom = (s.tx w).dropFrom ∧ r'.rolledBack = (s.tx w).rolledBack ∧
      r'.acked = (s.tx w).acked ∧ r'.notified = (s.tx w).notified ∧
      r'.wasCommitted = (s.tx w).wasCommitted ∧ r'.appended = true := by
    rw [← hr]; simp
  obtain ⟨e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12⟩ := e
  have htx : ∀ d, (s.set w r').tx d = if d = w then r' else s.tx d := by intro d; simp
  have hst : ∀ d, ((s.set w r').tx d).st = (s.tx d).st := by
    intro d; rw [htx]; split <;> simp_all
  have hpc : ∀ d, ((s.set w r').tx d).pc = (s.tx d).pc := by
    intro d; rw [htx]; split <;> simp_all
  have hsub : ∀ d, ((s.set w r').tx d).submitted = (s.tx d).submitted := by
    intro d; rw [htx]; split <;> simp_all
  have hfail : ∀ d, ((s.set w r').tx d).failed = (s.tx d).failed := by
    intro d; rw [htx]; split <;> simp_all
  have hexcl : ∀ d, ((s.set w r').tx d).excl = (s.tx d).excl := by
    intro d; rw [htx]; split <;> simp_all
  have hheld : ∀ d, ((s.set w r').tx d).held = (s.tx d).held := by
    intro d; rw [htx]; split <;> simp_all
  have hdrop : ∀ d, ((s.set w r').tx d).dropFrom = (s.tx d).dropFrom := by
    intro d; rw [htx]; split <;> simp_all
  have hrb : ∀ d, ((s.set w r').tx d).rolledBack = (s.tx d).rolledBack := by
    intro d; rw [htx]; split <;> simp_all
  have hack : ∀ d, ((s.set w r').tx d).acked = (s.tx d).acked := by
    intro d; rw [htx]; split <;> simp_all
  have hnot : ∀ d, ((s.set w r').tx d).notified = (s.tx d).notified := by
    intro d; rw [htx]; split <;> simp_all
  have hwc : ∀ d, ((s.set w r').tx d).wasCommitted = (s.tx d).wasCommitted := by
    intro d; rw [htx]; split <;> simp_all
  have happ : ∀ d, ((s.set w r').tx d).appended = true ↔
      (d = w ∨ (s.tx d).appended = true) := by
    intro d; rw [htx]; split <;> simp_all
  have happ2 : ∀ d, d ≠ w → ((s.set w r').tx d).appended = (s.tx d).appended := by
    intro d hd; rw [htx]; simp [hd]
  have htk : ∀ d, ((s.set w r').tx d).ticket = (s.tx d).ticket := by
    intro d; simp only [TxRec.ticket, hpc, hdrop]
  have hhl : ∀ d, ((s.set w r').tx d).holdsLock = (s.tx d).holdsLock := by
    intro d; simp only [TxRec.holdsLock, hpc, hheld]
  have hiss : ∀ d, ((s.set w r').tx d).issuing = (s.tx d).issuing := by
    intro d; simp only [TxRec.issuing, hpc, hdrop]
  have hwu : ∀ d, ((s.set w r').tx d).writingUnissued = (s.tx d).writingUnissued := by
    intro d; simp only [TxRec.writingUnissued, hpc, hdrop]
  have hunis : (s.set w r').unissued = s.unissued := by
    simp only [Sys.unissued_eq, set_lead, hwu]
  have hient : (s.set w r').issuedEntry = s.issuedEntry := by
    simp only [Sys.issuedEntry_eq, set_lead, hiss]
  have hslot : (s.set w r').issuedSlot = s.issuedSlot := by
    simp only [Sys.issuedSlot_eq, set_lead, hiss, hpc]
  have hqueue : (s.set w r').queue = s.queue := by
    simp only [Sys.queue, hunis, hient, set_g]
  have hg : (s.set w r').g = s.g := rfl
  have hlead : (s.set w r').lead = s.lead := rfl
  have hlog : (s.set w r').log = s.log := rfl
  have hsync : (s.set w r').synced = s.synced := rfl
  have hlock : (s.set w r').lockHolder = s.lockHolder := rfl
  have hbatch : ∀ d, (s.set w r').batchOf d = s.batchOf d := fun _ => rfl
  have hinlog : ∀ d, (s.set w r').InLog d ↔ s.InLog d := fun _ => Iff.rfl
  have hinsync : ∀ d, (s.set w r').InSynced d ↔ s.InSynced d := fun _ => Iff.rfl
  generalize (s.set w r') = s' at *
  obtain ⟨hst0, hlog0, htk0, hgtx0, hlead0, hq0, hiss0, hmark0, hord0, hholes0, huniq0⟩ := hs
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    have := hst0 d
    cases hp : (s.tx d).pc <;>
      simp only [StatusInv, StatusOf, hpc, hp, hst, hheld, hexcl, hsub, hdrop, hrb] at this ⊢ <;>
      first
      | exact this
      | (have hdw : d ≠ w := fun h => hwpc (h ▸ hp)
         rw [happ2 d hdw]; exact this)
  · intro d
    have := hlog0 d
    simp only [LogInv, hpc, hst, hfail, hrb, hack, hnot, hwc, happ, hinlog, hinsync, hbatch, hg,
      hlog, hsync] at this ⊢
    obtain ⟨l1, l2, l3⟩ := this
    refine ⟨?_, ?_, l3⟩
    · rintro (h | h)
      · subst h; exact hin
      · exact l1 h
    · intro h1 h2
      rcases l2 h1 h2 with h | h
      · exact Or.inl (Or.inr h)
      · exact Or.inr h
  · intro d
    have := htk0 d
    simp only [TicketInv, htk, hqueue, hg, hinlog, hpc, hlog] at this ⊢
    exact this
  · intro d
    have := hgtx0 d
    simp only [GroupTxInv, hhl, hpc, hst, hlock, hg, hlead] at this ⊢
    exact this
  · simp only [LeadInv, hlead] at hlead0 ⊢
    split
    · rename_i hl; rw [hl] at hlead0; simpa [hg] using hlead0
    · rename_i l b hl
      rw [hl] at hlead0
      simp only [LeadOf, hheld, hpc, hdrop, hsub, hiss, hwu, hunis, hslot, hg, hinlog, hlog, hst,
        happ] at hlead0 ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlead0
      exact ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, fun h => Or.inr (h12 h)⟩
  · simp only [QueueInv, hqueue, hunis, hg, hinlog, hst, htk, hpc, hdrop, hheld, hlead] at hq0 ⊢
    exact hq0
  · simp only [IssueInv, hslot, hg, hst, hrb, htk, hlead, hpc, hheld] at hiss0 ⊢
    exact hiss0
  · simpa only [MarkInv, hg, hlog, hsync] using hmark0
  · simpa only [OrderInv, hqueue, hslot, hg] using hord0
  · intro h hh
    obtain ⟨d, hd⟩ := hholes0 h (by simpa [hg] using hh)
    exact ⟨d, by rw [htk]; exact hd⟩
  · intro d e t hd he
    rw [htk] at hd he
    exact huniq0 d e t hd he

end GroupCommit

namespace GroupCommit
open Sys

theorem own_facts {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .own2 ∨ (s.tx c).pc = .own3) :
    b.advanced = some b.writing.ticket ∧ ⟨b.writing.tx, some b.writing.ticket⟩ ∈ s.log :=
  (leadOf_of_lead hs hl).2.2.2.2.2.2.1 hpc

/-- `own_logical_log_record`, first part: the owner of the batch record gets `log_appended`. -/
theorem step_own2 {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .own2) :
    Inv ((s.set b.writing.tx { s.tx b.writing.tx with appended := true }).set c
      { (s.set b.writing.tx { s.tx b.writing.tx with appended := true }).tx c with pc := .own3 }) := by
  have hin : s.InLog b.writing.tx := ⟨_, (own_facts hs hl (Or.inl hpc)).2, rfl⟩
  have hs1 := inv_set_appended hs hin
  generalize hs1def : s.set b.writing.tx { s.tx b.writing.tx with appended := true } = s1 at *
  have hl1 : s1.lead = some (c, b) := by rw [← hs1def]; exact hl
  have hpc1 : (s1.tx c).pc = .own2 := by
    rw [← hs1def]
    by_cases h : c = b.writing.tx
    · simp only [set_tx, h, ite_true]; rw [← h]; exact hpc
    · simp [h, hpc]
  have happ1 : (s1.tx b.writing.tx).appended = true := by rw [← hs1def]; simp
  have hc := hs1.status c
  simp only [StatusInv, StatusOf, hpc1] at hc
  have hlc := hs1.log c
  have hlo := leadOf_of_lead hs1 hl1
  have hwu : ({ s1.tx c with pc := .own3 } : TxRec).writingUnissued = (s1.tx c).writingUnissued := by
    simp [TxRec.writingUnissued, hpc1]
  have hiss : ({ s1.tx c with pc := .own3 } : TxRec).issuing = (s1.tx c).issuing := by
    simp [TxRec.issuing, hpc1]
  have hsl : (({ s1.tx c with pc := .own3 } : TxRec).issuing || Pc.own3 == .own2 ||
      Pc.own3 == .own3) = ((s1.tx c).issuing || (s1.tx c).pc == .own2 || (s1.tx c).pc == .own3) := by
    simp [TxRec.issuing, hpc1, Pc.beq_eq]
  refine step_holder_local _ hs1 hl1 ?_ hiss hwu hsl ?_ ?_ ?_ ?_ rfl (by simp)
    (by simp [Pc.afterLeave]) (by simp)
  · simp [TxRec.obs, TxRec.ticket, hpc1, Pc.waitTicket, Pc.beforeLeave, Pc.afterLeave, Pc.beq_eq]
  · simp [hpc1, Pc.rollingBackOther, Pc.removingOther]
  · simp [StatusOf, hc]
  · simp only [LogInv, sys_simps, ite_true, batchOf_holder hl1, hpc1] at hlc ⊢
    grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
  · simp only [LeadOf, sys_simps, ite_true, holder_unissued_eq hl1 hwu,
      holder_issuedSlot_eq hl1 hsl, hiss, hwu, hpc1] at hlo ⊢
    obtain ⟨h1, _, _, _, h5, h6, h7, h8, h9, _, _, _⟩ := hlo
    refine ⟨h1, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, h5, h6,
      fun _ => h7 (by simp), fun _ _ _ => h8 (by simp [Pc.leading]) (by simp) (by simp),
      fun _ => h9 (by simp [Pc.leading]), by simp [Pc.rollingBackOther],
      by simp [Pc.removingOther], ?_⟩
    intro _
    by_cases hwc : b.writing.tx = c
    · simp only [hwc, ite_true]; rw [hwc] at happ1; exact happ1
    · simp only [hwc, ite_false]; exact happ1

end GroupCommit

namespace GroupCommit
open Sys

/-- `own_logical_log_record` in the fixed code, second part: `finish_issue`, maybe
`finish_abandoned_group_waiter`, then `advance_group_or_sync`. The new state is given
by its parts. -/
theorem step_own3_gen {s : Sys} {c : Nat} {b : Batch} (hs : Inv s) (hl : s.lead = some (c, b))
    (hpc : (s.tx c).pc = .own3) {s' : Sys} {p : Pc} {b2 : Batch}
    (hadv : (∃ e rest, b.rest = e :: rest ∧ p = .upgrade ∧
        b2 = { b with writing := e, rest := rest }) ∨
      (b.rest = [] ∧ p = .syncLog ∧ b2 = b))
    (hg : s'.g = { s.g with issued := none, abandoned := setErase b.writing.tx s.g.abandoned })
    (hlead : s'.lead = some (c, b2)) (hlog : s'.log = s.log) (hsync : s'.synced = s.synced)
    (hlock : s'.lockHolder = s.lockHolder)
    (htxc : s'.tx c = { s.tx c with pc := p })
    (htxw : b.writing.tx ≠ c → s'.tx b.writing.tx =
      if b.writing.tx ∈ s.g.abandoned then
        { s.tx b.writing.tx with st := .removed, wasCommitted := true, notified := true }
      else s.tx b.writing.tx)
    (htxd : ∀ d, d ≠ c → d ≠ b.writing.tx → s'.tx d = s.tx d) :
    Inv s' := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hpc] at hc
  have hlo := leadOf_of_lead hs hl
  obtain ⟨hadvw, hwlog⟩ := own_facts hs hl (Or.inr hpc)
  have hwin : s.InLog b.writing.tx := ⟨_, hwlog, rfl⟩
  have hunis : s.unissued = b.rest := by simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hpc]
  have hent : s.issuedEntry = none := by simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hpc]
  have hslot : s.issuedSlot = some b.writing := by
    simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hpc]
  have hiss : s.g.issued = some b.writing.tx := by rw [hs.issue.1, hslot]; rfl
  have hq : s.queue = b.rest ++ s.g.pending := by simp [Sys.queue, hent, hunis]
  have hretry : s.g.retry = [] := hlo.2.2.2.2.2.2.2.2.1 (Or.inl (by simp [hpc, Pc.leading]))
  have hab : ∀ x ∈ s.g.abandoned, x = b.writing.tx := by
    intro x hx; have := (hs.issue.2.2.2 x hx).1; rw [hiss] at this; exact (Option.some.inj this).symm
  have hwapp : (s.tx b.writing.tx).appended = true := hlo.2.2.2.2.2.2.2.2.2.2.2 hpc
  have hwlive : (s.tx b.writing.tx).st = .live := (hs.issue.2.1 _ (by simp [hiss])).1
  have hwrb : (s.tx b.writing.tx).rolledBack = false := (hs.issue.2.1 _ (by simp [hiss])).2
  have hwle : b.writing.ticket ≤ s.g.writtenThrough := (hlo.2.2.2.2.1 _ (by simp [hadvw])).1
  have hnq : ∀ e ∈ s.queue, e.tx ≠ b.writing.tx := by
    intro e he h; exact (hs.queue.2.2.1 e he).2.2.1 (h ▸ hwin)
  have habw : b.writing.tx ∈ s.g.abandoned → (s.tx b.writing.tx).pc = .dropped ∧
      (s.tx b.writing.tx).held = false ∧ b.writing.tx ≠ c := by
    intro h
    have := hs.issue.2.2.2 _ h
    refine ⟨this.2.1, this.2.2.2, ?_⟩
    intro hwc; have h2 := this.2.1; rw [hwc, hpc] at h2; cases h2
  have hp : p = .upgrade ∨ p = .syncLog := by
    rcases hadv with ⟨_, _, _, h, _⟩ | ⟨_, h, _⟩ <;> simp [h]
  have hpcd : ∀ d, d ≠ c → (s'.tx d).pc = (s.tx d).pc := by
    intro d hd
    by_cases hdw : d = b.writing.tx
    · subst hdw; rw [htxw hd]; split <;> rfl
    · rw [htxd d hd hdw]
  have hfield : ∀ d, (s'.tx d).held = (s.tx d).held ∧ (s'.tx d).dropFrom = (s.tx d).dropFrom ∧
      (s'.tx d).rolledBack = (s.tx d).rolledBack ∧ (s'.tx d).appended = (s.tx d).appended ∧
      (s'.tx d).submitted = (s.tx d).submitted ∧ (s'.tx d).failed = (s.tx d).failed ∧
      (s'.tx d).excl = (s.tx d).excl ∧ (s'.tx d).acked = (s.tx d).acked := by
    intro d
    by_cases hd : d = c
    · subst hd; rw [htxc]; simp
    · by_cases hdw : d = b.writing.tx
      · subst hdw; rw [htxw hd]; split <;> simp
      · rw [htxd d hd hdw]; simp
  have hstd : ∀ d, (d ≠ b.writing.tx ∨ b.writing.tx ∉ s.g.abandoned) →
      (s'.tx d).st = (s.tx d).st ∧ (s'.tx d).wasCommitted = (s.tx d).wasCommitted ∧
      (s'.tx d).notified = (s.tx d).notified := by
    intro d h
    by_cases hd : d = c
    · subst hd; rw [htxc]; simp
    · by_cases hdw : d = b.writing.tx
      · subst hdw
        rcases h with h | h
        · exact absurd rfl h
        · rw [htxw hd]; simp [h]
      · rw [htxd d hd hdw]; simp
  have hticket : ∀ d, (s'.tx d).ticket = (s.tx d).ticket := by
    intro d
    by_cases hd : d = c
    · subst hd; rw [htxc]
      rcases hp with h | h <;>
        simp [TxRec.ticket, h, hpc, Pc.waitTicket, Pc.beforeLeave]
    · simp only [TxRec.ticket, hpcd d hd, (hfield d).2.1]
  have hafter : ∀ d, (s'.tx d).pc.afterLeave = (s.tx d).pc.afterLeave := by
    intro d
    by_cases hd : d = c
    · subst hd; rw [htxc]; rcases hp with h | h <;> simp [h, hpc, Pc.afterLeave]
    · rw [hpcd d hd]
  have hpl : ∀ d, ((s'.tx d).pc = .dLeave ∧ (s'.tx d).dropFrom.ticket = none ∧
      (s'.tx d).held = true) ↔ ((s.tx d).pc = .dLeave ∧ (s.tx d).dropFrom.ticket = none ∧
      (s.tx d).held = true) := by
    intro d
    by_cases hd : d = c
    · subst hd; rw [htxc]; rcases hp with h | h <;> simp [h, hpc]
    · rw [hpcd d hd, (hfield d).2.1, (hfield d).1]
  have hholds : ∀ d, (s'.tx d).holdsLock = (s.tx d).holdsLock := by
    intro d
    by_cases hd : d = c
    · subst hd; rw [htxc]; rcases hp with h | h <;> simp [TxRec.holdsLock, h, hpc]
    · simp only [TxRec.holdsLock, hpcd d hd, (hfield d).1]
  have hwq : ∀ d, d ≠ b.writing.tx → (s'.tx d).st = (s.tx d).st := fun d h => (hstd d (Or.inl h)).1
  have huni : s'.unissued = b.rest := by
    rcases hadv with ⟨e, rest, hr, hpu, hb2⟩ | ⟨hr, hps, hb2⟩
    · simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hpu, hb2, hr]
    · simp [Sys.unissued_eq, hlead, htxc, TxRec.writingUnissued, hps, hb2, hr]
  have hent' : s'.issuedEntry = none := by
    rcases hp with h | h <;> simp [Sys.issuedEntry_eq, hlead, htxc, TxRec.issuing, h]
  have hslot' : s'.issuedSlot = none := by
    rcases hp with h | h <;> simp [Sys.issuedSlot_eq, hlead, htxc, TxRec.issuing, h]
  have hqueue : s'.queue = s.queue := by
    rw [hq]; simp [Sys.queue, hent', huni, hg]
  have hinlog : ∀ d, s'.InLog d ↔ s.InLog d := by intro d; simp [Sys.InLog, hlog]
  have hinsync : ∀ d, s'.InSynced d ↔ s.InSynced d := by
    intro d; simp [Sys.InSynced, hlog, hsync]
  have hbatch : ∀ d, d ≠ c → s'.batchOf d = none := by
    intro d hd; simp [Sys.batchOf_eq, hlead, Ne.symm hd]
  have hbatch0 : ∀ d, d ≠ c → s.batchOf d = none := by
    intro d hd; simp [Sys.batchOf_eq, hl, Ne.symm hd]
  have hbatchc : s'.batchOf c = some b2 := by simp [Sys.batchOf_eq, hlead]
  have hab' : s'.g.abandoned = [] := by
    rw [hg]
    simp only
    cases h : s.g.abandoned with
    | nil => rfl
    | cons x xs =>
      apply List.eq_nil_iff_forall_not_mem.2
      intro y hy
      rw [mem_setErase] at hy
      exact hy.1 (hab y (h ▸ hy.2))
  have hsame : ∀ d, d ≠ c → ¬(d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned) →
      s'.tx d = s.tx d := by
    intro d hd h
    by_cases hdw : d = b.writing.tx
    · subst hdw
      have ha : b.writing.tx ∉ s.g.abandoned := fun ha => h ⟨rfl, ha⟩
      rw [htxw hd]; simp [ha]
    · exact htxd d hd hdw
  have hcapp : s.InLog c → (s.tx c).appended = true := by
    intro hin
    rcases (hs.log c).2.1 hin hc.1 with h | h | h
    · exact h
    · rw [hiss] at h; rw [← Option.some.inj h]; exact hwapp
    · rw [hpc] at h; cases h.1
  have hcin : s.InLog c ∨ ∃ e ∈ b.rest, e.tx = c := by
    have h8 := hlo.2.2.2.2.2.2.2.1
    simp only [hpc, Pc.leading, hunis, hslot] at h8
    rcases h8 (by simp) (by simp) (by simp) with h | h | h
    · exact Or.inl h
    · exact Or.inr h
    · simp at h; rw [← h]; exact Or.inl hwin
  have hwa_ne : ∀ d, d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned → d ≠ c := by
    rintro d ⟨rfl, ha⟩; exact (habw ha).2.2
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d
    by_cases hd : d = c
    · subst hd; rw [StatusInv, htxc]
      rcases hp with h | h <;> simp [StatusOf, h, hc]
    · by_cases hwa : d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned
      · obtain ⟨hdw, ha⟩ := hwa; subst hdw
        rw [StatusInv, htxw hd]
        obtain ⟨h1, h2, _⟩ := habw ha
        simp [StatusOf, h1, h2, ha]
      · rw [StatusInv, hsame d hd hwa]; exact hs.status d
  · intro d
    have hld := hs.log d
    by_cases hd : d = c
    · subst hd
      simp only [LogInv, htxc, hg, hinlog, hinsync, hbatchc, hlog, hsync] at hld ⊢
      simp only [hpc, batchOf_holder hl, hiss] at hld
      have hsyn : p = .syncLog → s.InLog d := by
        intro h
        rcases hadv with ⟨_, _, _, h', _⟩ | ⟨hr, _, _⟩
        · rw [h] at h'; cases h'
        · rcases hcin with h'' | ⟨e, he, _⟩
          · exact h''
          · rw [hr] at he; cases he
      rcases hp with h | h <;> simp only [h] <;> grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
    · by_cases hwa : d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned
      · obtain ⟨hdw, ha⟩ := hwa; subst hdw
        obtain ⟨h1, _, _⟩ := habw ha
        simp only [LogInv, htxw hd, ha, ite_true, hg, hinlog, hinsync, hbatch _ hd, hlog, hsync,
          hbatch0 _ hd] at hld ⊢
        simp only [h1] at hld ⊢
        grind [Pc.waitTicket, Pc.isPrefixSynced, Pc.rollingBackOther, Pc.removingOther]
      · simp only [LogInv, hsame d hd hwa, hg, hinlog, hinsync, hbatch d hd, hlog, hsync,
          hbatch0 d hd, hiss] at hld ⊢
        have hdw : d = b.writing.tx → (s.tx d).appended = true := by
          intro h; subst h; exact hwapp
        grind
  · intro d
    have htd := hs.ticket d
    simp only [TicketInv, hticket, hqueue, hg, hinlog, hlog] at htd ⊢
    intro t ht
    obtain ⟨h1, h2, h3, h4, h5⟩ := htd t ht
    refine ⟨h1, h2, h3, h4, ?_⟩
    intro hw
    by_cases hd : d = c
    · subst hd; rw [htxc] at hw; rcases hp with h | h <;> simp [h] at hw
    · rw [hpcd d hd] at hw; exact h5 hw
  · intro d
    have hgd := hs.groupTx d
    by_cases hd : d = c
    · subst hd
      simp only [GroupTxInv, hholds, hlock] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩ <;> rw [htxc] <;> rcases hp with h | h <;> simp [h, Pc.afterLeave]
    · simp only [GroupTxInv, hholds, hlock, hg, hlead] at hgd ⊢
      refine ⟨hgd.1, ?_, ?_, ?_⟩
      · intro h1 h2
        rw [hafter] at h1
        have h2' : d ∉ s.g.abandoned ∨ d = b.writing.tx := by
          by_cases hdw : d = b.writing.tx
          · exact Or.inr hdw
          · left; intro ha; exact h2 (mem_setErase.2 ⟨hdw, ha⟩)
        rcases h2' with h2' | hdw
        · obtain ⟨a1, a2, a3⟩ := hgd.2.1 h1 h2'
          exact ⟨by simp, a2, a3⟩
        · subst hdw
          refine ⟨by simp, ?_, ?_⟩
          · intro ht
            have := (hs.queue.2.2.2.2.2.2.2.1 _ ht).2
            rw [h1] at this; cases this
          · intro e he h
            rcases hs.queue.2.2.2.1 e he with h' | h'
            · rw [h] at h'; rw [ticket_none_of_afterLeave h1] at h'; cases h'
            · rw [h] at h'; rw [h'.1] at h1; simp [Pc.afterLeave] at h1
      · intro h1 h2
        exfalso
        rw [hpcd d hd] at h1
        by_cases hwa : d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned
        · obtain ⟨hdw, ha⟩ := hwa; subst hdw
          rw [htxw hd] at h2; simp [ha] at h2
        · rw [hsame d hd hwa] at h2
          rcases hgd.2.2.1 h1 h2 with h | ⟨l, hl', hl2⟩
          · exact hwa ⟨hab d h, hab d h ▸ h⟩
          · rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
      · intro h1 h2
        exfalso
        rw [hpcd d hd] at h1
        by_cases hwa : d = b.writing.tx ∧ b.writing.tx ∈ s.g.abandoned
        · obtain ⟨hdw, ha⟩ := hwa; subst hdw
          rw [htxw hd] at h2; simp [ha] at h2
        · rw [hsame d hd hwa] at h2
          obtain ⟨l, hl', hl2⟩ := hgd.2.2.2 h1 h2
          rw [hl] at hl'; simp at hl'; subst hl'; rw [hpc] at hl2; cases hl2
  · simp only [LeadInv, hlead, LeadOf, htxc, hg, huni, hslot', hinlog, hlog]
    obtain ⟨h1, _, _, _, h5, _, _, _, _, _, _, _⟩ := hlo
    rcases hadv with ⟨e, rest, hr, hpu, hb2⟩ | ⟨hr, hps, hb2⟩
    · subst hpu; subst hb2
      have he : s.g.writtenThrough < e.ticket :=
        hs.order.1 e (by rw [hq, hr]; simp)
      refine ⟨h1, by simp [Pc.holdsBatch, Pc.leading], by simp, by simp, ?_, ?_, by simp, ?_,
        fun _ => hretry, by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
      · intro a ha; simp [hadvw] at ha; subst ha; exact ⟨hwle, by simp; omega⟩
      · intro _; simp [hadvw]; omega
      · intro _ _ _
        rcases hcin with h | ⟨x, hx, hxc⟩
        · exact Or.inl h
        · right; left; exact ⟨x, hx, hxc⟩
    · subst hps; subst hb2
      refine ⟨h1, by simp [Pc.holdsBatch, Pc.leading], fun _ => hr, by simp, h5,
        by simp [TxRec.issuing, TxRec.writingUnissued], by simp, by simp, fun _ => hretry,
        by simp [Pc.rollingBackOther], by simp [Pc.removingOther], by simp⟩
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hs.queue
    simp only [QueueInv, hqueue, huni, hg, hinlog, hlead]
    rw [hunis] at q5 q6 q7
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨a1, a2, a3, a4, a5⟩ := q3 e he
      exact ⟨a1, a2, a3, by rw [hwq e.tx (hnq e he)]; exact a4, a5⟩
    · intro e he
      rcases q4 e he with h | h
      · left; rw [hticket]; exact h
      · right; exact (hpl e.tx).2 h
    · intro e he
      rcases q5 e he with h | h | h
      · left; rw [hticket]; exact h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr (by simpa [hl] using h))
    · intro x hx
      exact ⟨(q8 x hx).1, by rw [hafter]; exact (q8 x hx).2⟩
    · intro x hx
      refine ⟨by rw [hafter]; exact (q9 x hx).1, fun h => (q9 x hx).2 (mem_setErase.1 h).2⟩
  · have hiss' : s'.g.issued = none := by rw [hg]
    simp only [IssueInv, hiss', hslot', hab']
    simp
  · obtain ⟨m1, m2, m3, m4, m5⟩ := hs.mark
    simp only [MarkInv, hg, hsync, hlog]
    exact ⟨m1, m2, m3, m4, m5⟩
  · obtain ⟨o1, o2, o3⟩ := hs.order
    simp only [OrderInv, hqueue, hslot', hg]
    exact ⟨o1, by simp, o3⟩
  · intro h hh
    simp only [hg, hretry] at hh
    cases hh
  · intro d e t hd he
    rw [hticket] at hd he
    exact hs.unique d e t hd he

end GroupCommit
