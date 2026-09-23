import GroupCommit.Lemmas

/-!
# Frame lemmas

Most steps change the record of one transaction and nothing that the other
parts of the invariant read. These lemmas prove the unchanged parts once.
-/

namespace GroupCommit
open Sys

/-- The fields of a record that the invariant reads for other transactions. -/
structure Obs where
  st : St
  ticket : Option Nat
  afterLeave : Bool
  pendingLeaver : Bool
  dropped : Bool
  rolledBack : Bool
  appended : Bool
deriving DecidableEq

def TxRec.obs (r : TxRec) : Obs :=
  { st := r.st, ticket := r.ticket, afterLeave := r.pc.afterLeave,
    pendingLeaver := r.pc == .dLeave && r.dropFrom.ticket.isNone && r.held,
    dropped := r.pc == .dropped, rolledBack := r.rolledBack, appended := r.appended }

theorem lead_ne_of_notHolder {s : Sys} {c l : Nat} (hnh : NotHolder s c)
    (hl : l ∈ (s.lead.map (·.1)).toList) : l ≠ c := by
  intro h; subst h
  simp only [Option.mem_toList, Option.map_eq_some_iff] at hl
  obtain ⟨⟨l', b⟩, hb, rfl⟩ := hl
  exact hnh _ _ hb rfl

theorem obs_st {r r' : TxRec} (h : r.obs = r'.obs) : r.st = r'.st := by
  have := congrArg Obs.st h; simpa [TxRec.obs] using this
theorem obs_ticket {r r' : TxRec} (h : r.obs = r'.obs) : r.ticket = r'.ticket := by
  have := congrArg Obs.ticket h; simpa [TxRec.obs] using this
theorem obs_afterLeave {r r' : TxRec} (h : r.obs = r'.obs) : r.pc.afterLeave = r'.pc.afterLeave := by
  have := congrArg Obs.afterLeave h; simpa [TxRec.obs] using this
theorem obs_pendingLeaver {r r' : TxRec} (h : r.obs = r'.obs) :
    (r.pc = .dLeave ∧ r.dropFrom.ticket = none ∧ r.held = true) ↔
      (r'.pc = .dLeave ∧ r'.dropFrom.ticket = none ∧ r'.held = true) := by
  have := congrArg Obs.pendingLeaver h
  simp only [TxRec.obs] at this
  constructor
  · intro ⟨h1, h2, h3⟩
    have : (r'.pc == .dLeave && r'.dropFrom.ticket.isNone && r'.held) = true := by
      rw [← this]; simp [h1, h2, h3]
    simpa [Option.isNone_iff_eq_none, and_assoc] using this
  · intro ⟨h1, h2, h3⟩
    have : (r.pc == .dLeave && r.dropFrom.ticket.isNone && r.held) = true := by
      rw [this]; simp [h1, h2, h3]
    simpa [Option.isNone_iff_eq_none, and_assoc] using this
theorem obs_dropped {r r' : TxRec} (h : r.obs = r'.obs) : r.pc = .dropped ↔ r'.pc = .dropped := by
  have := congrArg Obs.dropped h
  simp only [TxRec.obs] at this
  constructor
  · intro h1; have : (r'.pc == .dropped) = true := by rw [← this]; simp [h1]
    simpa using this
  · intro h1; have : (r.pc == .dropped) = true := by rw [this]; simp [h1]
    simpa using this
theorem obs_rolledBack {r r' : TxRec} (h : r.obs = r'.obs) : r.rolledBack = r'.rolledBack := by
  have := congrArg Obs.rolledBack h; simpa [TxRec.obs] using this
theorem obs_appended {r r' : TxRec} (h : r.obs = r'.obs) : r.appended = r'.appended := by
  have := congrArg Obs.appended h; simpa [TxRec.obs] using this

/-- The parts of the invariant that do not talk about transaction `c` itself. -/
structure Others (s : Sys) (c : Nat) : Prop where
  status : ∀ d, d ≠ c → StatusInv s d
  log : ∀ d, d ≠ c → LogInv s d
  ticket : ∀ d, d ≠ c → TicketInv s d
  groupTx : ∀ d, d ≠ c → GroupTxInv s d
  lead : LeadInv s
  queue : QueueInv s
  issue : IssueInv s
  mark : MarkInv s
  order : OrderInv s
  holes : HoleOwners s
  unique : TicketsUnique s

theorem frame_set_lock {s : Sys} {c : Nat} {r' : TxRec} (hs : Inv s) (hnh : NotHolder s c)
    (hobs : (s.tx c).obs = r'.obs) (hab : c ∉ s.g.abandoned) : Others (s.set c r') c := by
  obtain ⟨hst, hlog, htk, hgtx, hlead, hq, hiss, hmark, hord, hholes, huniq⟩ := hs
  have hsame : ∀ d, d ≠ c → (s.set c r').tx d = s.tx d := by
    intro d hd; simp [hd]
  have hx : ∀ x, (s.set c r').tx x = s.tx x ∨ (x = c ∧ (s.set c r').tx x = r') := by
    intro x; by_cases hxc : x = c <;> simp [hxc]
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d hd; simpa [StatusInv, hd] using hst d
  · intro d hd; simpa [LogInv, hd] using hlog d
  · intro d hd; simpa [TicketInv, hd, queue_set _ hnh] using htk d
  · intro d hd
    have hg := hgtx d
    simp only [GroupTxInv, set_tx, hd, ite_false, set_g, set_lead, set_lockHolder] at hg ⊢
    refine ⟨hg.1, hg.2.1, ?_, ?_⟩
    · intro h1 h2
      rcases hg.2.2.1 h1 h2 with h | ⟨l, hl, hl2⟩
      · exact Or.inl h
      · exact Or.inr ⟨l, hl, by simpa [lead_ne_of_notHolder hnh hl] using hl2⟩
    · intro h1 h2
      obtain ⟨l, hl, hl2⟩ := hg.2.2.2 h1 h2
      exact ⟨l, hl, by simpa [lead_ne_of_notHolder hnh hl] using hl2⟩
  · simp only [LeadInv, set_lead] at hlead ⊢
    split
    · rename_i hl; rw [hl] at hlead; simpa using hlead
    · rename_i l b hl
      rw [hl] at hlead
      have hlc : l ≠ c := hnh l b hl
      simp only [LeadOf, set_tx, hlc, ite_false, set_g, set_log, inLog_set,
        unissued_set _ hnh, issuedSlot_set _ hnh] at hlead ⊢
      refine ⟨hlead.1, hlead.2.1, hlead.2.2.1, hlead.2.2.2.1, hlead.2.2.2.2.1,
        hlead.2.2.2.2.2.1, hlead.2.2.2.2.2.2.1, hlead.2.2.2.2.2.2.2.1, hlead.2.2.2.2.2.2.2.2.1,
        ?_, ?_, ?_⟩
      · intro w hw
        obtain ⟨h1, h2, h3, h4, h5⟩ := hlead.2.2.2.2.2.2.2.2.2.1 w hw
        by_cases hwc : w = c
        · subst hwc
          simp only [ite_true]
          exact ⟨h1, h2, (obs_st hobs) ▸ h3, (obs_dropped hobs).1 h4, h5⟩
        · simp only [hwc, ite_false]; exact ⟨h1, h2, h3, h4, h5⟩
      · intro w hw
        obtain ⟨h1, h2, h3, h4⟩ := hlead.2.2.2.2.2.2.2.2.2.2.1 w hw
        by_cases hwc : w = c
        · subst hwc
          simp only [ite_true]
          exact ⟨h1, h2, (obs_st hobs) ▸ h3, (obs_dropped hobs).1 h4⟩
        · simp only [hwc, ite_false]; exact ⟨h1, h2, h3, h4⟩
      · intro hp
        have h12 := hlead.2.2.2.2.2.2.2.2.2.2.2 hp
        by_cases hwc : b.writing.tx = c
        · simp only [hwc, ite_true]; rw [← obs_appended hobs]; simpa [hwc] using h12
        · simp only [hwc, ite_false]; exact h12
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hq
    simp only [QueueInv, queue_set _ hnh, unissued_set _ hnh, set_g, set_lead, inLog_set]
    refine ⟨q1, q2, ?_, ?_, ?_, q6, q7, ?_, ?_⟩
    · intro e he
      obtain ⟨h1, h2, h3, h4, h5⟩ := q3 e he
      refine ⟨h1, h2, h3, ?_, h5⟩
      by_cases hec : e.tx = c
      · simp only [set_tx, hec, ite_true]; rw [← obs_st hobs]; simpa [hec] using h4
      · simpa [hec] using h4
    · intro e he
      rcases q4 e he with h | h
      · left
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; rw [← obs_ticket hobs]; simpa [hec] using h
        · simpa [hec] using h
      · right
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; exact (obs_pendingLeaver hobs).1 (by simpa [hec] using h)
        · simpa [hec] using h
    · intro e he
      rcases q5 e he with h | h | h
      · left
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; rw [← obs_ticket hobs]; simpa [hec] using h
        · simpa [hec] using h
      · exact Or.inr (Or.inl h)
      · exact Or.inr (Or.inr h)
    · intro x hx
      refine ⟨(q8 x hx).1, ?_⟩
      by_cases hxc : x = c
      · simp only [set_tx, hxc, ite_true]; rw [← obs_afterLeave hobs]; simpa [hxc] using (q8 x hx).2
      · simpa [hxc] using (q8 x hx).2
    · intro x hx
      refine ⟨?_, (q9 x hx).2⟩
      by_cases hxc : x = c
      · simp only [set_tx, hxc, ite_true]; rw [← obs_afterLeave hobs]; simpa [hxc] using (q9 x hx).1
      · simpa [hxc] using (q9 x hx).1
  · obtain ⟨i1, i2, i3, i4⟩ := hiss
    simp only [IssueInv, issuedSlot_set _ hnh, set_g, set_lead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      obtain ⟨h1, h2⟩ := i2 w hw
      by_cases hwc : w = c
      · simp only [set_tx, hwc, ite_true]
        subst hwc; exact ⟨(obs_st hobs) ▸ h1, (obs_rolledBack hobs) ▸ h2⟩
      · simp only [set_tx, hwc, ite_false]; exact ⟨h1, h2⟩
    · intro e he l hl hel
      rcases i3 e he l hl hel with h | h
      · left
        by_cases hec : e.tx = c
        · simp only [set_tx, hec, ite_true]; rw [← obs_ticket hobs]; simpa [hec] using h
        · simpa [hec] using h
      · exact Or.inr h
    · intro x hxa
      obtain ⟨h1, h2, h3, h4⟩ := i4 x hxa
      refine ⟨h1, ?_⟩
      by_cases hxc : x = c
      · exact absurd (hxc ▸ hxa) hab
      · simp only [set_tx, hxc, ite_false]; exact ⟨h2, h3, h4⟩
  · simpa [MarkInv] using hmark
  · simpa [OrderInv, queue_set _ hnh, issuedSlot_set _ hnh] using hord
  · intro h hh
    obtain ⟨d, hd⟩ := hholes h hh
    refine ⟨d, ?_⟩
    by_cases hdc : d = c
    · subst hdc; simp; rw [← obs_ticket hobs]; exact hd
    · simpa [hdc] using hd
  · intro d e t hd he
    have hd' : (s.tx d).ticket = some t := by
      by_cases hdc : d = c
      · subst hdc; simp at hd; rw [obs_ticket hobs]; exact hd
      · simpa [hdc] using hd
    have he' : (s.tx e).ticket = some t := by
      by_cases hec : e = c
      · subst hec; simp at he; rw [obs_ticket hobs]; exact he
      · simpa [hec] using he
    exact huniq d e t hd' he'

end GroupCommit

namespace GroupCommit
open Sys

/-- No part of the invariant reads the record of `c` for another transaction. -/
structure Unreferenced (s : Sys) (c : Nat) : Prop where
  notHolder : NotHolder s c
  notQueued : ∀ e ∈ s.queue, e.tx ≠ c
  notIssued : s.g.issued ≠ some c
  notAbandoned : c ∉ s.g.abandoned
  notTaken : c ∉ s.g.taken
  notWithdrawn : c ∉ s.g.withdrawn
  notTarget : ∀ l b, s.lead = some (l, b) →
    (s.tx l).pc ≠ .dRbOtherA c ∧ (s.tx l).pc ≠ .dRbOtherB c
  ticketFree : ∀ t, (s.tx c).ticket = some t → t ∉ s.g.retry

theorem Sys.lockHolder_set_with (s : Sys) (c : Nat) (r : TxRec) (o : Option Nat) :
    ((s.set c r).withLock o).tx = upd s.tx c r := rfl

/-- A step of `c` that changes only the record of `c` and maybe takes or
releases the commit lock keeps the parts of the invariant about other
transactions, if no such part reads the record of `c`. -/
theorem frame_unref {s : Sys} {c : Nat} {r' : TxRec} {o : Option Nat} (hs : Inv s)
    (hu : Unreferenced s c) (hr' : r'.ticket = none)
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d)) :
    Others ((s.set c r').withLock o) c := by
  let s' := (s.set c r').withLock o
  show Others s' c
  obtain ⟨hst, hlog, htk, hgtx, hlead, hq, hiss, hmark, hord, hholes, huniq⟩ := hs
  have hnh := hu.notHolder
  have hunis : s'.unissued = s.unissued := unissued_set r' hnh
  have hient : s'.issuedEntry = s.issuedEntry := issuedEntry_set r' hnh
  have hslot : s'.issuedSlot = s.issuedSlot := issuedSlot_set r' hnh
  have hqueue : s'.queue = s.queue := queue_set r' hnh
  have htx : ∀ d, d ≠ c → s'.tx d = s.tx d := by intro d hd; simp [s', hd]
  have hsg : s'.g = s.g := rfl
  have hslead : s'.lead = s.lead := rfl
  have hsin : ∀ d, s'.InLog d = s.InLog d := fun _ => rfl
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d hd; simpa [StatusInv, htx d hd] using hst d
  · intro d hd
    have := hlog d
    simp only [LogInv, htx d hd] at this ⊢
    exact this
  · intro d hd
    have := htk d
    simp only [TicketInv, htx d hd, hqueue] at this ⊢
    exact this
  · intro d hd
    have hg := hgtx d
    simp only [GroupTxInv, htx d hd] at hg ⊢
    refine ⟨?_, hg.2.1, ?_, ?_⟩
    · rw [hg.1]; exact (hlock d hd).symm
    · intro h1 h2
      rcases hg.2.2.1 h1 h2 with h | ⟨l, hl, hl2⟩
      · exact Or.inl h
      · exact Or.inr ⟨l, hl, by simpa [s', lead_ne_of_notHolder hnh hl] using hl2⟩
    · intro h1 h2
      obtain ⟨l, hl, hl2⟩ := hg.2.2.2 h1 h2
      exact ⟨l, hl, by simpa [s', lead_ne_of_notHolder hnh hl] using hl2⟩
  · simp only [LeadInv] at hlead ⊢
    show (match s.lead with
      | none => s'.g.taken = [] ∧ s'.g.withdrawn = []
      | some (l, b) => LeadOf s' l b)
    split
    · rename_i hl; rw [hl] at hlead; simpa [s'] using hlead
    · rename_i l b hl
      rw [hl] at hlead
      have hlc : l ≠ c := hnh l b hl
      have htl : s'.tx l = s.tx l := htx l hlc
      simp only [LeadOf, htl, hunis, hslot] at hlead ⊢
      obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11, h12⟩ := hlead
      have hwc : ∀ w, w ∈ (s.tx l).pc.rollingBackOther.toList → w ≠ c := by
        intro w hw hwc; subst hwc
        cases hp : (s.tx l).pc <;> simp [Pc.rollingBackOther, hp] at hw
        subst hw; exact (hu.notTarget l b hl).1 hp
      have hwc' : ∀ w, w ∈ (s.tx l).pc.removingOther.toList → w ≠ c := by
        intro w hw hwc; subst hwc
        cases hp : (s.tx l).pc <;> simp [Pc.removingOther, hp] at hw
        subst hw; exact (hu.notTarget l b hl).2 hp
      have hwn : (s.tx l).pc = .own3 → b.writing.tx ≠ c := by
        intro hp hwc
        apply hu.notIssued
        rw [hiss.1, Sys.issuedSlot_eq, hl]
        simp [hp, ← hwc]
      exact ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9,
        fun w hw => by simpa [htx w (hwc w hw), hsg, hsin] using h10 w hw,
        fun w hw => by simpa [htx w (hwc' w hw), hsg, hsin] using h11 w hw,
        fun hp => by rw [htx _ (hwn hp)]; exact h12 hp⟩
  · obtain ⟨q1, q2, q3, q4, q5, q6, q7, q8, q9⟩ := hq
    have hq' : ∀ e ∈ s.queue, s'.tx e.tx = s.tx e.tx := fun e he => htx _ (hu.notQueued e he)
    have hpend : ∀ e ∈ s.g.pending, e.tx ≠ c := fun e he =>
      hu.notQueued e (by simp [Sys.queue, he])
    have hun : ∀ e ∈ s.unissued, e.tx ≠ c := fun e he =>
      hu.notQueued e (by simp [Sys.queue, he])
    have htk' : ∀ x ∈ s.g.taken, x ≠ c := fun x hx h => hu.notTaken (h ▸ hx)
    have hwd' : ∀ x ∈ s.g.withdrawn, x ≠ c := fun x hx h => hu.notWithdrawn (h ▸ hx)
    simp only [QueueInv, hqueue, hunis]
    simp only [hsg, hslead, hsin]
    refine ⟨q1, q2, fun e he => by simpa [hq' e he] using q3 e he,
      fun e he => by simpa [htx _ (hpend e he)] using q4 e he,
      fun e he => by simpa [htx _ (hun e he)] using q5 e he, q6, q7,
      fun x hx => by simpa [htx _ (htk' x hx)] using q8 x hx,
      fun x hx => by simpa [htx _ (hwd' x hx)] using q9 x hx⟩
  · obtain ⟨i1, i2, i3, i4⟩ := hiss
    simp only [IssueInv, hslot, hsg, hslead]
    refine ⟨i1, ?_, ?_, ?_⟩
    · intro w hw
      have hwc : w ≠ c := by
        intro h; subst h; simp at hw; exact hu.notIssued hw
      simpa [htx w hwc] using i2 w hw
    · intro e he l hl hel
      rcases i3 e he l hl hel with h | h
      · left
        have hec : e.tx ≠ c := by
          intro h'
          have hsl : s.issuedSlot.map (·.tx) = some c := by
            simp only [Option.mem_toList] at he; rw [he, ← h']; rfl
          exact hu.notIssued (i1.trans hsl)
        simpa [htx _ hec] using h
      · exact Or.inr h
    · intro x hx
      have hxc : x ≠ c := fun h => hu.notAbandoned (h ▸ hx)
      simpa [htx x hxc] using i4 x hx
  · simpa [MarkInv, s'] using hmark
  · simpa [OrderInv, hqueue, hslot, hsg] using hord
  · intro h hh
    obtain ⟨d, hd⟩ := hholes h hh
    refine ⟨d, ?_⟩
    have hdc : d ≠ c := by intro h'; subst h'; exact hu.ticketFree h hd hh
    simpa [htx d hdc] using hd
  · intro d e t hd he
    have hdc : d ≠ c := by intro h'; subst h'; simp [s', hr'] at hd
    have hec : e ≠ c := by intro h'; subst h'; simp [s', hr'] at he
    exact huniq d e t (by simpa [htx d hdc] using hd) (by simpa [htx e hec] using he)

end GroupCommit

namespace GroupCommit
open Sys

/-- `frame_set_lock` for a step that may also take or release the commit lock. -/
theorem frame_set {s : Sys} {c : Nat} {r' : TxRec} {o : Option Nat} (hs : Inv s)
    (hnh : NotHolder s c) (hobs : (s.tx c).obs = r'.obs) (hab : c ∉ s.g.abandoned)
    (hlock : ∀ d, d ≠ c → (o = some d ↔ s.lockHolder = some d)) :
    Others ((s.set c r').withLock o) c := by
  let s' := (s.set c r').withLock o
  show Others s' c
  obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11⟩ := frame_set_lock hs hnh hobs hab
  refine ⟨h1, h2, h3, ?_, h5, h6, h7, h8, h9, h10, h11⟩
  intro d hd
  have hg := h4 d hd
  have hgo := hs.groupTx d
  simp only [GroupTxInv] at hg hgo ⊢
  refine ⟨?_, hg.2.1, hg.2.2.1, hg.2.2.2⟩
  have : (s'.tx d) = (s.tx d) := by simp [s', hd]
  rw [this, hgo.1]
  exact (hlock d hd).symm

/-- Assemble the invariant from a frame lemma and the parts about `c`. -/
theorem inv_of_parts {s' : Sys} {c : Nat} (hframe : Others s' c)
    (hst : StatusInv s' c) (hlog : LogInv s' c) (htk : TicketInv s' c)
    (hgtx : GroupTxInv s' c) : Inv s' := by
  obtain ⟨h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11⟩ := hframe
  refine ⟨?_, ?_, ?_, ?_, h5, h6, h7, h8, h9, h10, h11⟩
  · intro d; by_cases hd : d = c
    · subst hd; exact hst
    · exact h1 d hd
  · intro d; by_cases hd : d = c
    · subst hd; exact hlog
    · exact h2 d hd
  · intro d; by_cases hd : d = c
    · subst hd; exact htk
    · exact h3 d hd
  · intro d; by_cases hd : d = c
    · subst hd; exact hgtx
    · exact h4 d hd

end GroupCommit
