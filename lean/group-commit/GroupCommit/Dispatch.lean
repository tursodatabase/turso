import GroupCommit.Steps.Leave

/-!
# Every step of the fixed protocol keeps the invariant
-/

namespace GroupCommit
open Sys

theorem batch_cases (s : Sys) (c : Nat) : s.batchOf c = none ∨ ∃ b, s.lead = some (c, b) := by
  cases hl : s.lead with
  | none => left; simp [Sys.batchOf_eq, hl]
  | some p =>
    obtain ⟨l, b⟩ := p
    by_cases hlc : l = c
    · subst hlc; right; exact ⟨b, rfl⟩
    · left; simp [Sys.batchOf_eq, hl, hlc]

theorem inv_step_start {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .idle) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch <;> simp only [step, hp] at h
  · split at h
    · rename_i hn; cases h; exact step_start hs hp hn
    · cases h
  · split at h
    · rename_i hn; cases h
      exact step_start_exclusive hs hp hn.1 (by simpa using hn.2)
    · cases h
  all_goals cases h

theorem inv_step_begin {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .begin) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch <;> simp only [step, hp] at h
  · split at h
    · rename_i hn; cases h; exact step_enqueue hs hp (by simpa using hn.1)
    · split at h
      · rename_i _ hn; split at h
        · rename_i hfree; cases h; exact step_begin_direct hs hp (by simpa using hfree)
        · cases h
      · rename_i _ hn; cases h; exact step_begin_exclusive hs hp (by simpa using hn)
  all_goals cases h

theorem inv_step_await {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .await t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch <;> simp only [step, hp] at h
  · split at h
    · rename_i hd; cases h; exact step_ack hs hp hd
    · cases h
  · simp only [Group.takeRetry] at h
    by_cases hhit : s.g.retry.contains t = true
    · simp only [hhit, ↓reduceIte, Option.some.injEq] at h
      subst h
      have hc := hs.status c
      simp only [StatusInv, StatusOf, hp] at hc
      have hlk : s.lockHolder ≠ some c := by
        intro h'; have := (hs.groupTx c).1.2 h'; simp [TxRec.holdsLock, hp, hc.2.1] at this
      exact step_take_retry_aux (o := s.lockHolder) _ hs (by simp [hp, Pc.waitTicket])
        (by simp [hp]) (by simpa using hhit) rfl (fun _ _ => Iff.rfl) hlk
    · simp only [hhit, Bool.false_eq_true, ↓reduceIte, Option.some.injEq] at h
      subst h; exact step_await_miss hs hp
  all_goals cases h

theorem inv_step_awaitLock {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .awaitLock t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch <;> simp only [step, hp] at h
  · split at h
    · rename_i hfree; cases h; exact step_lock_ok hs hp (by simpa using hfree)
    · cases h; exact step_lock_busy hs hp
  all_goals cases h

theorem inv_step_awaitLocked {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .awaitLocked t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch <;> simp only [step, hp] at h
  · simp only [Group.takeRetry] at h
    by_cases hhit : s.g.retry.contains t = true
    · simp only [hhit, ↓reduceIte, Option.some.injEq] at h
      subst h
      have hlk : s.lockHolder = some c := (hs.groupTx c).1.1 (by simp [TxRec.holdsLock, hp])
      exact step_take_retry_aux (o := none) _ hs (by simp [hp, Pc.waitTicket])
        (by simp [hp]) (by simpa using hhit) rfl
        (fun d hd => by rw [hlk]; simp [Ne.symm hd]) (by simp)
    · simp only [hhit, Bool.false_eq_true, ↓reduceIte, Option.some.injEq] at h
      subst h; exact step_locked_miss hs hp (by simpa using hhit)
  all_goals cases h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step_awaitWork {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .awaitWork t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Group.takeWork] at h
    have hsp : ∀ {s'' : Sys}, (s.set c { s.tx c with pc := .syncPrefix t, held := true }).withG s.g =
        s'' → Inv s'' := by
      intro s'' e; subst e; exact step_work_sync hs hp
    have hnone : ∀ {s'' : Sys}, ((s.set c { s.tx c with pc := .await t }).withG s.g).withLock none =
        s'' → Inv s'' := by
      intro s'' e; subst e; exact step_work_none hs hp
    by_cases hr : s.g.retry.isEmpty = true
    · simp only [hr, Bool.not_true, Bool.false_eq_true, ↓reduceIte] at h
      have hr' : s.g.retry = [] := by simpa using hr
      cases hpend : s.g.pending with
      | nil =>
        simp only [hpend] at h
        by_cases hw : s.g.writtenThrough > s.g.durableThrough
        · simp only [hw, ↓reduceIte, Option.some.injEq] at h; exact hsp h
        · simp only [hw, ↓reduceIte, Option.some.injEq] at h; exact hnone h
      | cons e rest =>
        simp only [hpend, Option.some.injEq] at h
        subst h
        exact step_take_lead_aux _ hs hp hr' hpend rfl
    · simp only [hr, Bool.not_false, ↓reduceIte] at h
      by_cases hw : s.g.writtenThrough > s.g.durableThrough
      · simp only [hw, ↓reduceIte, Option.some.injEq] at h; exact hsp h
      · simp only [hw, ↓reduceIte, Option.some.injEq] at h; exact hnone h
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_upgrade {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .upgrade) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · exact step_direct_upgrade hs hb hp
    · exact step_holder_upgrade hs hl hp
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_write {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .write) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Option.some.injEq] at h
      subst h; exact step_direct_write hs hb hp
    · simp only [batchOf_holder hl, Group.tryIssue] at h
      by_cases hw : s.g.withdrawn.contains b.writing.tx = true
      · simp only [hw, ↓reduceIte, Option.some.injEq] at h
        subst h
        have hw' : b.writing.tx ∈ s.g.withdrawn := by simpa using hw
        cases hrest : b.rest with
        | nil =>
          simp only [Sys.advance, batchOf_holder hl, hrest]
          exact step_skip_last_aux _ hs hl hp hw' hrest rfl
        | cons e rest =>
          simp only [Sys.advance, batchOf_holder hl, hrest, Sys.setBatch]
          exact step_skip_next_aux _ hs hl hp hw' hrest rfl
      · simp only [hw, Bool.false_eq_true, ↓reduceIte, Option.some.injEq] at h
        subst h
        exact step_issue_aux _ hs hl hp (by simpa using hw) rfl
  | alt => simp [step, hp] at h
  | fail =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Option.some.injEq] at h
      subst h; exact step_direct_write_fail hs hb hp
    · simp [batchOf_holder hl] at h
  | drop => simp [step, hp] at h

theorem holder_of_clause16 {s : Sys} {c : Nat} (hs : Inv s)
    (h : (s.tx c).pc = .writeIssued ∨ (s.tx c).pc.rollingBackOther.isSome ∨
      (s.tx c).pc.removingOther.isSome) : ∃ b, s.lead = some (c, b) := by
  have := (hs.log c).2.2.2.2.2.2.2.2.2.2.2.2.2.2.2 h
  rcases batch_cases s c with hb | hb
  · rw [hb] at this; cases this
  · exact hb

theorem inv_step_writeIssued {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .writeIssued) (h : step .fixed s c ch = some s') : Inv s' := by
  obtain ⟨b, hl⟩ := holder_of_clause16 hs (Or.inl hp)
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_holder_write_issued hs hl hp
  | fail =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_holder_write_issued_fail hs hl hp
  | alt => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step_finish {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .finish) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Option.some.injEq] at h
      subst h
      exact step_direct_finish_gen hs hb hp rfl rfl rfl rfl rfl (by simp)
        (fun d hd => by simp [hd])
    · have xf := issuedFacts hs hl hp
      simp only [batchOf_holder hl, Option.some.injEq, Sys.setBatch,
        noteWritten_up xf.retry xf.written] at h
      subst h
      exact step_finish_aux _ hs hl hp rfl
  | fail =>
    simp only [step, hp, Option.some.injEq] at h
    subst h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simpa only [hp] using step_direct_io_fail hs hb hp
    · simpa only [hp] using step_holder_io_fail hs hl hp
  | alt => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_own2 {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .own2) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · have hlive : (s.tx c).st = .live := (direct_status hs (Or.inr (Or.inr (Or.inr (Or.inl hp))))).1
      simp only [Sys.writingOwner, hb, hlive, ne_eq, reduceCtorEq, not_false_eq_true, ↓reduceIte,
        Sys.set_set, Sys.set_tx, Option.some.injEq] at h
      subst h
      simpa only [hlive] using step_direct_own2 hs hb hp
    · have hslot : s.issuedSlot = some b.writing := by
        simp [Sys.issuedSlot_eq, hl, hp]
      have hiss : s.g.issued = some b.writing.tx := by rw [hs.issue.1, hslot]; rfl
      have hlive := (hs.issue.2.1 b.writing.tx (by simp [hiss])).1
      simp only [Sys.writingOwner, batchOf_holder hl, hlive, ne_eq, reduceCtorEq,
        not_false_eq_true, ↓reduceIte, Option.some.injEq] at h
      subst h
      simpa only [hlive] using step_own2 hs hl hp
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_syncLog {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .syncLog) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · exact step_direct_sync hs hb hp
    · exact step_holder_sync hs hl hp
  | fail =>
    simp only [step, hp, Option.some.injEq] at h
    subst h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · exact step_sync_log_fail_nobatch hs hp hb
    · exact step_holder_sync_fail hs hl hp
  | alt => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_endCommit {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hf : (s.tx c).failed = false)
    (hp : (s.tx c).pc = .endCommit) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Sys.setBatch, Option.isSome_none, Bool.false_eq_true, ↓reduceIte,
        Option.some.injEq] at h
      subst h
      exact step_end_commit_nobatch hs hp hf hb
    · have hlo := leadOf_of_lead hs hl
      have hretry : s.g.retry = [] := hlo.2.2.2.2.2.2.2.2.1 (Or.inl (by simp [hp, Pc.leading]))
      simp only [batchOf_holder hl, Sys.setBatch, Option.isSome_some, ↓reduceIte,
        reduceCtorEq, Option.some.injEq, markDurable_noHoles hretry hs.order.2.2] at h
      subst h
      exact step_end_commit_batch_gen hs hl hp hf rfl rfl rfl rfl rfl (by simp)
        (fun d hd => by simp [hd])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_committed {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .committed) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Sys.unlockIfHeld] at h
    by_cases hh : (s.tx c).held = true
    · simp only [sys_simps, ↓reduceIte, hh, Option.some.injEq] at h
      subst h
      simpa only [sys_simps, ↓reduceIte] using step_finalize_held hs hp hh
    · have hh' : (s.tx c).held = false := by simpa using hh
      simp only [sys_simps, ↓reduceIte, hh', Bool.false_eq_true, Option.some.injEq] at h
      subst h
      simpa only [sys_simps, ↓reduceIte, hh'] using step_finalize_free hs hp hh'
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_prefixSynced {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hf : (s.tx c).failed = false)
    (hp : (s.tx c).pc = .prefixSynced t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    have hc := hs.status c
    simp only [StatusInv, StatusOf, hp] at hc
    simp only [step, hp, Sys.unlockIfHeld, Sys.withG_tx, hc.1, ne_eq, reduceCtorEq,
      not_false_eq_true, ↓reduceIte, hc.2.1, Sys.set_set, Sys.set_withLock,
      Option.some.injEq] at h
    subst h
    exact step_prefix_synced_gen hs hp hf rfl rfl rfl rfl rfl (by simp [hc.1])
      (fun d hd => by simp [hd])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem finishIssue_eq (g : Group) (w : Nat) :
    g.finishIssue w =
      ({ g with issued := none, abandoned := setErase w g.abandoned }, g.abandoned.contains w) :=
  rfl

theorem inv_step_own3 {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .own3) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · have hheld := (direct_status hs (Or.inr (Or.inr (Or.inr (Or.inr (Or.inl hp)))))).2
      have hln := lead_none_of_direct hs hheld hb
      have hiss0 : s.g.issued = none := by
        rw [hs.issue.1]; simp [Sys.issuedSlot_eq, hln]
      obtain ⟨e1, e2⟩ := group_eq_of_finishIssue (c := c) hiss0 (abandoned_nil hs hiss0)
      simp only [Sys.writingOwner, hb, e1, e2, Bool.false_eq_true, false_and, ↓reduceIte,
        Sys.advance, Sys.withG_batchOf]
      exact step_direct_own3 hs hb hp
    · have hwo : s.writingOwner c = b.writing.tx := by simp [Sys.writingOwner, batchOf_holder hl]
      rw [hwo, finishIssue_eq]
      by_cases hab : b.writing.tx ∈ s.g.abandoned
      · obtain ⟨_, hwdrop, hwlive, hwheld⟩ := hs.issue.2.2.2 _ hab
        have hwc : b.writing.tx ≠ c := by intro h'; rw [h', hp] at hwdrop; cases hwdrop
        have hcont : s.g.abandoned.contains b.writing.tx = true := by simpa using hab
        simp only [hcont, ne_eq, hwc, not_false_eq_true, and_self, ↓reduceIte,
          Sys.finishAbandoned, Sys.withG_tx, hwlive, Sys.unlockIfHeld, Sys.set_tx, Bool.or_true,
          decide_true, hwheld, Bool.false_eq_true]
        cases hrest : b.rest with
        | nil =>
          simp only [Sys.advance, sys_simps, batchOf_holder hl, hrest]
          refine step_own3_gen hs hl hp (p := .syncLog) (b2 := b) (Or.inr ⟨hrest, rfl, rfl⟩)
            (by simp) (by simp [hl]) (by simp) (by simp) (by simp) (by simp [Ne.symm hwc])
            (fun _ => by simp [hab, hwc, Ne.symm hwc, hwheld]) (fun d hd hdw => by simp [hd, hdw])
        | cons e rest =>
          simp only [Sys.advance, sys_simps, batchOf_holder hl, hrest, Sys.setBatch]
          refine step_own3_gen hs hl hp (p := .upgrade) (b2 := { b with writing := e, rest := rest })
            (Or.inl ⟨e, rest, hrest, rfl, rfl⟩)
            (by simp) (by simp) (by simp) (by simp) (by simp) (by simp [Ne.symm hwc])
            (fun _ => by simp [hab, hwc, Ne.symm hwc, hwheld]) (fun d hd hdw => by simp [hd, hdw])
      · have hcont : s.g.abandoned.contains b.writing.tx = false := by simpa using hab
        simp only [hcont, Bool.false_eq_true, false_and, ↓reduceIte]
        cases hrest : b.rest with
        | nil =>
          simp only [Sys.advance, sys_simps, batchOf_holder hl, hrest]
          refine step_own3_gen hs hl hp (p := .syncLog) (b2 := b) (Or.inr ⟨hrest, rfl, rfl⟩)
            (by simp) (by simp [hl]) (by simp) (by simp) (by simp) (by simp)
            (fun hwc => by simp [hab, hwc]) (fun d hd hdw => by simp [hd])
        | cons e rest =>
          simp only [Sys.advance, sys_simps, batchOf_holder hl, hrest, Sys.setBatch]
          refine step_own3_gen hs hl hp (p := .upgrade) (b2 := { b with writing := e, rest := rest })
            (Or.inl ⟨e, rest, hrest, rfl, rfl⟩)
            (by simp) (by simp) (by simp) (by simp) (by simp) (by simp)
            (fun hwc => by simp [hab, hwc]) (fun d hd hdw => by simp [hd])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step_commitEnd {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .commitEnd) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_commit_end hs hp
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_syncPrefix {s s' : Sys} {c t : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .syncPrefix t) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_sync_prefix hs hp
  | fail =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_sync_prefix_fail hs hp
  | alt => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dR1 {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dR1) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Option.some.injEq] at h
      subst h; exact step_dR1_nobatch hs hb hp
    · rw [batchOf_holder hl] at h
      obtain ⟨w, rest, adv⟩ := b
      cases adv with
      | none =>
        simp only [Option.some.injEq] at h
        subst h; exact step_holder_dR1 hs hl hp
      | some a =>
        simp only [Option.some.injEq] at h
        subst h
        rw [noteWritten_same (retry_nil_of_dR1 hs hl hp) (advanced_le hs hl rfl)]
        exact step_holder_dR1 hs hl hp
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step_dR2 {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dR2) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp only [hb, Sys.afterRelease, Sys.setBatch, ↓reduceIte, Option.some.injEq] at h
      subst h; exact step_dR2_nobatch hs hb hp
    · simp only [batchOf_holder hl] at h
      have hlo := leadOf_of_lead hs hl
      have hrel : ∀ {L : List Entry} {s'' : Sys}, s.unissued = L → s.issuedEntry = none →
          s.issuedSlot = none →
          Sys.afterRelease .fixed (s.withG (s.g.requeue .fixed L)) c = s'' → Inv s'' := by
        intro L s'' hu he hsl e
        subst e
        exact step_release_gen hs hl (Or.inl hp) hu he hsl
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
          (fun d hd => by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl, hd])
      rcases hlo.2.2.2.1 (Or.inr hp) with ⟨hup, hsub⟩ | hfin | ⟨hsl, hrest⟩
      · simp only [hsub, hup, Bool.not_false, true_or, and_self, ↓reduceIte,
          Option.some.injEq] at h
        exact hrel (by simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hp, hup])
          (by simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hp, hup])
          (by simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hp, hup]) h
      · have hadv : b.advanced ≠ some b.writing.ticket :=
          hlo.2.2.2.2.2.1 (Or.inl (by simp [TxRec.issuing, hp, hfin]))
        simp only [hfin, reduceCtorEq, or_self, and_false, ↓reduceIte, hadv, decide_false,
          Bool.not_false, or_true, and_self] at h
        by_cases hwc : b.writing.tx = c
        · simp only [hwc, ne_eq, not_true_eq_false, ↓reduceIte, Option.some.injEq] at h
          subst h
          exact step_give_up_gen hs hl hp hfin (Or.inr ⟨hwc, by simp⟩) (by simp) (by simp)
            (by simp) (by simp) (by simp [hfin]) (fun d hd => by simp [hd])
        · simp only [hwc, ne_eq, not_false_eq_true, ↓reduceIte, Group.releaseIssued] at h
          by_cases hab : b.writing.tx ∈ s.g.abandoned
          · have hcont : s.g.abandoned.contains b.writing.tx = true := by simpa using hab
            simp only [hcont, ↓reduceIte, Option.some.injEq] at h
            subst h
            exact step_abandon_gen hs hl hp hfin hab (by simp) (by simp) (by simp) (by simp)
              (by simp) (by simp [hfin]) (fun d hd => by simp [hd])
          · have hcont : s.g.abandoned.contains b.writing.tx = false := by simpa using hab
            simp only [hcont, Bool.false_eq_true, ↓reduceIte, Option.some.injEq] at h
            subst h
            exact step_give_up_gen hs hl hp hfin (Or.inl ⟨hwc, hab, by simp⟩) (by simp) (by simp)
              (by simp) (by simp) (by simp [hfin]) (fun d hd => by simp [hd])
      · have hdf : (s.tx c).dropFrom ≠ .upgrade ∧ (s.tx c).dropFrom ≠ .write ∧
            (s.tx c).dropFrom ≠ .finish := by
          rcases hsl with h' | h' <;> simp [h']
        simp only [hdf.1, hdf.2.1, hdf.2.2, or_self, and_false, ↓reduceIte,
          Option.some.injEq] at h
        exact hrel (by simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hp, hdf.1])
          (by simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hp, hdf.2.2])
          (by simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hp, hdf.2.2]) h
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step_dRbOtherA {s s' : Sys} {c w : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dRbOtherA w) (h : step .fixed s c ch = some s') : Inv s' := by
  obtain ⟨b, hl⟩ :=
    holder_of_clause16 (c := c) hs (Or.inr (Or.inl (by simp [hp, Pc.rollingBackOther])))
  obtain ⟨hwc, _, _, hwpc, _⟩ :=
    (leadOf_of_lead hs hl).2.2.2.2.2.2.2.2.2.1 w (by simp [hp, Pc.rollingBackOther])
  have hwheld : (s.tx w).held = false := by
    have := hs.status w; simp only [StatusInv, StatusOf, hwpc] at this; exact this.1
  cases ch with
  | main =>
    simp only [step, hp, Sys.unlockIfHeld, Sys.set_tx, ↓reduceIte, hwheld, Bool.false_eq_true,
      Option.some.injEq] at h
    subst h
    exact step_rb_other_a_gen hs hl hp (by simp) (by simp) (by simp) (by simp) (by simp)
      (by simp [Ne.symm hwc]) (by simp [hwc, hwheld]) (fun d hd hdw => by simp [hd, hdw])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dRbOtherB {s s' : Sys} {c w : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dRbOtherB w) (h : step .fixed s c ch = some s') : Inv s' := by
  obtain ⟨b, hl⟩ :=
    holder_of_clause16 (c := c) hs (Or.inr (Or.inr (by simp [hp, Pc.removingOther])))
  obtain ⟨hwc, _, _, _⟩ :=
    (leadOf_of_lead hs hl).2.2.2.2.2.2.2.2.2.2.1 w (by simp [hp, Pc.removingOther])
  cases ch with
  | main =>
    simp only [step, hp, Sys.set_tx, Option.some.injEq] at h
    subst h
    exact step_rb_other_b_gen hs hl hp (by simp) (by simp) (by simp) (by simp) (by simp)
      (by simp [Ne.symm hwc]) (by simp [hwc]) (fun d hd hdw => by simp [hd, hdw])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dR2e {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dR2e) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    rcases batch_cases s c with hb | ⟨b, hl⟩
    · simp [hb] at h
    · simp only [batchOf_holder hl, Option.some.injEq] at h
      subst h
      exact step_release_gen (L := b.rest) hs hl (Or.inr hp)
        (by simp [Sys.unissued_eq, hl, TxRec.writingUnissued, hp])
        (by simp [Sys.issuedEntry_eq, hl, TxRec.issuing, hp])
        (by simp [Sys.issuedSlot_eq, hl, TxRec.issuing, hp])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl])
        (fun d hd => by simp [Sys.afterRelease, Sys.setBatch, batchOf_holder hl, hd])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dLeave {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dLeave) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp] at h
    by_cases hiss : s.g.issued = some c
    · have e : s.g.leave c (s.tx c).dropFrom.ticket =
          ({ s.g with abandoned := setInsert c s.g.abandoned }, true) := by
        simp [Group.leave, hiss]
      simp only [e, Option.some.injEq] at h
      subst h
      exact step_leave_abandon_gen hs hp hiss (by simp) (by simp) (by simp) (by simp) (by simp)
        (by simp) (fun d hd => by simp [hd])
    · have e2 := (leave_fields (t := (s.tx c).dropFrom.ticket) hiss).1
      have e : s.g.leave c (s.tx c).dropFrom.ticket =
          ((s.g.leave c (s.tx c).dropFrom.ticket).1, false) := by
        rw [← e2]
      rw [e] at h
      simp only [Option.some.injEq] at h
      subst h
      exact step_leave_gen hs hp hiss (by simp) (by simp) (by simp) (by simp) (by simp)
        (by simp) (fun d hd => by simp [hd])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dC1 {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dC1) (h : step .fixed s c ch = some s') : Inv s' := by
  have hc := hs.status c
  simp only [StatusInv, StatusOf, hp] at hc
  cases ch with
  | main =>
    simp only [step, hp] at h
    by_cases hlive : (s.tx c).st = .live
    · by_cases happ : (s.tx c).appended = true
      · simp only [hlive, happ, and_self, ↓reduceIte, Option.some.injEq] at h
        subst h
        simpa only [hlive, happ] using step_dC1_commit hs hp hlive happ
      · have happ' : (s.tx c).appended = false := by simpa using happ
        simp only [hlive, happ', Bool.false_eq_true, and_false, ↓reduceIte, Option.some.injEq] at h
        subst h
        simpa only [hlive, happ'] using step_dC1_rollback hs hp hlive happ'
    · have hcom : (s.tx c).st = .committed := by
        rcases hc with ⟨h1, _⟩ | ⟨h1, _⟩
        · exact absurd h1 hlive
        · exact h1
      simp only [hcom, reduceCtorEq, false_and, ↓reduceIte, Option.some.injEq] at h
      subst h
      simpa only [hcom] using step_dC1_committed hs hp hcom
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dRbA {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dRbA) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Sys.unlockIfHeld] at h
    by_cases hh : (s.tx c).held = true
    · simp only [sys_simps, ↓reduceIte, hh, Option.some.injEq] at h
      subst h
      simpa only [sys_simps, ↓reduceIte, hh] using step_dRbA (o := none) hs hp (by simp [hh])
    · have hh' : (s.tx c).held = false := by simpa using hh
      simp only [sys_simps, ↓reduceIte, hh', Bool.false_eq_true, Option.some.injEq] at h
      subst h
      exact step_dRbA (o := s.lockHolder) hs hp (by simp [hh'])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dRbB {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dRbB) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Option.some.injEq] at h
    subst h; exact step_dRbB hs hp
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

theorem inv_step_dCommitted {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hp : (s.tx c).pc = .dCommitted) (h : step .fixed s c ch = some s') : Inv s' := by
  cases ch with
  | main =>
    simp only [step, hp, Sys.unlockIfHeld] at h
    by_cases hh : (s.tx c).held = true
    · simp only [sys_simps, ↓reduceIte, hh, Option.some.injEq] at h
      subst h
      simpa only [sys_simps, ↓reduceIte, hh] using step_dCommitted (o := none) hs hp (by simp [hh])
    · have hh' : (s.tx c).held = false := by simpa using hh
      simp only [sys_simps, ↓reduceIte, hh', Bool.false_eq_true, Option.some.injEq] at h
      subst h
      exact step_dCommitted (o := s.lockHolder) hs hp (by simp [hh'])
  | alt => simp [step, hp] at h
  | fail => simp [step, hp] at h
  | drop => simp [step, hp] at h

end GroupCommit

namespace GroupCommit
open Sys

theorem inv_step {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (hf : (s.tx c).failed = false) (h : step .fixed s c ch = some s') : Inv s' := by
  have hst := hs.status c
  cases hp : (s.tx c).pc with
  | idle => exact inv_step_start hs hp h
  | begin => exact inv_step_begin hs hp h
  | await t => exact inv_step_await hs hp h
  | awaitLock t => exact inv_step_awaitLock hs hp h
  | awaitLocked t => exact inv_step_awaitLocked hs hp h
  | awaitWork t => exact inv_step_awaitWork hs hp h
  | upgrade => exact inv_step_upgrade hs hp h
  | write => exact inv_step_write hs hp h
  | writeChecked => simp [StatusInv, StatusOf, hp] at hst
  | writeIssued => exact inv_step_writeIssued hs hp h
  | finish => exact inv_step_finish hs hp h
  | own2 => exact inv_step_own2 hs hp h
  | own3 => exact inv_step_own3 hs hp h
  | own4 => simp [StatusInv, StatusOf, hp] at hst
  | syncLog => exact inv_step_syncLog hs hp h
  | endCommit => exact inv_step_endCommit hs hf hp h
  | commitEnd => exact inv_step_commitEnd hs hp h
  | committed => exact inv_step_committed hs hp h
  | syncPrefix t => exact inv_step_syncPrefix hs hp h
  | prefixSynced t => exact inv_step_prefixSynced hs hf hp h
  | done => cases ch <;> simp [step, hp] at h
  | dR1 => exact inv_step_dR1 hs hp h
  | dR2 => exact inv_step_dR2 hs hp h
  | dR2c => simp [StatusInv, StatusOf, hp] at hst
  | dRbOtherA w => exact inv_step_dRbOtherA hs hp h
  | dRbOtherB w => exact inv_step_dRbOtherB hs hp h
  | dR2d => simp [StatusInv, StatusOf, hp] at hst
  | dR2e => exact inv_step_dR2e hs hp h
  | dR3 => simp [StatusInv, StatusOf, hp] at hst
  | dLeave => exact inv_step_dLeave hs hp h
  | dC1 => exact inv_step_dC1 hs hp h
  | dC2 => simp [StatusInv, StatusOf, hp] at hst
  | dRbA => exact inv_step_dRbA hs hp h
  | dRbB => exact inv_step_dRbB hs hp h
  | dCommitted => exact inv_step_dCommitted hs hp h
  | dropped => cases ch <;> simp [step, hp] at h

/-- Every step of a thread of the fixed protocol keeps the invariant. -/
theorem inv_next {s s' : Sys} {c : Nat} {ch : Choice} (hs : Inv s)
    (h : next .fixed s c ch = some s') : Inv s' := by
  unfold next at h
  by_cases hd : ch = .drop
  · simp only [hd, ↓reduceIte] at h
    split at h
    · rename_i hdp
      simp only [Option.some.injEq] at h
      subst h
      rcases batch_cases s c with hb | ⟨b, hl⟩
      · exact step_drop_nobatch hs hdp hb
      · exact step_holder_drop hs hl hdp
    · cases h
  · simp only [hd, ↓reduceIte] at h
    by_cases hf : (s.tx c).failed = true
    · simp [hf] at h
    · simp only [hf, Bool.false_eq_true, ↓reduceIte] at h
      exact inv_step hs (by simpa using hf) h

/-- A change of the `mvcc_group_commit` pragma keeps the invariant. -/
theorem inv_toggle {s : Sys} (hs : Inv s) (e : Bool) : Inv (s.withEnabled e) :=
  ⟨hs.status, hs.log, hs.ticket, hs.groupTx, hs.lead, hs.queue, hs.issue, hs.mark, hs.order,
    hs.holes, hs.unique⟩

/-- Every step of the system keeps the invariant. -/
theorem inv_sys_step {s s' : Sys} (hs : Inv s) (h : Step .fixed s s') : Inv s' := by
  cases h with
  | thread _ _ _ h => exact inv_next hs h
  | toggle => exact inv_toggle hs _

theorem inv_init : Inv init := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro d; simp [StatusInv, StatusOf, init]
  · intro d; simp [LogInv, init, Sys.InLog, Sys.InSynced, Pc.waitTicket, Pc.rollingBackOther,
      Pc.removingOther]
  · intro d; simp [TicketInv, init, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave]
  · intro d; simp [GroupTxInv, init, TxRec.holdsLock, Pc.afterLeave]
  · simp [LeadInv, init]
  · simp [QueueInv, init, Sys.queue, Sys.issuedEntry_eq, Sys.unissued_eq]
  · simp [IssueInv, init, Sys.issuedSlot_eq]
  · simp [MarkInv, init]
  · simp [OrderInv, init, Sys.queue, Sys.issuedEntry_eq, Sys.unissued_eq, Sys.issuedSlot_eq]
  · intro h hh; simp [init] at hh
  · intro d e t hd; simp [init, TxRec.ticket, Pc.waitTicket, Pc.beforeLeave] at hd

/-- The invariant holds in every reachable state of the fixed protocol, for any number of
transactions. -/
theorem inv_reachable {s : Sys} (h : Reachable .fixed s) : Inv s := by
  induction h with
  | init => exact inv_init
  | step _ hstep ih => exact inv_sys_step ih hstep

end GroupCommit
