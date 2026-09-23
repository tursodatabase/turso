import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# A Truncate checkpoint keeps the invariant

The checkpoint runs only when no transaction exists. Then every stamp in the
chain is a commit timestamp, at most one version is current, and the next
reader sees the latest committed value. The checkpoint writes that value to
the B-tree, and its two GC passes remove every version.
-/

set_option linter.deprecated false
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace MvccGc.CheckpointProof

open MvccGc

/-- With no transactions, every stamp in the chain is a timestamp. -/
theorem no_ids {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    ∀ x, Stamp.id x ∉ stamps v := by
  intro x hx
  obtain ⟨u, hu, -⟩ := h.idStamp v hv x hx
  rw [hnil] at hu
  simp at hu

theorem begin_shape {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    v.beginAt = none ∨ ∃ b, v.beginAt = some (.ts b) ∧ s.ckptMax < b ∧ b ≤ s.lastCommitted := by
  cases hb : v.beginAt with
  | none => exact Or.inl rfl
  | some p =>
    cases p with
    | id x => exact absurd (mem_stamps.2 (Or.inl hb)) (no_ids h hnil hv x)
    | ts b =>
      right
      obtain ⟨h1, -, h3⟩ := h.tsStamp v hv b (mem_stamps.2 (Or.inl hb))
      refine ⟨b, rfl, h1, ?_⟩
      simp only [St.histTs, List.mem_map] at h3
      obtain ⟨e, he, heb⟩ := h3
      rcases h.histEpoch e he with h4 | ⟨u, hu, -⟩
      · omega
      · rw [hnil] at hu; simp at hu

theorem end_shape {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    v.endAt = none ∨ ∃ e, v.endAt = some (.ts e) ∧ s.ckptMax < e ∧ e ≤ s.lastCommitted ∧
      e < s.clock ∧ e ∈ s.histTs := by
  cases he : v.endAt with
  | none => exact Or.inl rfl
  | some p =>
    cases p with
    | id x => exact absurd (mem_stamps.2 (Or.inr he)) (no_ids h hnil hv x)
    | ts e =>
      right
      obtain ⟨h1, h2, h3⟩ := h.tsStamp v hv e (mem_stamps.2 (Or.inr he))
      refine ⟨e, rfl, h1, ?_, h2, h3⟩
      simp only [St.histTs, List.mem_map] at h3
      obtain ⟨p, hp, hpe⟩ := h3
      rcases h.histEpoch p hp with h4 | ⟨u, hu, -⟩
      · omega
      · rw [hnil] at hu; simp at hu

theorem vis_current {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    isVisible s.txs s.fin s.nextReader v = isCurrent v := by
  rw [isVisible_eq (h.known hv)]
  have hl := h.lastLt
  rcases begin_shape h hnil hv with hb | ⟨b, hb, hb1, hb2⟩
  · simp [hb, resolve, visB, isCurrent]
  · rcases end_shape h hnil hv with he | ⟨e, he, -, -, he3, -⟩
    · have : b < s.clock := by omega
      simp [hb, he, resolve, visB, visE, isCurrent, St.nextReader, this]
    · have : ¬ (s.clock < e) := by omega
      simp [hb, he, resolve, visB, visE, isCurrent, St.nextReader, this]

theorem inv_current {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    isBtreeInvalidating s.txs s.fin s.nextReader v = (isCurrent v || v.endAt.isSome) := by
  rw [isBtreeInvalidating_eq (h.known hv) (h.readerOk h.nextReader_mem hv),
    ← isVisible_eq (fin := s.fin) (h.known hv), vis_current h hnil hv]
  rcases end_shape h hnil hv with he | ⟨e, he, -, -, he3, -⟩
  · simp [he, resolve, invE]
  · have : e < s.clock := he3
    simp [he, resolve, invE, St.nextReader, this]

/-- The shape of every version at a checkpoint. -/
def Shaped (lo hi : Nat) (v : Version) : Prop :=
  (v.beginAt = none ∨ ∃ b, v.beginAt = some (.ts b) ∧ lo < b ∧ b ≤ hi) ∧
  (v.endAt = none ∨ ∃ e, v.endAt = some (.ts e) ∧ lo < e ∧ e ≤ hi)

theorem shaped {s : St} (h : Inv s) (hnil : s.txs = []) {v : Version} (hv : v ∈ s.chain) :
    Shaped s.ckptMax s.lastCommitted v := by
  refine ⟨begin_shape h hnil hv, ?_⟩
  rcases end_shape h hnil hv with he | ⟨e, he, h1, h2, -, -⟩
  · exact Or.inl he
  · exact Or.inr ⟨e, he, h1, h2⟩

theorem selectStep_eq {fin : Finalized} {lo hi : Nat} {st : Selection} {v : Version}
    (hv : Shaped lo hi v) :
    selectStep [] fin hi (if lo = 0 then none else some lo) st v =
      if isAbortedGarbage v then st
      else { existsInDb := st.existsInDb || v.resident,
             chosen := if v.endAt.isNone || st.existsInDb || v.resident then some v
               else st.chosen } := by
  obtain ⟨hb, he⟩ := hv
  unfold selectStep
  rcases hb with hb | ⟨b, hb, hb1, hb2⟩ <;> rcases he with he | ⟨e, he, he1, he2⟩
  · simp [resolveStamp, hb, he, isAbortedGarbage]
  · have g1 : ¬ (hi < e) := by omega
    by_cases hlo : lo = 0
    · simp [resolveStamp, hb, he, isAbortedGarbage, g1, hlo]
      cases v; simp_all; try (split <;> rfl)
    · have g2 : ¬ (e ≤ lo) := by omega
      simp [resolveStamp, hb, he, isAbortedGarbage, g1, hlo, g2]
      cases v; simp_all; try (split <;> rfl)
  · have g1 : ¬ (hi < b) := by omega
    by_cases hlo : lo = 0
    · simp [resolveStamp, hb, he, isAbortedGarbage, g1, hlo]
      cases v; simp_all; try (split <;> rfl)
    · have g2 : ¬ (b ≤ lo) := by omega
      have g3 : lo < b := hb1
      simp [resolveStamp, hb, he, isAbortedGarbage, g1, hlo, g2, g3]
      cases v; simp_all; try (split <;> rfl)
  · have g1 : ¬ (hi < b) := by omega
    have g4 : ¬ (hi < e) := by omega
    by_cases hlo : lo = 0
    · simp [resolveStamp, hb, he, isAbortedGarbage, g1, g4, hlo]
      cases v; simp_all; try (split <;> rfl)
    · have g2 : ¬ (b ≤ lo) := by omega
      have g5 : ¬ (e ≤ lo) := by omega
      simp [resolveStamp, hb, he, isAbortedGarbage, g1, g4, hlo, g2, g5]
      cases v; simp_all; try (split <;> rfl)

/-- The selection step on a shaped chain. -/
def sel (st : Selection) (v : Version) : Selection :=
  if isAbortedGarbage v then st
  else { existsInDb := st.existsInDb || v.resident,
         chosen := if v.endAt.isNone || st.existsInDb || v.resident then some v else st.chosen }

theorem fold_select_eq {fin : Finalized} {lo hi : Nat} {vs : List Version} {st : Selection}
    (hsh : ∀ v ∈ vs, Shaped lo hi v) :
    vs.foldl (selectStep [] fin hi (if lo = 0 then none else some lo)) st = vs.foldl sel st := by
  induction vs generalizing st with
  | nil => rfl
  | cons v rest ih =>
    simp only [List.foldl_cons]
    rw [selectStep_eq (hsh v (by simp))]
    exact ih (fun w hw => hsh w (by simp [hw]))

theorem fold_garbage {vs : List Version} {st : Selection}
    (hg : ∀ v ∈ vs, isAbortedGarbage v = true) : vs.foldl sel st = st := by
  induction vs generalizing st with
  | nil => rfl
  | cons v rest ih =>
    simp only [List.foldl_cons]
    rw [show sel st v = st by simp [sel, hg v (by simp)]]
    exact ih (fun w hw => hg w (by simp [hw]))

theorem fold_chosen {vs : List Version} {st : Selection} :
    (vs.foldl sel st).chosen = st.chosen ∨
      ∃ v ∈ vs, isAbortedGarbage v = false ∧ (vs.foldl sel st).chosen = some v := by
  induction vs generalizing st with
  | nil => exact Or.inl rfl
  | cons v rest ih =>
    simp only [List.foldl_cons]
    rcases @ih (sel st v) with h1 | ⟨w, hw, hwg, hwc⟩
    · rw [h1]
      by_cases hg : isAbortedGarbage v = true
      · left; simp [sel, hg]
      · by_cases hp : (v.endAt.isNone || st.existsInDb || v.resident) = true
        · right; exact ⟨v, by simp, by simpa using hg, by simp [sel, hg, hp]⟩
        · left; simp [sel, hg, hp]
    · right; exact ⟨w, by simp [hw], hwg, hwc⟩

theorem fold_keeps_some {vs : List Version} {st : Selection} (h : st.chosen.isSome = true) :
    (vs.foldl sel st).chosen.isSome = true := by
  induction vs generalizing st with
  | nil => exact h
  | cons v rest ih =>
    simp only [List.foldl_cons]
    apply ih
    unfold sel
    split
    · exact h
    · split <;> simp_all

theorem fold_some_of {vs : List Version} {st : Selection} {v : Version} (hv : v ∈ vs)
    (hg : isAbortedGarbage v = false) (hp : v.endAt.isNone = true ∨ v.resident = true) :
    (vs.foldl sel st).chosen.isSome = true := by
  induction vs generalizing st with
  | nil => simp at hv
  | cons w rest ih =>
    simp only [List.foldl_cons]
    simp at hv
    rcases hv with rfl | hv
    · apply fold_keeps_some
      simp only [sel, hg]
      rcases hp with hp | hp <;> simp [hp]
    · exact ih hv

theorem fold_last_current {m1 m2 : List Version} {c : Version} {st : Selection}
    (hg : isAbortedGarbage c = false) (hc : c.endAt = none)
    (hm2 : ∀ w ∈ m2, isAbortedGarbage w = true) :
    ((m1 ++ c :: m2).foldl sel st).chosen = some c := by
  rw [List.foldl_append, List.foldl_cons, fold_garbage hm2]
  simp [sel, hg, hc]

theorem settled_empty {w : Version} (hsh : Shaped lo hi w) :
    settled [] w = !isAbortedGarbage w := by
  obtain ⟨hb, he⟩ := hsh
  rcases hb with hb | ⟨b, hb, -, -⟩ <;> rcases he with he | ⟨e, he, -, -⟩ <;>
    simp [settled, hb, he, resolve, RS.isAt, isAbortedGarbage]

theorem checkpoint_value {s : St} (h : Inv s) (hnil : s.txs = []) :
    applyCheckpointWrite s.btree
      (checkpointSelect [] s.fin s.chain s.lastCommitted
        (if s.ckptMax = 0 then none else some s.ckptMax)) = valueAt s.init s.hist s.clock := by
  have hwrote : s.wrote = [] := by
    cases hw : s.wrote with
    | nil => rfl
    | cons p rest =>
      obtain ⟨u, hu, -⟩ := h.wroteKeys p (by simp [hw])
      rw [hnil] at hu; simp at hu
  have hexp : s.expected s.nextReader = valueAt s.init s.hist s.clock := by
    simp [St.expected, hwrote, lookupWrote, St.nextReader]
  have hread := h.reads s.nextReader h.nextReader_mem
  rw [h.read_eq h.nextReader_mem, hexp] at hread
  rw [← hread]
  have hsh := fun v (hv : v ∈ s.chain) => shaped h hnil hv
  unfold checkpointSelect
  rw [fold_select_eq hsh]
  have hvis : ∀ v ∈ s.chain, isVisible s.txs s.fin s.nextReader v = isCurrent v :=
    fun v hv => vis_current h hnil hv
  have hinv : ∀ v ∈ s.chain,
      isBtreeInvalidating s.txs s.fin s.nextReader v = (isCurrent v || v.endAt.isSome) :=
    fun v hv => inv_current h hnil hv
  rw [rd_congr (vis := isCurrent) (inv := fun v => isCurrent v || v.endAt.isSome)
    (fun v hv => hvis v hv) (fun v hv => hinv v hv)]
  unfold rd
  cases hf : s.chain.find? isCurrent with
  | some c =>
    obtain ⟨hcc, m1, m2, hsplit, -⟩ := List.find?_eq_some_iff_append.1 hf
    have hcm : c ∈ s.chain := by rw [hsplit]; simp
    have hc1 : (∃ b, c.beginAt = some (.ts b)) ∧ c.endAt = none := by
      simp only [isCurrent, Bool.and_eq_true, Option.isNone_iff_eq_none] at hcc
      refine ⟨?_, hcc.2⟩
      cases hb : c.beginAt with
      | none => rw [hb] at hcc; simp at hcc
      | some p =>
        cases p with
        | ts b => exact ⟨b, rfl⟩
        | id x => rw [hb] at hcc; simp at hcc
    obtain ⟨⟨b, hb⟩, hce⟩ := hc1
    have hlive : live s.txs c = true := by
      simp [live, hb, hce, resolve, RS.isAt]
    have hm2 : ∀ w ∈ m2, isAbortedGarbage w = true := by
      intro w hw
      have hwc : w ∈ s.chain := by rw [hsplit]; simp [hw]
      rcases h.liveLast m1 c m2 hsplit hlive w hw with hg | hns
      · exact hg
      · rw [hnil, settled_empty (hsh w hwc)] at hns
        simpa using hns
    have hcg : isAbortedGarbage c = false := by simp [isAbortedGarbage, hb]
    rw [hsplit, fold_last_current hcg hce hm2]
    simp [applyCheckpointWrite, hce]
  | none =>
    have hnocur : ∀ v ∈ s.chain, isCurrent v = false := by
      intro v hv
      cases hc : isCurrent v
      · rfl
      · exact absurd (List.find?_eq_none.1 hf v hv) (by simp [hc])
    simp only
    have hchosen := @fold_chosen s.chain { existsInDb := false, chosen := none }
    have hend : ∀ v, (s.chain.foldl sel { existsInDb := false, chosen := none }).chosen = some v →
        v.endAt.isSome = true := by
      intro v hv
      rcases hchosen with h1 | ⟨w, hw, hwg, hwc⟩
      · rw [h1] at hv; simp at hv
      · rw [hwc] at hv
        have hvw : w = v := Option.some.inj hv
        subst hvw
        have hcur := hnocur w hw
        obtain ⟨hb, he⟩ := hsh w hw
        rcases he with he | ⟨e, he, -, -⟩
        · rcases hb with hb | ⟨b, hb, -, -⟩
          · simp [isAbortedGarbage, hb, he] at hwg
          · simp [isCurrent, hb, he] at hcur
        · simp [he]
    cases hany : s.chain.any (fun v => isCurrent v || v.endAt.isSome)
    · have hall : ∀ v ∈ s.chain, isAbortedGarbage v = true := by
        intro v hv
        have h1 : (isCurrent v || v.endAt.isSome) = false := by
          cases hx : (isCurrent v || v.endAt.isSome)
          · rfl
          · have : s.chain.any (fun v => isCurrent v || v.endAt.isSome) = true :=
              List.any_eq_true.2 ⟨v, hv, hx⟩
            rw [hany] at this; exact absurd this (by decide)
        obtain ⟨hb, he⟩ := hsh v hv
        simp only [Bool.or_eq_false_iff] at h1
        rcases he with he | ⟨e, he, -, -⟩
        · rcases hb with hb | ⟨b, hb, -, -⟩
          · simp [isAbortedGarbage, hb, he]
          · simp [isCurrent, hb, he] at h1
        · simp [he] at h1
      rw [fold_garbage hall]
      simp [applyCheckpointWrite]
    · simp only [ite_true]
      cases hbt : s.btree with
      | none =>
        cases hc : (s.chain.foldl sel { existsInDb := false, chosen := none }).chosen with
        | none => simp [applyCheckpointWrite]
        | some v => simp [applyCheckpointWrite, hend v hc]
      | some b0 =>
        obtain ⟨w, hw, hwinv⟩ := List.any_eq_true.1 hany
        have hwe : w.endAt.isSome = true := by
          simp only [Bool.or_eq_true] at hwinv
          rcases hwinv with h1 | h1
          · rw [hnocur w hw] at h1; cases h1
          · exact h1
        obtain ⟨-, he⟩ := hsh w hw
        rcases he with he | ⟨e, he, -, -⟩
        · rw [he] at hwe; cases hwe
        · obtain ⟨he1, he3, he4⟩ := h.tsStamp w hw e (mem_stamps.2 (Or.inr he))
          have hep := h.epochChange_of he4 he1 he3
          obtain ⟨r, hr, hres, hrinv⟩ := h.evidence s.nextReader h.nextReader_mem (by simp [hbt])
            (Or.inl hep)
          have hrg : isAbortedGarbage r = false := by
            cases hg : isAbortedGarbage r
            · rfl
            · rw [garbage_not_invalidating hg] at hrinv; cases hrinv
          have hsome := fold_some_of (st := { existsInDb := false, chosen := none }) hr hrg
            (Or.inr hres)
          cases hc : (s.chain.foldl sel { existsInDb := false, chosen := none }).chosen with
          | none => rw [hc] at hsome; cases hsome
          | some v => simp [applyCheckpointWrite, hend v hc]

theorem gc_shaped {vs : List Version} {lo newMax : Nat} {m : Option Nat}
    (hsh : ∀ v ∈ vs, Shaped lo newMax v) :
    gcChain vs none newMax false m true = rule3 none newMax false m (vs.filter isCurrent) := by
  unfold gcChain
  simp only [ite_true]
  congr 1
  rw [List.filter_filter]
  apply List.filter_congr
  intro v hv
  obtain ⟨hb, he⟩ := hsh v hv
  rcases he with he | ⟨e, he, -, he2⟩
  · rcases hb with hb | ⟨b, hb, -, -⟩
    · simp [keepByRule2, isAbortedGarbage, isCurrent, hb, he]
    · simp [keepByRule2, isAbortedGarbage, isCurrent, hb, he]
  · have : ¬ (newMax < e) := by omega
    rcases hb with hb | ⟨b, hb, -, -⟩ <;>
      simp [keepByRule2, isAbortedGarbage, isCurrent, hb, he, belowLwm, this]

theorem rule3_single {lwm : Option Nat} {ckptMax : Nat} {passive : Bool} {m : Option Nat}
    {rv : Version} : rule3 lwm ckptMax passive m [rv] = [] ∨ rule3 lwm ckptMax passive m [rv] = [rv] := by
  unfold rule3
  simp only
  split
  · split
    · exact Or.inl rfl
    · exact Or.inr rfl
  · exact Or.inr rfl

theorem rule3_current {newMax m : Nat} {rv : Version} {b : Nat} (hb : rv.beginAt = some (.ts b))
    (he : rv.endAt = none) (hbm : b ≤ newMax) (hmat : rv.mat ≠ 0) (hm : rv.mat ≤ m) :
    rule3 none newMax false (some m) [rv] = [] := by
  unfold rule3
  simp [hb, he, matForReaders, hmat, hm, hbm]

theorem rule3_nil {lwm : Option Nat} {ckptMax : Nat} {passive : Bool} {m : Option Nat} :
    rule3 lwm ckptMax passive m [] = [] := rfl

def stampOne (fin : Finalized) (frame snapshot : Nat) (v : Version) : Version :=
  let terminal :=
    if v.endAt.isSome then resolveStamp [] fin v.endAt else resolveStamp [] fin v.beginAt
  match terminal with
  | some t => if t ≤ snapshot then { v with mat := frame } else v
  | none => v

theorem stampChain_eq {fin : Finalized} {frame snapshot : Nat} {vs : List Version} :
    stampChain [] fin frame snapshot vs = vs.map (stampOne fin frame snapshot) := rfl

theorem stampOne_stamps {fin : Finalized} {frame snapshot : Nat} {v : Version} :
    (stampOne fin frame snapshot v).beginAt = v.beginAt ∧
      (stampOne fin frame snapshot v).endAt = v.endAt := by
  unfold stampOne
  simp only
  split
  · split <;> simp
  · simp

theorem stampOne_current {fin : Finalized} {frame snapshot b : Nat} {v : Version}
    (hb : v.beginAt = some (.ts b)) (he : v.endAt = none) (hbs : b ≤ snapshot) :
    (stampOne fin frame snapshot v).mat = frame := by
  simp [stampOne, hb, he, resolveStamp, hbs]

theorem shaped_stamp {fin : Finalized} {frame snapshot lo hi : Nat} {v : Version}
    (hv : Shaped lo hi v) : Shaped lo hi (stampOne fin frame snapshot v) := by
  obtain ⟨h1, h2⟩ := stampOne_stamps (fin := fin) (frame := frame) (snapshot := snapshot) (v := v)
  unfold Shaped
  rw [h1, h2]
  exact hv

theorem isCurrent_stamp {fin : Finalized} {frame snapshot : Nat} {v : Version} :
    isCurrent (stampOne fin frame snapshot v) = isCurrent v := by
  obtain ⟨h1, h2⟩ := stampOne_stamps (fin := fin) (frame := frame) (snapshot := snapshot) (v := v)
  unfold isCurrent
  rw [h1, h2]

theorem checkpoint_empties {s : St} (h : Inv s) (hnil : s.txs = []) {floor : Option Nat} :
    gcChain
      (if (checkpointSelect [] s.fin s.chain s.lastCommitted
            (if s.ckptMax = 0 then none else some s.ckptMax)).isSome then
          gcChain (stampChain [] s.fin (s.walPos + 1) s.lastCommitted s.chain) none
            (max s.ckptMax s.lastCommitted) false floor true
        else s.chain)
      none (max s.ckptMax s.lastCommitted) false (some (s.walPos + 1)) true = [] := by
  have hnm : max s.ckptMax s.lastCommitted = s.lastCommitted := by
    have := h.ckptLe; omega
  rw [hnm]
  have hsh : ∀ v ∈ s.chain, Shaped s.ckptMax s.lastCommitted v := fun v hv => shaped h hnil hv
  have hshs : ∀ v ∈ stampChain [] s.fin (s.walPos + 1) s.lastCommitted s.chain,
      Shaped s.ckptMax s.lastCommitted v := by
    intro v hv
    rw [stampChain_eq, List.mem_map] at hv
    obtain ⟨w, hw, rfl⟩ := hv
    exact shaped_stamp (hsh w hw)
  have hcur : (s.chain.filter isCurrent).length ≤ 1 := by
    have := h.unique s.nextReader h.nextReader_mem
    rwa [List.filter_congr (fun v hv => vis_current h hnil hv)] at this
  have hfilt : (stampChain [] s.fin (s.walPos + 1) s.lastCommitted s.chain).filter isCurrent =
      (s.chain.filter isCurrent).map (stampOne s.fin (s.walPos + 1) s.lastCommitted) := by
    rw [stampChain_eq, List.filter_map]
    congr 1
    apply List.filter_congr
    intro v _
    simp [isCurrent_stamp]
  generalize hc : s.chain.filter isCurrent = cur at hcur hfilt
  match cur, hcur with
  | [], _ =>
    by_cases hsel : (checkpointSelect [] s.fin s.chain s.lastCommitted
        (if s.ckptMax = 0 then none else some s.ckptMax)).isSome = true
    · rw [if_pos hsel, gc_shaped hshs, hfilt]
      rfl
    · rw [if_neg hsel, gc_shaped hsh, hc]
      rfl
  | [c], _ =>
    have hcm : c ∈ s.chain ∧ isCurrent c = true := by
      have : c ∈ s.chain.filter isCurrent := by rw [hc]; simp
      exact List.mem_filter.1 this
    obtain ⟨hcm, hcc⟩ := hcm
    obtain ⟨⟨b, hb⟩, hce⟩ : (∃ b, c.beginAt = some (.ts b)) ∧ c.endAt = none := by
      simp only [isCurrent, Bool.and_eq_true, Option.isNone_iff_eq_none] at hcc
      refine ⟨?_, hcc.2⟩
      cases hb : c.beginAt with
      | none => rw [hb] at hcc; simp at hcc
      | some p =>
        cases p with
        | ts b => exact ⟨b, rfl⟩
        | id x => rw [hb] at hcc; simp at hcc
    have hbl : b ≤ s.lastCommitted := by
      rcases (hsh c hcm).1 with h1 | ⟨b', hb', -, h2⟩
      · rw [hb] at h1; cases h1
      · rw [hb] at hb'; cases hb'; exact h2
    have hsel : (checkpointSelect [] s.fin s.chain s.lastCommitted
        (if s.ckptMax = 0 then none else some s.ckptMax)).isSome = true := by
      unfold checkpointSelect
      rw [fold_select_eq hsh]
      exact fold_some_of hcm (by simp [isAbortedGarbage, hb]) (Or.inl (by simp [hce]))
    rw [if_pos hsel, gc_shaped hshs, hfilt]
    simp only [List.map_cons, List.map_nil]
    have hc' := stampOne_stamps (fin := s.fin) (frame := s.walPos + 1)
      (snapshot := s.lastCommitted) (v := c)
    have hmat := stampOne_current (fin := s.fin) (frame := s.walPos + 1) hb hce hbl
    rcases rule3_single (lwm := none) (ckptMax := s.lastCommitted) (passive := false) (m := floor)
      (rv := stampOne s.fin (s.walPos + 1) s.lastCommitted c) with hr | hr
    · rw [hr]; rfl
    · rw [hr, gc_shaped (fun v hv => by simp at hv; subst hv; exact shaped_stamp (hsh c hcm))]
      have : [stampOne s.fin (s.walPos + 1) s.lastCommitted c].filter isCurrent =
          [stampOne s.fin (s.walPos + 1) s.lastCommitted c] := by
        simp [isCurrent_stamp, hcc]
      rw [this]
      exact rule3_current (by rw [hc'.1, hb]) (by rw [hc'.2, hce]) hbl (by rw [hmat]; omega)
        (by rw [hmat]; exact Nat.le_refl _)

end MvccGc.CheckpointProof

namespace MvccGc.CheckpointProof

open MvccGc

/-- The state right after a checkpoint, in a clean form. -/
def after (s : St) (b : Option Nat) (fin' : Finalized) : St :=
  { s with btree := b, ckptMax := s.lastCommitted, walPos := s.walPos + 1,
           backfill := s.walPos + 1, chain := [], fin := fin' }

theorem inv_after {s : St} (h : Inv s) (hnil : s.txs = []) {fin' : Finalized} :
    Inv (after s (valueAt s.init s.hist s.clock) fin') := by
  have hwrote : s.wrote = [] := by
    cases hw : s.wrote with
    | nil => rfl
    | cons p rest =>
      obtain ⟨u, hu, -⟩ := h.wroteKeys p (by simp [hw])
      rw [hnil] at hu; simp at hu
  have hhist : ∀ e ∈ s.hist, e.1 ≤ s.lastCommitted := by
    intro e he
    rcases h.histEpoch e he with h1 | ⟨u, hu, -⟩
    · exact h1
    · rw [hnil] at hu; simp at hu
  have hval : valueAt s.init s.hist s.clock = valueAt s.init s.hist (s.lastCommitted + 1) := by
    apply valueAt_congr
    intro e he
    have h1 := hhist e he
    have h2 := h.histLt e he
    have h3 := h.lastLt
    omega
  have hreaders : ∀ t ∈ (after s (valueAt s.init s.hist s.clock) fin').readers,
      t = (after s (valueAt s.init s.hist s.clock) fin').nextReader := by
    intro t ht
    rcases mem_readers.1 ht with h1 | ⟨h2, -⟩
    · exact h1
    · change t ∈ s.txs at h2; rw [hnil] at h2; simp at h2
  have hnoTx : ∀ t, t ∉ (after s (valueAt s.init s.hist s.clock) fin').txs := by
    intro t ht; change t ∈ s.txs at ht; rw [hnil] at ht; simp at ht
  have hnoV : ∀ v, v ∉ (after s (valueAt s.init s.hist s.clock) fin').chain := by
    intro v hv; simp [after] at hv
  exact {
    idsNodup := by rw [show (after s _ fin').txs = s.txs from rfl, hnil]; simp
    idLt := fun t ht => absurd ht (hnoTx t)
    beginGt := fun t ht => absurd ht (hnoTx t)
    beginLt := fun t ht => absurd ht (hnoTx t)
    readMarkEq := fun t ht => absurd ht (hnoTx t)
    endBounds := fun t ht => absurd ht (hnoTx t)
    tsNodup := by rw [show (after s _ fin').txs = s.txs from rfl, hnil]; simp
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := fun t ht => absurd ht (hnoTx t)
    histPreparedNone := fun t ht => absurd ht (hnoTx t)
    histEpoch := fun e he => Or.inl (hhist e he)
    lastLt := h.lastLt
    ckptLe := Nat.le_refl _
    beginNotHist := fun t ht => absurd ht (hnoTx t)
    tsStamp := fun v hv => absurd hv (hnoV v)
    idStamp := fun v hv => absurd hv (hnoV v)
    idStampWrote := fun v hv => absurd hv (hnoV v)
    noMat := fun v hv => absurd hv (hnoV v)
    btreeOk := hval
    wroteKeys := fun p hp => by
      change p ∈ s.wrote at hp; rw [hwrote] at hp; simp at hp
    wroteNodup := h.wroteNodup
    walEq := rfl
    reads := fun t ht => by
      have := hreaders t ht
      subst this
      simp [St.read, St.expected, after, readRow, mvccSide, btreeSideValid, hwrote, lookupWrote,
        St.nextReader]
    unique := fun t _ => by simp [after]
    evidence := fun t ht _ hc => by
      exfalso
      have := hreaders t ht
      subst this
      rcases hc with hc | hc
      · unfold epochChangeBefore at hc
        obtain ⟨e, he, hec⟩ := List.any_eq_true.1 hc
        have h1 := hhist e he
        have hec' : s.lastCommitted < e.1 := by
          simp only [after, Bool.and_eq_true, decide_eq_true_eq] at hec
          exact of_decide_eq_true hec.1
        omega
      · simp [wroteRow, after, hwrote, lookupWrote] at hc
    noResident := fun _ v hv => absurd hv (hnoV v)
    liveLast := fun l1 c l2 hsplit => by simp [after] at hsplit
    pendingOrder := fun x hx => absurd hx (hnoTx x)
    deletesSee := fun v hv => absurd hv (hnoV v)
    ownEnds := fun v hv => absurd hv (hnoV v)
    deleterSaw := fun v hv => absurd hv (hnoV v)
    writerStamp := fun t ht => absurd ht (hnoTx t)
    endAfterBegin := fun v hv => absurd hv (hnoV v)
    rewrittenCommitted := fun t ht => absurd ht (hnoTx t) }

end MvccGc.CheckpointProof

namespace MvccGc

open CheckpointProof in
theorem inv_checkpoint {s s' : St} {pinned : Bool} (h : Inv s)
    (hs : stepCheckpoint s pinned false = .ok s') : Inv s' := by
  unfold stepCheckpoint at hs
  dsimp only at hs
  split at hs
  · cases hs
  · rename_i hne
    have hnil : s.txs = [] := by
      cases htx : s.txs with
      | nil => rfl
      | cons u rest => rw [htx] at hne; simp at hne
    simp only [Bool.false_eq_true, ite_false] at hs
    cases hs
    have hempty := checkpoint_empties h hnil
      (floor := if pinned = true then some (min s.walPos (s.walPos + 1)) else some (s.walPos + 1))
    have hval := checkpoint_value h hnil
    have hnm : max s.ckptMax s.lastCommitted = s.lastCommitted := by
      have := h.ckptLe; omega
    have key := inv_after (fin' := s.fin.filter fun p => (referencedIds []).contains p.1) h hnil
    unfold after at key
    rw [← hval] at key
    rw [hnm] at hempty ⊢
    rw [hempty]
    exact key

end MvccGc
