import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# Getting the end timestamp keeps the invariant

`stepPrepare` gives the transaction its end timestamp `e = clock` after the
first-committer-wins check. The check passes only when the transaction saw
every committed change to the row. So a reader that begins after `e` sees
exactly what the transaction saw, with the transaction's own writes as
committed.
-/

set_option linter.deprecated false
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false

namespace MvccGc.PrepareProof

open MvccGc

theorem split_of_findTx {txs : List Tx} {x : Nat} {t : Tx} (hf : findTx txs x = some t) :
    ∃ l1 l2, txs = l1 ++ t :: l2 := by
  induction txs with
  | nil => simp [findTx] at hf
  | cons u rest ih =>
    rw [findTx_cons] at hf
    by_cases hu : u.id = x
    · rw [if_pos hu] at hf
      cases hf
      exact ⟨[], rest, rfl⟩
    · rw [if_neg hu] at hf
      obtain ⟨l1, l2, h1⟩ := ih hf
      exact ⟨u :: l1, l2, by simp [h1]⟩

theorem map_update_other {l : List Tx} {x : Nat} {f : Tx → Tx} (h : ∀ u ∈ l, u.id ≠ x) :
    l.map (fun u => if u.id = x then f u else u) = l := by
  induction l with
  | nil => rfl
  | cons u rest ih =>
    simp only [List.map_cons]
    rw [if_neg (h u (by simp)), ih (fun w hw => h w (by simp [hw]))]

theorem not_mem_sides {l1 l2 : List Tx} {t : Tx} (hnd : ((l1 ++ t :: l2).map Tx.id).Nodup) :
    (∀ u ∈ l1, u.id ≠ t.id) ∧ (∀ u ∈ l2, u.id ≠ t.id) := by
  rw [List.map_append, List.map_cons] at hnd
  have hp := (List.perm_middle (a := t.id) (l₁ := l1.map Tx.id) (l₂ := l2.map Tx.id)).nodup_iff.1 hnd
  rw [List.nodup_cons] at hp
  constructor
  · intro u hu heq
    exact hp.1 (List.mem_append.2 (Or.inl (List.mem_map.2 ⟨u, hu, heq⟩)))
  · intro u hu heq
    exact hp.1 (List.mem_append.2 (Or.inr (List.mem_map.2 ⟨u, hu, heq⟩)))

theorem updateTx_split {l1 l2 : List Tx} {t : Tx} {f : Tx → Tx}
    (hnd : ((l1 ++ t :: l2).map Tx.id).Nodup) :
    updateTx (l1 ++ t :: l2) t.id f = l1 ++ f t :: l2 := by
  obtain ⟨h1, h2⟩ := not_mem_sides hnd
  simp only [updateTx, List.map_append, List.map_cons, ite_true]
  rw [map_update_other h1, map_update_other h2]

/-- After the step, only the stamp naming the transaction resolves differently. -/
theorem resolve_prep {txs : List Tx} {t : Tx} {st : TxState} {s : Option Stamp}
    (hf : findTx txs t.id = some t) :
    resolve (updateTx txs t.id (fun u => { u with state := st })) s =
      if s = some (.id t.id) then stateClass t.id st else resolve txs s := by
  cases s with
  | none => rfl
  | some p =>
    cases p with
    | ts c => rfl
    | id y =>
      rw [resolve_id, resolve_id,
        findTx_updateTx (f := fun u => { u with state := st }) (fun _ => rfl)]
      by_cases hy : y = t.id
      · subst hy
        simp [hf]
      · have hy' : some (Stamp.id y) ≠ some (Stamp.id t.id) := by simp [hy]
        rw [if_neg hy']
        cases hfy : findTx txs y with
        | none => rfl
        | some u =>
          have hu := (findTx_some hfy).2
          have : u.id ≠ t.id := by omega
          simp [this]

theorem lookupTxState_of_findTx {txs : List Tx} {fin : Finalized} {z : Nat} {u : Tx}
    (hf : findTx txs z = some u) : lookupTxState txs fin z = some u.state := by
  simp [lookupTxState, hf]

/-- Every resolved time in the chain is below the clock. -/
theorem time_lt_clock {s : St} (h : Inv s) {v : Version} (hv : v ∈ s.chain) {p : Stamp}
    (hp : p ∈ stamps v) {c : Nat} (hc : resolve s.txs (some p) = .time c) : c < s.clock := by
  cases p with
  | ts c' =>
    simp [resolve] at hc
    subst hc
    exact (h.tsStamp v hv c' hp).2.1
  | id z =>
    obtain ⟨u, hu, huz, -⟩ := h.idStamp v hv z hp
    subst huz
    rw [resolve_id, h.findTx_mem hu] at hc
    have hb := h.endBounds u hu
    cases hs : u.state <;> simp [stateClass, hs] at hc <;> subst hc <;>
      exact (hb _ (by simp [txEnds, hs])).2

theorem resolve_some_ne_none {txs : List Tx} {p : Stamp} : resolve txs (some p) ≠ .none := by
  cases p with
  | ts c => simp [resolve]
  | id x =>
    rw [resolve_id]
    cases findTx txs x with
    | none => simp
    | some u => cases hs : u.state <;> simp [stateClass, hs]

theorem settled_of_none {txs : List Tx} {w : Version} (hb : w.beginAt = none) :
    settled txs w = (resolve txs w.endAt).isAt := by
  simp [settled, hb, resolve]

theorem settled_of_some {txs : List Tx} {w : Version} {p : Stamp} (hb : w.beginAt = some p) :
    settled txs w = (resolve txs w.beginAt).isAt := by
  have hne := resolve_some_ne_none (txs := txs) (p := p)
  simp only [settled, hb]
  cases hr : resolve txs (some p) <;> simp_all [RS.isAt]

theorem conflict_of_doomed {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs)
    (hact : t.state = .active) (hd : doomed s.txs t s.chain = true) :
    hasVersionConflict s.txs s.fin t s.clock s.chain = true := by
  obtain ⟨w, hw, hwd⟩ := List.any_eq_true.1 hd
  refine List.any_eq_true.2 ⟨w, hw, ?_⟩
  have htp : resolve s.txs (some (.id t.id)) = .pending t.id := by
    simp [resolve, h.findTx_mem ht, hact]
  simp only [Bool.and_eq_true, Bool.or_eq_true] at hwd
  obtain ⟨hset, hafter⟩ := hwd
  have beginConflict : ∀ p, w.beginAt = some p → (resolve s.txs w.beginAt).isAt = true →
      beginConflicts s.txs s.fin t s.clock w = true := by
    intro p hp hat
    unfold beginConflicts
    rw [hp]
    cases p with
    | ts b => rfl
    | id o =>
      have ho : o ≠ t.id := by
        intro heq; subst heq; rw [hp, htp] at hat; simp [RS.isAt] at hat
      dsimp only
      rw [if_neg ho]
      obtain ⟨u, hu, huo, -⟩ := h.idStamp w hw o (mem_stamps.2 (Or.inl hp))
      subst huo
      rw [lookupTxState_of_findTx (h.findTx_mem hu)]
      rw [hp, resolve_id, h.findTx_mem hu] at hat
      have hb := h.endBounds u hu
      cases hs : u.state <;> simp [stateClass, hs, RS.isAt] at hat ⊢
      exact (hb _ (by simp [txEnds, hs])).2
  unfold versionConflicts
  cases he : w.endAt with
  | none =>
    cases hb : w.beginAt with
    | none =>
      rw [settled_of_none hb, he] at hset
      simp [resolve, RS.isAt] at hset
    | some p =>
      have hat : (resolve s.txs w.beginAt).isAt = true := by
        rw [← settled_of_some hb]; exact hset
      simp only [Option.isNone_some, Bool.false_eq_true, ite_false, Bool.false_or]
      simp only [endCheck, he]
      exact beginConflict p hb hat
  | some q =>
    cases q with
    | ts c =>
      simp only [decide_eq_true_eq, Bool.or_eq_true]
      by_cases hc : c > t.beginTs
      · left; exact hc
      · exfalso
        have hend : resolve s.txs w.endAt = .time c := by simp [he, resolve]
        rw [hend] at hafter
        simp only [RS.after, hc, decide_false, Bool.false_eq_true, or_false] at hafter
        cases hrb : resolve s.txs w.beginAt with
        | time b =>
          rw [hrb] at hafter
          simp [RS.after] at hafter
          have := h.endAfterBegin w hw b c hrb (by simp [he, resolve])
          omega
        | _ => rw [hrb] at hafter; simp [RS.after] at hafter
    | id z =>
      simp only [Bool.false_or]
      obtain ⟨u, hu, huz, -⟩ := h.idStamp w hw z (mem_stamps.2 (Or.inr he))
      subst huz
      have hfz := h.findTx_mem hu
      have hbu := h.endBounds u hu
      have hendR : resolve s.txs w.endAt = stateClass u.id u.state := by
        rw [he, resolve_id, hfz]
      cases hb : w.beginAt with
      | none =>
        simp only [Option.isNone_none, ite_true]
        rw [settled_of_none hb, hendR] at hset
        unfold tombstoneConflicts
        rw [he]
        have hzt : u.id ≠ t.id := by
          intro heq
          have := h.findTx_mem ht
          rw [← heq, hfz] at this
          cases this
          simp [stateClass, hact, RS.isAt] at hset
        dsimp only
        rw [if_neg hzt, lookupTxState_of_findTx hfz]
        cases hs : u.state <;> simp [stateClass, hs, RS.isAt] at hset ⊢
        exact (hbu _ (by simp [txEnds, hs])).2
      | some p =>
        have hat : (resolve s.txs w.beginAt).isAt = true := by
          rw [← settled_of_some hb]; exact hset
        simp only [Option.isNone_some, Bool.false_eq_true, ite_false]
        unfold endCheck
        rw [he]
        dsimp only
        by_cases hzt : u.id = t.id
        · exfalso
          have he' : w.endAt = some (.id t.id) := by rw [he, hzt]
          rcases h.deleterSaw w hw t ht he' hact with hb' | hb' | ⟨b, hrb, hlt⟩
          · rw [hb] at hb'; simp at hb'
          · rw [hb', htp] at hat; simp [RS.isAt] at hat
          · rw [he', htp, hrb] at hafter
            simp [RS.after] at hafter
            omega
        · rw [if_neg hzt, lookupTxState_of_findTx hfz]
          cases hs : u.state with
          | committed c =>
            dsimp only
            by_cases hc : c > t.beginTs
            · simp [hc]
            · exfalso
              have hend : resolve s.txs w.endAt = .time c := by
                rw [hendR]; simp [stateClass, hs]
              rw [hend] at hafter
              simp [RS.after, hc] at hafter
              cases hrb : resolve s.txs w.beginAt with
              | time b =>
                rw [hrb] at hafter
                simp [RS.after] at hafter
                have := h.endAfterBegin w hw b c hrb hend
                omega
              | _ => rw [hrb] at hafter; simp [RS.after] at hafter
          | active => exact beginConflict p hb hat
          | preparing c => exact beginConflict p hb hat
          | aborted => exact beginConflict p hb hat
          | terminated => exact beginConflict p hb hat

/-- A resolved time in the chain is never a transaction's begin timestamp. -/
theorem time_ne_begin {s : St} (h : Inv s) {v : Version} (hv : v ∈ s.chain) {p : Stamp}
    (hp : p ∈ stamps v) {c : Nat} (hc : resolve s.txs (some p) = .time c) {u : Tx}
    (hu : u ∈ s.txs) : c ≠ u.beginTs := by
  cases p with
  | ts c' =>
    simp [resolve] at hc
    subst hc
    intro heq
    exact h.beginNotHist u hu (heq ▸ (h.tsStamp v hv c' hp).2.2)
  | id z =>
    obtain ⟨w, hw, hwz, -⟩ := h.idStamp v hv z hp
    subst hwz
    rw [resolve_id, h.findTx_mem hw] at hc
    have hend : c ∈ txEnds w := by
      cases hs : w.state <;> simp [stateClass, hs] at hc <;> simp [txEnds, hs, hc]
    have hnd := h.tsNodup
    rw [List.nodup_append] at hnd
    intro heq
    exact hnd.2.2 u.beginTs (List.mem_map.2 ⟨u, hu, rfl⟩) c
      (List.mem_flatMap.2 ⟨w, hw, hend⟩) heq.symm

theorem not_doomed {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (hact : t.state = .active)
    (hno : hasVersionConflict s.txs s.fin t s.clock s.chain = false) :
    doomed s.txs t s.chain = false := by
  cases hd : doomed s.txs t s.chain
  · rfl
  · rw [conflict_of_doomed h ht hact hd] at hno; exact hno

/-- Every change the transaction saw has a timestamp before its begin. -/
theorem times_before {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (hact : t.state = .active)
    (hno : hasVersionConflict s.txs s.fin t s.clock s.chain = false) {v : Version}
    (hv : v ∈ s.chain) :
    (∀ b, resolve s.txs v.beginAt = .time b → b < t.beginTs) ∧
      (∀ c, resolve s.txs v.endAt = .time c → c < t.beginTs) := by
  have hnd := not_doomed h ht hact hno
  have hvd : (settled s.txs v && ((resolve s.txs v.beginAt).after t.beginTs ||
      (resolve s.txs v.endAt).after t.beginTs)) = false := by
    unfold doomed at hnd
    cases hx : (settled s.txs v && ((resolve s.txs v.beginAt).after t.beginTs ||
      (resolve s.txs v.endAt).after t.beginTs))
    · rfl
    · have : s.chain.any (fun w => settled s.txs w && ((resolve s.txs w.beginAt).after t.beginTs ||
          (resolve s.txs w.endAt).after t.beginTs)) = true := List.any_eq_true.2 ⟨v, hv, hx⟩
      rw [hnd] at this; exact absurd this (by decide)
  have hvc : versionConflicts s.txs s.fin t s.clock v = false := by
    unfold hasVersionConflict at hno
    cases hx : versionConflicts s.txs s.fin t s.clock v
    · rfl
    · have := List.any_eq_true.2 ⟨v, hv, hx⟩
      rw [hno] at this; exact absurd this (by decide)
  constructor
  · intro b hb
    cases hbeg : v.beginAt with
    | none => rw [hbeg] at hb; simp [resolve] at hb
    | some p =>
      have hset : settled s.txs v = true := by
        rw [settled_of_some hbeg, hb]; rfl
      rw [hset, hb] at hvd
      simp [RS.after] at hvd
      have hne := time_ne_begin h hv (mem_stamps.2 (Or.inl hbeg)) (hbeg ▸ hb) ht
      omega
  · intro c hc
    cases hend : v.endAt with
    | none => rw [hend] at hc; simp [resolve] at hc
    | some q =>
      have hne := time_ne_begin h hv (mem_stamps.2 (Or.inr hend)) (hend ▸ hc) ht
      cases q with
      | ts c' =>
        rw [hend] at hc; simp [resolve] at hc; subst hc
        unfold versionConflicts at hvc
        rw [hend] at hvc
        simp only [Bool.or_eq_false_iff, decide_eq_false_iff_not] at hvc
        omega
      | id y =>
        have hset : settled s.txs v = true := by
          cases hbeg : v.beginAt with
          | none => rw [settled_of_none hbeg, hc]; rfl
          | some p =>
            rw [settled_of_some hbeg, hbeg]
            cases p with
            | ts b => rfl
            | id z =>
              rcases h.deletesSee v hv z y hbeg hend with hzy | hz
              · subst hzy
                rw [hend] at hc
                rw [hc]; rfl
              · exact hz
        rw [hset, hc] at hvd
        simp [RS.after] at hvd
        omega

/-- The transaction list after `t` gets its end timestamp. -/
def prepTxs (s : St) (t : Tx) : List Tx :=
  updateTx s.txs t.id (fun u => { u with state := .preparing s.clock })

/-- The next reader after the step. -/
def prepReader (s : St) : Tx :=
  { id := s.nextTx, beginTs := s.clock + 1, state := .active, readMark := s.walPos,
    rewritten := false }

theorem resolve_prepTxs {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (st : Option Stamp) :
    resolve (prepTxs s t) st = if st = some (.id t.id) then .time s.clock else resolve s.txs st := by
  unfold prepTxs
  rw [resolve_prep (h.findTx_mem ht)]
  rfl

theorem id_lt_next {s : St} (h : Inv s) {v : Version} (hv : v ∈ s.chain) {y : Nat}
    (hy : Stamp.id y ∈ stamps v) : y < s.nextTx := by
  obtain ⟨u, hu, huy, -⟩ := h.idStamp v hv y hy
  subst huy
  exact h.idLt u hu

theorem prep_end_views {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (hact : t.state = .active)
    (hno : hasVersionConflict s.txs s.fin t s.clock s.chain = false) {v : Version}
    (hv : v ∈ s.chain) :
    visE (prepReader s) (resolve (prepTxs s t) v.endAt) = visE t (resolve s.txs v.endAt) ∧
      invE (prepReader s) (resolve (prepTxs s t) v.endAt) = invE t (resolve s.txs v.endAt) := by
  have hte := (times_before h ht hact hno hv).2
  have htl := h.beginLt t ht
  have htp : resolve s.txs (some (.id t.id)) = .pending t.id := by
    simp [resolve, h.findTx_mem ht, hact]
  rw [resolve_prepTxs h ht]
  cases he : v.endAt with
  | none => simp [resolve, visE, invE]
  | some q =>
    cases q with
    | ts c =>
      have hc := hte c (by simp [he, resolve])
      have f1 : ¬ (s.clock + 1 < c) := by omega
      have f2 : ¬ (t.beginTs < c) := by omega
      have f3 : c < s.clock + 1 := by omega
      simp [resolve, visE, invE, prepReader, f1, f2, f3, hc]
    | id y =>
      by_cases hy : y = t.id
      · subst hy
        simp only [ite_true, htp]
        have f1 : ¬ (s.clock + 1 < s.clock) := by omega
        simp [visE, invE, prepReader, f1]
      · have hne : some (Stamp.id y) ≠ some (Stamp.id t.id) := by simp [hy]
        rw [if_neg hne]
        have hyn := id_lt_next h hv (mem_stamps.2 (Or.inr he))
        cases hr : resolve s.txs (some (.id y)) with
        | time c =>
          have hc := hte c (by rw [he]; exact hr)
          have f1 : ¬ (s.clock + 1 < c) := by omega
          have f2 : ¬ (t.beginTs < c) := by omega
          have f3 : c < s.clock + 1 := by omega
          simp [visE, invE, prepReader, f1, f2, f3, hc]
        | pending z =>
          have hz : z = y := by
            rw [resolve_id] at hr
            cases hfy : findTx s.txs y with
            | none => rw [hfy] at hr; simp at hr
            | some u =>
              rw [hfy] at hr
              cases hs : u.state <;> simp [stateClass, hs] at hr
              exact hr.symm
          subst hz
          have f1 : z ≠ s.nextTx := by omega
          simp [visE, invE, prepReader, hy, f1]
        | none => simp [visE, invE]
        | dead z => simp [visE, invE]
        | missing z => simp [visE, invE]

theorem prep_views {s : St} {t : Tx} {fin' : Finalized} (h : Inv s) (ht : t ∈ s.txs)
    (hact : t.state = .active) (hno : hasVersionConflict s.txs s.fin t s.clock s.chain = false)
    {v : Version} (hv : v ∈ s.chain) :
    isVisible (prepTxs s t) fin' (prepReader s) v = isVisible s.txs s.fin t v ∧
      isBtreeInvalidating (prepTxs s t) fin' (prepReader s) v =
        isBtreeInvalidating s.txs s.fin t v := by
  have hk := h.known hv
  have hresEq := resolve_prepTxs h ht
  have hk' : known (prepTxs s t) v := by
    obtain ⟨hb, he⟩ := resolve_ne_missing_of_known hk
    apply known_of_resolve
    · intro y hy
      rw [hresEq] at hy
      split at hy
      · simp at hy
      · exact hb y hy
    · intro y hy
      rw [hresEq] at hy
      split at hy
      · simp at hy
      · exact he y hy
  have hr : readerOk s.txs t v := h.readerOk (mem_readers.2 (Or.inr ⟨ht, hact⟩)) hv
  have hr' : readerOk (prepTxs s t) (prepReader s) v := by
    intro y hy hyr
    exfalso
    have := id_lt_next h hv (mem_stamps.2 (Or.inr hy))
    simp [prepReader] at hyr
    omega
  have hend := prep_end_views h ht hact hno hv
  have htb := (times_before h ht hact hno hv).1
  have htl := h.beginLt t ht
  have htp : resolve s.txs (some (.id t.id)) = .pending t.id := by
    simp [resolve, h.findTx_mem ht, hact]
  have hvis : isVisible (prepTxs s t) fin' (prepReader s) v = isVisible s.txs s.fin t v := by
    rw [isVisible_eq hk', isVisible_eq hk, hend.1, hresEq]
    cases hb : v.beginAt with
    | none => simp [resolve, visB]
    | some p =>
      cases p with
      | ts b =>
        have hb' := htb b (by simp [hb, resolve])
        have f1 : b < s.clock + 1 := by omega
        simp [resolve, visB, prepReader, f1, hb']
      | id z =>
        by_cases hz : z = t.id
        · subst hz
          simp only [ite_true, htp]
          rcases h.ownEnds v hv t.id hb (by rw [htp]; rfl) with he | he
          · rw [he]; simp [visB, prepReader, resolve, visE]
          · have f1 : ¬ (s.clock + 1 < s.clock) := by omega
            rw [he, htp]; simp [visB, visE, prepReader, f1]
        · have hne : some (Stamp.id z) ≠ some (Stamp.id t.id) := by simp [hz]
          rw [if_neg hne]
          have hzn := id_lt_next h hv (mem_stamps.2 (Or.inl hb))
          cases hr2 : resolve s.txs (some (.id z)) with
          | time b =>
            have hb' := htb b (by rw [hb]; exact hr2)
            have f1 : b < s.clock + 1 := by omega
            simp [visB, prepReader, f1, hb']
          | pending y =>
            have hy : y = z := by
              rw [resolve_id] at hr2
              cases hfy : findTx s.txs z with
              | none => rw [hfy] at hr2; simp at hr2
              | some u =>
                rw [hfy] at hr2
                cases hs : u.state <;> simp [stateClass, hs] at hr2
                exact hr2.symm
            subst hy
            have f1 : y ≠ s.nextTx := by omega
            simp [visB, prepReader, hz, f1]
          | none => simp [visB]
          | dead y => simp [visB]
          | missing y => simp [visB]
  refine ⟨hvis, ?_⟩
  rw [isBtreeInvalidating_eq (fin := fin') hk' hr', isBtreeInvalidating_eq (fin := s.fin) hk hr,
    ← isVisible_eq (fin := fin') hk', ← isVisible_eq (fin := s.fin) hk, hvis, hend.2]

/-- The prepared transaction. -/
def prepTx (s : St) (t : Tx) : Tx := { t with state := .preparing s.clock }

theorem prep_split {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) :
    ∃ l1 l2, s.txs = l1 ++ t :: l2 ∧ prepTxs s t = l1 ++ prepTx s t :: l2 := by
  obtain ⟨l1, l2, hsplit⟩ := split_of_findTx (h.findTx_mem ht)
  refine ⟨l1, l2, hsplit, ?_⟩
  unfold prepTxs
  rw [hsplit]
  have hnd := h.idsNodup
  rw [hsplit] at hnd
  exact updateTx_split hnd

theorem mem_prepTxs {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) {u : Tx} :
    u ∈ prepTxs s t ↔ u = prepTx s t ∨ (u ∈ s.txs ∧ u.id ≠ t.id) := by
  obtain ⟨l1, l2, hsplit, hprep⟩ := prep_split h ht
  have hnd := h.idsNodup
  rw [hsplit] at hnd
  obtain ⟨h1, h2⟩ := not_mem_sides hnd
  rw [hprep, hsplit]
  simp only [List.mem_append, List.mem_cons]
  constructor
  · rintro (hu | hu | hu)
    · exact Or.inr ⟨Or.inl hu, h1 u hu⟩
    · exact Or.inl hu
    · exact Or.inr ⟨Or.inr (Or.inr hu), h2 u hu⟩
  · rintro (hu | ⟨hu | hu | hu, hne⟩)
    · exact Or.inr (Or.inl hu)
    · exact Or.inl hu
    · subst hu; exact absurd rfl hne
    · exact Or.inr (Or.inr hu)

theorem resolve_prep_ne {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) {st : Option Stamp}
    (hne : st ≠ some (.id t.id)) : resolve (prepTxs s t) st = resolve s.txs st := by
  rw [resolve_prepTxs h ht, if_neg hne]

theorem no_stamp_of_not_wrote {s : St} {t : Tx} (h : Inv s) (hw : lookupWrote s.wrote t.id = none)
    {v : Version} (hv : v ∈ s.chain) : Stamp.id t.id ∉ stamps v := by
  intro hst
  have := h.idStampWrote v hv t.id hst
  simp [wroteRow, hw] at this

/-- Resolution is unchanged on a version with no stamp that names `t`. -/
theorem sameResolve_prep {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) {v : Version}
    (hv : Stamp.id t.id ∉ stamps v) : SameResolve s.txs (prepTxs s t) v := by
  constructor
  · apply resolve_prep_ne h ht
    intro hb; exact hv (mem_stamps.2 (Or.inl hb))
  · apply resolve_prep_ne h ht
    intro he; exact hv (mem_stamps.2 (Or.inr he))

/-- For another reader, an active `t` and a `t` prepared at `clock` look the same:
the end timestamp is after the reader began. -/
theorem other_rs_eq {s : St} {t r : Tx} (h : Inv s) (ht : t ∈ s.txs) (htact : t.state = .active)
    (hne : r.id ≠ t.id) (hrl : r.beginTs < s.clock) (st : Option Stamp) (en : Bool) :
    visB r en (resolve (prepTxs s t) st) = visB r en (resolve s.txs st) ∧
      visE r (resolve (prepTxs s t) st) = visE r (resolve s.txs st) ∧
      invE r (resolve (prepTxs s t) st) = invE r (resolve s.txs st) := by
  rw [resolve_prepTxs h ht]
  split
  · rename_i hst
    subst hst
    have htp : resolve s.txs (some (.id t.id)) = .pending t.id := by
      simp [resolve, h.findTx_mem ht, htact]
    rw [htp]
    have f1 : ¬ (s.clock < r.beginTs) := by omega
    have f2 : t.id ≠ r.id := fun heq => hne heq.symm
    simp [visB, visE, invE, f1, f2, hrl]
  · exact ⟨rfl, rfl, rfl⟩

theorem known_prep {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) {v : Version}
    (hv : v ∈ s.chain) : known (prepTxs s t) v := by
  have hk := h.known hv
  have hresEq := resolve_prepTxs h ht
  obtain ⟨hb, he⟩ := resolve_ne_missing_of_known hk
  apply known_of_resolve
  · intro y hy
    rw [hresEq] at hy
    split at hy
    · simp at hy
    · exact hb y hy
  · intro y hy
    rw [hresEq] at hy
    split at hy
    · simp at hy
    · exact he y hy

theorem prep_other_views {s : St} {t r : Tx} {fin' : Finalized} (h : Inv s) (ht : t ∈ s.txs)
    (htact : t.state = .active) (hr : r ∈ s.txs) (hract : r.state = .active)
    (hne : r.id ≠ t.id) {v : Version} (hv : v ∈ s.chain) :
    isVisible (prepTxs s t) fin' r v = isVisible s.txs s.fin r v ∧
      isBtreeInvalidating (prepTxs s t) fin' r v = isBtreeInvalidating s.txs s.fin r v := by
  have hk := h.known hv
  have hk' := known_prep h ht hv
  have hrd : r ∈ s.readers := mem_readers.2 (Or.inr ⟨hr, hract⟩)
  have hrOk := h.readerOk hrd hv
  have hrOk' : readerOk (prepTxs s t) r v := by
    intro y hy hyr
    subst hyr
    rw [resolve_prep_ne h ht (by simp [hne])]
    exact hrOk r.id hy rfl
  have hrl := h.beginLt r hr
  have eb := other_rs_eq h ht htact hne hrl v.beginAt v.endAt.isNone
  have ee := other_rs_eq h ht htact hne hrl v.endAt true
  have hvis : isVisible (prepTxs s t) fin' r v = isVisible s.txs s.fin r v := by
    rw [isVisible_eq hk', isVisible_eq hk, eb.1, ee.2.1]
  refine ⟨hvis, ?_⟩
  rw [isBtreeInvalidating_eq (fin := fin') hk' hrOk', isBtreeInvalidating_eq (fin := s.fin) hk hrOk,
    ← isVisible_eq (fin := fin') hk', ← isVisible_eq (fin := s.fin) hk, hvis, ee.2.2]

/-- A live committed version not ended by `t` makes `t` fail validation. -/
theorem live_conflicts {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (htact : t.state = .active)
    {v : Version} (hv : v ∈ s.chain) (hlive : live s.txs v = true)
    (hend : v.endAt ≠ some (.id t.id)) : versionConflicts s.txs s.fin t s.clock v = true := by
  have htp : resolve s.txs (some (.id t.id)) = .pending t.id := by
    simp [resolve, h.findTx_mem ht, htact]
  unfold live at hlive
  simp only [Bool.and_eq_true, Bool.not_eq_eq_eq_not, Bool.not_true] at hlive
  obtain ⟨hb, he⟩ := hlive
  cases hbeg : v.beginAt with
  | none => rw [hbeg] at hb; simp [resolve, RS.isAt] at hb
  | some p =>
    have hbc : beginConflicts s.txs s.fin t s.clock v = true := by
      unfold beginConflicts
      rw [hbeg]
      cases p with
      | ts b => rfl
      | id o =>
        have ho : o ≠ t.id := by
          intro heq; subst heq; rw [hbeg, htp] at hb; simp [RS.isAt] at hb
        dsimp only
        rw [if_neg ho]
        obtain ⟨u, hu, huo, -⟩ := h.idStamp v hv o (mem_stamps.2 (Or.inl hbeg))
        subst huo
        rw [lookupTxState_of_findTx (h.findTx_mem hu)]
        rw [hbeg, resolve_id, h.findTx_mem hu] at hb
        have hbd := h.endBounds u hu
        cases hs : u.state <;> simp [stateClass, hs, RS.isAt] at hb ⊢
        exact (hbd _ (by simp [txEnds, hs])).2
    unfold versionConflicts
    cases hendv : v.endAt with
    | none =>
      simp only [hbeg, Option.isNone_some, Bool.false_eq_true, ite_false, Bool.false_or]
      simp only [endCheck, hendv]
      exact hbc
    | some q =>
      cases q with
      | ts c' => rw [hendv] at he; simp [resolve, RS.isAt] at he
      | id y =>
        have hyt : y ≠ t.id := by intro heq; subst heq; exact hend hendv
        simp only [hbeg, Option.isNone_some, Bool.false_eq_true, ite_false, Bool.false_or]
        unfold endCheck
        rw [hendv]
        dsimp only
        rw [if_neg hyt]
        obtain ⟨u, hu, huy, -⟩ := h.idStamp v hv y (mem_stamps.2 (Or.inr hendv))
        subst huy
        rw [lookupTxState_of_findTx (h.findTx_mem hu)]
        rw [hendv, resolve_id, h.findTx_mem hu] at he
        cases hs : u.state <;> simp [stateClass, hs, RS.isAt] at he ⊢ <;> exact hbc

/-- Reads, uniqueness and evidence carry over when every new reader sees the
same as some old reader. -/
theorem reads_transfer {s s' : St} (h : Inv s) (hc : s'.chain = s.chain)
    (hb : s'.btree = s.btree) (hck : s'.ckptMax = s.ckptMax)
    (hmatch : ∀ r ∈ s'.readers, ∃ r0 ∈ s.readers,
      (∀ v ∈ s.chain, isVisible s'.txs s'.fin r v = isVisible s.txs s.fin r0 v ∧
        isBtreeInvalidating s'.txs s'.fin r v = isBtreeInvalidating s.txs s.fin r0 v) ∧
      s'.expected r = s.expected r0 ∧
      (epochChangeBefore s' r.beginTs = true ∨ wroteRow s' r.id = true →
        epochChangeBefore s r0.beginTs = true ∨ wroteRow s r0.id = true)) :
    (∀ t ∈ s'.readers, s'.read t = s'.expected t) ∧
    (∀ t ∈ s'.readers, (s'.chain.filter (isVisible s'.txs s'.fin t)).length ≤ 1) ∧
    (∀ t ∈ s'.readers, s'.btree.isSome = true →
      (epochChangeBefore s' t.beginTs = true ∨ wroteRow s' t.id = true) →
      ∃ v ∈ s'.chain, v.resident = true ∧ isBtreeInvalidating s'.txs s'.fin t v = true) := by
  have huniq : ∀ t ∈ s'.readers, (s'.chain.filter (isVisible s'.txs s'.fin t)).length ≤ 1 := by
    intro t ht
    obtain ⟨r0, hr0, hv, -, -⟩ := hmatch t ht
    rw [hc, List.filter_congr (fun v hv' => (hv v hv').1)]
    exact h.unique r0 hr0
  refine ⟨?_, huniq, ?_⟩
  · intro t ht
    obtain ⟨r0, hr0, hv, hexp, -⟩ := hmatch t ht
    have hm : ∀ v ∈ s'.chain, v.mat = 0 := by rw [hc]; exact h.noMat
    show readRow s'.txs s'.fin t s'.chain s'.btree s'.ckptMax = s'.expected t
    rw [readRow_eq hm (huniq t ht), hexp, ← h.reads r0 hr0, h.read_eq hr0, hc, hb]
    exact rd_congr (fun v hv' => (hv v hv').1) (fun v hv' => (hv v hv').2)
  · intro t ht hbs hcond
    obtain ⟨r0, hr0, hv, -, hev⟩ := hmatch t ht
    rw [hb] at hbs
    obtain ⟨w, hw, hres, hinv⟩ := h.evidence r0 hr0 hbs (hev hcond)
    refine ⟨w, hc ▸ hw, hres, ?_⟩
    rw [(hv w hw).2]
    exact hinv

def prepHist (s : St) (wo : Option (Option Nat)) : List (Nat × Option Nat) :=
  match wo with
  | some w => (s.clock, w) :: s.hist
  | none => s.hist

def prepAfter (s : St) (t : Tx) (wo : Option (Option Nat)) : St :=
  { s with txs := prepTxs s t, clock := s.clock + 1, hist := prepHist s wo }

theorem prepHist_mem {s : St} {wo : Option (Option Nat)} {e : Nat × Option Nat} :
    e ∈ prepHist s wo ↔ e ∈ s.hist ∨ (∃ w, wo = some w ∧ e = (s.clock, w)) := by
  unfold prepHist
  cases wo with
  | none => simp
  | some w => simp [or_comm]

theorem valueAt_prepHist {s : St} (h : Inv s) {wo : Option (Option Nat)} {b : Nat}
    (hb : b ≤ s.clock) : valueAt s.init (prepHist s wo) b = valueAt s.init s.hist b := by
  unfold prepHist
  cases wo with
  | none => rfl
  | some w =>
    rw [valueAt_cons, if_neg (by omega)]

theorem epoch_prepHist {s : St} (h : Inv s) {t : Tx} {wo : Option (Option Nat)} {b : Nat}
    (hb : b ≤ s.clock) :
    epochChangeBefore (prepAfter s t wo) b = epochChangeBefore s b := by
  unfold epochChangeBefore prepAfter prepHist
  cases wo with
  | none => rfl
  | some w =>
    simp only [List.any_cons]
    have : ¬ (s.clock < b) := by omega
    simp [this]

theorem mem_readers_prep {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs)
    {wo : Option (Option Nat)} {r : Tx} (hr : r ∈ (prepAfter s t wo).readers) :
    r = prepReader s ∨ (r ∈ s.txs ∧ r.state = .active ∧ r.id ≠ t.id) := by
  rcases mem_readers.1 hr with h1 | ⟨h2, h3⟩
  · exact Or.inl h1
  · right
    rcases (mem_prepTxs h ht).1 h2 with h4 | ⟨h4, h5⟩
    · subst h4; simp [prepTx] at h3
    · exact ⟨h4, h3, h5⟩

theorem lookupWrote_next {s : St} (h : Inv s) : lookupWrote s.wrote s.nextTx = none := by
  apply lookupWrote_none_of
  intro p hp heq
  obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
  have := h.idLt u hu
  omega

theorem sameCompare_next {s : St} (h : Inv s) {v : Version} (hv : v ∈ s.chain)
    {o : Option Stamp} (ho : ∀ st, o = some st → st ∈ stamps v) :
    SameCompare s.nextReader (prepReader s) (resolve s.txs o) := by
  cases o with
  | none => simp [resolve, SameCompare]
  | some st =>
    cases hr : resolve s.txs (some st) with
    | time c =>
      have hc := time_lt_clock h hv (ho st rfl) hr
      simp only [SameCompare, prepReader, St.nextReader]
      omega
    | pending y => simp [SameCompare, prepReader, St.nextReader]
    | none => trivial
    | dead y => trivial
    | missing y => trivial

theorem prep_match {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (hact : t.state = .active)
    {wo : Option (Option Nat)} (hw : lookupWrote s.wrote t.id = wo)
    (hno : wo.isSome = true → hasVersionConflict s.txs s.fin t s.clock s.chain = false) :
    ∀ r ∈ (prepAfter s t wo).readers, ∃ r0 ∈ s.readers,
      (∀ v ∈ s.chain,
        isVisible (prepAfter s t wo).txs (prepAfter s t wo).fin r v = isVisible s.txs s.fin r0 v ∧
        isBtreeInvalidating (prepAfter s t wo).txs (prepAfter s t wo).fin r v =
          isBtreeInvalidating s.txs s.fin r0 v) ∧
      (prepAfter s t wo).expected r = s.expected r0 ∧
      (epochChangeBefore (prepAfter s t wo) r.beginTs = true ∨
          wroteRow (prepAfter s t wo) r.id = true →
        epochChangeBefore s r0.beginTs = true ∨ wroteRow s r0.id = true) := by
  intro r hr
  have hnext := lookupWrote_next h
  rcases mem_readers_prep h ht hr with rfl | ⟨hr1, hr2, hr3⟩
  · cases wo with
    | some w =>
      refine ⟨t, mem_readers.2 (Or.inr ⟨ht, hact⟩), fun v hv => prep_views h ht hact (hno rfl) hv,
        ?_, fun _ => Or.inr (by simp [wroteRow, hw])⟩
      show (match lookupWrote s.wrote s.nextTx with
        | some w => w
        | none => valueAt s.init (prepHist s (some w)) (s.clock + 1)) = s.expected t
      rw [hnext]
      simp only [prepHist, valueAt_cons, St.expected, hw]
      simp
    | none =>
      refine ⟨s.nextReader, h.nextReader_mem, ?_, ?_, ?_⟩
      · intro v hv
        have hns := no_stamp_of_not_wrote h hw hv
        have hsr := sameResolve_prep h ht hns
        have hk := h.known hv
        have hb := sameCompare_next h hv (o := v.beginAt) (fun st hst => mem_stamps.2 (Or.inl hst))
        have he := sameCompare_next h hv (o := v.endAt) (fun st hst => mem_stamps.2 (Or.inr hst))
        have hrOkN := h.readerOk h.nextReader_mem hv
        have hrOkP : readerOk s.txs (prepReader s) v := by
          intro y hy hyr
          exfalso
          have := id_lt_next h hv (mem_stamps.2 (Or.inr hy))
          simp [prepReader] at hyr
          omega
        have hrOkP' : readerOk (prepTxs s t) (prepReader s) v := by
          intro y hy hyr
          exfalso
          have := id_lt_next h hv (mem_stamps.2 (Or.inr hy))
          simp [prepReader] at hyr
          omega
        constructor
        · show isVisible (prepTxs s t) s.fin (prepReader s) v = _
          rw [isVisible_same hsr hk, isVisible_sameCompare hk hb he]
        · show isBtreeInvalidating (prepTxs s t) s.fin (prepReader s) v = _
          rw [isBtreeInvalidating_same hsr hk hrOkP hrOkP',
            isBtreeInvalidating_sameCompare hk hrOkN hrOkP hb he]
      · show (match lookupWrote s.wrote s.nextTx with
          | some w => w
          | none => valueAt s.init s.hist (s.clock + 1)) =
          (match lookupWrote s.wrote s.nextTx with
          | some w => w
          | none => valueAt s.init s.hist s.clock)
        rw [hnext]
        simp only
        apply valueAt_congr
        intro e he
        have := h.histLt e he
        omega
      · intro hcond
        rcases hcond with hc | hc
        · left
          have : epochChangeBefore (prepAfter s t none) (s.clock + 1) =
              epochChangeBefore s s.clock := by
            apply epochChangeBefore_congr rfl rfl
            intro e he
            have := h.histLt e he
            omega
          show epochChangeBefore s s.clock = true
          rw [← this]; exact hc
        · right; exact hc
  · refine ⟨r, mem_readers.2 (Or.inr ⟨hr1, hr2⟩),
      fun v hv => prep_other_views h ht hact hr1 hr2 hr3 hv, ?_, ?_⟩
    · have hrl := h.beginLt r hr1
      show (match lookupWrote s.wrote r.id with
        | some w => w
        | none => valueAt s.init (prepHist s wo) r.beginTs) = s.expected r
      simp only [St.expected]
      rw [valueAt_prepHist h (by omega)]
      rfl
    · intro hcond
      have hrl := h.beginLt r hr1
      rw [epoch_prepHist h (by omega)] at hcond
      exact hcond

theorem settled_prep_of_not_belongs {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) {w : Version}
    (hnb : belongsTo t.id w = false) : settled (prepTxs s t) w = settled s.txs w := by
  cases hb : w.beginAt with
  | none =>
    rw [settled_of_none hb, settled_of_none hb]
    have hne : w.endAt ≠ some (.id t.id) := by
      intro he
      simp [belongsTo, hb, he] at hnb
    rw [resolve_prep_ne h ht hne]
  | some p =>
    rw [settled_of_some hb, settled_of_some hb]
    have hne : w.beginAt ≠ some (.id t.id) := by
      intro he
      simp [belongsTo, he] at hnb
    rw [resolve_prep_ne h ht hne]

theorem tx_eq_of_id {s : St} (h : Inv s) {u t : Tx} (hu : u ∈ s.txs) (ht : t ∈ s.txs)
    (heq : u.id = t.id) : u = t := by
  have h1 := h.findTx_mem hu
  have h2 := h.findTx_mem ht
  rw [heq, h2] at h1
  exact (Option.some.inj h1).symm

theorem inv_prepared {s : St} {t : Tx} (h : Inv s) (ht : t ∈ s.txs) (hact : t.state = .active)
    {wo : Option (Option Nat)} (hw : lookupWrote s.wrote t.id = wo)
    (hno : wo.isSome = true → hasVersionConflict s.txs s.fin t s.clock s.chain = false) :
    Inv (prepAfter s t wo) := by
  have hmemT : ∀ u, u ∈ prepTxs s t ↔ u = prepTx s t ∨ (u ∈ s.txs ∧ u.id ≠ t.id) :=
    fun u => mem_prepTxs h ht
  obtain ⟨l1, l2, hsplit, hprep⟩ := prep_split h ht
  have htl := h.beginLt t ht
  have hrw : t.rewritten = false := by
    cases hr : t.rewritten
    · rfl
    · obtain ⟨e, he⟩ := h.rewrittenCommitted t ht hr; rw [hact] at he; cases he
  have hends : txEnds t = [] := by simp [txEnds, hact]
  have hendsT : txEnds (prepTx s t) = [s.clock] := by simp [txEnds, prepTx]
  have htpend : resolve s.txs (some (.id t.id)) = .pending t.id := by
    simp [resolve, h.findTx_mem ht, hact]
  have htime : resolve (prepTxs s t) (some (.id t.id)) = .time s.clock := by
    rw [resolve_prepTxs h ht]; simp
  obtain ⟨hreads, huniq, hevid⟩ :=
    reads_transfer (s' := prepAfter s t wo) h rfl rfl rfl (prep_match h ht hact hw hno)
  have histTsMem : ∀ c, c ∈ (prepAfter s t wo).histTs ↔ c ∈ s.histTs ∨ (wo.isSome = true ∧ c = s.clock) := by
    intro c
    simp only [St.histTs, prepAfter, List.mem_map]
    constructor
    · rintro ⟨e, he, rfl⟩
      rcases prepHist_mem.1 he with he | ⟨w, hwo, rfl⟩
      · exact Or.inl ⟨e, he, rfl⟩
      · exact Or.inr ⟨by simp [hwo], rfl⟩
    · rintro (⟨e, he, rfl⟩ | ⟨hs, rfl⟩)
      · exact ⟨e, prepHist_mem.2 (Or.inl he), rfl⟩
      · cases wo with
        | none => simp at hs
        | some w => exact ⟨(s.clock, w), prepHist_mem.2 (Or.inr ⟨w, rfl, rfl⟩), rfl⟩
  have hliveLast : ∀ m1 c m2, s.chain = m1 ++ c :: m2 → live (prepTxs s t) c = true →
      ∀ w ∈ m2, isGarbage w = true ∨ settled (prepTxs s t) w = false := by
    intro m1 c m2 hsc hlive w hw'
    have hc : c ∈ s.chain := by rw [hsc]; simp
    have hwc : w ∈ s.chain := by rw [hsc]; simp [hw']
    by_cases hcb : c.beginAt = some (.id t.id)
    · have htp : (resolve s.txs (some (.id t.id))).isAt = false := by rw [htpend]; rfl
      rcases h.ownEnds c hc t.id hcb htp with hce | hce
      · have hlp : livePendingOf t.id c = true := by simp [livePendingOf, hcb, hce]
        rcases h.pendingOrder t ht hact m1 c m2 hsc hlp with hd | hord
        · exfalso
          cases wo with
          | none => exact no_stamp_of_not_wrote h hw hc (mem_stamps.2 (Or.inl hcb))
          | some w => rw [not_doomed h ht hact (hno rfl)] at hd; cases hd
        · rcases hord w hw' with hg | ⟨hns, hnb⟩
          · exact Or.inl hg
          · right; rw [settled_prep_of_not_belongs h ht hnb]; exact hns
      · exfalso
        unfold live at hlive
        rw [hce, htime] at hlive
        simp [RS.isAt] at hlive
    · by_cases hce : c.endAt = some (.id t.id)
      · exfalso
        unfold live at hlive
        rw [hce, htime] at hlive
        simp [RS.isAt] at hlive
      · have hlive0 : live s.txs c = true := by
          unfold live at hlive ⊢
          rw [resolve_prep_ne h ht hcb, resolve_prep_ne h ht hce] at hlive
          exact hlive
        cases wo with
        | some w =>
          exfalso
          have hvc := live_conflicts h ht hact hc hlive0 hce
          have := hno rfl
          unfold hasVersionConflict at this
          rw [List.any_eq_false] at this
          exact this c hc (by simp [hvc])
        | none =>
          rcases h.liveLast m1 c m2 hsc hlive0 w hw' with hg | hns
          · exact Or.inl hg
          · right
            rw [settled_same (sameResolve_prep h ht (no_stamp_of_not_wrote h hw hwc))]
            exact hns
  have hactiveOther : ∀ y ∈ prepTxs s t, y.state = .active → y ∈ s.txs ∧ y.id ≠ t.id := by
    intro y hy hyact
    rcases (hmemT y).1 hy with rfl | hy'
    · simp [prepTx] at hyact
    · exact hy'
  have hpendingOrder : ∀ y ∈ prepTxs s t, y.state = .active → ∀ m1 n m2, s.chain = m1 ++ n :: m2 →
      livePendingOf y.id n = true → doomed (prepTxs s t) y s.chain = true ∨
        ∀ w ∈ m2, isGarbage w = true ∨
          (settled (prepTxs s t) w = false ∧ belongsTo y.id w = false) := by
    intro y hy hyact m1 n m2 hsn hlp
    obtain ⟨hy1, hy2⟩ := hactiveOther y hy hyact
    have hyl := h.beginLt y hy1
    cases wo with
    | some w =>
      left
      obtain ⟨v, hv, hst⟩ := h.writerStamp t ht hact (by simp [wroteRow, hw])
      unfold doomed
      refine List.any_eq_true.2 ⟨v, hv, ?_⟩
      simp only [Bool.and_eq_true, Bool.or_eq_true]
      rcases mem_stamps.1 hst with hb | he
      · refine ⟨?_, Or.inl ?_⟩
        · rw [settled_of_some hb, hb, htime]; rfl
        · rw [hb, htime]; simp [RS.after]; omega
      · refine ⟨?_, Or.inr ?_⟩
        · rcases h.deleterSaw v hv t ht he hact with hb' | hb' | ⟨b, hrb, -⟩
          · rw [settled_of_none hb', he, htime]; rfl
          · rw [settled_of_some hb', hb', htime]; rfl
          · cases hbeg : v.beginAt with
            | none => rw [hbeg] at hrb; simp [resolve] at hrb
            | some p =>
              have hne : v.beginAt ≠ some (.id t.id) := by
                intro heq; rw [heq, htpend] at hrb; cases hrb
              rw [settled_of_some hbeg, resolve_prep_ne h ht hne, hrb]; rfl
        · rw [he, htime]; simp [RS.after]; omega
    | none =>
      have hsame : ∀ w ∈ s.chain, SameResolve s.txs (prepTxs s t) w :=
        fun w hw' => sameResolve_prep h ht (no_stamp_of_not_wrote h hw hw')
      rw [doomed_same hsame]
      rcases h.pendingOrder y hy1 hyact m1 n m2 hsn hlp with hd | hord
      · exact Or.inl hd
      · right
        intro w hw'
        have hwc : w ∈ s.chain := by rw [hsn]; simp [hw']
        rw [settled_same (hsame w hwc)]
        exact hord w hw'
  have hbeginMem : ∀ u ∈ prepTxs s t, ∃ u0 ∈ s.txs, u.id = u0.id ∧ u.beginTs = u0.beginTs ∧
      u.readMark = u0.readMark ∧ (u.state = u0.state ∨ (u0 = t ∧ u = prepTx s t)) := by
    intro u hu
    rcases (hmemT u).1 hu with rfl | ⟨hu1, -⟩
    · exact ⟨t, ht, rfl, rfl, rfl, Or.inr ⟨rfl, rfl⟩⟩
    · exact ⟨u, hu1, rfl, rfl, rfl, Or.inl rfl⟩
  have htsNodup : ((prepTxs s t).map Tx.beginTs ++ (prepTxs s t).flatMap txEnds).Nodup := by
    have hold := h.tsNodup
    rw [hsplit] at hold
    rw [hprep]
    simp only [List.map_append, List.map_cons, List.flatMap_append, List.flatMap_cons, hends,
      hendsT, List.nil_append] at hold ⊢
    have hperm : ((l1.map Tx.beginTs ++ (prepTx s t).beginTs :: l2.map Tx.beginTs) ++
        (l1.flatMap txEnds ++ ([s.clock] ++ l2.flatMap txEnds))).Perm
        (s.clock :: ((l1.map Tx.beginTs ++ t.beginTs :: l2.map Tx.beginTs) ++
          (l1.flatMap txEnds ++ l2.flatMap txEnds))) := by
      have : (prepTx s t).beginTs = t.beginTs := rfl
      rw [this]
      have e1 : (l1.map Tx.beginTs ++ t.beginTs :: l2.map Tx.beginTs) ++
          (l1.flatMap txEnds ++ ([s.clock] ++ l2.flatMap txEnds)) =
          ((l1.map Tx.beginTs ++ t.beginTs :: l2.map Tx.beginTs) ++ l1.flatMap txEnds) ++
            s.clock :: l2.flatMap txEnds := by simp
      rw [e1]
      refine List.perm_middle.trans ?_
      simp
    rw [hperm.nodup_iff, List.nodup_cons]
    refine ⟨?_, hold⟩
    intro hmem
    rw [← List.flatMap_append, ← List.map_cons, ← List.map_append] at hmem
    simp only [List.mem_append, List.mem_map, List.mem_flatMap] at hmem
    have hts : ∀ u ∈ s.txs, u.beginTs ≠ s.clock := fun u hu => by
      have := h.beginLt u hu; omega
    rcases hmem with ⟨u, hu, hu'⟩ | ⟨u, hu, he⟩
    · have hu2 : u ∈ s.txs := by rw [hsplit]; simpa using hu
      exact hts u hu2 hu'
    · have hu2 : u ∈ s.txs := by
        rw [hsplit]
        simp only [List.mem_append, List.mem_cons] at hu ⊢
        rcases hu with hu | hu
        · exact Or.inl hu
        · exact Or.inr (Or.inr hu)
      have := (h.endBounds u hu2 _ he).2
      omega
  have hlookupSame : ∀ x, lookupWrote (prepAfter s t wo).wrote x = lookupWrote s.wrote x := fun _ => rfl
  exact {
    idsNodup := by
      show ((updateTx s.txs t.id (fun u => { u with state := .preparing s.clock })).map Tx.id).Nodup
      rw [map_id_updateTx (f := fun u => { u with state := .preparing s.clock }) (fun _ => rfl)]
      exact h.idsNodup
    idLt := fun u hu => by
      obtain ⟨u0, hu0, hid, -⟩ := hbeginMem u hu
      rw [hid]; exact h.idLt u0 hu0
    beginGt := fun u hu => by
      obtain ⟨u0, hu0, -, hb, -⟩ := hbeginMem u hu
      rw [hb]; exact h.beginGt u0 hu0
    beginLt := fun u hu => by
      obtain ⟨u0, hu0, -, hb, -⟩ := hbeginMem u hu
      show u.beginTs < s.clock + 1
      have := h.beginLt u0 hu0; omega
    readMarkEq := fun u hu => by
      obtain ⟨u0, hu0, -, -, hr, -⟩ := hbeginMem u hu
      rw [hr]; exact h.readMarkEq u0 hu0
    endBounds := fun u hu e he => by
      show u.beginTs < e ∧ e < s.clock + 1
      rcases (hmemT u).1 hu with rfl | ⟨hu1, -⟩
      · rw [hendsT] at he
        simp at he; subst he
        exact ⟨htl, by omega⟩
      · have := h.endBounds u hu1 e he; omega
    tsNodup := htsNodup
    histSorted := by
      show (prepHist s wo).Pairwise _
      unfold prepHist
      cases wo with
      | none => exact h.histSorted
      | some w =>
        rw [List.pairwise_cons]
        exact ⟨fun a ha => h.histLt a ha, h.histSorted⟩
    histLt := fun e he => by
      show e.1 < s.clock + 1
      rcases prepHist_mem.1 he with he | ⟨w, -, rfl⟩
      · have := h.histLt e he; omega
      · simp
    histPrepared := fun u hu e he w hlw => by
      show (e, w) ∈ prepHist s wo
      rcases (hmemT u).1 hu with rfl | ⟨hu1, -⟩
      · rw [hendsT] at he
        simp at he; subst he
        have : wo = some w := by rw [← hw]; exact hlw
        subst this
        simp [prepHist]
      · exact prepHist_mem.2 (Or.inl (h.histPrepared u hu1 e he w hlw))
    histPreparedNone := fun u hu e he hlw => by
      rw [histTsMem]
      rcases (hmemT u).1 hu with rfl | ⟨hu1, -⟩
      · rw [hendsT] at he
        simp at he; subst he
        have : wo = none := by rw [← hw]; exact hlw
        subst this
        simp
        exact h.clock_not_hist
      · have h1 := h.histPreparedNone u hu1 e he hlw
        have h2 := (h.endBounds u hu1 e he).2
        rintro (h3 | ⟨-, h3⟩)
        · exact h1 h3
        · omega
    histEpoch := fun e he => by
      rcases prepHist_mem.1 he with he | ⟨w, -, rfl⟩
      · rcases h.histEpoch e he with h1 | ⟨u, hu, hue⟩
        · exact Or.inl h1
        · right
          have hut : u.id ≠ t.id := by
            intro heq
            have := tx_eq_of_id h hu ht heq
            subst this
            rw [hends] at hue; simp at hue
          exact ⟨u, (hmemT u).2 (Or.inr ⟨hu, hut⟩), hue⟩
      · right
        exact ⟨prepTx s t, (hmemT _).2 (Or.inl rfl), by rw [hendsT]; simp⟩
    lastLt := by show s.lastCommitted < s.clock + 1; have := h.lastLt; omega
    ckptLe := h.ckptLe
    beginNotHist := fun u hu => by
      rw [histTsMem]
      obtain ⟨u0, hu0, -, hb, -⟩ := hbeginMem u hu
      rw [hb]
      have h1 := h.beginNotHist u0 hu0
      have h2 := h.beginLt u0 hu0
      rintro (h3 | ⟨-, h3⟩)
      · exact h1 h3
      · omega
    tsStamp := fun v hv c hc => by
      obtain ⟨h1, h2, h3⟩ := h.tsStamp v hv c hc
      exact ⟨h1, by show c < s.clock + 1; omega, (histTsMem c).2 (Or.inl h3)⟩
    idStamp := fun v hv x hx => by
      obtain ⟨u, hu, hux, hnt, hnc⟩ := h.idStamp v hv x hx
      by_cases hut : u.id = t.id
      · have := tx_eq_of_id h hu ht hut
        subst this
        refine ⟨prepTx s u, (hmemT _).2 (Or.inl rfl), hux, by simp [prepTx], ?_⟩
        intro e he; simp [prepTx] at he
      · exact ⟨u, (hmemT u).2 (Or.inr ⟨hu, hut⟩), hux, hnt, hnc⟩
    idStampWrote := fun v hv x hx => h.idStampWrote v hv x hx
    noMat := h.noMat
    btreeOk := by
      show s.btree = valueAt s.init (prepHist s wo) (s.ckptMax + 1)
      rw [valueAt_prepHist h (by have := h.ckpt_lt_clock; omega)]
      exact h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
      by_cases hut : u.id = t.id
      · have := tx_eq_of_id h hu ht hut
        subst this
        exact ⟨prepTx s u, (hmemT _).2 (Or.inl rfl), hup⟩
      · exact ⟨u, (hmemT u).2 (Or.inr ⟨hu, hut⟩), hup⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := hreads
    unique := huniq
    evidence := hevid
    noResident := h.noResident
    liveLast := hliveLast
    pendingOrder := hpendingOrder
    deletesSee := fun v hv z x hb he => by
      rcases h.deletesSee v hv z x hb he with h1 | h1
      · exact Or.inl h1
      · right
        change (resolve (prepTxs s t) (some (.id z))).isAt = true
        by_cases hz : z = t.id
        · subst hz; rw [htime]; rfl
        · rw [resolve_prep_ne h ht (by simp [hz])]; exact h1
    ownEnds := fun v hv x hb hnat => by
      have hx : x ≠ t.id := by
        intro heq; subst heq
        change (resolve (prepTxs s t) (some (.id t.id))).isAt = false at hnat
        rw [htime] at hnat; cases hnat
      change (resolve (prepTxs s t) (some (.id x))).isAt = false at hnat
      rw [resolve_prep_ne h ht (by simp [hx])] at hnat
      exact h.ownEnds v hv x hb hnat
    deleterSaw := fun v hv u hu he huact => by
      obtain ⟨hu1, hu2⟩ := hactiveOther u hu huact
      rcases h.deleterSaw v hv u hu1 he huact with h1 | h1 | ⟨b, hrb, hlt⟩
      · exact Or.inl h1
      · exact Or.inr (Or.inl h1)
      · right; right
        have hne : v.beginAt ≠ some (.id t.id) := by
          intro heq; rw [heq, htpend] at hrb; cases hrb
        exact ⟨b, by change resolve (prepTxs s t) v.beginAt = _; rw [resolve_prep_ne h ht hne]; exact hrb, hlt⟩
    writerStamp := fun u hu huact hwr => by
      obtain ⟨hu1, -⟩ := hactiveOther u hu huact
      exact h.writerStamp u hu1 huact hwr
    endAfterBegin := fun v hv b c hb hc => by
      change resolve (prepTxs s t) v.beginAt = _ at hb
      change resolve (prepTxs s t) v.endAt = _ at hc
      by_cases hvb : v.beginAt = some (.id t.id)
      · rw [hvb, htime] at hb
        cases hb
        have htp : (resolve s.txs (some (.id t.id))).isAt = false := by rw [htpend]; rfl
        rcases h.ownEnds v hv t.id hvb htp with he | he
        · rw [he] at hc; simp [resolve] at hc
        · rw [he, htime] at hc; cases hc; exact Nat.le_refl _
      · rw [resolve_prep_ne h ht hvb] at hb
        by_cases hve : v.endAt = some (.id t.id)
        · rw [hve, htime] at hc
          cases hc
          cases hbeg : v.beginAt with
          | none => rw [hbeg] at hb; simp [resolve] at hb
          | some p =>
            have := time_lt_clock h hv (mem_stamps.2 (Or.inl hbeg)) (hbeg ▸ hb)
            omega
        · rw [resolve_prep_ne h ht hve] at hc
          exact h.endAfterBegin v hv b c hb hc
    rewrittenCommitted := fun u hu hr => by
      rcases (hmemT u).1 hu with rfl | ⟨hu1, -⟩
      · simp [prepTx, hrw] at hr
      · exact h.rewrittenCommitted u hu1 hr }

end MvccGc.PrepareProof

namespace MvccGc

open PrepareProof in
theorem inv_prepare {s s' : St} {x : Nat} {t : Tx} (h : Inv s) (hft : findTx s.txs x = some t)
    (hact : t.state = .active) (hs : stepPrepare s t = .ok s') : Inv s' := by
  have ht := (findTx_some hft).1
  unfold stepPrepare at hs
  dsimp only at hs
  split at hs
  · cases hs
  · rename_i hcond
    cases hs
    exact inv_prepared (wo := lookupWrote s.wrote t.id) h ht hact rfl (fun hsome => by
      cases hc : hasVersionConflict s.txs s.fin t s.clock s.chain
      · rfl
      · exfalso; apply hcond; simp [hsome, hc])

end MvccGc
