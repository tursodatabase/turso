import MvccGc.Proof.Facts

/-!
# Lemmas about the transaction list, the write log and `valueAt`
-/

set_option linter.deprecated false

namespace MvccGc

theorem findTx_cons {u : Tx} {rest : List Tx} {y : Nat} :
    findTx (u :: rest) y = if u.id = y then some u else findTx rest y := rfl

theorem findTx_updateTx {txs : List Tx} {x y : Nat} {f : Tx → Tx} (hf : ∀ t, (f t).id = t.id) :
    findTx (updateTx txs x f) y = (findTx txs y).map (fun t => if t.id = x then f t else t) := by
  induction txs with
  | nil => rfl
  | cons u rest ih =>
    simp only [updateTx, List.map_cons] at ih ⊢
    rw [findTx_cons, findTx_cons]
    by_cases hux : u.id = x
    · rw [if_pos hux]
      by_cases huy : u.id = y
      · rw [if_pos (by rw [hf]; exact huy), if_pos huy]
        simp [hux]
      · rw [if_neg (by rw [hf]; exact huy), if_neg huy, ih]
    · rw [if_neg hux]
      by_cases huy : u.id = y
      · rw [if_pos huy, if_pos huy]
        simp [hux]
      · rw [if_neg huy, if_neg huy, ih]

theorem findTx_removeTx {txs : List Tx} {x y : Nat} :
    findTx (removeTx txs x) y = if y = x then none else findTx txs y := by
  induction txs with
  | nil => simp [removeTx, findTx]
  | cons u rest ih =>
    simp only [removeTx] at ih ⊢
    by_cases hux : u.id = x
    · rw [List.filter_cons_of_neg (by simp [hux]), ih, findTx_cons]
      by_cases hy : y = x
      · simp [hy]
      · rw [if_neg hy, if_neg hy, if_neg (by omega)]
    · rw [List.filter_cons_of_pos (by simp [hux]), findTx_cons, findTx_cons, ih]
      by_cases huy : u.id = y
      · have : y ≠ x := by omega
        rw [if_pos huy, if_neg this, if_pos huy]
      · rw [if_neg huy, if_neg huy]

theorem findTx_append_single {txs : List Tx} {u : Tx} {y : Nat} :
    findTx (txs ++ [u]) y =
      match findTx txs y with
      | some t => some t
      | none => if u.id = y then some u else none := by
  induction txs with
  | nil => simp [findTx]
  | cons w rest ih =>
    simp only [List.cons_append]
    rw [findTx_cons, findTx_cons]
    by_cases hw : w.id = y
    · simp [hw]
    · rw [if_neg hw, if_neg hw, ih]

theorem map_id_updateTx {txs : List Tx} {x : Nat} {f : Tx → Tx} (hf : ∀ t, (f t).id = t.id) :
    (updateTx txs x f).map Tx.id = txs.map Tx.id := by
  simp only [updateTx, List.map_map]
  apply List.map_congr_left
  intro t _
  by_cases h : t.id = x <;> simp [h, hf]

theorem mem_updateTx {txs : List Tx} {x : Nat} {f : Tx → Tx} {t : Tx} :
    t ∈ updateTx txs x f ↔ ∃ u ∈ txs, (if u.id = x then f u else u) = t := by
  simp [updateTx]

theorem mem_removeTx {txs : List Tx} {x : Nat} {t : Tx} :
    t ∈ removeTx txs x ↔ t ∈ txs ∧ t.id ≠ x := by
  simp [removeTx]

theorem lookupWrote_cons {p : Nat × Option Nat} {rest : List (Nat × Option Nat)} {y : Nat} :
    lookupWrote (p :: rest) y = if p.1 = y then some p.2 else lookupWrote rest y := by
  obtain ⟨k, v⟩ := p
  rfl

theorem lookupWrote_filter_ne {w : List (Nat × Option Nat)} {x y : Nat} (hy : y ≠ x) :
    lookupWrote (w.filter (fun p => decide (p.1 ≠ x))) y = lookupWrote w y := by
  induction w with
  | nil => rfl
  | cons p rest ih =>
    by_cases hp : p.1 = x
    · rw [List.filter_cons_of_neg (by simp [hp]), ih, lookupWrote_cons, if_neg (by omega)]
    · rw [List.filter_cons_of_pos (by simp [hp]), lookupWrote_cons, lookupWrote_cons, ih]

theorem lookupWrote_filter_eq {w : List (Nat × Option Nat)} {x : Nat} :
    lookupWrote (w.filter (fun p => decide (p.1 ≠ x))) x = none := by
  induction w with
  | nil => rfl
  | cons p rest ih =>
    by_cases hp : p.1 = x
    · rw [List.filter_cons_of_neg (by simp [hp]), ih]
    · rw [List.filter_cons_of_pos (by simp [hp]), lookupWrote_cons, if_neg hp, ih]

theorem lookupWrote_setWrote {w : List (Nat × Option Nat)} {x y : Nat} {v : Option Nat} :
    lookupWrote (setWrote w x v) y = if y = x then some v else lookupWrote w y := by
  unfold setWrote
  rw [lookupWrote_cons]
  by_cases hy : y = x
  · subst hy; simp
  · rw [if_neg (Ne.symm hy), if_neg hy, lookupWrote_filter_ne hy]

theorem lookupWrote_dropWrote {w : List (Nat × Option Nat)} {x y : Nat} :
    lookupWrote (dropWrote w x) y = if y = x then none else lookupWrote w y := by
  unfold dropWrote
  by_cases hy : y = x
  · subst hy; rw [if_pos rfl, lookupWrote_filter_eq]
  · rw [if_neg hy, lookupWrote_filter_ne hy]

theorem lookupWrote_mem {w : List (Nat × Option Nat)} {x : Nat} {v : Option Nat}
    (h : lookupWrote w x = some v) : (x, v) ∈ w := by
  induction w with
  | nil => simp [lookupWrote] at h
  | cons p rest ih =>
    rw [lookupWrote_cons] at h
    split at h
    · cases h; rename_i hp; simp [← hp]
    · exact List.mem_cons_of_mem _ (ih h)

theorem lookupWrote_none_of {w : List (Nat × Option Nat)} {x : Nat} (h : ∀ p ∈ w, p.1 ≠ x) :
    lookupWrote w x = none := by
  induction w with
  | nil => rfl
  | cons p rest ih =>
    rw [lookupWrote_cons, if_neg (h p (by simp))]
    exact ih (fun q hq => h q (by simp [hq]))

theorem valueAt_cons {init : Option Nat} {hist : List (Nat × Option Nat)} {e : Nat}
    {w : Option Nat} {s : Nat} :
    valueAt init ((e, w) :: hist) s = if e < s then w else valueAt init hist s := by
  unfold valueAt
  by_cases h : e < s <;> simp [h]

/-- `valueAt` only depends on which change timestamps lie below the bound. -/
theorem valueAt_congr {init : Option Nat} {hist : List (Nat × Option Nat)} {a b : Nat}
    (h : ∀ e ∈ hist, e.1 < a ↔ e.1 < b) : valueAt init hist a = valueAt init hist b := by
  unfold valueAt
  have : hist.find? (fun e => decide (e.1 < a)) = hist.find? (fun e => decide (e.1 < b)) :=
    find?_congr' (fun e he => by simp [h e he])
  rw [this]

theorem epochChangeBefore_congr {s s' : St} {a b : Nat} (hh : s'.hist = s.hist)
    (hc : s'.ckptMax = s.ckptMax) (h : ∀ e ∈ s.hist, e.1 < a ↔ e.1 < b) :
    epochChangeBefore s' b = epochChangeBefore s a := by
  unfold epochChangeBefore
  rw [hh, hc]
  apply any_congr'
  intro e he
  simp [h e he]

end MvccGc

namespace MvccGc

/-- How a transaction state resolves a stamp that names it. -/
def stateClass (y : Nat) : TxState → RS
  | .active => .pending y
  | .preparing e => .time e
  | .committed e => .time e
  | .aborted => .dead y
  | .terminated => .dead y

theorem resolve_id {txs : List Tx} {y : Nat} :
    resolve txs (some (.id y)) =
      match findTx txs y with
      | some t => stateClass y t.state
      | none => .missing y := by
  simp only [resolve]
  cases findTx txs y with
  | none => rfl
  | some t => cases t.state <;> rfl

theorem resolve_ts {txs : List Tx} {c : Nat} : resolve txs (some (.ts c)) = .time c := rfl

theorem resolve_none {txs : List Tx} : resolve txs none = .none := rfl

/-- A reader that is not `x` sees an active `x` and an aborted `x` the same way. -/
theorem views_pending_dead {t : Tx} {x : Nat} (hx : x ≠ t.id) (endNone : Bool) :
    visB t endNone (.pending x) = visB t endNone (.dead x) ∧
      visE t (.pending x) = visE t (.dead x) ∧ invE t (.pending x) = invE t (.dead x) := by
  simp [visB, visE, invE, hx]

/-- Two readers compare the same way against the resolved stamp. -/
def SameCompare (t t' : Tx) : RS → Prop
  | .time c => (t'.beginTs > c ↔ t.beginTs > c) ∧ (t'.beginTs < c ↔ t.beginTs < c)
  | .pending y => (y = t'.id ↔ y = t.id)
  | _ => True

theorem views_sameCompare {t t' : Tx} {r : RS} (h : SameCompare t t' r) (endNone : Bool) :
    visB t' endNone r = visB t endNone r ∧ visE t' r = visE t r ∧ invE t' r = invE t r := by
  cases r with
  | time c =>
    simp only [SameCompare] at h
    simp [visB, visE, invE, h.1, h.2]
  | pending y =>
    simp only [SameCompare] at h
    simp [visB, visE, invE, h]
  | none => simp [visB, visE, invE]
  | dead y => simp [visB, visE, invE]
  | missing y => simp [visB, visE, invE]

theorem isVisible_sameCompare {txs : List Tx} {fin : Finalized} {t t' : Tx} {v : Version}
    (hk : known txs v) (hb : SameCompare t t' (resolve txs v.beginAt))
    (he : SameCompare t t' (resolve txs v.endAt)) :
    isVisible txs fin t' v = isVisible txs fin t v := by
  rw [isVisible_eq hk, isVisible_eq hk, (views_sameCompare hb v.endAt.isNone).1,
    (views_sameCompare he true).2.1]

theorem isBtreeInvalidating_sameCompare {txs : List Tx} {fin : Finalized} {t t' : Tx}
    {v : Version} (hk : known txs v) (hr : readerOk txs t v) (hr' : readerOk txs t' v)
    (hb : SameCompare t t' (resolve txs v.beginAt)) (he : SameCompare t t' (resolve txs v.endAt)) :
    isBtreeInvalidating txs fin t' v = isBtreeInvalidating txs fin t v := by
  rw [isBtreeInvalidating_eq hk hr, isBtreeInvalidating_eq hk hr',
    (views_sameCompare hb v.endAt.isNone).1, (views_sameCompare he true).2.1,
    (views_sameCompare he true).2.2]

/-- `rd` over a mapped chain, when the map keeps what the reader sees. -/
theorem rd_map {vis inv vis' inv' : Version → Bool} {g : Version → Version} {vs : List Version}
    {btree : Option Nat} (hv : ∀ v ∈ vs, vis' (g v) = vis v) (hi : ∀ v ∈ vs, inv' (g v) = inv v)
    (hval : ∀ v ∈ vs, (g v).val = v.val) :
    rd vis' inv' (vs.map g) btree = rd vis inv vs btree := by
  unfold rd
  rw [List.find?_map, List.any_map]
  have hf : vs.find? (vis' ∘ g) = vs.find? vis := find?_congr' (fun v hv' => hv v hv')
  have ha : vs.any (inv' ∘ g) = vs.any inv := any_congr' (fun v hv' => hi v hv')
  rw [hf, ha]
  cases hfv : vs.find? vis with
  | none => rfl
  | some v =>
    simp only [Option.map_some]
    rw [hval v (List.mem_of_find?_eq_some hfv)]

theorem filter_map_length {p q : Version → Bool} {g : Version → Version} {vs : List Version}
    (h : ∀ v ∈ vs, p (g v) = q v) : ((vs.map g).filter p).length = (vs.filter q).length := by
  rw [List.filter_map, List.length_map]
  congr 1
  exact List.filter_congr (fun v hv => h v hv)

theorem map_eq_append_cons {α β : Type} {f : α → β} {l : List α} {l1' l2' : List β} {c' : β}
    (h : l.map f = l1' ++ c' :: l2') :
    ∃ l1 c l2, l = l1 ++ c :: l2 ∧ l1.map f = l1' ∧ f c = c' ∧ l2.map f = l2' := by
  induction l generalizing l1' with
  | nil => simp at h
  | cons a rest ih =>
    cases l1' with
    | nil =>
      simp at h
      exact ⟨[], a, rest, rfl, rfl, h.1, h.2⟩
    | cons b l1'' =>
      simp at h
      obtain ⟨rfl, h2⟩ := h
      obtain ⟨l1, c, l2, h3, h4, h5, h6⟩ := ih h2
      exact ⟨a :: l1, c, l2, by simp [h3], by simp [h4], h5, h6⟩

end MvccGc
