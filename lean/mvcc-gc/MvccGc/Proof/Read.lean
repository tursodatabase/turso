import MvccGc.Proof.View

/-!
# The read result

With no materialization stamps the dual cursor never skips the chain, and
with at most one visible version the newest visible one is the only one.
-/

set_option linter.deprecated false

namespace MvccGc

/-- The row a reader gets, given what it sees in each version. -/
def rd (vis inv : Version → Bool) (vs : List Version) (btree : Option Nat) : Option Nat :=
  match vs.find? vis with
  | some v => some v.val
  | none => if vs.any inv then none else btree

theorem chainIsWriteBuffer_of_noMat {txs : List Tx} {fin : Finalized} {t : Tx}
    {vs : List Version} {ckptMax : Nat} (h : ∀ v ∈ vs, v.mat = 0) (hne : vs ≠ []) :
    chainIsWriteBuffer txs fin t vs ckptMax = true := by
  match vs, hne with
  | [rv], _ =>
    have hm : rv.mat = 0 := h rv (by simp)
    unfold chainIsWriteBuffer
    cases hb : rv.beginAt with
    | none => simp [hb]
    | some st =>
      cases st with
      | id x => simp [hb]
      | ts b => simp [hb, hm]
  | _ :: _ :: _, _ => simp [chainIsWriteBuffer]

theorem find?_congr' {α : Type} {p q : α → Bool} {l : List α} (h : ∀ x ∈ l, p x = q x) :
    l.find? p = l.find? q := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    simp only [List.find?_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem any_congr' {α : Type} {p q : α → Bool} {l : List α} (h : ∀ x ∈ l, p x = q x) :
    l.any p = l.any q := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    simp only [List.any_cons, h a (by simp), ih (fun x hx => h x (by simp [hx]))]

theorem filter_congr' {α : Type} {p q : α → Bool} {l : List α} (h : ∀ x ∈ l, p x = q x) :
    l.filter p = l.filter q := List.filter_congr h

theorem find_reverse_of_unique {p : Version → Bool} {vs : List Version}
    (h : (vs.filter p).length ≤ 1) : vs.reverse.find? p = vs.find? p := by
  rw [← List.head?_filter, ← List.head?_filter, List.filter_reverse]
  generalize hf : vs.filter p = l at h
  match l, h with
  | [], _ => rfl
  | [_], _ => rfl

theorem readRow_eq {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version}
    {btree : Option Nat} {ckptMax : Nat} (hm : ∀ v ∈ vs, v.mat = 0)
    (hu : (vs.filter (isVisible txs fin t)).length ≤ 1) :
    readRow txs fin t vs btree ckptMax =
      rd (isVisible txs fin t) (isBtreeInvalidating txs fin t) vs btree := by
  unfold readRow rd mvccSide btreeSideValid lastVisible
  by_cases hne : vs = []
  · subst hne; simp
  · have hw := chainIsWriteBuffer_of_noMat (txs := txs) (fin := fin) (t := t) (ckptMax := ckptMax) hm hne
    have hisE : vs.isEmpty = false := by
      cases vs with
      | nil => exact absurd rfl hne
      | cons _ _ => rfl
    simp only [hisE, btreeCovers, hw, Bool.not_true, Bool.false_eq_true, ite_false]
    rw [find_reverse_of_unique hu]
    cases hf : vs.find? (isVisible txs fin t) with
    | some v => simp
    | none =>
      cases ha : vs.any (isBtreeInvalidating txs fin t) <;> simp

theorem rd_congr {vis inv vis' inv' : Version → Bool} {vs : List Version} {btree : Option Nat}
    (hv : ∀ v ∈ vs, vis' v = vis v) (hi : ∀ v ∈ vs, inv' v = inv v) :
    rd vis' inv' vs btree = rd vis inv vs btree := by
  unfold rd
  rw [find?_congr' hv, any_congr' hi]

end MvccGc
