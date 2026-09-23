import MvccGc.Proof.Basic

/-!
# Visibility in terms of resolved stamps

When every transaction id in a version is known, the visibility rules read
only the resolved stamps. Most steps keep the resolved stamps, so they keep
what every reader sees.
-/

set_option linter.deprecated false

namespace MvccGc

def visB (t : Tx) (endNone : Bool) : RS → Bool
  | .time b => decide (t.beginTs > b)
  | .pending x => decide (x = t.id) && endNone
  | _ => false

def visE (t : Tx) : RS → Bool
  | .time e => decide (t.beginTs < e)
  | .pending x => decide (x ≠ t.id)
  | _ => true

def invE (t : Tx) : RS → Bool
  | .time e => decide (t.beginTs > e)
  | .pending x => decide (x = t.id)
  | _ => false

def known (txs : List Tx) (v : Version) : Prop :=
  ∀ x, Stamp.id x ∈ stamps v → (findTx txs x).isSome = true

theorem known_begin {txs : List Tx} {v : Version} {x : Nat} (hk : known txs v)
    (h : v.beginAt = some (.id x)) : (findTx txs x).isSome = true :=
  hk x (mem_stamps.2 (Or.inl h))

theorem known_end {txs : List Tx} {v : Version} {x : Nat} (hk : known txs v)
    (h : v.endAt = some (.id x)) : (findTx txs x).isSome = true :=
  hk x (mem_stamps.2 (Or.inr h))

theorem isBeginVisible_eq {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hk : known txs v) :
    isBeginVisible txs fin t v = visB t v.endAt.isNone (resolve txs v.beginAt) := by
  unfold isBeginVisible
  cases hb : v.beginAt with
  | none => simp [resolve, visB]
  | some st =>
    cases st with
    | ts b => simp [resolve, visB]
    | id x =>
      have hx := known_begin hk hb
      cases hf : findTx txs x with
      | none => simp [hf] at hx
      | some u =>
        have hu := (findTx_some hf).2
        cases hs : u.state <;> simp [resolve, hf, hs, visB, hu, eq_comm]

theorem isEndVisible_eq {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hk : known txs v) :
    isEndVisible txs fin t v = visE t (resolve txs v.endAt) := by
  unfold isEndVisible
  cases he : v.endAt with
  | none => simp [resolve, visE]
  | some st =>
    cases st with
    | ts e => simp [resolve, visE]
    | id x =>
      have hx := known_end hk he
      cases hf : findTx txs x with
      | none => simp [hf] at hx
      | some u =>
        have hu := (findTx_some hf).2
        cases hs : u.state <;> simp [resolve, hf, hs, visE, hu, eq_comm]

theorem isVisible_eq {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hk : known txs v) :
    isVisible txs fin t v =
      (visB t v.endAt.isNone (resolve txs v.beginAt) && visE t (resolve txs v.endAt)) := by
  unfold isVisible
  rw [isBeginVisible_eq hk, isEndVisible_eq hk]

/-- The reader `t` is either not in the chain or is an active transaction. -/
def readerOk (txs : List Tx) (t : Tx) (v : Version) : Prop :=
  ∀ x, v.endAt = some (.id x) → x = t.id → resolve txs (some (.id x)) = .pending x

theorem isBtreeInvalidating_eq {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hk : known txs v) (hr : readerOk txs t v) :
    isBtreeInvalidating txs fin t v =
      ((visB t v.endAt.isNone (resolve txs v.beginAt) && visE t (resolve txs v.endAt)) ||
        invE t (resolve txs v.endAt)) := by
  unfold isBtreeInvalidating
  rw [← isVisible_eq hk]
  congr 1
  cases he : v.endAt with
  | none => simp [resolve, invE]
  | some st =>
    cases st with
    | ts e => simp [resolve, invE]
    | id x =>
      have hx := known_end hk he
      by_cases hxt : x = t.id
      · have := hr x he hxt
        simp [hxt] at this ⊢
        simp [this, invE]
      · cases hf : findTx txs x with
        | none => simp [hf] at hx
        | some u =>
          cases hs : u.state <;> simp [resolve, lookupTxState, hf, hs, invE, hxt]

end MvccGc
