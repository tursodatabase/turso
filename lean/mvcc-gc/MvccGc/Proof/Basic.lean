import MvccGc.Invariant

/-!
# The invariant and base lemmas
-/

set_option linter.deprecated false

namespace MvccGc

def St.histTs (s : St) : List Nat := s.hist.map (·.1)

structure Inv (s : St) : Prop where
  idsNodup : (s.txs.map Tx.id).Nodup
  idLt : ∀ t ∈ s.txs, t.id < s.nextTx
  beginGt : ∀ t ∈ s.txs, s.ckptMax < t.beginTs
  beginLt : ∀ t ∈ s.txs, t.beginTs < s.clock
  readMarkEq : ∀ t ∈ s.txs, t.readMark = s.walPos
  endBounds : ∀ t ∈ s.txs, ∀ e ∈ txEnds t, t.beginTs < e ∧ e < s.clock
  tsNodup : (s.txs.map Tx.beginTs ++ s.txs.flatMap txEnds).Nodup
  histSorted : s.hist.Pairwise (fun a b => b.1 < a.1)
  histLt : ∀ e ∈ s.hist, e.1 < s.clock
  histPrepared : ∀ t ∈ s.txs, ∀ e ∈ txEnds t, ∀ w, lookupWrote s.wrote t.id = some w → (e, w) ∈ s.hist
  histPreparedNone : ∀ t ∈ s.txs, ∀ e ∈ txEnds t, lookupWrote s.wrote t.id = none → e ∉ s.histTs
  histEpoch : ∀ e ∈ s.hist, e.1 ≤ s.lastCommitted ∨ ∃ t ∈ s.txs, e.1 ∈ txEnds t
  lastLt : s.lastCommitted < s.clock
  ckptLe : s.ckptMax ≤ s.lastCommitted
  beginNotHist : ∀ t ∈ s.txs, t.beginTs ∉ s.histTs
  tsStamp : ∀ v ∈ s.chain, ∀ c, Stamp.ts c ∈ stamps v → s.ckptMax < c ∧ c < s.clock ∧ c ∈ s.histTs
  idStamp : ∀ v ∈ s.chain, ∀ x, Stamp.id x ∈ stamps v →
    ∃ t ∈ s.txs, t.id = x ∧ t.state ≠ .terminated ∧ (∀ e, t.state = .committed e → t.rewritten = false)
  idStampWrote : ∀ v ∈ s.chain, ∀ x, Stamp.id x ∈ stamps v → wroteRow s x = true
  noMat : ∀ v ∈ s.chain, v.mat = 0
  btreeOk : s.btree = valueAt s.init s.hist (s.ckptMax + 1)
  wroteKeys : ∀ p ∈ s.wrote, ∃ t ∈ s.txs, t.id = p.1
  wroteNodup : (s.wrote.map Prod.fst).Nodup
  walEq : s.backfill = s.walPos
  reads : ∀ t ∈ s.readers, s.read t = s.expected t
  unique : ∀ t ∈ s.readers, (s.chain.filter (isVisible s.txs s.fin t)).length ≤ 1
  evidence : ∀ t ∈ s.readers, s.btree.isSome = true →
    (epochChangeBefore s t.beginTs = true ∨ wroteRow s t.id = true) →
    ∃ v ∈ s.chain, v.resident = true ∧ isBtreeInvalidating s.txs s.fin t v = true
  noResident : s.btree = none → ∀ v ∈ s.chain, v.resident = false
  liveLast : ∀ l1 c l2, s.chain = l1 ++ c :: l2 → live s.txs c = true →
    ∀ w ∈ l2, isGarbage w = true ∨ settled s.txs w = false
  pendingOrder : ∀ x ∈ s.txs, x.state = .active → ∀ l1 n l2, s.chain = l1 ++ n :: l2 →
    livePendingOf x.id n = true → doomed s.txs x s.chain = true ∨
      ∀ w ∈ l2, isGarbage w = true ∨ (settled s.txs w = false ∧ belongsTo x.id w = false)
  deletesSee : ∀ v ∈ s.chain, ∀ z x, v.beginAt = some (.id z) → v.endAt = some (.id x) →
    z = x ∨ (resolve s.txs (some (.id z))).isAt = true
  ownEnds : ∀ v ∈ s.chain, ∀ x, v.beginAt = some (.id x) →
    (resolve s.txs (some (.id x))).isAt = false → v.endAt = none ∨ v.endAt = some (.id x)
  deleterSaw : ∀ v ∈ s.chain, ∀ t ∈ s.txs, v.endAt = some (.id t.id) → t.state = .active →
    v.beginAt = none ∨ v.beginAt = some (.id t.id) ∨
      ∃ b, resolve s.txs v.beginAt = .time b ∧ b < t.beginTs
  writerStamp : ∀ t ∈ s.txs, t.state = .active → wroteRow s t.id = true →
    ∃ v ∈ s.chain, Stamp.id t.id ∈ stamps v
  endAfterBegin : ∀ v ∈ s.chain, ∀ b c, resolve s.txs v.beginAt = .time b →
    resolve s.txs v.endAt = .time c → b ≤ c
  rewrittenCommitted : ∀ t ∈ s.txs, t.rewritten = true → ∃ e, t.state = .committed e

/-! ## Small lemmas -/

theorem mem_stamps {st : Stamp} {v : Version} :
    st ∈ stamps v ↔ v.beginAt = some st ∨ v.endAt = some st := by
  unfold stamps
  cases h1 : v.beginAt <;> cases h2 : v.endAt <;> simp [eq_comm]

theorem findTx_some {txs : List Tx} {x : Nat} {t : Tx} (h : findTx txs x = some t) :
    t ∈ txs ∧ t.id = x := by
  induction txs with
  | nil => simp [findTx] at h
  | cons u rest ih =>
    unfold findTx at h
    split at h
    · cases h; simp_all
    · have := ih h; simp_all

theorem findTx_none {txs : List Tx} {x : Nat} (h : findTx txs x = none) :
    ∀ t ∈ txs, t.id ≠ x := by
  induction txs with
  | nil => simp
  | cons u rest ih =>
    unfold findTx at h
    split at h
    · simp at h
    · intro t ht
      simp at ht
      rcases ht with rfl | ht
      · assumption
      · exact ih h t ht

theorem findTx_of_mem {txs : List Tx} {t : Tx} (hnd : (txs.map Tx.id).Nodup) (h : t ∈ txs) :
    findTx txs t.id = some t := by
  induction txs with
  | nil => simp at h
  | cons u rest ih =>
    simp only [List.map_cons, List.nodup_cons, List.mem_map] at hnd
    unfold findTx
    simp at h
    rcases h with rfl | h
    · simp
    · have hne : u.id ≠ t.id := by
        intro heq
        exact hnd.1 ⟨t, h, heq.symm⟩
      simp [hne, ih hnd.2 h]

theorem findTx_eq_none_of {txs : List Tx} {x : Nat} (h : ∀ t ∈ txs, t.id ≠ x) :
    findTx txs x = none := by
  induction txs with
  | nil => rfl
  | cons u rest ih =>
    unfold findTx
    have hu := h u (by simp)
    simp [hu]
    exact ih (fun t ht => h t (by simp [ht]))

end MvccGc
