import MvccGc.Proof.Read

/-!
# General facts that follow from the invariant, and transfer lemmas
-/

set_option linter.deprecated false

namespace MvccGc

theorem mem_readers {s : St} {t : Tx} :
    t ∈ s.readers ↔ t = s.nextReader ∨ (t ∈ s.txs ∧ t.state = .active) := by
  simp [St.readers]

theorem resolve_missing {txs : List Tx} {st : Option Stamp} {y : Nat}
    (h : resolve txs st = .missing y) : st = some (.id y) ∧ findTx txs y = none := by
  unfold resolve at h
  split at h
  · simp at h
  · simp at h
  · rename_i x
    split at h
    · rename_i u _
      split at h <;> simp at h
    · rename_i hf
      cases h; exact ⟨rfl, hf⟩

theorem known_of_resolve {txs : List Tx} {v : Version}
    (hb : ∀ y, resolve txs v.beginAt ≠ .missing y) (he : ∀ y, resolve txs v.endAt ≠ .missing y) :
    known txs v := by
  intro x hx
  rcases mem_stamps.1 hx with h | h
  · cases hf : findTx txs x
    · exact absurd (by simp [resolve, h, hf]) (hb x)
    · rfl
  · cases hf : findTx txs x
    · exact absurd (by simp [resolve, h, hf]) (he x)
    · rfl

theorem resolve_ne_missing_of_known {txs : List Tx} {v : Version} (hk : known txs v) :
    (∀ y, resolve txs v.beginAt ≠ .missing y) ∧ (∀ y, resolve txs v.endAt ≠ .missing y) := by
  constructor
  · intro y hy
    obtain ⟨hst, hf⟩ := resolve_missing hy
    have := known_begin hk hst
    simp [hf] at this
  · intro y hy
    obtain ⟨hst, hf⟩ := resolve_missing hy
    have := known_end hk hst
    simp [hf] at this

/-- Both stamps of the version resolve the same way under both transaction lists. -/
def SameResolve (txs txs' : List Tx) (v : Version) : Prop :=
  resolve txs' v.beginAt = resolve txs v.beginAt ∧ resolve txs' v.endAt = resolve txs v.endAt

theorem SameResolve.known {txs txs' : List Tx} {v : Version} (hs : SameResolve txs txs' v)
    (hk : known txs v) : known txs' v := by
  obtain ⟨hb, he⟩ := resolve_ne_missing_of_known hk
  exact known_of_resolve (by rw [hs.1]; exact hb) (by rw [hs.2]; exact he)

theorem isVisible_same {txs txs' : List Tx} {fin fin' : Finalized} {t : Tx} {v : Version}
    (hs : SameResolve txs txs' v) (hk : known txs v) :
    isVisible txs' fin' t v = isVisible txs fin t v := by
  rw [isVisible_eq hk, isVisible_eq (hs.known hk), hs.1, hs.2]

theorem isBtreeInvalidating_same {txs txs' : List Tx} {fin fin' : Finalized} {t : Tx}
    {v : Version} (hs : SameResolve txs txs' v) (hk : known txs v) (hr : readerOk txs t v)
    (hr' : readerOk txs' t v) :
    isBtreeInvalidating txs' fin' t v = isBtreeInvalidating txs fin t v := by
  rw [isBtreeInvalidating_eq hk hr, isBtreeInvalidating_eq (hs.known hk) hr', hs.1, hs.2]

theorem settled_same {txs txs' : List Tx} {v : Version} (hs : SameResolve txs txs' v) :
    settled txs' v = settled txs v := by
  unfold settled; rw [hs.1, hs.2]

theorem live_same {txs txs' : List Tx} {v : Version} (hs : SameResolve txs txs' v) :
    live txs' v = live txs v := by
  unfold live; rw [hs.1, hs.2]

theorem doomed_same {txs txs' : List Tx} {t : Tx} {vs : List Version}
    (hs : ∀ w ∈ vs, SameResolve txs txs' w) : doomed txs' t vs = doomed txs t vs := by
  unfold doomed
  apply any_congr'
  intro w hw
  rw [settled_same (hs w hw), (hs w hw).1, (hs w hw).2]

namespace Inv

variable {s : St}

theorem findTx_mem (h : Inv s) {t : Tx} (ht : t ∈ s.txs) : findTx s.txs t.id = some t :=
  findTx_of_mem h.idsNodup ht

theorem known (h : Inv s) {v : Version} (hv : v ∈ s.chain) : MvccGc.known s.txs v := by
  intro x hx
  obtain ⟨t, ht, htx, -⟩ := h.idStamp v hv x hx
  subst htx
  simp [h.findTx_mem ht]

theorem nextTx_not_ref (h : Inv s) {v : Version} (hv : v ∈ s.chain) :
    Stamp.id s.nextTx ∉ stamps v := by
  intro hx
  obtain ⟨t, ht, htx, -⟩ := h.idStamp v hv _ hx
  have := h.idLt t ht
  omega

theorem readerOk (h : Inv s) {t : Tx} (ht : t ∈ s.readers) {v : Version} (hv : v ∈ s.chain) :
    MvccGc.readerOk s.txs t v := by
  intro x hx hxt
  rcases mem_readers.1 ht with rfl | ⟨htx, hact⟩
  · exfalso
    subst hxt
    exact h.nextTx_not_ref hv (mem_stamps.2 (Or.inr hx))
  · subst hxt
    simp [resolve, h.findTx_mem htx, hact]

theorem read_eq (h : Inv s) {t : Tx} (ht : t ∈ s.readers) :
    s.read t = rd (isVisible s.txs s.fin t) (isBtreeInvalidating s.txs s.fin t) s.chain s.btree :=
  readRow_eq h.noMat (h.unique t ht)

theorem nextReader_mem (_h : Inv s) : s.nextReader ∈ s.readers := by
  simp [St.readers]

theorem clock_not_hist (h : Inv s) : s.clock ∉ s.histTs := by
  intro hc
  simp only [St.histTs, List.mem_map] at hc
  obtain ⟨e, he, hec⟩ := hc
  have := h.histLt e he
  omega

theorem ckpt_lt_clock (h : Inv s) : s.ckptMax < s.clock := by
  have := h.ckptLe; have := h.lastLt; omega

end Inv

end MvccGc
