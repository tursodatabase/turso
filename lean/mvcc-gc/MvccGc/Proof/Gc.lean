import MvccGc.Proof.Facts

/-!
# Garbage collection keeps the invariant

Without materialization stamps, `gc_version_chain` is a filter. It removes
aborted garbage, and a version that ended at `e ≤ lwm` when it is not
B-tree resident and a current version exists. Every open reader began after
`e`, so it cannot see the removed version. A resident version still hides
the B-tree row, so the reader gets the same row.
-/

set_option linter.deprecated false

namespace MvccGc

def gcKeep (lwm : Option Nat) (ckptMax : Nat) (m : Option Nat) (hasCurrent : Bool)
    (v : Version) : Bool :=
  !isAbortedGarbage v && keepByRule2 lwm ckptMax false m hasCurrent v

theorem rule3_noMat {lwm : Option Nat} {ckptMax : Nat} {m : Option Nat} {vs : List Version}
    (hm : ∀ v ∈ vs, v.mat = 0) : rule3 lwm ckptMax false m vs = vs := by
  unfold rule3
  match vs with
  | [rv] =>
    have h0 : rv.mat = 0 := hm rv (by simp)
    simp only
    split
    · simp [matForReaders, h0]
    · rfl
  | [] => rfl
  | _ :: _ :: _ => rfl

theorem gcChain_noMat {vs : List Version} {lwm : Option Nat} {ckptMax : Nat} {m : Option Nat}
    (hm : ∀ v ∈ vs, v.mat = 0) :
    gcChain vs lwm ckptMax false m true =
      vs.filter (gcKeep lwm ckptMax m ((vs.filter (fun rv => !isAbortedGarbage rv)).any isCurrent)) := by
  unfold gcChain
  simp only [ite_true]
  rw [rule3_noMat]
  · rw [List.filter_filter]
    apply List.filter_congr
    intro v _
    simp [gcKeep, Bool.and_comm]
  · intro v hv
    have := (List.mem_filter.1 hv).1
    exact hm v (List.mem_filter.1 this).1

theorem computeLwm_foldl {txs : List Tx} {acc : Option Nat} :
    (∀ c, txs.foldl (fun acc t => if isOpen t then minOpt acc t.beginTs else acc) acc = some c →
      (∀ a, acc = some a → c ≤ a) ∧ ∀ t ∈ txs, isOpen t = true → c ≤ t.beginTs) ∧
    (txs.foldl (fun acc t => if isOpen t then minOpt acc t.beginTs else acc) acc = none →
      acc = none ∧ ∀ t ∈ txs, isOpen t = false) := by
  induction txs generalizing acc with
  | nil =>
    simp only [List.foldl_nil, List.not_mem_nil, false_implies, implies_true, and_true]
    refine ⟨?_, fun h => h⟩
    intro c hc a ha
    rw [hc] at ha
    simp at ha
    omega
  | cons u rest ih =>
    simp only [List.foldl_cons]
    constructor
    · intro c hc
      have ⟨h1, h2⟩ := (@ih (if isOpen u then minOpt acc u.beginTs else acc)).1 c hc
      constructor
      · intro a ha
        by_cases hu : isOpen u = true
        · have := h1 (min a u.beginTs) (by simp [hu, ha, minOpt])
          omega
        · exact h1 a (by simp [hu, ha])
      · intro t ht hot
        simp at ht
        rcases ht with rfl | ht
        · cases acc with
          | none => exact h1 t.beginTs (by simp [hot, minOpt])
          | some a =>
            have := h1 (min a t.beginTs) (by simp [hot, minOpt])
            omega
        · exact h2 t ht hot
    · intro hn
      have ⟨h1, h2⟩ := (@ih (if isOpen u then minOpt acc u.beginTs else acc)).2 hn
      by_cases hu : isOpen u = true
      · simp [hu, minOpt] at h1
        cases acc <;> simp at h1
      · simp [hu] at h1
        refine ⟨h1, ?_⟩
        intro t ht
        simp at ht
        rcases ht with rfl | ht
        · simpa using hu
        · exact h2 t ht

theorem computeLwm_le {txs : List Tx} {t : Tx} (ht : t ∈ txs) (ho : isOpen t = true) :
    ∃ c, computeLwm txs = some c ∧ c ≤ t.beginTs := by
  unfold computeLwm
  cases hc : txs.foldl (fun acc t => if isOpen t then minOpt acc t.beginTs else acc) none with
  | none =>
    have := (computeLwm_foldl.2 hc).2 t ht
    simp [ho] at this
  | some c => exact ⟨c, rfl, (computeLwm_foldl.1 c hc).2 t ht ho⟩

theorem lwm_le_active {txs : List Tx} {lwm : Option Nat} {t : Tx} (ha : lwmAllowed lwm (computeLwm txs))
    (ht : t ∈ txs) (ho : isOpen t = true) {e : Nat} (he : belowLwm lwm e = true) :
    e ≤ t.beginTs := by
  obtain ⟨c, hc, hle⟩ := computeLwm_le ht ho
  rw [hc] at ha
  cases lwm with
  | none => simp [lwmAllowed] at ha
  | some l =>
    simp [lwmAllowed] at ha
    simp [belowLwm] at he
    omega

theorem filter_eq_append_cons {α : Type} {p : α → Bool} {l l1' l2' : List α} {c : α}
    (h : l.filter p = l1' ++ c :: l2') : ∃ l1 l2, l = l1 ++ c :: l2 ∧ l2.filter p = l2' := by
  induction l generalizing l1' with
  | nil => simp at h
  | cons a rest ih =>
    by_cases hp : p a = true
    · rw [List.filter_cons_of_pos hp] at h
      cases l1' with
      | nil =>
        simp at h
        obtain ⟨rfl, h2⟩ := h
        exact ⟨[], rest, rfl, h2⟩
      | cons b l1'' =>
        simp at h
        obtain ⟨rfl, h2⟩ := h
        obtain ⟨l1, l2, h3, h4⟩ := ih h2
        exact ⟨a :: l1, l2, by simp [h3], h4⟩
    · rw [List.filter_cons_of_neg hp] at h
      obtain ⟨l1, l2, h3, h4⟩ := ih h
      exact ⟨a :: l1, l2, by simp [h3], h4⟩

end MvccGc

namespace MvccGc

theorem isGarbage_eq (v : Version) : isGarbage v = isAbortedGarbage v := rfl

theorem garbage_not_visible {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hg : isAbortedGarbage v = true) : isVisible txs fin t v = false := by
  simp [isAbortedGarbage] at hg
  simp [isVisible, isBeginVisible, hg.1]

theorem garbage_not_invalidating {txs : List Tx} {fin : Finalized} {t : Tx} {v : Version}
    (hg : isAbortedGarbage v = true) : isBtreeInvalidating txs fin t v = false := by
  have hv := garbage_not_visible (txs := txs) (fin := fin) (t := t) hg
  simp [isAbortedGarbage] at hg
  simp [isBtreeInvalidating, hv, hg.2]

theorem garbage_not_settled {txs : List Tx} {v : Version} (hg : isAbortedGarbage v = true) :
    settled txs v = false := by
  simp [isAbortedGarbage] at hg
  simp [settled, resolve, hg.1, hg.2, RS.isAt]

namespace Inv

variable {s : St}

theorem gc_removed (h : Inv s) {lwm : Option Nat} {m : Option Nat} {hc : Bool} {v : Version}
    (hv : v ∈ s.chain) (hk : gcKeep lwm s.ckptMax m hc v = false) :
    isAbortedGarbage v = true ∨
      ∃ e, v.endAt = some (.ts e) ∧ belowLwm lwm e = true ∧ v.resident = false := by
  unfold gcKeep at hk
  cases hg : isAbortedGarbage v
  · right
    simp [hg] at hk
    unfold keepByRule2 at hk
    cases he : v.endAt with
    | none => simp [he] at hk
    | some st =>
      cases st with
      | id x => simp [he] at hk
      | ts e =>
        simp only [he] at hk
        have hte := (h.tsStamp v hv e (mem_stamps.2 (Or.inr he))).1
        by_cases hb : belowLwm lwm e = true
        · rw [if_pos hb] at hk
          have hr : v.resident = false := by
            revert hk
            unfold inBtree
            cases v.resident <;> simp [hte]
          exact ⟨e, rfl, hb, hr⟩
        · simp [hb] at hk
  · left; rfl

theorem gc_keeps_resident (h : Inv s) {lwm : Option Nat} {m : Option Nat} {hc : Bool} {v : Version}
    (hv : v ∈ s.chain) (hr : v.resident = true) (hg : isAbortedGarbage v = false) :
    gcKeep lwm s.ckptMax m hc v = true := by
  cases hk : gcKeep lwm s.ckptMax m hc v
  · rcases h.gc_removed hv hk with hg' | ⟨e, -, -, hr'⟩
    · simp [hg] at hg'
    · simp [hr] at hr'
  · rfl

/-- A version that GC removes ended before every reader began. -/
theorem gc_removed_before_reader (h : Inv s) {lwm : Option Nat}
    (ha : lwmAllowed lwm (computeLwm s.txs)) {t : Tx} (ht : t ∈ s.readers) {e : Nat}
    (he : e ∈ s.histTs) (hbl : belowLwm lwm e = true) (hlt : e < s.clock) : e < t.beginTs := by
  rcases mem_readers.1 ht with rfl | ⟨htx, hact⟩
  · exact hlt
  · have hle := lwm_le_active ha htx (by simp [isOpen, hact]) hbl
    have hne : e ≠ t.beginTs := by
      intro heq
      exact h.beginNotHist t htx (heq ▸ he)
    omega

theorem epochChange_of (_h : Inv s) {e t : Nat} (he : e ∈ s.histTs) (h1 : s.ckptMax < e)
    (h2 : e < t) : epochChangeBefore s t = true := by
  unfold epochChangeBefore
  simp only [St.histTs, List.mem_map] at he
  obtain ⟨p, hp, rfl⟩ := he
  simp only [List.any_eq_true]
  exact ⟨p, hp, by simp [h1, h2]⟩

end Inv

theorem inv_gc {s : St} {lwm : Option Nat} (h : Inv s) (ha : lwmAllowed lwm (computeLwm s.txs)) :
    Inv { s with
      chain := gcChain s.chain lwm s.ckptMax false (inlineMinReaderMark s.txs s.backfill) true } := by
  generalize hm : inlineMinReaderMark s.txs s.backfill = m
  generalize hhc : (s.chain.filter (fun rv => !isAbortedGarbage rv)).any isCurrent = hc
  have hgc : gcChain s.chain lwm s.ckptMax false m true = s.chain.filter (gcKeep lwm s.ckptMax m hc) := by
    rw [gcChain_noMat h.noMat, hhc]
  rw [hgc]
  generalize hkeep : gcKeep lwm s.ckptMax m hc = keep at *
  have memc : ∀ {v}, v ∈ s.chain.filter keep → v ∈ s.chain := fun hv => (List.mem_filter.1 hv).1
  have keepOf : ∀ {v}, v ∈ s.chain → keep v = true → v ∈ s.chain.filter keep :=
    fun hv hk => List.mem_filter.2 ⟨hv, hk⟩
  have removed : ∀ {v}, v ∈ s.chain → keep v = false → isAbortedGarbage v = true ∨
      ∃ e, v.endAt = some (.ts e) ∧ belowLwm lwm e = true ∧ v.resident = false := by
    intro v hv hk; rw [← hkeep] at hk; exact h.gc_removed hv hk
  have removedInvisible : ∀ {t v}, t ∈ s.readers → v ∈ s.chain → keep v = false →
      isVisible s.txs s.fin t v = false := by
    intro t v ht hv hk
    rcases removed hv hk with hg | ⟨e, he, hb, -⟩
    · exact garbage_not_visible hg
    · have hts := h.tsStamp v hv e (mem_stamps.2 (Or.inr he))
      have hlt := h.gc_removed_before_reader ha ht hts.2.2 hb hts.2.1
      rw [isVisible_eq (h.known hv)]
      simp [he, resolve, visE]
      intro _; omega
  have visFilter : ∀ {t}, t ∈ s.readers →
      (s.chain.filter keep).filter (isVisible s.txs s.fin t) = s.chain.filter (isVisible s.txs s.fin t) := by
    intro t ht
    rw [List.filter_filter]
    apply List.filter_congr
    intro v hv
    cases hvis : isVisible s.txs s.fin t v
    · simp
    · cases hk : keep v
      · rw [removedInvisible ht hv hk] at hvis; simp at hvis
      · simp
  have uniqueNew : ∀ t ∈ s.readers, ((s.chain.filter keep).filter (isVisible s.txs s.fin t)).length ≤ 1 := by
    intro t ht; rw [visFilter ht]; exact h.unique t ht
  have noMatNew : ∀ v ∈ s.chain.filter keep, v.mat = 0 := fun v hv => h.noMat v (memc hv)
  have residentKept : ∀ {v}, v ∈ s.chain → v.resident = true → isAbortedGarbage v = false →
      v ∈ s.chain.filter keep := by
    intro v hv hr hg
    apply keepOf hv
    rw [← hkeep]; exact h.gc_keeps_resident hv hr hg
  have invKept : ∀ {t}, t ∈ s.readers → s.btree.isSome = true →
      s.chain.any (isBtreeInvalidating s.txs s.fin t) = true →
      (s.chain.filter keep).any (isBtreeInvalidating s.txs s.fin t) = true := by
    intro t ht hb hany
    obtain ⟨w, hw, hinv⟩ := List.any_eq_true.1 hany
    cases hk : keep w
    · rcases removed hw hk with hg | ⟨e, he, hbl, -⟩
      · rw [garbage_not_invalidating hg] at hinv; simp at hinv
      · have hts := h.tsStamp w hw e (mem_stamps.2 (Or.inr he))
        have hlt := h.gc_removed_before_reader ha ht hts.2.2 hbl hts.2.1
        obtain ⟨r, hr, hres, hrinv⟩ :=
          h.evidence t ht hb (Or.inl (h.epochChange_of hts.2.2 hts.1 hlt))
        have hrg : isAbortedGarbage r = false := by
          cases hg : isAbortedGarbage r
          · rfl
          · rw [garbage_not_invalidating hg] at hrinv; simp at hrinv
        exact List.any_eq_true.2 ⟨r, residentKept hr hres hrg, hrinv⟩
    · exact List.any_eq_true.2 ⟨w, keepOf hw hk, hinv⟩
  have readSame : ∀ t ∈ s.readers,
      readRow s.txs s.fin t (s.chain.filter keep) s.btree s.ckptMax = s.read t := by
    intro t ht
    rw [readRow_eq noMatNew (uniqueNew t ht), h.read_eq ht]
    unfold rd
    rw [← List.head?_filter, ← List.head?_filter, visFilter ht]
    cases (s.chain.filter (isVisible s.txs s.fin t)).head? with
    | some v => rfl
    | none =>
      simp only
      cases hb : s.btree with
      | none => simp
      | some b0 =>
        have hbs : s.btree.isSome = true := by simp [hb]
        cases hany : s.chain.any (isBtreeInvalidating s.txs s.fin t)
        · have : (s.chain.filter keep).any (isBtreeInvalidating s.txs s.fin t) = false := by
            cases h2 : (s.chain.filter keep).any (isBtreeInvalidating s.txs s.fin t)
            · rfl
            · obtain ⟨w, hw, hinv⟩ := List.any_eq_true.1 h2
              have : s.chain.any (isBtreeInvalidating s.txs s.fin t) = true :=
                List.any_eq_true.2 ⟨w, memc hw, hinv⟩
              rw [hany] at this; exact absurd this (by decide)
          simp [this]
        · simp [invKept ht hbs hany]
  exact {
    idsNodup := h.idsNodup
    idLt := h.idLt
    beginGt := h.beginGt
    beginLt := h.beginLt
    readMarkEq := h.readMarkEq
    endBounds := h.endBounds
    tsNodup := h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := h.histPrepared
    histPreparedNone := h.histPreparedNone
    histEpoch := h.histEpoch
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := h.beginNotHist
    tsStamp := fun v hv => h.tsStamp v (memc hv)
    idStamp := fun v hv => h.idStamp v (memc hv)
    idStampWrote := fun v hv => h.idStampWrote v (memc hv)
    noMat := noMatNew
    btreeOk := h.btreeOk
    wroteKeys := h.wroteKeys
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := fun t ht => by
      show readRow s.txs s.fin t (s.chain.filter keep) s.btree s.ckptMax = s.expected t
      rw [readSame t ht]; exact h.reads t ht
    unique := uniqueNew
    evidence := fun t ht hb hc => by
      obtain ⟨r, hr, hres, hrinv⟩ := h.evidence t ht hb hc
      have hrg : isAbortedGarbage r = false := by
        cases hg : isAbortedGarbage r
        · rfl
        · rw [garbage_not_invalidating hg] at hrinv; simp at hrinv
      exact ⟨r, residentKept hr hres hrg, hres, hrinv⟩
    noResident := fun hb v hv => h.noResident hb v (memc hv)
    liveLast := fun l1' c l2' hsplit hlive w hw => by
      obtain ⟨l1, l2, hs, hl2⟩ := filter_eq_append_cons hsplit
      rw [← hl2] at hw
      exact h.liveLast l1 c l2 hs hlive w (List.mem_filter.1 hw).1
    pendingOrder := fun x hx hact l1' n l2' hsplit hn => by
      obtain ⟨l1, l2, hs, hl2⟩ := filter_eq_append_cons hsplit
      rcases h.pendingOrder x hx hact l1 n l2 hs hn with hd | hord
      · left
        unfold doomed at hd ⊢
        obtain ⟨w, hw, hwd⟩ := List.any_eq_true.1 hd
        refine List.any_eq_true.2 ⟨w, ?_, hwd⟩
        cases hk : keep w
        · exfalso
          rcases removed hw hk with hg | ⟨e, he, hbl, -⟩
          · rw [garbage_not_settled hg] at hwd; simp at hwd
          · have hle := lwm_le_active ha hx (by simp [isOpen, hact]) hbl
            have hend : resolve s.txs w.endAt = .time e := by simp [he, resolve]
            simp only [Bool.and_eq_true, Bool.or_eq_true] at hwd
            rcases hwd.2 with hb | hb
            · cases hrb : resolve s.txs w.beginAt with
              | time b =>
                have := h.endAfterBegin w hw b e hrb hend
                rw [hrb] at hb; simp [RS.after] at hb; omega
              | _ => rw [hrb] at hb; simp [RS.after] at hb
            · rw [hend] at hb; simp [RS.after] at hb; omega
        · exact keepOf hw hk
      · right
        intro w hw
        rw [← hl2] at hw
        exact hord w (List.mem_filter.1 hw).1
    deletesSee := fun v hv => h.deletesSee v (memc hv)
    ownEnds := fun v hv => h.ownEnds v (memc hv)
    deleterSaw := fun v hv => h.deleterSaw v (memc hv)
    writerStamp := fun t ht hact hw => by
      obtain ⟨v, hv, hst⟩ := h.writerStamp t ht hact hw
      refine ⟨v, ?_, hst⟩
      cases hk : keep v
      · exfalso
        rcases removed hv hk with hg | ⟨e, he, -, -⟩
        · simp [isAbortedGarbage] at hg
          rcases mem_stamps.1 hst with hb | hb
          · rw [hg.1] at hb; simp at hb
          · rw [hg.2] at hb; simp at hb
        · rcases mem_stamps.1 hst with hb | hb
          · have hpend : (resolve s.txs (some (.id t.id))).isAt = false := by
              simp [resolve, h.findTx_mem ht, hact, RS.isAt]
            rcases h.ownEnds v hv t.id hb hpend with he' | he' <;> rw [he] at he' <;> simp at he'
          · rw [he] at hb; simp at hb
      · exact keepOf hv hk
    endAfterBegin := fun v hv => h.endAfterBegin v (memc hv)
    rewrittenCommitted := h.rewrittenCommitted }

end MvccGc
