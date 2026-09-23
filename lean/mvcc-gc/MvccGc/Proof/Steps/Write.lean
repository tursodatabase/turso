import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# Writes and deletes keep the invariant

A write ends the version that the writer sees, if there is one, and adds a
version that the writer began. A delete ends the version that the writer
sees, or adds a tombstone when the row is only in the B-tree. Other readers
do not see a pending stamp, so they read the same row, and the writer reads
its own last write.

`write_no_panic` and `delete_no_panic` show that the cursor never takes its
panic paths.
-/

set_option linter.deprecated false

namespace MvccGc.WriteProof

theorem insertNewestFirst_eq {txs : List Tx} {nv : Version} :
    ∀ (L : List Version), ∃ a b, insertNewestFirst txs nv L = a ++ nv :: b ∧ L = a ++ b ∧
      ∀ w ∈ a, orderKey txs nv < orderKey txs w
  | [] => ⟨[], [], rfl, rfl, by simp⟩
  | v :: older => by
    unfold insertNewestFirst
    split
    · exact ⟨[], v :: older, rfl, rfl, by simp⟩
    · rename_i hlt
      obtain ⟨a, b, h1, h2, h3⟩ := insertNewestFirst_eq older
      refine ⟨v :: a, b, by rw [h1]; rfl, by rw [h2]; rfl, ?_⟩
      intro w hw
      simp only [List.mem_cons] at hw
      rcases hw with rfl | hw
      · omega
      · exact h3 w hw

theorem insertVersion_eq {txs : List Tx} {vs : List Version} {nv : Version} :
    ∃ m1 m2, insertVersion txs vs nv = m1 ++ nv :: m2 ∧ vs = m1 ++ m2 ∧
      ∀ w ∈ m2, orderKey txs nv < orderKey txs w := by
  obtain ⟨a, b, h1, h2, h3⟩ := insertNewestFirst_eq (txs := txs) (nv := nv) vs.reverse
  refine ⟨b.reverse, a.reverse, ?_, ?_, ?_⟩
  · unfold insertVersion; rw [h1]; simp
  · have := congrArg List.reverse h2
    simpa using this
  · intro w hw; exact h3 w (by simpa using hw)

theorem deleteNewestFirst_deleted {txs : List Tx} {fin : Finalized} {t : Tx} :
    ∀ {L L' : List Version}, deleteNewestFirst txs fin t L = .deleted L' →
      ∃ a V b, L = a ++ V :: b ∧
        L' = a ++ { V with endAt := some (.id t.id), mat := 0 } :: b ∧
        isVisible txs fin t V = true ∧ isWriteWriteConflict txs fin t V = false ∧
        ∀ w ∈ a, isVisible txs fin t w = false
  | [], L', h => by simp [deleteNewestFirst] at h
  | rv :: older, L', h => by
    unfold deleteNewestFirst at h
    simp only at h
    split at h
    · simp at h
    · rename_i hc
      split at h
      · rename_i hv
        split at h
        · rename_i vs hd
          cases h
          obtain ⟨a, V, b, h1, h2, h3, h4, h5⟩ := deleteNewestFirst_deleted hd
          refine ⟨rv :: a, V, b, by rw [h1]; rfl, by rw [h2]; rfl, h3, h4, ?_⟩
          intro w hw
          simp only [List.mem_cons] at hw
          rcases hw with rfl | hw
          · simpa using hv
          · exact h5 w hw
        · rename_i r hr
          exact (hr _ h).elim
      · rename_i hv
        cases h
        refine ⟨[], rv, older, rfl, rfl, by simpa using hv, ?_, by simp⟩
        simp at hv
        simpa [hv] using hc

theorem deleteNewestFirst_notFound {txs : List Tx} {fin : Finalized} {t : Tx} :
    ∀ {L : List Version}, deleteNewestFirst txs fin t L = .notFound →
      ∀ w ∈ L, isVisible txs fin t w = false
  | [], _ => by simp
  | rv :: older, h => by
    unfold deleteNewestFirst at h
    simp only at h
    split at h
    · simp at h
    · split at h
      · rename_i hv
        have hrec : deleteNewestFirst txs fin t older = .notFound := by
          split at h
          · simp at h
          · rename_i r _; exact h
        intro w hw
        simp only [List.mem_cons] at hw
        rcases hw with rfl | hw
        · simpa using hv
        · exact deleteNewestFirst_notFound hrec w hw
      · simp at h

theorem deleteFromChain_deleted {txs : List Tx} {fin : Finalized} {t : Tx} {vs vs' : List Version}
    (h : deleteFromChain txs fin t vs = .deleted vs') :
    ∃ l1 V l2, vs = l1 ++ V :: l2 ∧
      vs' = l1 ++ { V with endAt := some (.id t.id), mat := 0 } :: l2 ∧
      isVisible txs fin t V = true ∧ isWriteWriteConflict txs fin t V = false ∧
      ∀ w ∈ l2, isVisible txs fin t w = false := by
  unfold deleteFromChain at h
  split at h
  · rename_i L hL
    cases h
    obtain ⟨a, V, b, h1, h2, h3, h4, h5⟩ := deleteNewestFirst_deleted hL
    refine ⟨b.reverse, V, a.reverse, ?_, ?_, h3, h4, ?_⟩
    · have := congrArg List.reverse h1
      simpa using this
    · rw [h2]; simp
    · intro w hw; exact h5 w (by simpa using hw)
  · rename_i r hr
    exact (hr _ h).elim

theorem deleteFromChain_notFound {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version}
    (h : deleteFromChain txs fin t vs = .notFound) :
    ∀ w ∈ vs, isVisible txs fin t w = false := by
  unfold deleteFromChain at h
  split at h
  · simp at h
  · intro w hw
    exact deleteNewestFirst_notFound h w (by simpa using hw)

theorem mvccSide_eq {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version} {ckptMax : Nat}
    (hm : ∀ v ∈ vs, v.mat = 0) :
    mvccSide txs fin t vs ckptMax = (vs.reverse.find? (isVisible txs fin t)).map (·.val) := by
  unfold mvccSide lastVisible
  by_cases hne : vs = []
  · subst hne; simp
  · have hw := chainIsWriteBuffer_of_noMat (txs := txs) (fin := fin) (t := t)
      (ckptMax := ckptMax) hm hne
    have hisE : vs.isEmpty = false := by
      cases vs with
      | nil => exact absurd rfl hne
      | cons _ _ => rfl
    simp [hisE, btreeCovers, hw]

theorem btreeSideValid_eq {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version}
    {ckptMax : Nat} (hm : ∀ v ∈ vs, v.mat = 0) :
    btreeSideValid txs fin t vs ckptMax = !(vs.any (isBtreeInvalidating txs fin t)) := by
  unfold btreeSideValid
  by_cases hne : vs = []
  · subst hne; simp
  · have hw := chainIsWriteBuffer_of_noMat (txs := txs) (fin := fin) (t := t)
      (ckptMax := ckptMax) hm hne
    have hisE : vs.isEmpty = false := by
      cases vs with
      | nil => exact absurd rfl hne
      | cons _ _ => rfl
    simp [hisE, btreeCovers, hw]

theorem mvccSide_none {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version} {ckptMax : Nat}
    (hm : ∀ v ∈ vs, v.mat = 0) (h : mvccSide txs fin t vs ckptMax = none) :
    ∀ w ∈ vs, isVisible txs fin t w = false := by
  rw [mvccSide_eq hm] at h
  simp only [Option.map_eq_none_iff, List.find?_eq_none] at h
  intro w hw
  simpa using h w (by simpa using hw)

theorem mvccSide_some {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version} {ckptMax : Nat}
    {x : Nat} (h : mvccSide txs fin t vs ckptMax = some x) :
    ∃ w ∈ vs, isVisible txs fin t w = true := by
  unfold mvccSide lastVisible at h
  split at h
  · simp at h
  · split at h
    · simp at h
    · cases hf : vs.reverse.find? (isVisible txs fin t) with
      | none => rw [hf] at h; simp at h
      | some w =>
        exact ⟨w, by simpa using List.mem_of_find?_eq_some hf, List.find?_some hf⟩

theorem deleteFromChain_ne_notFound {txs : List Tx} {fin : Finalized} {t : Tx} {vs : List Version}
    {w : Version} (hw : w ∈ vs) (hv : isVisible txs fin t w = true) :
    deleteFromChain txs fin t vs ≠ .notFound := by
  intro h
  rw [deleteFromChain_notFound h w hw] at hv
  simp at hv

theorem tail_of_insert {α : Type} {m1 xs m2 l1 l2 : List α} {n : α}
    (h : m1 ++ xs ++ m2 = l1 ++ n :: l2) (hn : n ∉ xs) :
    ∃ p1 p2, m1 ++ m2 = p1 ++ n :: p2 ∧ ∀ w ∈ l2, w ∈ xs ∨ w ∈ p2 := by
  induction m1 generalizing l1 with
  | nil =>
    simp only [List.nil_append] at h ⊢
    rcases List.append_eq_append_iff.1 h with ⟨a', h1, h2⟩ | ⟨c', h1, h2⟩
    · exact ⟨a', l2, h2, fun w hw => Or.inr hw⟩
    · cases c' with
      | nil =>
        simp at h2
        exact ⟨[], l2, by simp [h2], fun w hw => Or.inr hw⟩
      | cons d c'' =>
        simp only [List.cons_append, List.cons.injEq] at h2
        exact absurd (by rw [h1, ← h2.1]; simp) hn
  | cons a m1' ih =>
    cases l1 with
    | nil =>
      simp only [List.cons_append, List.nil_append, List.cons.injEq] at h
      obtain ⟨rfl, rfl⟩ := h
      refine ⟨[], m1' ++ m2, by simp, ?_⟩
      intro w hw
      simp only [List.mem_append] at hw
      rcases hw with (hw | hw) | hw
      · exact Or.inr (by simp [hw])
      · exact Or.inl hw
      · exact Or.inr (by simp [hw])
    | cons b l1' =>
      simp only [List.cons_append, List.cons.injEq] at h
      obtain ⟨rfl, h2⟩ := h
      obtain ⟨p1, p2, h3, h4⟩ := ih (l1 := l1') (by simpa using h2)
      exact ⟨a :: p1, p2, by simp [h3], h4⟩

theorem split_at_inserted {α : Type} {m1 m2 l1 l2 : List α} {X n : α}
    (h : m1 ++ [X] ++ m2 = l1 ++ n :: l2) :
    n ∈ m1 ++ m2 ∨ (n = X ∧ l2 = m2) := by
  induction m1 generalizing l1 with
  | nil =>
    cases l1 with
    | nil =>
      simp only [List.nil_append, List.singleton_append, List.cons.injEq] at h
      exact Or.inr ⟨h.1.symm, h.2.symm⟩
    | cons b l1' =>
      simp only [List.nil_append, List.cons_append, List.cons.injEq] at h
      left
      rw [h.2]; simp
  | cons a m1' ih =>
    cases l1 with
    | nil =>
      simp only [List.cons_append, List.nil_append, List.cons.injEq] at h
      left
      rw [← h.1]; simp
    | cons b l1' =>
      simp only [List.cons_append, List.cons.injEq] at h
      rcases ih (l1 := l1') (by simpa using h.2) with h3 | h3
      · left; simp only [List.cons_append, List.mem_cons]; exact Or.inr h3
      · exact Or.inr h3

theorem rd_skip_unseen {vis inv : Version → Bool} {m1 xs m2 : List Version} {btree : Option Nat}
    (hx : ∀ X ∈ xs, vis X = false ∧ inv X = false) :
    rd vis inv (m1 ++ xs ++ m2) btree = rd vis inv (m1 ++ m2) btree := by
  unfold rd
  have hf : xs.find? vis = none := List.find?_eq_none.2 (fun X hX => by simp [(hx X hX).1])
  have ha : xs.any inv = false := List.any_eq_false.2 (fun X hX => by simp [(hx X hX).2])
  simp only [List.find?_append, List.any_append, hf, ha, Option.or_none, Bool.or_false]

theorem filter_length_skip_unseen {p : Version → Bool} {m1 xs m2 : List Version}
    (hx : ∀ X ∈ xs, p X = false) :
    ((m1 ++ xs ++ m2).filter p).length = ((m1 ++ m2).filter p).length := by
  have : xs.filter p = [] := List.filter_eq_nil_iff.2 (fun X hX => by simp [hx X hX])
  simp [List.filter_append, this]

theorem rd_single_visible {vis inv : Version → Bool} {l : List Version} {btree : Option Nat}
    {X : Version} (hX : X ∈ l) (hvX : vis X = true)
    (honly : ∀ w ∈ l, vis w = true → w = X) :
    rd vis inv l btree = some X.val := by
  unfold rd
  cases hf : l.find? vis with
  | none =>
    have := List.find?_eq_none.1 hf X hX
    simp [hvX] at this
  | some w =>
    have hw := List.mem_of_find?_eq_some hf
    have hvw := List.find?_some hf
    rw [honly w hw hvw]

theorem rd_none_visible {vis inv : Version → Bool} {l : List Version} {btree : Option Nat}
    (hnone : ∀ w ∈ l, vis w = false) (hinv : ∃ w ∈ l, inv w = true) :
    rd vis inv l btree = none := by
  unfold rd
  have hf : l.find? vis = none := List.find?_eq_none.2 (fun w hw => by simp [hnone w hw])
  have ha : l.any inv = true := List.any_eq_true.2 hinv
  simp [hf, ha]

structure KeptVersion (s : St) (t : Tx) (w w' : Version) : Prop where
  beginEq : w'.beginAt = w.beginAt
  valEq : w'.val = w.val
  residentEq : w'.resident = w.resident
  matZero : w'.mat = 0
  stampsSub : ∀ st, st ∈ stamps w' → st ∈ stamps w ∨ st = .id t.id
  stampsKeep : ∀ u ∈ s.txs, u.state = .active → u.id ≠ t.id →
    Stamp.id u.id ∈ stamps w → Stamp.id u.id ∈ stamps w'
  viewEq : ∀ r ∈ s.readers, r.id ≠ t.id →
    isVisible s.txs s.fin r w' = isVisible s.txs s.fin r w ∧
      isBtreeInvalidating s.txs s.fin r w' = isBtreeInvalidating s.txs s.fin r w
  settledEq : settled s.txs w' = settled s.txs w
  liveEq : live s.txs w' = live s.txs w
  garbageEq : isGarbage w' = isGarbage w
  belongsEq : ∀ y, y ≠ t.id → belongsTo y w' = belongsTo y w
  pendingEq : ∀ y, y ≠ t.id → livePendingOf y w' = true → w' = w
  endAfterEq : ∀ b, (resolve s.txs w'.endAt).after b = (resolve s.txs w.endAt).after b
  deletesSee : ∀ z x, w'.beginAt = some (.id z) → w'.endAt = some (.id x) →
    z = x ∨ (resolve s.txs (some (.id z))).isAt = true
  ownEnds : ∀ x, w'.beginAt = some (.id x) →
    (resolve s.txs (some (.id x))).isAt = false → w'.endAt = none ∨ w'.endAt = some (.id x)
  deleterSaw : ∀ u ∈ s.txs, w'.endAt = some (.id u.id) → u.state = .active →
    w'.beginAt = none ∨ w'.beginAt = some (.id u.id) ∨
      ∃ b, resolve s.txs w'.beginAt = .time b ∧ b < u.beginTs
  endAfterBegin : ∀ b c, resolve s.txs w'.beginAt = .time b →
    resolve s.txs w'.endAt = .time c → b ≤ c

structure AddedVersion (s : St) (t : Tx) (X : Version) : Prop where
  matZero : X.mat = 0
  stampsT : ∀ st, st ∈ stamps X → st = .id t.id
  viewNone : ∀ r ∈ s.readers, r.id ≠ t.id →
    isVisible s.txs s.fin r X = false ∧ isBtreeInvalidating s.txs s.fin r X = false
  notSettled : settled s.txs X = false
  notLive : live s.txs X = false
  notBelongs : ∀ y, y ≠ t.id → belongsTo y X = false
  notPending : ∀ y, y ≠ t.id → livePendingOf y X = false
  resident : s.btree = none → X.resident = false
  deletesSee : ∀ z x, X.beginAt = some (.id z) → X.endAt = some (.id x) →
    z = x ∨ (resolve s.txs (some (.id z))).isAt = true
  ownEnds : ∀ x, X.beginAt = some (.id x) →
    (resolve s.txs (some (.id x))).isAt = false → X.endAt = none ∨ X.endAt = some (.id x)
  deleterSaw : ∀ u ∈ s.txs, X.endAt = some (.id u.id) → u.state = .active →
    X.beginAt = none ∨ X.beginAt = some (.id u.id) ∨
      ∃ b, resolve s.txs X.beginAt = .time b ∧ b < u.beginTs
  endAfterBegin : ∀ b c, resolve s.txs X.beginAt = .time b →
    resolve s.txs X.endAt = .time c → b ≤ c

theorem wroteRow_set {s : St} {t : Tx} {wv : Option Nat} {c : List Version} {y : Nat} :
    wroteRow { s with chain := c, wrote := setWrote s.wrote t.id wv } y =
      (decide (y = t.id) || wroteRow s y) := by
  unfold wroteRow
  simp only [lookupWrote_setWrote]
  by_cases hy : y = t.id <;> simp [hy]

theorem sameTx {s : St} (h : Inv s) {t u : Tx} (htx : t ∈ s.txs) (hu : u ∈ s.txs)
    (hut : u.id = t.id) : u = t := by
  have h1 := h.findTx_mem hu
  have h2 := h.findTx_mem htx
  rw [hut, h2] at h1
  exact (Option.some.inj h1).symm

theorem readerEq {s : St} (h : Inv s) {t r : Tx} (htx : t ∈ s.txs) (hr : r ∈ s.readers)
    (hrt : r.id = t.id) : r = t := by
  rcases mem_readers.1 hr with rfl | ⟨hr, -⟩
  · have := h.idLt t htx
    simp [St.nextReader] at hrt
    omega
  · exact sameTx h htx hr hrt

theorem inv_of_new_chain {s : St} {t : Tx} {g : Version → Version} {xs m1 m2 : List Version}
    {wv : Option Nat} (h : Inv s) (htx : t ∈ s.txs) (hact : t.state = .active)
    (hsplit : m1 ++ m2 = s.chain.map g)
    (hg : ∀ w ∈ s.chain, KeptVersion s t w (g w))
    (hx : ∀ X ∈ xs, AddedVersion s t X)
    (hread : rd (isVisible s.txs s.fin t) (isBtreeInvalidating s.txs s.fin t) (m1 ++ xs ++ m2)
      s.btree = wv)
    (huniq : ((m1 ++ xs ++ m2).filter (isVisible s.txs s.fin t)).length ≤ 1)
    (hevid : s.btree.isSome = true → ∃ v ∈ m1 ++ xs ++ m2, v.resident = true ∧
      isBtreeInvalidating s.txs s.fin t v = true)
    (hpend : ∀ l1 n l2, m1 ++ xs ++ m2 = l1 ++ n :: l2 → livePendingOf t.id n = true →
      doomed s.txs t (m1 ++ xs ++ m2) = true ∨
        ∀ w ∈ l2, isGarbage w = true ∨ (settled s.txs w = false ∧ belongsTo t.id w = false))
    (hwriter : ∃ v ∈ m1 ++ xs ++ m2, Stamp.id t.id ∈ stamps v) :
    Inv { s with chain := m1 ++ xs ++ m2, wrote := setWrote s.wrote t.id wv } := by
  have memNew : ∀ {v}, v ∈ m1 ++ xs ++ m2 → v ∈ xs ∨ ∃ w ∈ s.chain, v = g w := by
    intro v hv
    simp only [List.mem_append] at hv
    have hmap : ∀ {v}, v ∈ m1 ++ m2 → ∃ w ∈ s.chain, v = g w := by
      intro v hv
      rw [hsplit] at hv
      obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hv
      exact ⟨w, hw, rfl⟩
    rcases hv with (hv | hv) | hv
    · exact Or.inr (hmap (List.mem_append_left _ hv))
    · exact Or.inl hv
    · exact Or.inr (hmap (List.mem_append_right _ hv))
  have memOld : ∀ {w}, w ∈ s.chain → g w ∈ m1 ++ xs ++ m2 := by
    intro w hw
    have : g w ∈ m1 ++ m2 := by rw [hsplit]; exact List.mem_map_of_mem hw
    simp only [List.mem_append] at this ⊢
    rcases this with h1 | h1
    · exact Or.inl (Or.inl h1)
    · exact Or.inr h1
  have noMatNew : ∀ v ∈ m1 ++ xs ++ m2, v.mat = 0 := by
    intro v hv
    rcases memNew hv with hvx | ⟨w, hw, rfl⟩
    · exact (hx v hvx).matZero
    · exact (hg w hw).matZero
  have uniqueNew : ∀ r ∈ s.readers,
      ((m1 ++ xs ++ m2).filter (isVisible s.txs s.fin r)).length ≤ 1 := by
    intro r hr
    by_cases hrt : r.id = t.id
    · have := readerEq h htx hr hrt; subst this; exact huniq
    · rw [filter_length_skip_unseen (fun X hX => ((hx X hX).viewNone r hr hrt).1), hsplit,
        filter_map_length (fun w hw => ((hg w hw).viewEq r hr hrt).1)]
      exact h.unique r hr
  have doomedEq : ∀ y, doomed s.txs y (m1 ++ xs ++ m2) = doomed s.txs y s.chain := by
    intro y
    unfold doomed
    have hxs : xs.any (fun w => settled s.txs w &&
        ((resolve s.txs w.beginAt).after y.beginTs || (resolve s.txs w.endAt).after y.beginTs)) =
          false :=
      List.any_eq_false.2 (fun X hX => by simp [(hx X hX).notSettled])
    rw [List.any_append, List.any_append, hxs, Bool.or_false, ← List.any_append, hsplit,
      List.any_map]
    apply any_congr'
    intro w hw
    simp only [Function.comp, (hg w hw).settledEq, (hg w hw).beginEq, (hg w hw).endAfterEq]
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
    histPrepared := fun u hu e he w hw => by
      by_cases hut : u.id = t.id
      · have := sameTx h htx hu hut; subst this; simp [txEnds, hact] at he
      · have : lookupWrote s.wrote u.id = some w := by
          simpa [lookupWrote_setWrote, hut] using hw
        exact h.histPrepared u hu e he w this
    histPreparedNone := fun u hu e he hw => by
      by_cases hut : u.id = t.id
      · have := sameTx h htx hu hut; subst this; simp [txEnds, hact] at he
      · have : lookupWrote s.wrote u.id = none := by
          simpa [lookupWrote_setWrote, hut] using hw
        exact h.histPreparedNone u hu e he this
    histEpoch := h.histEpoch
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := h.beginNotHist
    tsStamp := fun v hv c hc => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact absurd ((hx v hvx).stampsT _ hc) (by simp)
      · rcases (hg w hw).stampsSub _ hc with hc' | hc'
        · exact h.tsStamp w hw c hc'
        · simp at hc'
    idStamp := fun v hv y hy => by
      have htOk : ∃ u ∈ s.txs, u.id = t.id ∧ u.state ≠ .terminated ∧
          (∀ e, u.state = .committed e → u.rewritten = false) :=
        ⟨t, htx, rfl, by simp [hact], by simp [hact]⟩
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · have := (hx v hvx).stampsT _ hy
        simp only [Stamp.id.injEq] at this
        subst this; exact htOk
      · rcases (hg w hw).stampsSub _ hy with hy' | hy'
        · exact h.idStamp w hw y hy'
        · simp only [Stamp.id.injEq] at hy'
          subst hy'; exact htOk
    idStampWrote := fun v hv y hy => by
      rw [wroteRow_set]
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · have := (hx v hvx).stampsT _ hy
        simp only [Stamp.id.injEq] at this
        simp [this]
      · rcases (hg w hw).stampsSub _ hy with hy' | hy'
        · simp [h.idStampWrote w hw y hy']
        · simp only [Stamp.id.injEq] at hy'
          simp [hy']
    noMat := noMatNew
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      simp only [setWrote, List.mem_cons, List.mem_filter] at hp
      rcases hp with rfl | ⟨hp, -⟩
      · exact ⟨t, htx, rfl⟩
      · exact h.wroteKeys p hp
    wroteNodup := by
      show ((setWrote s.wrote t.id wv).map Prod.fst).Nodup
      simp only [setWrote, List.map_cons, List.nodup_cons]
      refine ⟨by simp, ?_⟩
      exact (List.filter_sublist.map Prod.fst).nodup h.wroteNodup
    walEq := h.walEq
    reads := fun r hr => by
      show readRow s.txs s.fin r (m1 ++ xs ++ m2) s.btree s.ckptMax =
        St.expected { s with chain := m1 ++ xs ++ m2, wrote := setWrote s.wrote t.id wv } r
      rw [readRow_eq noMatNew (uniqueNew r hr)]
      by_cases hrt : r.id = t.id
      · have := readerEq h htx hr hrt; subst this
        rw [hread]
        simp [St.expected, lookupWrote_setWrote]
      · rw [rd_skip_unseen (fun X hX => (hx X hX).viewNone r hr hrt), hsplit,
          rd_map (fun w hw => ((hg w hw).viewEq r hr hrt).1)
            (fun w hw => ((hg w hw).viewEq r hr hrt).2) (fun w hw => (hg w hw).valEq),
          ← h.read_eq hr, h.reads r hr]
        simp [St.expected, lookupWrote_setWrote, hrt]
    unique := uniqueNew
    evidence := fun r hr hb hc => by
      by_cases hrt : r.id = t.id
      · have := readerEq h htx hr hrt; subst this; exact hevid hb
      · have hc' : epochChangeBefore s r.beginTs = true ∨ wroteRow s r.id = true := by
          rcases hc with hc | hc
          · exact Or.inl hc
          · right; rw [wroteRow_set] at hc; simpa [hrt] using hc
        obtain ⟨w, hw, hres, hinv⟩ := h.evidence r hr hb hc'
        exact ⟨g w, memOld hw, by rw [(hg w hw).residentEq]; exact hres,
          by rw [((hg w hw).viewEq r hr hrt).2]; exact hinv⟩
    noResident := fun hb v hv => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact (hx v hvx).resident hb
      · rw [(hg w hw).residentEq]; exact h.noResident hb w hw
    liveLast := fun l1' c l2' hs hlive w hw => by
      have hcx : c ∉ xs := fun hc => by rw [(hx c hc).notLive] at hlive; simp at hlive
      obtain ⟨p1, p2, hp, hl2⟩ := tail_of_insert hs hcx
      rw [hsplit] at hp
      obtain ⟨k1, c0, k2, hk, -, hgc, hgk⟩ := map_eq_append_cons hp
      have hc0 : c0 ∈ s.chain := by rw [hk]; simp
      have hlive0 : live s.txs c0 = true := by rw [← (hg c0 hc0).liveEq, hgc]; exact hlive
      rcases hl2 w hw with hwx | hwp
      · exact Or.inr (hx w hwx).notSettled
      · rw [← hgk] at hwp
        obtain ⟨w0, hw0, rfl⟩ := List.mem_map.1 hwp
        have hw0c : w0 ∈ s.chain := by rw [hk]; simp [hw0]
        rcases h.liveLast k1 c0 k2 hk hlive0 w0 hw0 with hg0 | hs0
        · left; rw [(hg w0 hw0c).garbageEq]; exact hg0
        · right; rw [(hg w0 hw0c).settledEq]; exact hs0
    pendingOrder := fun y hy hyact l1' n l2' hs hn => by
      by_cases hyt : y.id = t.id
      · have := sameTx h htx hy hyt; subst this; exact hpend l1' n l2' hs hn
      · have hnx : n ∉ xs := fun hnx => by
          rw [(hx n hnx).notPending y.id hyt] at hn; simp at hn
        obtain ⟨p1, p2, hp, hl2⟩ := tail_of_insert hs hnx
        rw [hsplit] at hp
        obtain ⟨k1, c0, k2, hk, -, hgc, hgk⟩ := map_eq_append_cons hp
        have hc0 : c0 ∈ s.chain := by rw [hk]; simp
        have hgc0 : g c0 = c0 := (hg c0 hc0).pendingEq y.id hyt (by rw [hgc]; exact hn)
        have hn0 : livePendingOf y.id c0 = true := by rw [← hgc0, hgc]; exact hn
        rcases h.pendingOrder y hy hyact k1 c0 k2 hk hn0 with hd | hord
        · left; show doomed s.txs y (m1 ++ xs ++ m2) = true; rw [doomedEq]; exact hd
        · right
          intro w hw
          rcases hl2 w hw with hwx | hwp
          · exact Or.inr ⟨(hx w hwx).notSettled, (hx w hwx).notBelongs y.id hyt⟩
          · rw [← hgk] at hwp
            obtain ⟨w0, hw0, rfl⟩ := List.mem_map.1 hwp
            have hw0c : w0 ∈ s.chain := by rw [hk]; simp [hw0]
            rw [(hg w0 hw0c).garbageEq, (hg w0 hw0c).settledEq, (hg w0 hw0c).belongsEq y.id hyt]
            exact hord w0 hw0
    deletesSee := fun v hv => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact (hx v hvx).deletesSee
      · exact (hg w hw).deletesSee
    ownEnds := fun v hv => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact (hx v hvx).ownEnds
      · exact (hg w hw).ownEnds
    deleterSaw := fun v hv => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact (hx v hvx).deleterSaw
      · exact (hg w hw).deleterSaw
    writerStamp := fun u hu huact hw => by
      by_cases hut : u.id = t.id
      · rw [hut]; exact hwriter
      · have hw' : wroteRow s u.id = true := by rw [wroteRow_set] at hw; simpa [hut] using hw
        obtain ⟨w, hwc, hst⟩ := h.writerStamp u hu huact hw'
        exact ⟨g w, memOld hwc, (hg w hwc).stampsKeep u hu huact hut hst⟩
    endAfterBegin := fun v hv => by
      rcases memNew hv with hvx | ⟨w, hw, rfl⟩
      · exact (hx v hvx).endAfterBegin
      · exact (hg w hw).endAfterBegin
    rewrittenCommitted := h.rewrittenCommitted }

theorem KeptVersion.same {s : St} {t : Tx} (h : Inv s) {w : Version} (hw : w ∈ s.chain) :
    KeptVersion s t w w where
  beginEq := rfl
  valEq := rfl
  residentEq := rfl
  matZero := h.noMat w hw
  stampsSub := fun _ hst => Or.inl hst
  stampsKeep := fun _ _ _ _ hst => hst
  viewEq := fun _ _ _ => ⟨rfl, rfl⟩
  settledEq := rfl
  liveEq := rfl
  garbageEq := rfl
  belongsEq := fun _ _ => rfl
  pendingEq := fun _ _ _ => rfl
  endAfterEq := fun _ => rfl
  deletesSee := h.deletesSee w hw
  ownEnds := h.ownEnds w hw
  deleterSaw := h.deleterSaw w hw
  endAfterBegin := h.endAfterBegin w hw

theorem resolve_self {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs) (hact : t.state = .active) :
    resolve s.txs (some (.id t.id)) = .pending t.id := by
  simp [resolve, h.findTx_mem htx, hact]

theorem addedVersion_new {s : St} {t : Tx} {v : Nat} {res : Bool} (h : Inv s) (htx : t ∈ s.txs)
    (hact : t.state = .active) (hres : s.btree = none → res = false) :
    AddedVersion s t (newVersion t.id v res) where
  matZero := rfl
  stampsT := fun st hst => by simpa [stamps, newVersion, eq_comm] using hst
  viewNone := fun r _ hrt => by
    have hrt' : t.id ≠ r.id := fun e => hrt e.symm
    constructor
    · simp [isVisible, isBeginVisible, newVersion, h.findTx_mem htx, hact, hrt]
    · simp [isBtreeInvalidating, isVisible, isBeginVisible, newVersion, h.findTx_mem htx, hact,
        hrt]
  notSettled := by simp [settled, newVersion, resolve_self h htx hact]
  notLive := by simp [live, newVersion, resolve_self h htx hact, RS.isAt]
  notBelongs := fun y hy => by
    have : t.id ≠ y := fun e => hy e.symm
    simp [belongsTo, newVersion, this]
  notPending := fun y hy => by
    have : t.id ≠ y := fun e => hy e.symm
    simp [livePendingOf, newVersion, this]
  resident := hres
  deletesSee := fun _ _ _ he => by simp [newVersion] at he
  ownEnds := fun _ _ _ => Or.inl rfl
  deleterSaw := fun _ _ he => by simp [newVersion] at he
  endAfterBegin := fun _ _ _ he => by simp [newVersion, resolve] at he

theorem addedVersion_tombstone {s : St} {t : Tx} {v : Nat} (h : Inv s) (htx : t ∈ s.txs)
    (hact : t.state = .active) (hb : s.btree ≠ none) :
    AddedVersion s t (tombstone t.id v) where
  matZero := rfl
  stampsT := fun st hst => by simpa [stamps, tombstone, eq_comm] using hst
  viewNone := fun r _ hrt => by
    have hrt' : t.id ≠ r.id := fun e => hrt e.symm
    constructor
    · simp [isVisible, isBeginVisible, tombstone]
    · simp [isBtreeInvalidating, isVisible, isBeginVisible, tombstone, lookupTxState,
        h.findTx_mem htx, hact, hrt']
  notSettled := by
    simp only [settled, tombstone, resolve_none, resolve_self h htx hact]
    rfl
  notLive := by simp [live, tombstone, resolve, RS.isAt]
  notBelongs := fun y hy => by
    have : t.id ≠ y := fun e => hy e.symm
    simp [belongsTo, tombstone, this]
  notPending := fun _ _ => by simp [livePendingOf, tombstone]
  resident := fun hb' => absurd hb' hb
  deletesSee := fun _ _ hb' _ => by simp [tombstone] at hb'
  ownEnds := fun _ hb' _ => by simp [tombstone] at hb'
  deleterSaw := fun _ _ _ _ => Or.inl rfl
  endAfterBegin := fun _ _ hb' _ => by simp [tombstone, resolve] at hb'

theorem ended_endAt {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs) (hact : t.state = .active)
    {V : Version} (hV : V ∈ s.chain) (hvis : isVisible s.txs s.fin t V = true)
    (hnc : isWriteWriteConflict s.txs s.fin t V = false) :
    resolve s.txs V.endAt = .none ∨ ∃ z, resolve s.txs V.endAt = .dead z := by
  have hk := h.known hV
  rw [isVisible_eq hk] at hvis
  simp only [Bool.and_eq_true] at hvis
  have hve := hvis.2
  cases he : V.endAt with
  | none => left; rfl
  | some st =>
    cases st with
    | ts e =>
      simp [isWriteWriteConflict, he] at hnc
      rw [he] at hve
      simp [resolve, visE] at hve
      omega
    | id z =>
      have hz := known_end hk he
      cases hf : findTx s.txs z with
      | none => simp [hf] at hz
      | some u =>
        have hu := findTx_some hf
        by_cases hzt : z = t.id
        · exfalso
          subst hzt
          rw [h.findTx_mem htx] at hf
          cases hf
          rw [he] at hve
          simp [resolve, h.findTx_mem htx, hact, visE] at hve
        · simp only [isWriteWriteConflict, he, hzt, if_false, lookupTxState, hf] at hnc
          right
          refine ⟨z, ?_⟩
          cases hs : u.state <;> simp [hs] at hnc <;> simp [resolve, hf, hs]

theorem ended_beginAt {s : St} (h : Inv s) {t : Tx} {V : Version} (hV : V ∈ s.chain)
    (hvis : isVisible s.txs s.fin t V = true) :
    (∃ b, resolve s.txs V.beginAt = .time b ∧ b < t.beginTs) ∨
      V.beginAt = some (.id t.id) := by
  have hk := h.known hV
  rw [isVisible_eq hk] at hvis
  simp only [Bool.and_eq_true] at hvis
  have hvb := hvis.1
  cases hb : V.beginAt with
  | none => rw [hb] at hvb; simp [resolve, visB] at hvb
  | some st =>
    cases st with
    | ts b =>
      left
      refine ⟨b, rfl, ?_⟩
      rw [hb] at hvb
      simpa [resolve, visB] using hvb
    | id z =>
      have hz := known_begin hk hb
      cases hf : findTx s.txs z with
      | none => simp [hf] at hz
      | some u =>
        have hu := findTx_some hf
        rw [hb] at hvb
        cases hs : u.state with
        | active =>
          right
          simp [resolve, hf, hs, visB] at hvb
          rw [hvb.1]
        | preparing e =>
          left
          exact ⟨e, by simp [resolve, hf, hs], by simp [resolve, hf, hs, visB] at hvb; omega⟩
        | committed e =>
          left
          exact ⟨e, by simp [resolve, hf, hs], by simp [resolve, hf, hs, visB] at hvb; omega⟩
        | aborted => simp [resolve, hf, hs, visB] at hvb
        | terminated => simp [resolve, hf, hs, visB] at hvb

theorem keptVersion_ended {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs)
    (hact : t.state = .active)
    {V : Version} (hV : V ∈ s.chain) (hvis : isVisible s.txs s.fin t V = true)
    (hnc : isWriteWriteConflict s.txs s.fin t V = false) :
    KeptVersion s t V { V with endAt := some (.id t.id), mat := 0 } := by
  have hE := ended_endAt h htx hact hV hvis hnc
  have hB := ended_beginAt h hV hvis
  have hself := resolve_self h htx hact
  have hk := h.known hV
  have hk' : known s.txs { V with endAt := some (.id t.id), mat := 0 } := by
    intro y hy
    rcases mem_stamps.1 hy with hy | hy
    · exact known_begin hk hy
    · simp only [Option.some.injEq, Stamp.id.injEq] at hy
      subst hy; simp [h.findTx_mem htx]
  have hbNone : V.beginAt.isNone = false := by
    rcases hB with ⟨b, hb, -⟩ | hb
    · cases hv : V.beginAt with
      | none => rw [hv] at hb; simp [resolve] at hb
      | some _ => rfl
    · rw [hb]; rfl
  have hEnotAt : (resolve s.txs V.endAt).isAt = false := by
    rcases hE with hE | ⟨z, hE⟩ <;> rw [hE] <;> rfl
  have hEafter : ∀ b, (resolve s.txs V.endAt).after b = false := by
    intro b
    rcases hE with hE | ⟨z, hE⟩ <;> rw [hE] <;> rfl
  exact {
    beginEq := rfl
    valEq := rfl
    residentEq := rfl
    matZero := rfl
    stampsSub := fun st hst => by
      rcases mem_stamps.1 hst with hst | hst
      · exact Or.inl (mem_stamps.2 (Or.inl hst))
      · simp only [Option.some.injEq] at hst
        exact Or.inr hst.symm
    stampsKeep := fun u hu huact hut hst => by
      rcases mem_stamps.1 hst with hst | hst
      · exact mem_stamps.2 (Or.inl hst)
      · exfalso
        have : resolve s.txs V.endAt = .pending u.id := by
          rw [hst]; simp [resolve, h.findTx_mem hu, huact]
        rcases hE with hE | ⟨z, hE⟩ <;> rw [hE] at this <;> simp at this
    viewEq := fun r hr hrt => by
      have hrt' : t.id ≠ r.id := fun e => hrt e.symm
      have hvisEq : isVisible s.txs s.fin r { V with endAt := some (.id t.id), mat := 0 } =
          isVisible s.txs s.fin r V := by
        rw [isVisible_eq hk', isVisible_eq hk]
        simp only [hself]
        have h1 : visE r (resolve s.txs V.endAt) = true := by
          rcases hE with hE | ⟨z, hE⟩ <;> rw [hE] <;> rfl
        have h2 : visE r (RS.pending t.id) = true := by simp [visE, hrt']
        rw [h1, h2]
        rcases hB with ⟨b, hb, -⟩ | hb
        · rw [hb]; simp [visB]
        · rw [hb, hself]; simp [visB, hrt']
      refine ⟨hvisEq, ?_⟩
      have hro : readerOk s.txs r { V with endAt := some (.id t.id), mat := 0 } := by
        intro y hy hyr
        simp only [Option.some.injEq, Stamp.id.injEq] at hy
        exact absurd (hy ▸ hyr) hrt'
      rw [isBtreeInvalidating_eq hk' hro, isBtreeInvalidating_eq hk (h.readerOk hr hV),
        ← isVisible_eq hk', ← isVisible_eq hk, hvisEq]
      simp only [hself]
      have h1 : invE r (resolve s.txs V.endAt) = false := by
        rcases hE with hE | ⟨z, hE⟩ <;> rw [hE] <;> rfl
      have h2 : invE r (RS.pending t.id) = false := by simp [invE, hrt']
      rw [h1, h2]
    settledEq := by
      unfold settled
      simp only
      rcases hB with ⟨b, hb, -⟩ | hb
      · rw [hb]
      · rw [hb, hself]
    liveEq := by
      unfold live
      simp only [hself, hEnotAt]
      rfl
    garbageEq := by
      unfold isGarbage
      simp [hbNone]
    belongsEq := fun y _ => by
      unfold belongsTo
      simp [hbNone]
    pendingEq := fun y _ hp => by simp [livePendingOf] at hp
    endAfterEq := fun b => by
      simp only [hself, hEafter]
      rfl
    deletesSee := fun z x hz hx => by
      simp only [Option.some.injEq, Stamp.id.injEq] at hx
      simp only at hz
      rcases hB with ⟨b, hb, -⟩ | hb
      · right; rw [hz] at hb; rw [hb]; rfl
      · left; rw [hz] at hb; simp only [Option.some.injEq, Stamp.id.injEq] at hb; omega
    ownEnds := fun x hx hnat => by
      simp only at hx
      rcases hB with ⟨b, hb, -⟩ | hb
      · rw [hx] at hb; rw [hb] at hnat; simp [RS.isAt] at hnat
      · right
        rw [hx] at hb; simp only [Option.some.injEq, Stamp.id.injEq] at hb
        rw [hb]
    deleterSaw := fun u hu hue _ => by
      simp only [Option.some.injEq, Stamp.id.injEq] at hue
      have := sameTx h htx hu hue.symm
      subst this
      simp only
      rcases hB with hb | hb
      · exact Or.inr (Or.inr hb)
      · exact Or.inr (Or.inl hb)
    endAfterBegin := fun b c _ hc => by
      simp only [hself] at hc
      simp at hc }

theorem map_eq_self {α : Type} {f : α → α} {l : List α} (h : ∀ a ∈ l, f a = a) :
    l.map f = l := by
  induction l with
  | nil => rfl
  | cons a rest ih =>
    simp only [List.map_cons, h a (by simp), ih (fun b hb => h b (by simp [hb]))]

theorem time_in_hist {s : St} (h : Inv s) {v : Version} (hv : v ∈ s.chain) {b : Nat}
    (hb : resolve s.txs v.beginAt = .time b) : b ∈ s.histTs ∧ s.ckptMax < b := by
  cases hvb : v.beginAt with
  | none => rw [hvb] at hb; simp [resolve] at hb
  | some st =>
    cases st with
    | ts c =>
      rw [hvb] at hb
      simp [resolve] at hb
      subst hb
      have := h.tsStamp v hv c (mem_stamps.2 (Or.inl hvb))
      exact ⟨this.2.2, this.1⟩
    | id z =>
      rw [hvb] at hb
      cases hf : findTx s.txs z with
      | none => simp [resolve, hf] at hb
      | some u =>
        obtain ⟨hu, huz⟩ := findTx_some hf
        have hw := h.idStampWrote v hv z (mem_stamps.2 (Or.inl hvb))
        unfold wroteRow at hw
        obtain ⟨w, hwz⟩ := Option.isSome_iff_exists.1 hw
        have hbe : b ∈ txEnds u := by
          cases hs : u.state <;> simp [resolve, hf, hs] at hb <;> simp [txEnds, hs, hb]
        have hhist := h.histPrepared u hu b hbe w (by rw [huz]; exact hwz)
        have hbnd := h.endBounds u hu b hbe
        have hgt := h.beginGt u hu
        refine ⟨?_, by omega⟩
        simp only [St.histTs, List.mem_map]
        exact ⟨(b, w), hhist, rfl⟩

theorem livePending_visible {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs)
    (hact : t.state = .active) {n : Version} (hn : livePendingOf t.id n = true) :
    isVisible s.txs s.fin t n = true := by
  simp only [livePendingOf, Bool.and_eq_true, beq_iff_eq] at hn
  have he : n.endAt = none := Option.isNone_iff_eq_none.1 hn.2
  simp [isVisible, isBeginVisible, isEndVisible, hn.1, he, h.findTx_mem htx, hact]

theorem orderKey_facts {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs) {w : Version}
    (hlt : t.beginTs < orderKey s.txs w) :
    belongsTo t.id w = false ∧
      (settled s.txs w = true → (resolve s.txs w.beginAt).after t.beginTs = true) := by
  constructor
  · unfold belongsTo
    cases hb : w.beginAt with
    | none => simp [orderKey, hb] at hlt
    | some st =>
      cases st with
      | ts c => simp
      | id z =>
        by_cases hz : z = t.id
        · subst hz; simp [orderKey, hb, h.findTx_mem htx] at hlt
        · simp [hz]
  · intro hset
    unfold settled at hset
    cases hb : w.beginAt with
    | none => simp [orderKey, hb] at hlt
    | some st =>
      cases st with
      | ts c =>
        simp only [orderKey, hb] at hlt
        simp [resolve, RS.after]; omega
      | id z =>
        rw [hb] at hset
        cases hf : findTx s.txs z with
        | none => simp [resolve, hf] at hset
        | some u =>
          obtain ⟨hu, -⟩ := findTx_some hf
          simp only [orderKey, hb, hf] at hlt
          cases hs : u.state with
          | active => simp [resolve, hf, hs] at hset
          | aborted => simp [resolve, hf, hs] at hset
          | terminated => simp [resolve, hf, hs] at hset
          | preparing e =>
            have := h.endBounds u hu e (by simp [txEnds, hs])
            simp [resolve, hf, hs, RS.after]; omega
          | committed e =>
            have := h.endBounds u hu e (by simp [txEnds, hs])
            simp [resolve, hf, hs, RS.after]; omega

theorem noMissing {txs : List Tx} {l : List Version}
    (hl : ∀ w ∈ l, ∀ y, w.beginAt = some (.id y) → (findTx txs y).isSome = true) :
    hasMissingTxRef txs l = false := by
  unfold hasMissingTxRef
  rw [List.any_eq_false]
  intro w hw
  cases hb : w.beginAt with
  | none => simp
  | some st =>
    cases st with
    | ts _ => simp
    | id y =>
      have := hl w hw y hb
      cases hf : findTx txs y with
      | none => rw [hf] at this; simp at this
      | some _ => simp [hf]

theorem end_visible_version {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs)
    (hact : t.state = .active)
    {vs : List Version} (hd : deleteFromChain s.txs s.fin t s.chain = .deleted vs) :
    ∃ g : Version → Version, s.chain.map g = vs ∧
      (∀ w ∈ s.chain, KeptVersion s t w (g w)) ∧
      (∀ w ∈ vs, isVisible s.txs s.fin t w = false) ∧
      (∃ w ∈ vs, isBtreeInvalidating s.txs s.fin t w = true ∧ Stamp.id t.id ∈ stamps w) ∧
      (s.btree.isSome = true →
        ∃ w ∈ vs, w.resident = true ∧ isBtreeInvalidating s.txs s.fin t w = true) := by
  obtain ⟨l1, V, l2, hc, hvs, hvis, hnc, hl2⟩ := deleteFromChain_deleted hd
  have htr : t ∈ s.readers := mem_readers.2 (Or.inr ⟨htx, hact⟩)
  have hV : V ∈ s.chain := by rw [hc]; simp
  have hl1 : ∀ w ∈ l1, isVisible s.txs s.fin t w = false := by
    have hu := h.unique t htr
    rw [hc, List.filter_append, List.filter_cons_of_pos hvis, List.length_append,
      List.length_cons] at hu
    have h0 : (l1.filter (isVisible s.txs s.fin t)).length = 0 := by omega
    rw [List.length_eq_zero_iff, List.filter_eq_nil_iff] at h0
    intro w hw
    simpa using h0 w hw
  have hV'vis : isVisible s.txs s.fin t { V with endAt := some (.id t.id), mat := 0 } = false := by
    simp [isVisible, isEndVisible, h.findTx_mem htx, hact]
  have hV'inv : isBtreeInvalidating s.txs s.fin t { V with endAt := some (.id t.id), mat := 0 } =
      true := by
    simp [isBtreeInvalidating]
  have hne : ∀ w, isVisible s.txs s.fin t w = false → w ≠ V := by
    intro w hw hwV
    rw [hwV, hvis] at hw
    simp at hw
  refine ⟨fun w => if w = V then { V with endAt := some (.id t.id), mat := 0 } else w,
    ?_, ?_, ?_, ?_, ?_⟩
  · rw [hc, hvs, List.map_append, List.map_cons, if_pos rfl,
      map_eq_self (fun w hw => if_neg (hne w (hl1 w hw))),
      map_eq_self (fun w hw => if_neg (hne w (hl2 w hw)))]
  · intro w hw
    by_cases hwV : w = V
    · subst hwV
      simp only
      exact keptVersion_ended h htx hact hV hvis hnc
    · simp only [if_neg hwV]
      exact KeptVersion.same h hw
  · intro w hw
    rw [hvs] at hw
    simp only [List.mem_append, List.mem_cons] at hw
    rcases hw with hw | rfl | hw
    · exact hl1 w hw
    · exact hV'vis
    · exact hl2 w hw
  · exact ⟨_, by rw [hvs]; simp, hV'inv, mem_stamps.2 (Or.inr rfl)⟩
  · intro hb
    have hcond : epochChangeBefore s t.beginTs = true ∨ wroteRow s t.id = true := by
      rcases ended_beginAt h hV hvis with ⟨b, hb', hlt⟩ | hb'
      · left
        obtain ⟨hbh, hbc⟩ := time_in_hist h hV hb'
        exact h.epochChange_of hbh hbc hlt
      · right
        exact h.idStampWrote V hV t.id (mem_stamps.2 (Or.inl hb'))
    obtain ⟨R, hR, hres, hinv⟩ := h.evidence t htr hb hcond
    rw [hc] at hR
    simp only [List.mem_append, List.mem_cons] at hR
    rw [hvs]
    rcases hR with hR | rfl | hR
    · exact ⟨R, by simp [hR], hres, hinv⟩
    · exact ⟨{ R with endAt := some (.id t.id), mat := 0 }, by simp, hres, hV'inv⟩
    · exact ⟨R, by simp [hR], hres, hinv⟩

theorem writer_sees_new_version {s : St} (h : Inv s) {t : Tx} (htx : t ∈ s.txs)
    (hact : t.state = .active)
    {v : Nat} {res : Bool} {m1 m2 : List Version}
    (hinvis : ∀ w ∈ m1 ++ m2, isVisible s.txs s.fin t w = false)
    (hord : ∀ w ∈ m2, orderKey s.txs (newVersion t.id v res) < orderKey s.txs w) :
    rd (isVisible s.txs s.fin t) (isBtreeInvalidating s.txs s.fin t)
        (m1 ++ [newVersion t.id v res] ++ m2) s.btree = some v ∧
    ((m1 ++ [newVersion t.id v res] ++ m2).filter (isVisible s.txs s.fin t)).length ≤ 1 ∧
    (∀ l1 n l2, m1 ++ [newVersion t.id v res] ++ m2 = l1 ++ n :: l2 →
      livePendingOf t.id n = true →
      doomed s.txs t (m1 ++ [newVersion t.id v res] ++ m2) = true ∨
        ∀ w ∈ l2, isGarbage w = true ∨
          (settled s.txs w = false ∧ belongsTo t.id w = false)) ∧
    (∃ w ∈ m1 ++ [newVersion t.id v res] ++ m2, Stamp.id t.id ∈ stamps w) := by
  have hNvis : isVisible s.txs s.fin t (newVersion t.id v res) = true := by
    simp [isVisible, isBeginVisible, isEndVisible, newVersion, h.findTx_mem htx, hact]
  have hkN : orderKey s.txs (newVersion t.id v res) = t.beginTs := by
    simp [orderKey, newVersion, h.findTx_mem htx]
  have hmem : newVersion t.id v res ∈ m1 ++ [newVersion t.id v res] ++ m2 := by simp
  have honly : ∀ w ∈ m1 ++ [newVersion t.id v res] ++ m2, isVisible s.txs s.fin t w = true →
      w = newVersion t.id v res := by
    intro w hw hvw
    simp only [List.mem_append, List.mem_singleton] at hw
    rcases hw with (hw | hw) | hw
    · rw [hinvis w (List.mem_append_left _ hw)] at hvw; simp at hvw
    · exact hw
    · rw [hinvis w (List.mem_append_right _ hw)] at hvw; simp at hvw
  refine ⟨?_, ?_, ?_, ⟨_, hmem, by simp [stamps, newVersion]⟩⟩
  · rw [rd_single_visible hmem hNvis honly]; rfl
  · have e1 : m1.filter (isVisible s.txs s.fin t) = [] :=
      List.filter_eq_nil_iff.2 (fun w hw => by simp [hinvis w (List.mem_append_left _ hw)])
    have e2 : m2.filter (isVisible s.txs s.fin t) = [] :=
      List.filter_eq_nil_iff.2 (fun w hw => by simp [hinvis w (List.mem_append_right _ hw)])
    simp [List.filter_append, e1, e2, hNvis]
  · intro l1 n l2 hs hn
    rcases split_at_inserted hs with hnm | ⟨rfl, hl2⟩
    · exfalso
      have := livePending_visible h htx hact hn
      rw [hinvis n hnm] at this; simp at this
    · by_cases hd : doomed s.txs t (m1 ++ [newVersion t.id v res] ++ m2) = true
      · exact Or.inl hd
      · right
        intro w hw
        rw [hl2] at hw
        have hlt := hord w hw
        rw [hkN] at hlt
        obtain ⟨hbel, hset⟩ := orderKey_facts h htx hlt
        right
        refine ⟨?_, hbel⟩
        cases hs' : settled s.txs w
        · rfl
        · exfalso
          apply hd
          unfold doomed
          apply List.any_eq_true.2
          refine ⟨w, by simp [hw], ?_⟩
          simp [hs', hset hs']

end MvccGc.WriteProof

namespace MvccGc

open WriteProof

theorem inv_write {s s' : St} {x v : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hact : t.state = .active) (hs : stepWrite s t v = .ok s') : Inv s' := by
  have htx : t ∈ s.txs := (findTx_some ht).1
  have htr : t ∈ s.readers := mem_readers.2 (Or.inr ⟨htx, hact⟩)
  unfold stepWrite at hs
  split at hs
  · rename_i y hm
    split at hs
    · rename_i vs hd
      unfold St.insert at hs
      dsimp only at hs
      split at hs
      · simp at hs
      · cases hs
        obtain ⟨g, hmap, hg, hinvis, -, hevid⟩ := end_visible_version h htx hact hd
        obtain ⟨m1, m2, hins, hvs, hord⟩ :=
          insertVersion_eq (txs := s.txs) (vs := vs) (nv := newVersion t.id v false)
        show Inv { s with
          chain := insertVersion s.txs vs (newVersion t.id v false)
          wrote := setWrote s.wrote t.id (some v) }
        rw [hins, show m1 ++ newVersion t.id v false :: m2 =
          m1 ++ [newVersion t.id v false] ++ m2 by simp]
        rw [hvs] at hinvis hevid
        obtain ⟨hread, huniq, hpend, hwriter⟩ := writer_sees_new_version h htx hact hinvis hord
        refine inv_of_new_chain h htx hact (hmap.trans hvs).symm hg ?_ hread huniq ?_ hpend hwriter
        · intro X hX
          simp only [List.mem_singleton] at hX
          subst hX
          exact addedVersion_new h htx hact (fun _ => rfl)
        · intro hb
          obtain ⟨w, hw, hres, hinv⟩ := hevid hb
          refine ⟨w, ?_, hres, hinv⟩
          simp only [List.mem_append] at hw ⊢
          rcases hw with hw | hw
          · exact Or.inl (Or.inl hw)
          · exact Or.inr hw
    · simp at hs
    · simp at hs
  · rename_i hm
    unfold St.insert at hs
    dsimp only at hs
    split at hs
    · simp at hs
    · cases hs
      have hnone := mvccSide_none h.noMat hm
      obtain ⟨m1, m2, hins, hvs, hord⟩ := insertVersion_eq (txs := s.txs) (vs := s.chain)
        (nv := newVersion t.id v (s.btree.isSome && btreeSideValid s.txs s.fin t s.chain s.ckptMax))
      show Inv { s with
        chain := insertVersion s.txs s.chain
          (newVersion t.id v (s.btree.isSome && btreeSideValid s.txs s.fin t s.chain s.ckptMax))
        wrote := setWrote s.wrote t.id (some v) }
      generalize hob : (s.btree.isSome && btreeSideValid s.txs s.fin t s.chain s.ckptMax) = ob at *
      rw [hins, show m1 ++ newVersion t.id v ob :: m2 = m1 ++ [newVersion t.id v ob] ++ m2 by simp]
      have hinvis : ∀ w ∈ m1 ++ m2, isVisible s.txs s.fin t w = false := by
        rw [← hvs]; exact hnone
      obtain ⟨hread, huniq, hpend, hwriter⟩ := writer_sees_new_version h htx hact hinvis hord
      have hmemOld : ∀ w ∈ s.chain, w ∈ m1 ++ [newVersion t.id v ob] ++ m2 := by
        intro w hw
        rw [hvs] at hw
        simp only [List.mem_append] at hw ⊢
        rcases hw with hw | hw
        · exact Or.inl (Or.inl hw)
        · exact Or.inr hw
      refine inv_of_new_chain (g := fun w => w) h htx hact (by rw [← hvs]; simp)
        (fun w hw => KeptVersion.same h hw)
        ?_ hread huniq ?_ hpend hwriter
      · intro X hX
        simp only [List.mem_singleton] at hX
        subst hX
        refine addedVersion_new h htx hact (fun hb => ?_)
        rw [← hob, hb]; rfl
      · intro hb
        cases ob
        · have hbsv : s.chain.any (isBtreeInvalidating s.txs s.fin t) = true := by
            rw [hb, btreeSideValid_eq h.noMat] at hob
            simpa using hob
          have hread0 : s.read t = none := by
            rw [h.read_eq htr]
            exact rd_none_visible hnone (List.any_eq_true.1 hbsv)
          have hexp : s.expected t = none := by rw [← h.reads t htr, hread0]
          have hcond : epochChangeBefore s t.beginTs = true ∨ wroteRow s t.id = true := by
            cases hw : lookupWrote s.wrote t.id with
            | some w => right; simp [wroteRow, hw]
            | none =>
              left
              unfold St.expected at hexp
              rw [hw] at hexp
              simp only at hexp
              cases hec : epochChangeBefore s t.beginTs
              · exfalso
                have hall : ∀ e ∈ s.hist, e.1 < t.beginTs ↔ e.1 < s.ckptMax + 1 := by
                  intro e he
                  have hno : ¬(s.ckptMax < e.1 ∧ e.1 < t.beginTs) := by
                    intro hlt
                    have : epochChangeBefore s t.beginTs = true := by
                      unfold epochChangeBefore
                      exact List.any_eq_true.2 ⟨e, he, by simp [hlt.1, hlt.2]⟩
                    rw [hec] at this; simp at this
                  have hgt := h.beginGt t htx
                  constructor
                  · intro h2
                    by_cases h3 : s.ckptMax < e.1
                    · exact absurd ⟨h3, h2⟩ hno
                    · omega
                  · intro h2; omega
                have hva := valueAt_congr (init := s.init) hall
                rw [hexp, ← h.btreeOk] at hva
                rw [← hva] at hb
                simp at hb
              · rfl
          obtain ⟨R, hR, hres, hinv⟩ := h.evidence t htr hb hcond
          exact ⟨R, hmemOld R hR, hres, hinv⟩
        · refine ⟨newVersion t.id v true, by simp, rfl, ?_⟩
          have hNvis : isVisible s.txs s.fin t (newVersion t.id v true) = true := by
            simp [isVisible, isBeginVisible, isEndVisible, newVersion, h.findTx_mem htx, hact]
          simp [isBtreeInvalidating, hNvis]

theorem inv_delete {s s' : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hact : t.state = .active) (hs : stepDelete s t = .ok s') : Inv s' := by
  have htx : t ∈ s.txs := (findTx_some ht).1
  have htr : t ∈ s.readers := mem_readers.2 (Or.inr ⟨htx, hact⟩)
  unfold stepDelete at hs
  split at hs
  · simp at hs
  · rename_i shown hread0
    dsimp only at hs
    split at hs
    · rename_i vs hd
      cases hs
      obtain ⟨g, hmap, hg, hinvis, ⟨V', hV'mem, hV'inv, hV'st⟩, hevid⟩ :=
        end_visible_version h htx hact hd
      show Inv { s with chain := vs, wrote := setWrote s.wrote t.id none }
      have hf := inv_of_new_chain (xs := []) (m1 := []) (m2 := vs) (wv := none) h htx hact
        (by simpa using hmap.symm) hg (fun X hX => by simp at hX)
        (by simpa using rd_none_visible hinvis ⟨V', hV'mem, hV'inv⟩)
        (by
          simp only [List.nil_append]
          rw [List.filter_eq_nil_iff.2 (fun w hw => by simp [hinvis w hw])]
          simp)
        (by simpa using hevid)
        (fun l1 n l2 hs hn => by
          exfalso
          have hvis := livePending_visible h htx hact hn
          have hmem : n ∈ vs := by
            simp only [List.nil_append] at hs
            rw [hs]; simp
          rw [hinvis n hmem] at hvis
          simp at hvis)
        ⟨V', by simpa using hV'mem, hV'st⟩
      simpa using hf
    · simp at hs
    · rename_i hd
      split at hs
      · rename_i hinB
        unfold St.insert at hs
        dsimp only at hs
        split at hs
        · simp at hs
        · cases hs
          have hnone := deleteFromChain_notFound hd
          have hmN : mvccSide s.txs s.fin t s.chain s.ckptMax = none := by
            simpa using hinB
          have hbt : s.btree = some shown := by
            unfold St.read readRow at hread0
            rw [hmN] at hread0
            simp only at hread0
            split at hread0
            · exact hread0
            · simp at hread0
          obtain ⟨m1, m2, hins, hvs, -⟩ :=
            insertVersion_eq (txs := s.txs) (vs := s.chain) (nv := tombstone t.id shown)
          show Inv { s with
            chain := insertVersion s.txs s.chain (tombstone t.id shown)
            wrote := setWrote s.wrote t.id none }
          rw [hins, show m1 ++ tombstone t.id shown :: m2 = m1 ++ [tombstone t.id shown] ++ m2 by
            simp]
          have hTvis : isVisible s.txs s.fin t (tombstone t.id shown) = false := by
            simp [isVisible, isBeginVisible, tombstone]
          have hTinv : isBtreeInvalidating s.txs s.fin t (tombstone t.id shown) = true := by
            simp [isBtreeInvalidating, tombstone]
          have hall : ∀ w ∈ m1 ++ [tombstone t.id shown] ++ m2,
              isVisible s.txs s.fin t w = false := by
            intro w hw
            simp only [List.mem_append, List.mem_singleton] at hw
            rcases hw with (hw | rfl) | hw
            · exact hnone w (by rw [hvs]; simp [hw])
            · exact hTvis
            · exact hnone w (by rw [hvs]; simp [hw])
          refine inv_of_new_chain (g := fun w => w) h htx hact (by rw [← hvs]; simp)
            (fun w hw => KeptVersion.same h hw) ?_ (rd_none_visible hall ⟨_, by simp, hTinv⟩) ?_
            (fun _ => ⟨tombstone t.id shown, by simp, rfl, hTinv⟩) ?_
            ⟨tombstone t.id shown, by simp, by simp [stamps, tombstone]⟩
          · intro X hX
            simp only [List.mem_singleton] at hX
            subst hX
            exact addedVersion_tombstone h htx hact (by rw [hbt]; simp)
          · rw [List.filter_eq_nil_iff.2 (fun w hw => by simp [hall w hw])]
            simp
          · intro l1 n l2 hs hn
            exfalso
            have hvis := livePending_visible h htx hact hn
            have hmem : n ∈ m1 ++ [tombstone t.id shown] ++ m2 := by rw [hs]; simp
            rw [hall n hmem] at hvis
            simp at hvis
      · simp at hs

theorem write_no_panic {s : St} {x v : Nat} {t : Tx} {msg : String} (h : Inv s)
    (ht : findTx s.txs x = some t) : stepWrite s t v ≠ .panic msg := by
  have htx : t ∈ s.txs := (findTx_some ht).1
  have hkn : ∀ w ∈ s.chain, ∀ y, w.beginAt = some (.id y) → (findTx s.txs y).isSome = true :=
    fun w hw y hy => known_begin (h.known hw) hy
  have hNk : ∀ res, ∀ y, (newVersion t.id v res).beginAt = some (.id y) →
      (findTx s.txs y).isSome = true := by
    intro res y hy
    simp only [newVersion, Option.some.injEq, Stamp.id.injEq] at hy
    subst hy
    simp [h.findTx_mem htx]
  intro hs
  unfold stepWrite at hs
  split at hs
  · rename_i y hm
    obtain ⟨w, hw, hvis⟩ := mvccSide_some hm
    split at hs
    · rename_i vs hd
      obtain ⟨l1, V, l2, hc, hvs, -, -, -⟩ := deleteFromChain_deleted hd
      unfold St.insert at hs
      dsimp only at hs
      rw [noMissing] at hs
      · simp at hs
      · intro u hu y hy
        simp only [List.mem_cons] at hu
        rcases hu with rfl | hu
        · exact hNk false y hy
        · rw [hvs] at hu
          simp only [List.mem_append, List.mem_cons] at hu
          rcases hu with hu | rfl | hu
          · exact hkn u (by rw [hc]; simp [hu]) y hy
          · exact hkn V (by rw [hc]; simp) y hy
          · exact hkn u (by rw [hc]; simp [hu]) y hy
    · simp at hs
    · rename_i hd
      exact deleteFromChain_ne_notFound hw hvis hd
  · unfold St.insert at hs
    dsimp only at hs
    rw [noMissing] at hs
    · simp at hs
    · intro u hu y hy
      simp only [List.mem_cons] at hu
      rcases hu with rfl | hu
      · exact hNk _ y hy
      · exact hkn u hu y hy

theorem delete_no_panic {s : St} {t : Tx} {msg : String} (h : Inv s) :
    stepDelete s t ≠ .panic msg := by
  have hkn : ∀ w ∈ s.chain, ∀ y, w.beginAt = some (.id y) → (findTx s.txs y).isSome = true :=
    fun w hw y hy => known_begin (h.known hw) hy
  intro hs
  unfold stepDelete at hs
  split at hs
  · simp at hs
  · dsimp only at hs
    split at hs
    · simp at hs
    · simp at hs
    · rename_i hd
      split at hs
      · unfold St.insert at hs
        dsimp only at hs
        rw [noMissing] at hs
        · simp at hs
        · intro u hu y hy
          simp only [List.mem_cons] at hu
          rcases hu with rfl | hu
          · simp [tombstone] at hy
          · exact hkn u hu y hy
      · rename_i hinB
        cases hm : mvccSide s.txs s.fin t s.chain s.ckptMax with
        | none => rw [hm] at hinB; simp at hinB
        | some z =>
          obtain ⟨w, hw, hvis⟩ := mvccSide_some hm
          exact deleteFromChain_ne_notFound hw hvis hd

end MvccGc
