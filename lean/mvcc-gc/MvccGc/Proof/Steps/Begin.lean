import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# The invariant at the start and after `begin`

`inv_initial` shows that the invariant holds in the start state.

`begin` adds the old next reader as an active transaction. No stamp in the
chain names its id `nextTx`, so every stamp resolves as before. The new next
reader begins at `clock + 1`. Every change in `hist` has a timestamp below
`clock`, so the new next reader reads the same row as the old one.
-/

set_option linter.deprecated false

namespace MvccGc.BeginProof

def after (s : St) : St :=
  { s with
    txs := s.txs ++ [s.nextReader]
    clock := s.clock + 1
    nextTx := s.nextTx + 1 }

theorem findTx_append_ne {txs : List Tx} {u : Tx} {y : Nat} (h : u.id ≠ y) :
    findTx (txs ++ [u]) y = findTx txs y := by
  rw [findTx_append_single]
  cases findTx txs y <;> simp [h]

theorem resolve_append_ne {txs : List Tx} {u : Tx} {st : Stamp} (h : st ≠ .id u.id) :
    resolve (txs ++ [u]) (some st) = resolve txs (some st) := by
  cases st with
  | ts c => rfl
  | id x =>
    have hx : u.id ≠ x := fun e => h (by rw [e])
    rw [resolve_id, resolve_id, findTx_append_ne hx]

theorem readerOk_of_same {txs txs' : List Tx} {t : Tx} {v : Version}
    (hs : SameResolve txs txs' v) (hr : readerOk txs t v) : readerOk txs' t v := by
  intro x hx hxt
  have he := hs.2
  rw [hx] at he
  rw [he]
  exact hr x hx hxt

variable {s : St}

theorem mem_after_txs {t : Tx} : t ∈ (after s).txs ↔ t ∈ s.txs ∨ t = s.nextReader := by
  show t ∈ s.txs ++ [s.nextReader] ↔ _
  simp

theorem mem_readers_after {t : Tx} (ht : t ∈ (after s).readers) :
    t = (after s).nextReader ∨ t ∈ s.readers := by
  rcases mem_readers.1 ht with h1 | ⟨h2, h3⟩
  · exact Or.inl h1
  · right
    rcases mem_after_txs.1 h2 with h2 | h2
    · exact mem_readers.2 (Or.inr ⟨h2, h3⟩)
    · exact mem_readers.2 (Or.inl h2)

theorem resolve_after (h : Inv s) {v : Version} (hv : v ∈ s.chain) {st : Stamp}
    (hst : st ∈ stamps v) : resolve (after s).txs (some st) = resolve s.txs (some st) := by
  apply resolve_append_ne
  intro e
  subst e
  exact h.nextTx_not_ref hv hst

theorem sameResolve_after (h : Inv s) {v : Version} (hv : v ∈ s.chain) :
    SameResolve s.txs (after s).txs v := by
  constructor
  · cases hb : v.beginAt with
    | none => rfl
    | some st => exact resolve_after h hv (mem_stamps.2 (Or.inl hb))
  · cases he : v.endAt with
    | none => rfl
    | some st => exact resolve_after h hv (mem_stamps.2 (Or.inr he))

theorem readerOk_new (h : Inv s) {txs : List Tx} {v : Version} (hv : v ∈ s.chain) :
    readerOk txs (after s).nextReader v := by
  intro x hx hxt
  exfalso
  obtain ⟨t, ht, htx, -⟩ := h.idStamp v hv x (mem_stamps.2 (Or.inr hx))
  have hlt := h.idLt t ht
  have hx1 : x = s.nextTx + 1 := hxt
  omega

theorem sameCompare_new (h : Inv s) {v : Version} (hv : v ∈ s.chain) {o : Option Stamp}
    (ho : ∀ st, o = some st → st ∈ stamps v) :
    SameCompare s.nextReader (after s).nextReader (resolve s.txs o) := by
  match o, ho with
  | none, _ => trivial
  | some (.ts c), ho =>
    have hc := (h.tsStamp v hv c (ho _ rfl)).2.1
    show (s.clock + 1 > c ↔ s.clock > c) ∧ (s.clock + 1 < c ↔ s.clock < c)
    omega
  | some (.id x), ho =>
    obtain ⟨t, ht, rfl, -⟩ := h.idStamp v hv x (ho _ rfl)
    have hlt := h.idLt t ht
    have hend := h.endBounds t ht
    have hr : resolve s.txs (some (.id t.id)) = stateClass t.id t.state := by
      rw [resolve_id, h.findTx_mem ht]
    rw [hr]
    cases hs : t.state with
    | active =>
      show (t.id = s.nextTx + 1 ↔ t.id = s.nextTx)
      omega
    | preparing e =>
      have he := (hend e (by simp [txEnds, hs])).2
      show (s.clock + 1 > e ↔ s.clock > e) ∧ (s.clock + 1 < e ↔ s.clock < e)
      omega
    | committed e =>
      have he := (hend e (by simp [txEnds, hs])).2
      show (s.clock + 1 > e ↔ s.clock > e) ∧ (s.clock + 1 < e ↔ s.clock < e)
      omega
    | aborted => trivial
    | terminated => trivial

theorem view_new (h : Inv s) {v : Version} (hv : v ∈ s.chain) :
    isVisible (after s).txs s.fin (after s).nextReader v = isVisible s.txs s.fin s.nextReader v ∧
    isBtreeInvalidating (after s).txs s.fin (after s).nextReader v =
      isBtreeInvalidating s.txs s.fin s.nextReader v := by
  have hs := sameResolve_after h hv
  have hk := h.known hv
  have hb := sameCompare_new h hv (o := v.beginAt) (fun _ e => mem_stamps.2 (Or.inl e))
  have he := sameCompare_new h hv (o := v.endAt) (fun _ e => mem_stamps.2 (Or.inr e))
  constructor
  · rw [isVisible_same hs hk, isVisible_sameCompare hk hb he]
  · rw [isBtreeInvalidating_same hs hk (readerOk_new h hv) (readerOk_new h hv),
      isBtreeInvalidating_sameCompare hk (h.readerOk h.nextReader_mem hv) (readerOk_new h hv) hb he]

theorem view_old (h : Inv s) {t : Tx} (ht : t ∈ s.readers) {v : Version} (hv : v ∈ s.chain) :
    isVisible (after s).txs s.fin t v = isVisible s.txs s.fin t v ∧
    isBtreeInvalidating (after s).txs s.fin t v = isBtreeInvalidating s.txs s.fin t v := by
  have hs := sameResolve_after h hv
  have hk := h.known hv
  have hr := h.readerOk ht hv
  exact ⟨isVisible_same hs hk, isBtreeInvalidating_same hs hk hr (readerOk_of_same hs hr)⟩

theorem lookupWrote_ge (h : Inv s) {x : Nat} (hx : s.nextTx ≤ x) : lookupWrote s.wrote x = none :=
  lookupWrote_none_of fun p hp => by
    obtain ⟨t, ht, htp⟩ := h.wroteKeys p hp
    have := h.idLt t ht
    omega

theorem hist_lt_iff (h : Inv s) : ∀ e ∈ s.hist, e.1 < s.clock ↔ e.1 < s.clock + 1 := by
  intro e he
  have := h.histLt e he
  omega

theorem expected_of_none {s : St} {t : Tx} (hw : lookupWrote s.wrote t.id = none) :
    s.expected t = valueAt s.init s.hist t.beginTs := by
  unfold St.expected
  rw [hw]

theorem reader_match (h : Inv s) {t : Tx} (ht : t ∈ (after s).readers) :
    ∃ t0 ∈ s.readers,
      (∀ v ∈ s.chain, isVisible (after s).txs s.fin t v = isVisible s.txs s.fin t0 v ∧
        isBtreeInvalidating (after s).txs s.fin t v = isBtreeInvalidating s.txs s.fin t0 v) ∧
      (after s).expected t = s.expected t0 ∧
      epochChangeBefore (after s) t.beginTs = epochChangeBefore s t0.beginTs ∧
      wroteRow (after s) t.id = wroteRow s t0.id := by
  rcases mem_readers_after ht with rfl | ht0
  · have h1 : lookupWrote s.wrote (s.nextTx + 1) = none := lookupWrote_ge h (by omega)
    have h0 : lookupWrote s.wrote s.nextTx = none := lookupWrote_ge h (by omega)
    refine ⟨s.nextReader, h.nextReader_mem, fun v hv => view_new h hv, ?_, ?_, ?_⟩
    · rw [expected_of_none (s := after s) (t := (after s).nextReader) h1,
        expected_of_none (t := s.nextReader) h0]
      exact (valueAt_congr (hist_lt_iff h)).symm
    · exact epochChangeBefore_congr rfl rfl (hist_lt_iff h)
    · show (lookupWrote s.wrote (s.nextTx + 1)).isSome = (lookupWrote s.wrote s.nextTx).isSome
      rw [h1, h0]
  · exact ⟨t, ht0, fun v hv => view_old h ht0 hv, rfl, rfl, rfl⟩

theorem nodup_insert_middle {A B : List Nat} {c : Nat} (hAB : (A ++ B).Nodup) (hc : c ∉ A ++ B) :
    ((A ++ [c]) ++ B).Nodup := by
  rw [List.append_assoc, List.singleton_append]
  exact List.perm_middle.nodup_iff.2 (List.nodup_cons.2 ⟨hc, hAB⟩)

theorem inv_after (h : Inv s) : Inv (after s) := by
  have uniqueNew : ∀ t ∈ (after s).readers,
      (s.chain.filter (isVisible (after s).txs s.fin t)).length ≤ 1 := by
    intro t ht
    obtain ⟨t0, ht0, hview, -⟩ := reader_match h ht
    rw [List.filter_congr (fun v hv => (hview v hv).1)]
    exact h.unique t0 ht0
  have noEnds : ∀ e, e ∉ txEnds s.nextReader := by
    intro e he
    simp [txEnds, St.nextReader] at he
  exact {
    idsNodup := by
      show ((s.txs ++ [s.nextReader]).map Tx.id).Nodup
      rw [List.map_append]
      refine (List.perm_append_singleton _ _).nodup_iff.2 (List.nodup_cons.2 ⟨?_, h.idsNodup⟩)
      intro hm
      obtain ⟨t, ht, hid⟩ := List.mem_map.1 hm
      have := h.idLt t ht
      have hid' : t.id = s.nextTx := hid
      omega
    idLt := fun t ht => by
      rcases mem_after_txs.1 ht with ht | rfl
      · have := h.idLt t ht
        show t.id < s.nextTx + 1
        omega
      · show s.nextTx < s.nextTx + 1
        omega
    beginGt := fun t ht => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.beginGt t ht
      · exact h.ckpt_lt_clock
    beginLt := fun t ht => by
      rcases mem_after_txs.1 ht with ht | rfl
      · have := h.beginLt t ht
        show t.beginTs < s.clock + 1
        omega
      · show s.clock < s.clock + 1
        omega
    readMarkEq := fun t ht => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.readMarkEq t ht
      · rfl
    endBounds := fun t ht e he => by
      rcases mem_after_txs.1 ht with ht | rfl
      · have := h.endBounds t ht e he
        show t.beginTs < e ∧ e < s.clock + 1
        omega
      · exact absurd he (noEnds e)
    tsNodup := by
      show ((s.txs ++ [s.nextReader]).map Tx.beginTs ++
        (s.txs ++ [s.nextReader]).flatMap txEnds).Nodup
      have hnr : txEnds s.nextReader = [] := rfl
      rw [List.map_append, List.flatMap_append, List.flatMap_singleton, hnr, List.append_nil]
      refine nodup_insert_middle h.tsNodup ?_
      intro hm
      rcases List.mem_append.1 hm with hm | hm
      · obtain ⟨t, ht, hb⟩ := List.mem_map.1 hm
        have := h.beginLt t ht
        have hb' : t.beginTs = s.clock := hb
        omega
      · obtain ⟨t, ht, he⟩ := List.mem_flatMap.1 hm
        have := h.endBounds t ht _ he
        have : s.nextReader.beginTs = s.clock := rfl
        omega
    histSorted := h.histSorted
    histLt := fun e he => by
      have := h.histLt e he
      show e.1 < s.clock + 1
      omega
    histPrepared := fun t ht e he w hw => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.histPrepared t ht e he w hw
      · exact absurd he (noEnds e)
    histPreparedNone := fun t ht e he hw => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.histPreparedNone t ht e he hw
      · exact absurd he (noEnds e)
    histEpoch := fun e he => by
      rcases h.histEpoch e he with h1 | ⟨t, ht, hte⟩
      · exact Or.inl h1
      · exact Or.inr ⟨t, mem_after_txs.2 (Or.inl ht), hte⟩
    lastLt := by
      have := h.lastLt
      show s.lastCommitted < s.clock + 1
      omega
    ckptLe := h.ckptLe
    beginNotHist := fun t ht => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.beginNotHist t ht
      · exact h.clock_not_hist
    tsStamp := fun v hv c hc => by
      have := h.tsStamp v hv c hc
      refine ⟨this.1, ?_, this.2.2⟩
      show c < s.clock + 1
      omega
    idStamp := fun v hv x hx => by
      obtain ⟨t, ht, rest⟩ := h.idStamp v hv x hx
      exact ⟨t, mem_after_txs.2 (Or.inl ht), rest⟩
    idStampWrote := h.idStampWrote
    noMat := h.noMat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨t, ht, htp⟩ := h.wroteKeys p hp
      exact ⟨t, mem_after_txs.2 (Or.inl ht), htp⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := fun t ht => by
      obtain ⟨t0, ht0, hview, hexp, -, -⟩ := reader_match h ht
      show readRow (after s).txs s.fin t s.chain s.btree s.ckptMax = (after s).expected t
      rw [readRow_eq h.noMat (uniqueNew t ht), hexp, ← h.reads t0 ht0, h.read_eq ht0]
      exact rd_congr (fun v hv => (hview v hv).1) (fun v hv => (hview v hv).2)
    unique := uniqueNew
    evidence := fun t ht hb hc => by
      obtain ⟨t0, ht0, hview, -, hep, hwr⟩ := reader_match h ht
      rw [hep, hwr] at hc
      obtain ⟨v, hv, hres, hinv⟩ := h.evidence t0 ht0 hb hc
      exact ⟨v, hv, hres, (hview v hv).2.trans hinv⟩
    noResident := h.noResident
    liveLast := fun l1 c l2 hs hl w hw => by
      have hs' : s.chain = l1 ++ c :: l2 := hs
      have hc : c ∈ s.chain := by rw [hs']; simp
      have hw' : w ∈ s.chain := by rw [hs']; simp [hw]
      have hl' : live s.txs c = true := (live_same (sameResolve_after h hc)).symm.trans hl
      rcases h.liveLast l1 c l2 hs' hl' w hw with hg | hst
      · exact Or.inl hg
      · exact Or.inr ((settled_same (sameResolve_after h hw')).trans hst)
    pendingOrder := fun x hx hact l1 n l2 hsplit hn => by
      have hsplit' : s.chain = l1 ++ n :: l2 := hsplit
      have hnm : n ∈ s.chain := by rw [hsplit']; simp
      rcases mem_after_txs.1 hx with hx | rfl
      · rcases h.pendingOrder x hx hact l1 n l2 hsplit' hn with hd | hord
        · left
          exact (doomed_same (txs := s.txs) (txs' := (after s).txs) (t := x) (vs := s.chain)
            (fun w hw => sameResolve_after h hw)).trans hd
        · right
          intro w hw
          have hw' : w ∈ s.chain := by rw [hsplit']; simp [hw]
          rcases hord w hw with hg | ⟨hst, hb⟩
          · exact Or.inl hg
          · exact Or.inr ⟨(settled_same (sameResolve_after h hw')).trans hst, hb⟩
      · exfalso
        simp only [livePendingOf, Bool.and_eq_true, beq_iff_eq] at hn
        exact h.nextTx_not_ref hnm (mem_stamps.2 (Or.inl hn.1))
    deletesSee := fun v hv z x hz hx => by
      rw [resolve_after h hv (mem_stamps.2 (Or.inl hz))]
      exact h.deletesSee v hv z x hz hx
    ownEnds := fun v hv x hx hres => by
      rw [resolve_after h hv (mem_stamps.2 (Or.inl hx))] at hres
      exact h.ownEnds v hv x hx hres
    deleterSaw := fun v hv t ht hend hact => by
      rcases mem_after_txs.1 ht with ht | rfl
      · rw [(sameResolve_after h hv).1]
        exact h.deleterSaw v hv t ht hend hact
      · exact absurd (mem_stamps.2 (Or.inr hend)) (h.nextTx_not_ref hv)
    writerStamp := fun t ht hact hw => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.writerStamp t ht hact hw
      · exfalso
        have hw' : (lookupWrote s.wrote s.nextTx).isSome = true := hw
        rw [lookupWrote_ge h (Nat.le_refl _)] at hw'
        simp at hw'
    endAfterBegin := fun v hv b c hb hc => by
      rw [(sameResolve_after h hv).1] at hb
      rw [(sameResolve_after h hv).2] at hc
      exact h.endAfterBegin v hv b c hb hc
    rewrittenCommitted := fun t ht hr => by
      rcases mem_after_txs.1 ht with ht | rfl
      · exact h.rewrittenCommitted t ht hr
      · simp [St.nextReader] at hr }

end MvccGc.BeginProof

namespace MvccGc

theorem inv_initial (init : Option Nat) : Inv (St.initial init) := {
  idsNodup := List.nodup_nil
  idLt := fun t ht => by simp [St.initial] at ht
  beginGt := fun t ht => by simp [St.initial] at ht
  beginLt := fun t ht => by simp [St.initial] at ht
  readMarkEq := fun t ht => by simp [St.initial] at ht
  endBounds := fun t ht => by simp [St.initial] at ht
  tsNodup := List.nodup_nil
  histSorted := List.Pairwise.nil
  histLt := fun e he => by simp [St.initial] at he
  histPrepared := fun t ht => by simp [St.initial] at ht
  histPreparedNone := fun t ht => by simp [St.initial] at ht
  histEpoch := fun e he => by simp [St.initial] at he
  lastLt := Nat.zero_lt_one
  ckptLe := Nat.le_refl 0
  beginNotHist := fun t ht => by simp [St.initial] at ht
  tsStamp := fun v hv => by simp [St.initial] at hv
  idStamp := fun v hv => by simp [St.initial] at hv
  idStampWrote := fun v hv => by simp [St.initial] at hv
  noMat := fun v hv => by simp [St.initial] at hv
  btreeOk := rfl
  wroteKeys := fun p hp => by simp [St.initial] at hp
  wroteNodup := List.nodup_nil
  walEq := rfl
  reads := fun _ _ => rfl
  unique := fun _ _ => Nat.zero_le _
  evidence := fun t _ _ hc => by
    simp [epochChangeBefore, wroteRow, St.initial, lookupWrote] at hc
  noResident := fun _ v hv => by simp [St.initial] at hv
  liveLast := fun l1 c l2 hs => by simp [St.initial] at hs
  pendingOrder := fun x hx => by simp [St.initial] at hx
  deletesSee := fun v hv => by simp [St.initial] at hv
  ownEnds := fun v hv => by simp [St.initial] at hv
  deleterSaw := fun v hv => by simp [St.initial] at hv
  writerStamp := fun t ht => by simp [St.initial] at ht
  endAfterBegin := fun v hv => by simp [St.initial] at hv
  rewrittenCommitted := fun t ht => by simp [St.initial] at ht }

theorem inv_begin {s : St} (h : Inv s) :
    Inv { s with
      txs := s.txs ++ [{ id := s.nextTx, beginTs := s.clock, state := .active,
                         readMark := s.walPos, rewritten := false }]
      clock := s.clock + 1
      nextTx := s.nextTx + 1 } :=
  BeginProof.inv_after h

end MvccGc
