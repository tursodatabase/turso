import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# Commit, rewrite and finish keep the invariant

`commit` moves a transaction from `preparing e` to `committed e`. Both states
resolve a stamp to `.time e`, so no reader sees a change.

`rewrite` replaces each stamp `Stamp.id x` with `Stamp.ts e`. The new stamp
resolves to the same `.time e`, so no reader sees a change.

`finish` removes a committed and rewritten transaction. No stamp names it, so
the resolved stamps do not change.
-/

set_option linter.deprecated false

namespace MvccGc.CommitProof

/-! ## Lists of transactions -/

theorem tx_unique {s : St} {x : Nat} {t u : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hu : u ∈ s.txs) (hux : u.id = x) : u = t := by
  have := h.findTx_mem hu
  rw [hux, ht] at this
  exact (Option.some.inj this).symm

theorem image_mem_updateTx {txs : List Tx} {x : Nat} {f : Tx → Tx} {u : Tx} (hu : u ∈ txs) :
    (if u.id = x then f u else u) ∈ updateTx txs x f :=
  mem_updateTx.2 ⟨u, hu, rfl⟩

theorem mem_updateTx_of_ne {txs : List Tx} {x : Nat} {f : Tx → Tx} {u : Tx} (hu : u ∈ txs)
    (hne : u.id ≠ x) : u ∈ updateTx txs x f := by
  have := image_mem_updateTx (x := x) (f := f) hu
  rwa [if_neg hne] at this

theorem flatMap_congr' {α β : Type} {f g : α → List β} {l : List α} (h : ∀ a ∈ l, f a = g a) :
    l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a rest ih =>
    rw [List.flatMap_cons, List.flatMap_cons, h a (by simp),
      ih (fun b hb => h b (List.mem_cons_of_mem _ hb))]

theorem flatMap_filter_sublist {α β : Type} {f : α → List β} {p : α → Bool} {l : List α} :
    ((l.filter p).flatMap f).Sublist (l.flatMap f) := by
  induction l with
  | nil => exact List.Sublist.refl _
  | cons a rest ih =>
    rw [List.flatMap_cons]
    by_cases hp : p a = true
    · rw [List.filter_cons_of_pos hp, List.flatMap_cons]
      exact List.Sublist.append (List.Sublist.refl _) ih
    · rw [List.filter_cons_of_neg hp]
      exact ih.trans (List.sublist_append_right _ _)

theorem filter_active_updateTx {txs : List Tx} {x : Nat} {f : Tx → Tx}
    (hx : ∀ u ∈ txs, u.id = x → u.state ≠ .active ∧ (f u).state ≠ .active) :
    (updateTx txs x f).filter (fun t => t.state = .active) =
      txs.filter (fun t => t.state = .active) := by
  induction txs with
  | nil => rfl
  | cons u rest ih =>
    have ih' := ih (fun w hw => hx w (List.mem_cons_of_mem _ hw))
    simp only [updateTx, List.map_cons] at ih' ⊢
    by_cases hu : u.id = x
    · have hn := hx u (by simp) hu
      rw [if_pos hu, List.filter_cons_of_neg (by simpa using hn.2),
        List.filter_cons_of_neg (by simpa using hn.1), ih']
    · rw [if_neg hu]
      by_cases ha : u.state = .active
      · rw [List.filter_cons_of_pos (by simpa using ha), List.filter_cons_of_pos (by simpa using ha),
          ih']
      · rw [List.filter_cons_of_neg (by simpa using ha), List.filter_cons_of_neg (by simpa using ha),
          ih']

theorem readers_updateTx {s s' : St} {x : Nat} {f : Tx → Tx}
    (hnext : s'.nextReader = s.nextReader) (htx : s'.txs = updateTx s.txs x f)
    (hx : ∀ u ∈ s.txs, u.id = x → u.state ≠ .active ∧ (f u).state ≠ .active) :
    s'.readers = s.readers := by
  unfold St.readers
  rw [hnext, htx, filter_active_updateTx hx]

theorem forall_mem_updateTx {txs : List Tx} {x : Nat} {f : Tx → Tx} {P : Tx → Prop}
    (h : ∀ u ∈ txs, P (if u.id = x then f u else u)) : ∀ t ∈ updateTx txs x f, P t := by
  intro t ht
  obtain ⟨u, hu, rfl⟩ := mem_updateTx.1 ht
  exact h u hu

theorem map_updateTx {α : Type} {txs : List Tx} {x : Nat} {f : Tx → Tx} {k : Tx → α}
    (hk : ∀ u ∈ txs, k (if u.id = x then f u else u) = k u) :
    (updateTx txs x f).map k = txs.map k := by
  unfold updateTx
  rw [List.map_map]
  exact List.map_congr_left (fun u hu => hk u hu)

theorem flatMap_updateTx {α : Type} {txs : List Tx} {x : Nat} {f : Tx → Tx} {k : Tx → List α}
    (hk : ∀ u ∈ txs, k (if u.id = x then f u else u) = k u) :
    (updateTx txs x f).flatMap k = txs.flatMap k := by
  unfold updateTx
  rw [List.flatMap_map]
  exact flatMap_congr' (fun u hu => hk u hu)

/-! ## Commit -/

theorem resolve_commit {txs : List Tx} {x e : Nat} {t : Tx} (ht : findTx txs x = some t)
    (hst : t.state = .preparing e) (st : Option Stamp) :
    resolve (updateTx txs x (fun u => { u with state := .committed e })) st = resolve txs st := by
  match st with
  | none => rfl
  | some (.ts c) => rfl
  | some (.id y) =>
    rw [resolve_id, resolve_id,
      findTx_updateTx (f := fun u => { u with state := .committed e }) (fun _ => rfl)]
    cases hf : findTx txs y with
    | none => rfl
    | some u =>
      simp only [Option.map_some]
      by_cases hux : u.id = x
      · rw [if_pos hux]
        have hyx : y = x := (findTx_some hf).2.symm.trans hux
        rw [hyx, ht] at hf
        cases hf
        simp [stateClass, hst]
      · rw [if_neg hux]

theorem sameResolve_commit {txs : List Tx} {x e : Nat} {t : Tx} (ht : findTx txs x = some t)
    (hst : t.state = .preparing e) (v : Version) :
    SameResolve txs (updateTx txs x (fun u => { u with state := .committed e })) v :=
  ⟨resolve_commit ht hst v.beginAt, resolve_commit ht hst v.endAt⟩

end MvccGc.CommitProof

namespace MvccGc

theorem inv_commit {s : St} {x e : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .preparing e) :
    Inv { s with txs := updateTx s.txs x (fun u => { u with state := .committed e }) } := by
  have uniq : ∀ u ∈ s.txs, u.id = x → u = t := fun u hu hux => CommitProof.tx_unique h ht hu hux
  have hends : ∀ u ∈ s.txs,
      txEnds (if u.id = x then { u with state := .committed e } else u) = txEnds u := by
    intro u hu
    split
    · rename_i hux
      rw [uniq u hu hux]
      simp [txEnds, hst]
    · rfl
  have hreaders : ({ s with txs := updateTx s.txs x (fun u => { u with state := .committed e }) } :
      St).readers = s.readers := by
    refine CommitProof.readers_updateTx rfl rfl ?_
    intro u hu hux
    rw [uniq u hu hux]
    simp [hst]
  have hres := CommitProof.resolve_commit ht hst
  have hsame := CommitProof.sameResolve_commit ht hst
  have hact : ∀ u ∈ updateTx s.txs x (fun u => { u with state := .committed e }),
      u.state = .active → u ∈ s.txs ∧ u.id ≠ x := by
    intro u hu hua
    obtain ⟨w, hw, rfl⟩ := mem_updateTx.1 hu
    split at hua
    · simp at hua
    · rename_i hwx
      rw [if_neg hwx]
      exact ⟨hw, hwx⟩
  have hvis : ∀ r (fin' : Finalized), ∀ v ∈ s.chain,
      isVisible (updateTx s.txs x (fun u => { u with state := .committed e })) fin' r v =
        isVisible s.txs s.fin r v :=
    fun r fin' v hv => isVisible_same (hsame v) (h.known hv)
  have hinv : ∀ r ∈ s.readers, ∀ v ∈ s.chain,
      isBtreeInvalidating (updateTx s.txs x (fun u => { u with state := .committed e })) s.fin r v =
        isBtreeInvalidating s.txs s.fin r v := by
    intro r hr v hv
    apply isBtreeInvalidating_same (hsame v) (h.known hv) (h.readerOk hr hv)
    intro y hy hyr
    rw [hres]
    exact h.readerOk hr hv y hy hyr
  have uniqueNew : ∀ r ∈ s.readers,
      (s.chain.filter (isVisible (updateTx s.txs x (fun u => { u with state := .committed e }))
        s.fin r)).length ≤ 1 := by
    intro r hr
    rw [List.filter_congr (fun v hv => hvis r s.fin v hv)]
    exact h.unique r hr
  exact {
    idsNodup := by
      show ((updateTx s.txs x _).map Tx.id).Nodup
      rw [map_id_updateTx (f := fun u => { u with state := .committed e }) (fun _ => rfl)]
      exact h.idsNodup
    idLt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.idLt u hu)
    beginGt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.beginGt u hu)
    beginLt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.beginLt u hu)
    readMarkEq := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.readMarkEq u hu)
    endBounds := CommitProof.forall_mem_updateTx (fun u hu => by
      rw [hends u hu]
      split <;> exact h.endBounds u hu)
    tsNodup := by
      show ((updateTx s.txs x _).map Tx.beginTs ++ (updateTx s.txs x _).flatMap txEnds).Nodup
      rw [CommitProof.map_updateTx (k := Tx.beginTs) (fun u _ => by split <;> rfl),
        CommitProof.flatMap_updateTx (k := txEnds) hends]
      exact h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := CommitProof.forall_mem_updateTx (fun u hu => by
      rw [hends u hu]
      split <;> exact h.histPrepared u hu)
    histPreparedNone := CommitProof.forall_mem_updateTx (fun u hu => by
      rw [hends u hu]
      split <;> exact h.histPreparedNone u hu)
    histEpoch := fun p hp => by
      rcases h.histEpoch p hp with hle | ⟨u, hu, hpe⟩
      · exact Or.inl hle
      · exact Or.inr ⟨_, CommitProof.image_mem_updateTx hu, by rw [hends u hu]; exact hpe⟩
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := CommitProof.forall_mem_updateTx (fun u hu => by
      split <;> exact h.beginNotHist u hu)
    tsStamp := h.tsStamp
    idStamp := fun v hv y hy => by
      obtain ⟨u, hu, huy, hterm, hcomm⟩ := h.idStamp v hv y hy
      refine ⟨_, CommitProof.image_mem_updateTx hu, ?_, ?_, ?_⟩
      · split <;> exact huy
      · split
        · simp
        · exact hterm
      · split
        · rename_i hux
          intro e' _
          show u.rewritten = false
          rw [uniq u hu hux]
          cases hr : t.rewritten
          · rfl
          · obtain ⟨e'', he''⟩ := h.rewrittenCommitted t (findTx_some ht).1 hr
            rw [hst] at he''
            cases he''
        · exact hcomm
    idStampWrote := h.idStampWrote
    noMat := h.noMat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
      exact ⟨_, CommitProof.image_mem_updateTx hu, by split <;> exact hup⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := fun r hr => by
      rw [hreaders] at hr
      show readRow (updateTx s.txs x _) s.fin r s.chain s.btree s.ckptMax = s.expected r
      rw [readRow_eq h.noMat (uniqueNew r hr), ← h.reads r hr, h.read_eq hr]
      exact rd_congr (fun v hv => hvis r s.fin v hv) (fun v hv => hinv r hr v hv)
    unique := fun r hr => by
      rw [hreaders] at hr
      exact uniqueNew r hr
    evidence := fun r hr hb hc => by
      rw [hreaders] at hr
      obtain ⟨v, hv, hres', hinv'⟩ := h.evidence r hr hb hc
      exact ⟨v, hv, hres', (hinv r hr v hv).trans hinv'⟩
    noResident := h.noResident
    liveLast := fun l1 c l2 hsplit hlive w hw => by
      have hlive' : live s.txs c = true := (live_same (hsame c)).symm.trans hlive
      rcases h.liveLast l1 c l2 hsplit hlive' w hw with hg | hs
      · exact Or.inl hg
      · exact Or.inr ((settled_same (hsame w)).trans hs)
    pendingOrder := fun y hy hya l1 n l2 hsplit hn => by
      obtain ⟨hy0, -⟩ := hact y hy hya
      rcases h.pendingOrder y hy0 hya l1 n l2 hsplit hn with hd | hord
      · exact Or.inl ((doomed_same (fun w _ => hsame w)).trans hd)
      · refine Or.inr (fun w hw => ?_)
        rcases hord w hw with hg | ⟨hs, hb⟩
        · exact Or.inl hg
        · exact Or.inr ⟨(settled_same (hsame w)).trans hs, hb⟩
    deletesSee := fun v hv z y hz hy => by
      rcases h.deletesSee v hv z y hz hy with hzy | hat
      · exact Or.inl hzy
      · exact Or.inr ((congrArg RS.isAt (hres _)).trans hat)
    ownEnds := fun v hv y hy hna =>
      h.ownEnds v hv y hy ((congrArg RS.isAt (hres _)).symm.trans hna)
    deleterSaw := fun v hv u hu hve hua => by
      obtain ⟨hu0, -⟩ := hact u hu hua
      rcases h.deleterSaw v hv u hu0 hve hua with h1 | h1 | ⟨b, hb, hlt⟩
      · exact Or.inl h1
      · exact Or.inr (Or.inl h1)
      · exact Or.inr (Or.inr ⟨b, (hres _).trans hb, hlt⟩)
    writerStamp := fun u hu hua hw => h.writerStamp u (hact u hu hua).1 hua hw
    endAfterBegin := fun v hv b c hb hc =>
      h.endAfterBegin v hv b c ((hres _).symm.trans hb) ((hres _).symm.trans hc)
    rewrittenCommitted := CommitProof.forall_mem_updateTx (fun u hu => by
      split
      · intro _
        exact ⟨e, rfl⟩
      · exact h.rewrittenCommitted u hu) }

end MvccGc

namespace MvccGc.CommitProof

/-! ## Visibility when both stamps resolve the same way -/

theorem known_transfer {txs txs' : List Tx} {v v' : Version} (hk : known txs v)
    (hb : resolve txs' v'.beginAt = resolve txs v.beginAt)
    (he : resolve txs' v'.endAt = resolve txs v.endAt) : known txs' v' := by
  obtain ⟨h1, h2⟩ := resolve_ne_missing_of_known hk
  exact known_of_resolve (by rw [hb]; exact h1) (by rw [he]; exact h2)

theorem isVisible_transfer {txs txs' : List Tx} {fin fin' : Finalized} {r : Tx} {v v' : Version}
    (hk : known txs v) (hb : resolve txs' v'.beginAt = resolve txs v.beginAt)
    (he : resolve txs' v'.endAt = resolve txs v.endAt) (hn : v'.endAt.isNone = v.endAt.isNone) :
    isVisible txs' fin' r v' = isVisible txs fin r v := by
  rw [isVisible_eq hk, isVisible_eq (known_transfer hk hb he), hb, he, hn]

theorem isBtreeInvalidating_transfer {txs txs' : List Tx} {fin fin' : Finalized} {r : Tx}
    {v v' : Version} (hk : known txs v) (hb : resolve txs' v'.beginAt = resolve txs v.beginAt)
    (he : resolve txs' v'.endAt = resolve txs v.endAt) (hn : v'.endAt.isNone = v.endAt.isNone)
    (hr : readerOk txs r v) (hr' : readerOk txs' r v') :
    isBtreeInvalidating txs' fin' r v' = isBtreeInvalidating txs fin r v := by
  rw [isBtreeInvalidating_eq hk hr, isBtreeInvalidating_eq (known_transfer hk hb he) hr', hb, he, hn]

/-! ## Rewrite -/

/-- What `rewriteVersion x e` does to one stamp. -/
def rwStamp (x e : Nat) (st : Option Stamp) : Option Stamp :=
  if st = some (.id x) then some (.ts e) else st

theorem rewriteVersion_beginAt {x e : Nat} {v : Version} :
    (rewriteVersion x e v).beginAt = rwStamp x e v.beginAt := by
  by_cases h1 : v.beginAt = some (.id x) <;> by_cases h2 : v.endAt = some (.id x) <;>
    simp [rewriteVersion, rwStamp, h1, h2]

theorem rewriteVersion_endAt {x e : Nat} {v : Version} :
    (rewriteVersion x e v).endAt = rwStamp x e v.endAt := by
  by_cases h1 : v.beginAt = some (.id x) <;> by_cases h2 : v.endAt = some (.id x) <;>
    simp [rewriteVersion, rwStamp, h1, h2]

theorem rewriteVersion_val {x e : Nat} {v : Version} : (rewriteVersion x e v).val = v.val := by
  by_cases h1 : v.beginAt = some (.id x) <;> by_cases h2 : v.endAt = some (.id x) <;>
    simp [rewriteVersion, h1, h2]

theorem rewriteVersion_resident {x e : Nat} {v : Version} :
    (rewriteVersion x e v).resident = v.resident := by
  by_cases h1 : v.beginAt = some (.id x) <;> by_cases h2 : v.endAt = some (.id x) <;>
    simp [rewriteVersion, h1, h2]

theorem rewriteVersion_mat {x e : Nat} {v : Version} (h : v.mat = 0) :
    (rewriteVersion x e v).mat = 0 := by
  by_cases h1 : v.beginAt = some (.id x) <;> by_cases h2 : v.endAt = some (.id x) <;>
    simp [rewriteVersion, h1, h2, h]

theorem rwStamp_isNone {x e : Nat} {st : Option Stamp} : (rwStamp x e st).isNone = st.isNone := by
  unfold rwStamp
  split
  · rename_i h; rw [h]; rfl
  · rfl

theorem rwStamp_none {x e : Nat} : rwStamp x e none = none := rfl

theorem rwStamp_id_ne {x e y : Nat} (hy : y ≠ x) : rwStamp x e (some (.id y)) = some (.id y) := by
  unfold rwStamp
  rw [if_neg (by simp [hy])]

theorem rwStamp_eq_id {x e y : Nat} {st : Option Stamp} :
    rwStamp x e st = some (.id y) ↔ st = some (.id y) ∧ y ≠ x := by
  unfold rwStamp
  split
  · rename_i h
    subst h
    simp only [Option.some.injEq, reduceCtorEq, false_iff, not_and]
    intro hxy
    cases hxy
    simp
  · rename_i h
    constructor
    · intro h'
      refine ⟨h', ?_⟩
      intro hyx
      subst hyx
      exact h h'
    · exact fun h' => h'.1

theorem rwStamp_eq_ts {x e c : Nat} {st : Option Stamp} (h : rwStamp x e st = some (.ts c)) :
    st = some (.ts c) ∨ (st = some (.id x) ∧ c = e) := by
  unfold rwStamp at h
  split at h
  · rename_i hst
    cases h
    exact Or.inr ⟨hst, rfl⟩
  · exact Or.inl h

theorem rwStamp_beq_id {x e y : Nat} {st : Option Stamp} (hy : y ≠ x) :
    (rwStamp x e st == some (.id y)) = (st == some (.id y)) := by
  unfold rwStamp
  split
  · rename_i h
    subst h
    have : x ≠ y := fun h => hy h.symm
    rw [Bool.eq_iff_iff, beq_iff_eq, beq_iff_eq]
    simp [this]
  · rfl

theorem mem_stamps_rw_id {x e y : Nat} {v : Version} :
    Stamp.id y ∈ stamps (rewriteVersion x e v) ↔ Stamp.id y ∈ stamps v ∧ y ≠ x := by
  rw [mem_stamps, mem_stamps, rewriteVersion_beginAt, rewriteVersion_endAt, rwStamp_eq_id,
    rwStamp_eq_id]
  constructor
  · rintro (h | h)
    · exact ⟨Or.inl h.1, h.2⟩
    · exact ⟨Or.inr h.1, h.2⟩
  · rintro ⟨h | h, hy⟩
    · exact Or.inl ⟨h, hy⟩
    · exact Or.inr ⟨h, hy⟩

theorem mem_stamps_rw_ts {x e c : Nat} {v : Version}
    (h : Stamp.ts c ∈ stamps (rewriteVersion x e v)) :
    Stamp.ts c ∈ stamps v ∨ (Stamp.id x ∈ stamps v ∧ c = e) := by
  rw [mem_stamps, rewriteVersion_beginAt, rewriteVersion_endAt] at h
  rcases h with h | h
  · rcases rwStamp_eq_ts h with h' | ⟨h', hc⟩
    · exact Or.inl (mem_stamps.2 (Or.inl h'))
    · exact Or.inr ⟨mem_stamps.2 (Or.inl h'), hc⟩
  · rcases rwStamp_eq_ts h with h' | ⟨h', hc⟩
    · exact Or.inl (mem_stamps.2 (Or.inr h'))
    · exact Or.inr ⟨mem_stamps.2 (Or.inr h'), hc⟩

theorem resolve_rewrite {txs : List Tx} {x : Nat} (st : Option Stamp) :
    resolve (updateTx txs x (fun u => { u with rewritten := true })) st = resolve txs st := by
  match st with
  | none => rfl
  | some (.ts c) => rfl
  | some (.id y) =>
    rw [resolve_id, resolve_id,
      findTx_updateTx (f := fun u => { u with rewritten := true }) (fun _ => rfl)]
    cases hf : findTx txs y with
    | none => rfl
    | some u =>
      simp only [Option.map_some]
      split <;> rfl

theorem resolve_rwStamp {txs txs' : List Tx} {x e : Nat}
    (hres : ∀ st, resolve txs' st = resolve txs st) (hx : resolve txs (some (.id x)) = .time e)
    (st : Option Stamp) : resolve txs' (rwStamp x e st) = resolve txs st := by
  unfold rwStamp
  split
  · rename_i h
    rw [h, hx]
    rfl
  · exact hres st

end MvccGc.CommitProof

namespace MvccGc

theorem inv_rewrite {s : St} {x e : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .committed e) (hrw : t.rewritten = false) :
    Inv { s with
      chain := s.chain.map (rewriteVersion x e)
      txs := updateTx s.txs x (fun u => { u with rewritten := true }) } := by
  have _ := hrw
  have htmem : t ∈ s.txs := (findTx_some ht).1
  have htid : t.id = x := (findTx_some ht).2
  have uniq : ∀ u ∈ s.txs, u.id = x → u = t := fun u hu hux => CommitProof.tx_unique h ht hu hux
  have hte : e ∈ txEnds t := by simp [txEnds, hst]
  have heb := h.endBounds t htmem e hte
  have hckpt : s.ckptMax < e := by have := h.beginGt t htmem; omega
  have hxres : resolve s.txs (some (.id x)) = .time e := by
    rw [resolve_id, ht]
    simp [stateClass, hst]
  have hres := CommitProof.resolve_rewrite (txs := s.txs) (x := x)
  have hrs := CommitProof.resolve_rwStamp (e := e) hres hxres
  have hB : ∀ v, resolve (updateTx s.txs x (fun u => { u with rewritten := true }))
      (rewriteVersion x e v).beginAt = resolve s.txs v.beginAt := by
    intro v
    rw [CommitProof.rewriteVersion_beginAt]
    exact hrs _
  have hE : ∀ v, resolve (updateTx s.txs x (fun u => { u with rewritten := true }))
      (rewriteVersion x e v).endAt = resolve s.txs v.endAt := by
    intro v
    rw [CommitProof.rewriteVersion_endAt]
    exact hrs _
  have hN : ∀ v : Version, (rewriteVersion x e v).endAt.isNone = v.endAt.isNone := by
    intro v
    rw [CommitProof.rewriteVersion_endAt, CommitProof.rwStamp_isNone]
  have hBN : ∀ v : Version, (rewriteVersion x e v).beginAt.isNone = v.beginAt.isNone := by
    intro v
    rw [CommitProof.rewriteVersion_beginAt, CommitProof.rwStamp_isNone]
  have hehist : ∀ v ∈ s.chain, Stamp.id x ∈ stamps v → e ∈ s.histTs := by
    intro v hv hx
    have hw := h.idStampWrote v hv x hx
    unfold wroteRow at hw
    obtain ⟨w, hw'⟩ := Option.isSome_iff_exists.1 hw
    have hmem := h.histPrepared t htmem e hte w (by rw [htid]; exact hw')
    exact List.mem_map.2 ⟨(e, w), hmem, rfl⟩
  have hreaders : ({ s with
      chain := s.chain.map (rewriteVersion x e)
      txs := updateTx s.txs x (fun u => { u with rewritten := true }) } : St).readers =
        s.readers := by
    refine CommitProof.readers_updateTx rfl rfl ?_
    intro u hu hux
    rw [uniq u hu hux]
    simp [hst]
  have hact : ∀ u ∈ updateTx s.txs x (fun u => { u with rewritten := true }),
      u.state = .active → u ∈ s.txs ∧ u.id ≠ x := by
    intro u hu hua
    obtain ⟨w, hw, rfl⟩ := mem_updateTx.1 hu
    split at hua
    · rename_i hwx
      rw [uniq w hw hwx] at hua
      simp [hst] at hua
    · rename_i hwx
      rw [if_neg hwx]
      exact ⟨hw, hwx⟩
  have hreaderOk : ∀ r ∈ s.readers, ∀ v ∈ s.chain,
      readerOk (updateTx s.txs x (fun u => { u with rewritten := true })) r
        (rewriteVersion x e v) := by
    intro r hr v hv y hy hyr
    rw [CommitProof.rewriteVersion_endAt, CommitProof.rwStamp_eq_id] at hy
    rw [hres]
    exact h.readerOk hr hv y hy.1 hyr
  have hvis : ∀ r (fin' : Finalized), ∀ v ∈ s.chain,
      isVisible (updateTx s.txs x (fun u => { u with rewritten := true })) fin' r
        (rewriteVersion x e v) = isVisible s.txs s.fin r v :=
    fun r fin' v hv => CommitProof.isVisible_transfer (h.known hv) (hB v) (hE v) (hN v)
  have hinv : ∀ r ∈ s.readers, ∀ v ∈ s.chain,
      isBtreeInvalidating (updateTx s.txs x (fun u => { u with rewritten := true })) s.fin r
        (rewriteVersion x e v) = isBtreeInvalidating s.txs s.fin r v :=
    fun r hr v hv => CommitProof.isBtreeInvalidating_transfer (h.known hv) (hB v) (hE v) (hN v)
      (h.readerOk hr hv) (hreaderOk r hr v hv)
  have uniqueNew : ∀ r ∈ s.readers,
      ((s.chain.map (rewriteVersion x e)).filter
        (isVisible (updateTx s.txs x (fun u => { u with rewritten := true })) s.fin r)).length ≤ 1 := by
    intro r hr
    rw [filter_map_length (fun v hv => hvis r s.fin v hv)]
    exact h.unique r hr
  have noMatNew : ∀ v ∈ s.chain.map (rewriteVersion x e), v.mat = 0 := by
    intro v' hv'
    obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
    exact CommitProof.rewriteVersion_mat (h.noMat v hv)
  have hsettledEq : ∀ v, settled (updateTx s.txs x (fun u => { u with rewritten := true }))
      (rewriteVersion x e v) = settled s.txs v := by
    intro v
    unfold settled
    rw [hB, hE]
  have hliveEq : ∀ v, live (updateTx s.txs x (fun u => { u with rewritten := true }))
      (rewriteVersion x e v) = live s.txs v := by
    intro v
    unfold live
    rw [hB, hE]
  have hgarb : ∀ v, isGarbage (rewriteVersion x e v) = isGarbage v := by
    intro v
    unfold isGarbage
    rw [hBN, hN]
  have hbelongs : ∀ y, y ≠ x → ∀ v, belongsTo y (rewriteVersion x e v) = belongsTo y v := by
    intro y hy v
    unfold belongsTo
    rw [CommitProof.rewriteVersion_beginAt, CommitProof.rewriteVersion_endAt,
      CommitProof.rwStamp_beq_id hy, CommitProof.rwStamp_beq_id hy, CommitProof.rwStamp_isNone]
  have hpend : ∀ y, y ≠ x → ∀ v, livePendingOf y (rewriteVersion x e v) = livePendingOf y v := by
    intro y hy v
    unfold livePendingOf
    rw [CommitProof.rewriteVersion_beginAt, CommitProof.rwStamp_beq_id hy, hN]
  have hdoomed : ∀ u, doomed (updateTx s.txs x (fun u => { u with rewritten := true })) u
      (s.chain.map (rewriteVersion x e)) = doomed s.txs u s.chain := by
    intro u
    unfold doomed
    rw [List.any_map]
    apply any_congr'
    intro w _
    simp only [Function.comp_apply]
    rw [hsettledEq, hB, hE]
  exact {
    idsNodup := by
      show ((updateTx s.txs x _).map Tx.id).Nodup
      rw [map_id_updateTx (f := fun u => { u with rewritten := true }) (fun _ => rfl)]
      exact h.idsNodup
    idLt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.idLt u hu)
    beginGt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.beginGt u hu)
    beginLt := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.beginLt u hu)
    readMarkEq := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.readMarkEq u hu)
    endBounds := CommitProof.forall_mem_updateTx (fun u hu => by split <;> exact h.endBounds u hu)
    tsNodup := by
      show ((updateTx s.txs x _).map Tx.beginTs ++ (updateTx s.txs x _).flatMap txEnds).Nodup
      rw [CommitProof.map_updateTx (k := Tx.beginTs) (fun u _ => by split <;> rfl),
        CommitProof.flatMap_updateTx (k := txEnds) (fun u _ => by split <;> rfl)]
      exact h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := CommitProof.forall_mem_updateTx (fun u hu => by
      split <;> exact h.histPrepared u hu)
    histPreparedNone := CommitProof.forall_mem_updateTx (fun u hu => by
      split <;> exact h.histPreparedNone u hu)
    histEpoch := fun p hp => by
      rcases h.histEpoch p hp with hle | ⟨u, hu, hpe⟩
      · exact Or.inl hle
      · exact Or.inr ⟨_, CommitProof.image_mem_updateTx hu, by split <;> exact hpe⟩
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := CommitProof.forall_mem_updateTx (fun u hu => by
      split <;> exact h.beginNotHist u hu)
    tsStamp := fun v' hv' c hc => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      rcases CommitProof.mem_stamps_rw_ts hc with hc' | ⟨hx', rfl⟩
      · exact h.tsStamp v hv c hc'
      · exact ⟨hckpt, heb.2, hehist v hv hx'⟩
    idStamp := fun v' hv' y hy => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      obtain ⟨hy', hyx⟩ := CommitProof.mem_stamps_rw_id.1 hy
      obtain ⟨u, hu, huy, hterm, hcomm⟩ := h.idStamp v hv y hy'
      exact ⟨u, CommitProof.mem_updateTx_of_ne hu (by rw [huy]; exact hyx), huy, hterm, hcomm⟩
    idStampWrote := fun v' hv' y hy => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      exact h.idStampWrote v hv y (CommitProof.mem_stamps_rw_id.1 hy).1
    noMat := noMatNew
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
      exact ⟨_, CommitProof.image_mem_updateTx hu, by split <;> exact hup⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := fun r hr => by
      rw [hreaders] at hr
      show readRow (updateTx s.txs x _) s.fin r (s.chain.map (rewriteVersion x e)) s.btree
        s.ckptMax = s.expected r
      rw [readRow_eq noMatNew (uniqueNew r hr), ← h.reads r hr, h.read_eq hr]
      exact rd_map (fun v hv => hvis r s.fin v hv) (fun v hv => hinv r hr v hv)
        (fun v _ => CommitProof.rewriteVersion_val)
    unique := fun r hr => by
      rw [hreaders] at hr
      exact uniqueNew r hr
    evidence := fun r hr hb hc => by
      rw [hreaders] at hr
      obtain ⟨v, hv, hres', hinv'⟩ := h.evidence r hr hb hc
      exact ⟨_, List.mem_map_of_mem hv, CommitProof.rewriteVersion_resident.trans hres',
        (hinv r hr v hv).trans hinv'⟩
    noResident := fun hb v' hv' => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      exact CommitProof.rewriteVersion_resident.trans (h.noResident hb v hv)
    liveLast := fun l1' c' l2' hsplit hl w' hw' => by
      obtain ⟨l1, c, l2, hs, -, rfl, rfl⟩ := map_eq_append_cons hsplit
      obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hw'
      rcases h.liveLast l1 c l2 hs ((hliveEq c).symm.trans hl) w hw with hg | hs'
      · exact Or.inl ((hgarb w).trans hg)
      · exact Or.inr ((hsettledEq w).trans hs')
    pendingOrder := fun y hy hya l1' n' l2' hsplit hn => by
      obtain ⟨hy0, hyx⟩ := hact y hy hya
      obtain ⟨l1, n, l2, hs, -, rfl, rfl⟩ := map_eq_append_cons hsplit
      rcases h.pendingOrder y hy0 hya l1 n l2 hs ((hpend y.id hyx n).symm.trans hn) with hd | hord
      · exact Or.inl ((hdoomed y).trans hd)
      · refine Or.inr (fun w' hw' => ?_)
        obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hw'
        rcases hord w hw with hg | ⟨hs', hb'⟩
        · exact Or.inl ((hgarb w).trans hg)
        · exact Or.inr ⟨(hsettledEq w).trans hs', (hbelongs y.id hyx w).trans hb'⟩
    deletesSee := fun v' hv' z y hz hy => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      rw [CommitProof.rewriteVersion_beginAt, CommitProof.rwStamp_eq_id] at hz
      rw [CommitProof.rewriteVersion_endAt, CommitProof.rwStamp_eq_id] at hy
      rcases h.deletesSee v hv z y hz.1 hy.1 with hzy | hat
      · exact Or.inl hzy
      · exact Or.inr ((congrArg RS.isAt (hres _)).trans hat)
    ownEnds := fun v' hv' y hy hna => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      rw [CommitProof.rewriteVersion_beginAt, CommitProof.rwStamp_eq_id] at hy
      rw [CommitProof.rewriteVersion_endAt]
      rcases h.ownEnds v hv y hy.1 ((congrArg RS.isAt (hres _)).symm.trans hna) with he' | he'
      · left
        rw [he', CommitProof.rwStamp_none]
      · right
        rw [he', CommitProof.rwStamp_id_ne hy.2]
    deleterSaw := fun v' hv' u hu hve hua => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      obtain ⟨hu0, hux⟩ := hact u hu hua
      rw [CommitProof.rewriteVersion_endAt, CommitProof.rwStamp_eq_id] at hve
      rcases h.deleterSaw v hv u hu0 hve.1 hua with h1 | h1 | ⟨b, hb, hlt⟩
      · left
        rw [CommitProof.rewriteVersion_beginAt, h1, CommitProof.rwStamp_none]
      · right
        left
        rw [CommitProof.rewriteVersion_beginAt, h1, CommitProof.rwStamp_id_ne hux]
      · right
        right
        exact ⟨b, (hB v).trans hb, hlt⟩
    writerStamp := fun u hu hua hw => by
      obtain ⟨hu0, hux⟩ := hact u hu hua
      obtain ⟨v, hv, hsv⟩ := h.writerStamp u hu0 hua hw
      exact ⟨_, List.mem_map_of_mem hv, CommitProof.mem_stamps_rw_id.2 ⟨hsv, hux⟩⟩
    endAfterBegin := fun v' hv' b c hb hc => by
      obtain ⟨v, hv, rfl⟩ := List.mem_map.1 hv'
      exact h.endAfterBegin v hv b c ((hB v).symm.trans hb) ((hE v).symm.trans hc)
    rewrittenCommitted := CommitProof.forall_mem_updateTx (fun u hu => by
      split
      · rename_i hux
        intro _
        refine ⟨e, ?_⟩
        show u.state = .committed e
        rw [uniq u hu hux, hst]
      · exact h.rewrittenCommitted u hu) }

end MvccGc

namespace MvccGc.CommitProof

/-! ## Finish -/

theorem filter_active_removeTx {txs : List Tx} {x : Nat}
    (hx : ∀ u ∈ txs, u.id = x → u.state ≠ .active) :
    (removeTx txs x).filter (fun t => t.state = .active) =
      txs.filter (fun t => t.state = .active) := by
  induction txs with
  | nil => rfl
  | cons u rest ih =>
    have ih' := ih (fun w hw => hx w (List.mem_cons_of_mem _ hw))
    simp only [removeTx] at ih' ⊢
    by_cases hu : u.id = x
    · rw [List.filter_cons_of_neg (by simp [hu]), ih',
        List.filter_cons_of_neg (by simpa using hx u (by simp) hu)]
    · rw [List.filter_cons_of_pos (by simp [hu])]
      by_cases ha : u.state = .active
      · rw [List.filter_cons_of_pos (by simpa using ha), List.filter_cons_of_pos (by simpa using ha),
          ih']
      · rw [List.filter_cons_of_neg (by simpa using ha), List.filter_cons_of_neg (by simpa using ha),
          ih']

theorem readers_removeTx {s s' : St} {x : Nat} (hnext : s'.nextReader = s.nextReader)
    (htx : s'.txs = removeTx s.txs x) (hx : ∀ u ∈ s.txs, u.id = x → u.state ≠ .active) :
    s'.readers = s.readers := by
  unfold St.readers
  rw [hnext, htx, filter_active_removeTx hx]

theorem resolve_removeTx {txs : List Tx} {x : Nat} {st : Option Stamp} (hst : st ≠ some (.id x)) :
    resolve (removeTx txs x) st = resolve txs st := by
  match st with
  | none => rfl
  | some (.ts c) => rfl
  | some (.id y) =>
    have hyx : y ≠ x := fun hyx => hst (by rw [hyx])
    rw [resolve_id, resolve_id, findTx_removeTx, if_neg hyx]

end MvccGc.CommitProof

namespace MvccGc

theorem inv_finish {s : St} {x e : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .committed e) (hrw : t.rewritten = true) :
    Inv { s with
      txs := removeTx s.txs x
      fin := if (lookupWrote s.wrote x).isSome then (x, e) :: s.fin else s.fin
      lastCommitted := max s.lastCommitted e
      wrote := dropWrote s.wrote x } := by
  have htmem : t ∈ s.txs := (findTx_some ht).1
  have uniq : ∀ u ∈ s.txs, u.id = x → u = t := fun u hu hux => CommitProof.tx_unique h ht hu hux
  have hte : e ∈ txEnds t := by simp [txEnds, hst]
  have heb := h.endBounds t htmem e hte
  have hnox : ∀ v ∈ s.chain, Stamp.id x ∉ stamps v := by
    intro v hv hx
    obtain ⟨u, hu, hux, -, hcomm⟩ := h.idStamp v hv x hx
    rw [uniq u hu hux] at hcomm
    have := hcomm e hst
    rw [hrw] at this
    cases this
  have hnoxB : ∀ v ∈ s.chain, v.beginAt ≠ some (.id x) :=
    fun v hv hb => hnox v hv (mem_stamps.2 (Or.inl hb))
  have hnoxE : ∀ v ∈ s.chain, v.endAt ≠ some (.id x) :=
    fun v hv hb => hnox v hv (mem_stamps.2 (Or.inr hb))
  have hsame : ∀ v ∈ s.chain, SameResolve s.txs (removeTx s.txs x) v :=
    fun v hv => ⟨CommitProof.resolve_removeTx (hnoxB v hv),
      CommitProof.resolve_removeTx (hnoxE v hv)⟩
  have hwrote : ∀ y, y ≠ x → lookupWrote (dropWrote s.wrote x) y = lookupWrote s.wrote y := by
    intro y hy
    rw [lookupWrote_dropWrote, if_neg hy]
  have hrid : ∀ r ∈ s.readers, r.id ≠ x := by
    intro r hr hrx
    rcases mem_readers.1 hr with rfl | ⟨hr0, hra⟩
    · have := h.idLt t htmem
      have htid := (findTx_some ht).2
      simp only [St.nextReader] at hrx
      omega
    · rw [uniq r hr0 hrx, hst] at hra
      cases hra
  have hreaders : ({ s with
      txs := removeTx s.txs x
      fin := if (lookupWrote s.wrote x).isSome then (x, e) :: s.fin else s.fin
      lastCommitted := max s.lastCommitted e
      wrote := dropWrote s.wrote x } : St).readers = s.readers := by
    refine CommitProof.readers_removeTx rfl rfl ?_
    intro u hu hux
    rw [uniq u hu hux, hst]
    simp
  have hexp : ∀ r ∈ s.readers, ({ s with
      txs := removeTx s.txs x
      fin := if (lookupWrote s.wrote x).isSome then (x, e) :: s.fin else s.fin
      lastCommitted := max s.lastCommitted e
      wrote := dropWrote s.wrote x } : St).expected r = s.expected r := by
    intro r hr
    dsimp only [St.expected]
    rw [hwrote r.id (hrid r hr)]
  have hvis : ∀ r (fin' : Finalized), ∀ v ∈ s.chain,
      isVisible (removeTx s.txs x) fin' r v = isVisible s.txs s.fin r v :=
    fun r fin' v hv => isVisible_same (hsame v hv) (h.known hv)
  have hinv : ∀ r ∈ s.readers, ∀ (fin' : Finalized), ∀ v ∈ s.chain,
      isBtreeInvalidating (removeTx s.txs x) fin' r v = isBtreeInvalidating s.txs s.fin r v := by
    intro r hr fin' v hv
    apply isBtreeInvalidating_same (hsame v hv) (h.known hv) (h.readerOk hr hv)
    intro y hy hyr
    have hyx : y ≠ x := fun hyx => hnoxE v hv (by rw [hy, hyx])
    rw [CommitProof.resolve_removeTx (by simp [hyx])]
    exact h.readerOk hr hv y hy hyr
  have uniqueNew : ∀ r ∈ s.readers, ∀ (fin' : Finalized),
      (s.chain.filter (isVisible (removeTx s.txs x) fin' r)).length ≤ 1 := by
    intro r hr fin'
    rw [List.filter_congr (fun v hv => hvis r fin' v hv)]
    exact h.unique r hr
  exact {
    idsNodup := List.Nodup.sublist (List.Sublist.map Tx.id List.filter_sublist) h.idsNodup
    idLt := fun u hu => h.idLt u (mem_removeTx.1 hu).1
    beginGt := fun u hu => h.beginGt u (mem_removeTx.1 hu).1
    beginLt := fun u hu => h.beginLt u (mem_removeTx.1 hu).1
    readMarkEq := fun u hu => h.readMarkEq u (mem_removeTx.1 hu).1
    endBounds := fun u hu => h.endBounds u (mem_removeTx.1 hu).1
    tsNodup := List.Nodup.sublist
      (List.Sublist.append (List.Sublist.map Tx.beginTs List.filter_sublist)
        CommitProof.flatMap_filter_sublist) h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := fun u hu e' he' w hw =>
      h.histPrepared u (mem_removeTx.1 hu).1 e' he' w
        ((hwrote u.id (mem_removeTx.1 hu).2).symm.trans hw)
    histPreparedNone := fun u hu e' he' hw =>
      h.histPreparedNone u (mem_removeTx.1 hu).1 e' he'
        ((hwrote u.id (mem_removeTx.1 hu).2).symm.trans hw)
    histEpoch := fun p hp => by
      rcases h.histEpoch p hp with hle | ⟨u, hu, hpe⟩
      · exact Or.inl (Nat.le_trans hle (Nat.le_max_left _ _))
      · by_cases hux : u.id = x
        · left
          rw [uniq u hu hux] at hpe
          simp only [txEnds, hst, List.mem_singleton] at hpe
          rw [hpe]
          exact Nat.le_max_right _ _
        · exact Or.inr ⟨u, mem_removeTx.2 ⟨hu, hux⟩, hpe⟩
    lastLt := Nat.max_lt.2 ⟨h.lastLt, heb.2⟩
    ckptLe := Nat.le_trans h.ckptLe (Nat.le_max_left _ _)
    beginNotHist := fun u hu => h.beginNotHist u (mem_removeTx.1 hu).1
    tsStamp := h.tsStamp
    idStamp := fun v hv y hy => by
      obtain ⟨u, hu, huy, hterm, hcomm⟩ := h.idStamp v hv y hy
      have hyx : y ≠ x := fun hyx => hnox v hv (hyx ▸ hy)
      exact ⟨u, mem_removeTx.2 ⟨hu, by rw [huy]; exact hyx⟩, huy, hterm, hcomm⟩
    idStampWrote := fun v hv y hy => by
      have hyx : y ≠ x := fun hyx => hnox v hv (hyx ▸ hy)
      show (lookupWrote (dropWrote s.wrote x) y).isSome = true
      rw [hwrote y hyx]
      exact h.idStampWrote v hv y hy
    noMat := h.noMat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      have hp' := List.mem_filter.1 hp
      have hpx : p.1 ≠ x := by simpa using hp'.2
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp'.1
      exact ⟨u, mem_removeTx.2 ⟨hu, by rw [hup]; exact hpx⟩, hup⟩
    wroteNodup := List.Nodup.sublist (List.Sublist.map Prod.fst List.filter_sublist) h.wroteNodup
    walEq := h.walEq
    reads := fun r hr => by
      rw [hreaders] at hr
      refine Eq.trans ?_ ((h.reads r hr).trans (hexp r hr).symm)
      show readRow (removeTx s.txs x) _ r s.chain s.btree s.ckptMax = s.read r
      rw [readRow_eq h.noMat (uniqueNew r hr _), h.read_eq hr]
      exact rd_congr (fun v hv => hvis r _ v hv) (fun v hv => hinv r hr _ v hv)
    unique := fun r hr => by
      rw [hreaders] at hr
      exact uniqueNew r hr _
    evidence := fun r hr hb hc => by
      rw [hreaders] at hr
      have hc' : epochChangeBefore s r.beginTs = true ∨ wroteRow s r.id = true := by
        rcases hc with hc | hc
        · exact Or.inl hc
        · right
          unfold wroteRow at hc ⊢
          rw [← hwrote r.id (hrid r hr)]
          exact hc
      obtain ⟨v, hv, hres', hinv'⟩ := h.evidence r hr hb hc'
      exact ⟨v, hv, hres', (hinv r hr _ v hv).trans hinv'⟩
    noResident := h.noResident
    liveLast := fun l1 c l2 hsplit hl w hw => by
      have hs : s.chain = l1 ++ c :: l2 := hsplit
      have hc : c ∈ s.chain := by rw [hs]; simp
      have hw' : w ∈ s.chain := by rw [hs]; simp [hw]
      rcases h.liveLast l1 c l2 hs ((live_same (hsame c hc)).symm.trans hl) w hw with hg | hs'
      · exact Or.inl hg
      · exact Or.inr ((settled_same (hsame w hw')).trans hs')
    pendingOrder := fun y hy hya l1 n l2 hsplit hn => by
      have hs : s.chain = l1 ++ n :: l2 := hsplit
      rcases h.pendingOrder y (mem_removeTx.1 hy).1 hya l1 n l2 hs hn with hd | hord
      · exact Or.inl ((doomed_same (fun w hw => hsame w hw)).trans hd)
      · refine Or.inr (fun w hw => ?_)
        have hw' : w ∈ s.chain := by rw [hs]; simp [hw]
        rcases hord w hw with hg | ⟨hs', hb'⟩
        · exact Or.inl hg
        · exact Or.inr ⟨(settled_same (hsame w hw')).trans hs', hb'⟩
    deletesSee := fun v hv z y hz hy => by
      rcases h.deletesSee v hv z y hz hy with hzy | hat
      · exact Or.inl hzy
      · exact Or.inr ((congrArg RS.isAt
          (CommitProof.resolve_removeTx (fun heq => hnoxB v hv (hz.trans heq)))).trans hat)
    ownEnds := fun v hv y hy hna =>
      h.ownEnds v hv y hy ((congrArg RS.isAt
        (CommitProof.resolve_removeTx (fun heq => hnoxB v hv (hy.trans heq)))).symm.trans hna)
    deleterSaw := fun v hv u hu hve hua => by
      rcases h.deleterSaw v hv u (mem_removeTx.1 hu).1 hve hua with h1 | h1 | ⟨b, hb, hlt⟩
      · exact Or.inl h1
      · exact Or.inr (Or.inl h1)
      · exact Or.inr (Or.inr ⟨b, (hsame v hv).1.trans hb, hlt⟩)
    writerStamp := fun u hu hua hw => by
      have hu' := mem_removeTx.1 hu
      have hw' : wroteRow s u.id = true := by
        unfold wroteRow at hw ⊢
        rw [← hwrote u.id hu'.2]
        exact hw
      exact h.writerStamp u hu'.1 hua hw'
    endAfterBegin := fun v hv b c hb hc =>
      h.endAfterBegin v hv b c ((hsame v hv).1.symm.trans hb) ((hsame v hv).2.symm.trans hc)
    rewrittenCommitted := fun u hu hr => h.rewrittenCommitted u (mem_removeTx.1 hu).1 hr }

end MvccGc
