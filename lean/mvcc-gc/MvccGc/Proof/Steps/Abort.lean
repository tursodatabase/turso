import MvccGc.Proof.Gc
import MvccGc.Proof.TxLemmas

/-!
# Abort, rollback and remove keep the invariant

`abort` changes each stamp that names the transaction `x` from `pending x` to
`dead x`. A reader that is not `x` sees both stamps the same way, and `x` is
no longer a reader.

`rollback` removes every stamp that names `x`. A version that `x` began
becomes garbage, and a version that `x` ended has no end again. A reader sees
a dead stamp and no stamp the same way, so no reader sees a change.

`remove` deletes a terminated transaction. No stamp names a terminated
transaction, so every stamp resolves as before.
-/

set_option linter.deprecated false

namespace MvccGc.AbortProof

theorem reads_transfer {s s' : St} {g : Version → Version} (h : Inv s)
    (hchain : s'.chain = s.chain.map g) (hbtree : s'.btree = s.btree)
    (hmat : ∀ v ∈ s'.chain, v.mat = 0)
    (hval : ∀ v ∈ s.chain, (g v).val = v.val)
    (hres : ∀ v ∈ s.chain, (g v).resident = v.resident)
    (hr : ∀ r ∈ s'.readers, r ∈ s.readers ∧ s'.expected r = s.expected r ∧
      epochChangeBefore s' r.beginTs = epochChangeBefore s r.beginTs ∧
      wroteRow s' r.id = wroteRow s r.id ∧
      ∀ v ∈ s.chain, isVisible s'.txs s'.fin r (g v) = isVisible s.txs s.fin r v ∧
        isBtreeInvalidating s'.txs s'.fin r (g v) = isBtreeInvalidating s.txs s.fin r v) :
    (∀ t ∈ s'.readers, (s'.chain.filter (isVisible s'.txs s'.fin t)).length ≤ 1) ∧
    (∀ t ∈ s'.readers, s'.read t = s'.expected t) ∧
    (∀ t ∈ s'.readers, s'.btree.isSome = true →
      (epochChangeBefore s' t.beginTs = true ∨ wroteRow s' t.id = true) →
      ∃ v ∈ s'.chain, v.resident = true ∧ isBtreeInvalidating s'.txs s'.fin t v = true) := by
  have huniq :
      ∀ t ∈ s'.readers, (s'.chain.filter (isVisible s'.txs s'.fin t)).length ≤ 1 := by
    intro t ht
    obtain ⟨hmem, -, -, -, hv⟩ := hr t ht
    rw [hchain, filter_map_length (fun v hv' => (hv v hv').1)]
    exact h.unique t hmem
  refine ⟨huniq, ?_, ?_⟩
  · intro t ht
    obtain ⟨hmem, hexp, -, -, hv⟩ := hr t ht
    unfold St.read
    rw [readRow_eq hmat (huniq t ht), hexp, hchain, hbtree,
      rd_map (fun v hv' => (hv v hv').1) (fun v hv' => (hv v hv').2) hval, ← h.read_eq hmem]
    exact h.reads t hmem
  · intro t ht hb hc
    obtain ⟨hmem, -, hep, hwr, hv⟩ := hr t ht
    rw [hbtree] at hb
    rw [hep, hwr] at hc
    obtain ⟨v, hv', hresv, hinv⟩ := h.evidence t hmem hb hc
    refine ⟨g v, ?_, ?_, ?_⟩
    · rw [hchain]; exact List.mem_map_of_mem hv'
    · rw [hres v hv']; exact hresv
    · rw [(hv v hv').2]; exact hinv

theorem readers_of {s s' : St} {x : Nat} (hn : s'.nextReader = s.nextReader)
    (hnx : s.nextTx ≠ x)
    (htx : ∀ r ∈ s'.txs, r.state = .active → r ∈ s.txs ∧ r.id ≠ x) {r : Tx}
    (hr : r ∈ s'.readers) : r ∈ s.readers ∧ r.id ≠ x := by
  rcases mem_readers.1 hr with rfl | ⟨hm, ha⟩
  · rw [hn]
    exact ⟨by simp [St.readers], hnx⟩
  · obtain ⟨hm', hne⟩ := htx r hm ha
    exact ⟨mem_readers.2 (Or.inr ⟨hm', ha⟩), hne⟩

theorem mem_upd {txs : List Tx} {x : Nat} {t : Tx} {st : TxState}
    (hnd : (txs.map Tx.id).Nodup) (ht : findTx txs x = some t) {t' : Tx} :
    t' ∈ updateTx txs x (fun u => { u with state := st }) ↔
      t' = { t with state := st } ∨ (t' ∈ txs ∧ t'.id ≠ x) := by
  rw [mem_updateTx]
  obtain ⟨htm, htx⟩ := findTx_some ht
  constructor
  · rintro ⟨u, hu, rfl⟩
    by_cases hux : u.id = x
    · left
      have : u = t := by
        have h1 := findTx_of_mem hnd hu
        rw [hux, ht] at h1
        exact (Option.some.inj h1).symm
      subst this
      simp [hux]
    · right
      simp [hux, hu]
  · rintro (rfl | ⟨hm, hne⟩)
    · exact ⟨t, htm, by simp [htx]⟩
    · exact ⟨t', hm, by simp [hne]⟩

theorem resolve_upd {txs : List Tx} {x : Nat} {t : Tx} {st : TxState}
    (ht : findTx txs x = some t) (y : Nat) :
    resolve (updateTx txs x (fun u => { u with state := st })) (some (.id y)) =
      if y = x then stateClass x st else resolve txs (some (.id y)) := by
  rw [resolve_id, findTx_updateTx (f := fun u => { u with state := st }) (fun _ => rfl)]
  by_cases hy : y = x
  · subst hy
    rw [ht]
    simp [(findTx_some ht).2]
  · rw [if_neg hy, resolve_id]
    cases hf : findTx txs y with
    | none => rfl
    | some u =>
      have : u.id ≠ x := by rw [(findTx_some hf).2]; exact hy
      simp [this]

theorem isSome_findTx_upd {txs : List Tx} {x y : Nat} {st : TxState} :
    (findTx (updateTx txs x (fun u => { u with state := st })) y).isSome =
      (findTx txs y).isSome := by
  rw [findTx_updateTx (f := fun u => { u with state := st }) (fun _ => rfl), Option.isSome_map]

theorem map_upd {β : Type} {txs : List Tx} {x : Nat} {st : TxState} {g : Tx → β}
    (hg : ∀ u : Tx, g { u with state := st } = g u) :
    (updateTx txs x (fun u => { u with state := st })).map g = txs.map g := by
  simp only [updateTx, List.map_map]
  apply List.map_congr_left
  intro u _
  by_cases hu : u.id = x
  · simp only [Function.comp_apply, if_pos hu]
    exact hg u
  · simp only [Function.comp_apply, if_neg hu]

theorem flatMap_upd {txs : List Tx} {x : Nat} {t : Tx} {st : TxState}
    (hnd : (txs.map Tx.id).Nodup) (ht : findTx txs x = some t)
    (he : txEnds { t with state := st } = txEnds t) :
    (updateTx txs x (fun u => { u with state := st })).flatMap txEnds = txs.flatMap txEnds := by
  rw [List.flatMap_def, List.flatMap_def]
  congr 1
  simp only [updateTx, List.map_map]
  apply List.map_congr_left
  intro u hu
  by_cases hux : u.id = x
  · have : u = t := by
      have h1 := findTx_of_mem hnd hu
      rw [hux, ht] at h1
      exact (Option.some.inj h1).symm
    subst this
    simp only [Function.comp_apply, if_pos hux]
    exact he
  · simp only [Function.comp_apply, if_neg hux]

theorem flatMap_filter_eq {l : List Tx} {p : Tx → Bool} {g : Tx → List Nat}
    (h : ∀ a ∈ l, p a = false → g a = []) : (l.filter p).flatMap g = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a rest ih =>
    have ih' := ih (fun b hb => h b (by simp [hb]))
    by_cases hp : p a = true
    · rw [List.filter_cons_of_pos hp, List.flatMap_cons, List.flatMap_cons, ih']
    · rw [List.filter_cons_of_neg hp, List.flatMap_cons, h a (by simp) (by simpa using hp),
        List.nil_append, ih']

def SameExceptAbort (x : Nat) (a b : RS) : Prop := a = b ∨ (a = .dead x ∧ b = .pending x)

theorem SameExceptAbort.isAt {x : Nat} {a b : RS} (h : SameExceptAbort x a b) :
    a.isAt = b.isAt := by
  rcases h with rfl | ⟨rfl, rfl⟩ <;> rfl

theorem SameExceptAbort.time {x : Nat} {a b : RS} (h : SameExceptAbort x a b) {c : Nat} :
    a = .time c ↔ b = .time c := by
  rcases h with rfl | ⟨rfl, rfl⟩ <;> simp

theorem SameExceptAbort.after {x : Nat} {a b : RS} (h : SameExceptAbort x a b) {c : Nat} :
    a.after c = b.after c := by
  rcases h with rfl | ⟨rfl, rfl⟩ <;> rfl

theorem SameExceptAbort.views {x : Nat} {a b : RS} (h : SameExceptAbort x a b) {r : Tx}
    (hx : x ≠ r.id) (e : Bool) :
    visB r e a = visB r e b ∧ visE r a = visE r b ∧ invE r a = invE r b := by
  rcases h with rfl | ⟨rfl, rfl⟩
  · exact ⟨rfl, rfl, rfl⟩
  · have := views_pending_dead hx e
    exact ⟨this.1.symm, this.2.1.symm, this.2.2.symm⟩

theorem settled_sameExceptAbort {txs txs' : List Tx} {x : Nat} {v : Version}
    (hb : SameExceptAbort x (resolve txs' v.beginAt) (resolve txs v.beginAt))
    (he : SameExceptAbort x (resolve txs' v.endAt) (resolve txs v.endAt)) :
    settled txs' v = settled txs v := by
  unfold settled
  rcases hb with hb | ⟨hb1, hb2⟩
  · rw [hb, he.isAt]
  · rw [hb1, hb2]

theorem live_sameExceptAbort {txs txs' : List Tx} {x : Nat} {v : Version}
    (hb : SameExceptAbort x (resolve txs' v.beginAt) (resolve txs v.beginAt))
    (he : SameExceptAbort x (resolve txs' v.endAt) (resolve txs v.endAt)) :
    live txs' v = live txs v := by
  unfold live
  rw [hb.isAt, he.isAt]

theorem doomed_sameExceptAbort {txs txs' : List Tx} {x : Nat} {t : Tx} {vs : List Version}
    (hc : ∀ st, SameExceptAbort x (resolve txs' st) (resolve txs st)) :
    doomed txs' t vs = doomed txs t vs := by
  unfold doomed
  apply any_congr'
  intro w _
  rw [settled_sameExceptAbort (hc _) (hc _), (hc w.beginAt).after, (hc w.endAt).after]

theorem abort_sameExceptAbort {txs : List Tx} {x : Nat} {t : Tx} (ht : findTx txs x = some t)
    (hst : t.state = .active) (st : Option Stamp) :
    SameExceptAbort x (resolve (updateTx txs x (fun u => { u with state := .aborted })) st)
      (resolve txs st) := by
  match st with
  | none => exact Or.inl rfl
  | some (.ts c) => exact Or.inl rfl
  | some (.id y) =>
    rw [resolve_upd ht]
    by_cases hy : y = x
    · subst hy
      rw [if_pos rfl]
      refine Or.inr ⟨rfl, ?_⟩
      simp [resolve, ht, hst]
    · rw [if_neg hy]
      exact Or.inl rfl

theorem abort_views {s : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .active) {r : Tx} (hr : r ∈ s.readers) (hrx : r.id ≠ x) {v : Version}
    (hv : v ∈ s.chain) :
    isVisible (updateTx s.txs x (fun u => { u with state := .aborted })) s.fin r v =
        isVisible s.txs s.fin r v ∧
      isBtreeInvalidating (updateTx s.txs x (fun u => { u with state := .aborted })) s.fin r v =
        isBtreeInvalidating s.txs s.fin r v := by
  have hk : known s.txs v := h.known hv
  have hk' : known (updateTx s.txs x (fun u => { u with state := .aborted })) v := by
    intro y hy
    rw [isSome_findTx_upd]
    exact hk y hy
  have hcb := abort_sameExceptAbort ht hst v.beginAt
  have hce := abort_sameExceptAbort ht hst v.endAt
  have hx : x ≠ r.id := fun e => hrx e.symm
  have hro : readerOk s.txs r v := h.readerOk hr hv
  have hro' : readerOk (updateTx s.txs x (fun u => { u with state := .aborted })) r v := by
    intro y hy hyr
    rw [resolve_upd ht, if_neg (by rw [hyr]; exact hrx)]
    exact hro y hy hyr
  constructor
  · rw [isVisible_eq hk', isVisible_eq hk, (hcb.views hx v.endAt.isNone).1,
      (hce.views hx true).2.1]
  · rw [isBtreeInvalidating_eq hk' hro', isBtreeInvalidating_eq hk hro,
      (hcb.views hx v.endAt.isNone).1, (hce.views hx true).2.1, (hce.views hx true).2.2]

theorem rollback_cases (x : Nat) (v : Version) :
    (v.beginAt = some (.id x) ∧
        rollbackVersion x v = { v with beginAt := none, endAt := none, mat := 0 }) ∨
    (v.beginAt ≠ some (.id x) ∧ v.endAt = some (.id x) ∧
        rollbackVersion x v = { v with endAt := none, mat := 0 }) ∨
    (v.beginAt ≠ some (.id x) ∧ v.endAt ≠ some (.id x) ∧ rollbackVersion x v = v) := by
  unfold rollbackVersion
  by_cases hb : v.beginAt = some (.id x)
  · exact Or.inl ⟨hb, by rw [if_pos hb]⟩
  · by_cases he : v.endAt = some (.id x)
    · exact Or.inr (Or.inl ⟨hb, he, by rw [if_neg hb, if_pos he]⟩)
    · exact Or.inr (Or.inr ⟨hb, he, by rw [if_neg hb, if_neg he]⟩)

theorem rollback_val (x : Nat) (v : Version) : (rollbackVersion x v).val = v.val := by
  rcases rollback_cases x v with ⟨-, heq⟩ | ⟨-, -, heq⟩ | ⟨-, -, heq⟩ <;> rw [heq]

theorem rollback_resident (x : Nat) (v : Version) :
    (rollbackVersion x v).resident = v.resident := by
  rcases rollback_cases x v with ⟨-, heq⟩ | ⟨-, -, heq⟩ | ⟨-, -, heq⟩ <;> rw [heq]

theorem rollback_mat {x : Nat} {v : Version} (h : v.mat = 0) : (rollbackVersion x v).mat = 0 := by
  rcases rollback_cases x v with ⟨-, heq⟩ | ⟨-, -, heq⟩ | ⟨-, -, heq⟩ <;> rw [heq]
  exact h

theorem rollback_of_endAt {x : Nat} {v : Version} {st : Stamp}
    (h : (rollbackVersion x v).endAt = some st) : rollbackVersion x v = v := by
  rcases rollback_cases x v with ⟨-, heq⟩ | ⟨-, -, heq⟩ | ⟨-, -, heq⟩ <;>
    rw [heq] at h ⊢
  · simp at h
  · simp at h

theorem rollback_garbage {x : Nat} {v : Version} (h : isGarbage v = true) :
    rollbackVersion x v = v := by
  simp [isGarbage] at h
  simp [rollbackVersion, h.1, h.2]

theorem mem_stamps_rollback {x : Nat} {v : Version} {st : Stamp}
    (h : st ∈ stamps (rollbackVersion x v)) : st ∈ stamps v ∧ st ≠ .id x := by
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;>
    rw [heq, mem_stamps] at h
  · simp at h
  · simp at h
    refine ⟨mem_stamps.2 (Or.inl h), ?_⟩
    rintro rfl
    exact hb h
  · refine ⟨mem_stamps.2 h, ?_⟩
    rintro rfl
    rcases h with h | h
    · exact hb h
    · exact he h

theorem mem_stamps_rollback_of {x : Nat} {v : Version} {st : Stamp} (h : st ∈ stamps v)
    (hne : st ≠ .id x)
    (hown : v.beginAt = some (.id x) → v.endAt = none ∨ v.endAt = some (.id x)) :
    st ∈ stamps (rollbackVersion x v) := by
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;> rw [heq]
  · exfalso
    rcases mem_stamps.1 h with h1 | h1
    · rw [hb] at h1
      exact hne (Option.some.inj h1).symm
    · rcases hown hb with h2 | h2 <;> rw [h2] at h1
      · simp at h1
      · exact hne (Option.some.inj h1).symm
  · rcases mem_stamps.1 h with h1 | h1
    · exact mem_stamps.2 (Or.inl h1)
    · rw [he] at h1
      exact absurd (Option.some.inj h1).symm hne
  · exact h

theorem rollback_resolve {txs : List Tx} {x : Nat} {t : Tx} (ht : findTx txs x = some t)
    (hst : t.state = .aborted) (st : Option Stamp) :
    resolve (updateTx txs x (fun u => { u with state := .terminated })) st = resolve txs st := by
  match st with
  | none => rfl
  | some (.ts c) => rfl
  | some (.id y) =>
    rw [resolve_upd ht]
    by_cases hy : y = x
    · subst hy
      rw [if_pos rfl]
      simp [resolve, ht, hst, stateClass]
    · rw [if_neg hy]

theorem rollback_settled {txs : List Tx} {x : Nat} (hx : resolve txs (some (.id x)) = .dead x)
    (v : Version) : settled txs (rollbackVersion x v) = settled txs v := by
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;> rw [heq]
  · simp [settled, hb, hx, resolve_none, RS.isAt]
  · unfold settled
    simp only [he, hx, resolve_none]
    cases resolve txs v.beginAt <;> rfl

theorem rollback_live {txs : List Tx} {x : Nat} (hx : resolve txs (some (.id x)) = .dead x)
    (v : Version) : live txs (rollbackVersion x v) = live txs v := by
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;> rw [heq]
  · simp [live, hb, hx, resolve_none, RS.isAt]
  · simp [live, he, hx, resolve_none, RS.isAt]

theorem rollback_doomedTerm {txs : List Tx} {x : Nat}
    (hx : resolve txs (some (.id x)) = .dead x) (c : Nat) (v : Version) :
    (settled txs (rollbackVersion x v) &&
        ((resolve txs (rollbackVersion x v).beginAt).after c ||
          (resolve txs (rollbackVersion x v).endAt).after c)) =
      (settled txs v &&
        ((resolve txs v.beginAt).after c || (resolve txs v.endAt).after c)) := by
  rw [rollback_settled hx]
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;> rw [heq]
  · have : settled txs v = false := by simp [settled, hb, hx]
    rw [this]
    rfl
  · simp [he, hx, resolve_none, RS.after]

theorem rollback_belongsTo {x y : Nat} {v : Version}
    (h : belongsTo y (rollbackVersion x v) = true) : belongsTo y v = true := by
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;>
    rw [heq] at h
  · simp [belongsTo] at h
  · simp [belongsTo] at h
    simp [belongsTo, h]
  · exact h

theorem visB_endNone {r : Tx} {b : RS} (hb : ∀ y, b ≠ .pending y) (e1 e2 : Bool) :
    visB r e1 b = visB r e2 b := by
  cases b with
  | pending y => exact absurd rfl (hb y)
  | _ => rfl

theorem rollback_views {s : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .aborted) {r : Tx} (hr : r ∈ s.readers) {v : Version} (hv : v ∈ s.chain) :
    isVisible (updateTx s.txs x (fun u => { u with state := .terminated })) s.fin r
        (rollbackVersion x v) = isVisible s.txs s.fin r v ∧
      isBtreeInvalidating (updateTx s.txs x (fun u => { u with state := .terminated })) s.fin r
        (rollbackVersion x v) = isBtreeInvalidating s.txs s.fin r v := by
  have hk : known s.txs v := h.known hv
  have hres := rollback_resolve ht hst
  have hro : readerOk s.txs r v := h.readerOk hr hv
  have hx : resolve s.txs (some (.id x)) = .dead x := by simp [resolve, ht, hst]
  have hxa : (resolve s.txs (some (.id x))).isAt = false := by rw [hx]; rfl
  have hk' : known (updateTx s.txs x (fun u => { u with state := .terminated }))
      (rollbackVersion x v) := by
    intro y hy
    rw [isSome_findTx_upd]
    exact hk y (mem_stamps_rollback hy).1
  have hro' : readerOk (updateTx s.txs x (fun u => { u with state := .terminated })) r
      (rollbackVersion x v) := by
    intro y hy hyr
    have heq := rollback_of_endAt hy
    rw [heq] at hy
    rw [hres]
    exact hro y hy hyr
  rcases rollback_cases x v with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩
  · have hg : isAbortedGarbage (rollbackVersion x v) = true := by rw [heq]; rfl
    rw [garbage_not_visible hg, garbage_not_invalidating hg]
    constructor
    · rw [isVisible_eq hk, hb, hx]
      simp [visB]
    · rw [isBtreeInvalidating_eq hk hro, hb, hx]
      rcases h.ownEnds v hv x hb hxa with he | he <;> rw [he] <;>
        simp [visB, invE, hx, resolve_none]
  · rw [heq] at hk' hro' ⊢
    have hnp : ∀ y, resolve s.txs v.beginAt ≠ .pending y := by
      intro y hy
      cases hvb : v.beginAt with
      | none => rw [hvb] at hy; cases hy
      | some st =>
        cases st with
        | ts c => rw [hvb] at hy; cases hy
        | id z =>
          rcases h.deletesSee v hv z x hvb he with e | hz
          · exact hb (by rw [hvb, e])
          · rw [hvb] at hy
            rw [hy] at hz
            cases hz
    constructor
    · rw [isVisible_eq hk', isVisible_eq hk]
      simp only [hres, he, hx, resolve_none, Option.isNone_none, Option.isNone_some]
      rw [visB_endNone hnp true false]
      simp [visE]
    · rw [isBtreeInvalidating_eq hk' hro', isBtreeInvalidating_eq hk hro]
      simp only [hres, he, hx, resolve_none, Option.isNone_none, Option.isNone_some]
      rw [visB_endNone hnp true false]
      simp [visE, invE]
  · rw [heq] at hk' hro' ⊢
    have hs : SameResolve s.txs (updateTx s.txs x (fun u => { u with state := .terminated })) v :=
      ⟨hres _, hres _⟩
    exact ⟨isVisible_same hs hk, isBtreeInvalidating_same hs hk hro hro'⟩

theorem resolve_remove {txs : List Tx} {x : Nat} {st : Option Stamp}
    (h : st ≠ some (.id x)) : resolve (removeTx txs x) st = resolve txs st := by
  match st with
  | none => rfl
  | some (.ts c) => rfl
  | some (.id y) =>
    have hy : y ≠ x := fun e => h (by rw [e])
    rw [resolve_id, resolve_id, findTx_removeTx, if_neg hy]

end MvccGc.AbortProof

namespace MvccGc

open AbortProof in
theorem inv_abort {s : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .active) :
    Inv { s with txs := updateTx s.txs x (fun u => { u with state := .aborted }) } := by
  have htm := (findTx_some ht).1
  have htx := (findTx_some ht).2
  have hmem : ∀ {t'}, t' ∈ updateTx s.txs x (fun u => { u with state := .aborted }) ↔
      t' = { t with state := .aborted } ∨ (t' ∈ s.txs ∧ t'.id ≠ x) :=
    mem_upd h.idsNodup ht
  have hnew :
      { t with state := .aborted } ∈ updateTx s.txs x (fun u => { u with state := .aborted }) :=
    hmem.2 (Or.inl rfl)
  have hold : ∀ {u}, u ∈ s.txs → u.id ≠ x →
      u ∈ updateTx s.txs x (fun u => { u with state := .aborted }) :=
    fun hu hne => hmem.2 (Or.inr ⟨hu, hne⟩)
  have hut : ∀ {u}, u ∈ s.txs → u.id = x → u = t := by
    intro u hu hux
    have h1 := h.findTx_mem hu
    rw [hux, ht] at h1
    exact (Option.some.inj h1).symm
  have hends : txEnds t = [] := by simp [txEnds, hst]
  have hsameAbort := abort_sameExceptAbort ht hst
  have hnx : s.nextTx ≠ x := by have := h.idLt t htm; omega
  have hactive : ∀ r ∈ updateTx s.txs x (fun u => { u with state := .aborted }),
      r.state = .active → r ∈ s.txs ∧ r.id ≠ x := by
    intro r hr ha
    rcases hmem.1 hr with rfl | h'
    · simp at ha
    · exact h'
  obtain ⟨hunique, hreads, hevidence⟩ := reads_transfer (g := id) h
    (s' := { s with txs := updateTx s.txs x (fun u => { u with state := .aborted }) })
    (List.map_id _).symm rfl h.noMat (fun _ _ => rfl) (fun _ _ => rfl) (by
      intro r hr
      obtain ⟨hr', hrx⟩ := readers_of (s := s)
        (s' := { s with txs := updateTx s.txs x (fun u => { u with state := .aborted }) })
        rfl hnx hactive hr
      exact ⟨hr', rfl, rfl, rfl, fun v hv => abort_views h ht hst hr' hrx hv⟩)
  exact {
    idsNodup := by
      show ((updateTx s.txs x (fun u => { u with state := .aborted })).map Tx.id).Nodup
      rw [map_upd (fun _ => rfl)]
      exact h.idsNodup
    idLt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.idLt t htm
      · exact h.idLt t' ht'
    beginGt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginGt t htm
      · exact h.beginGt t' ht'
    beginLt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginLt t htm
      · exact h.beginLt t' ht'
    readMarkEq := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.readMarkEq t htm
      · exact h.readMarkEq t' ht'
    endBounds := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.endBounds t' ht'
    tsNodup := by
      show ((updateTx s.txs x (fun u => { u with state := .aborted })).map Tx.beginTs ++
        (updateTx s.txs x (fun u => { u with state := .aborted })).flatMap txEnds).Nodup
      rw [map_upd (fun _ => rfl), flatMap_upd h.idsNodup ht (by simp [txEnds, hst])]
      exact h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.histPrepared t' ht'
    histPreparedNone := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.histPreparedNone t' ht'
    histEpoch := fun e he => by
      rcases h.histEpoch e he with h1 | ⟨u, hu, hue⟩
      · exact Or.inl h1
      · by_cases hux : u.id = x
        · rw [hut hu hux, hends] at hue
          simp at hue
        · exact Or.inr ⟨u, hold hu hux, hue⟩
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginNotHist t htm
      · exact h.beginNotHist t' ht'
    tsStamp := h.tsStamp
    idStamp := fun v hv y hy => by
      obtain ⟨u, hu, huy, hut', hur⟩ := h.idStamp v hv y hy
      by_cases hux : u.id = x
      · have := hut hu hux
        subst this
        exact ⟨_, hnew, huy, by simp, by simp⟩
      · exact ⟨u, hold hu hux, huy, hut', hur⟩
    idStampWrote := h.idStampWrote
    noMat := h.noMat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
      by_cases hux : u.id = x
      · have := hut hu hux
        subst this
        exact ⟨_, hnew, hup⟩
      · exact ⟨u, hold hu hux, hup⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := hreads
    unique := hunique
    evidence := hevidence
    noResident := h.noResident
    liveLast := fun l1 c l2 hs hl w hw => by
      rw [live_sameExceptAbort (hsameAbort _) (hsameAbort _)] at hl
      rcases h.liveLast l1 c l2 hs hl w hw with hg | hs'
      · exact Or.inl hg
      · right
        rw [settled_sameExceptAbort (hsameAbort _) (hsameAbort _)]
        exact hs'
    pendingOrder := fun y hy ha l1 n l2 hs hn => by
      obtain ⟨hy', -⟩ := hactive y hy ha
      rw [doomed_sameExceptAbort hsameAbort]
      rcases h.pendingOrder y hy' ha l1 n l2 hs hn with hd | hord
      · exact Or.inl hd
      · right
        intro w hw
        rcases hord w hw with hg | ⟨hs', hb⟩
        · exact Or.inl hg
        · right
          rw [settled_sameExceptAbort (hsameAbort _) (hsameAbort _)]
          exact ⟨hs', hb⟩
    deletesSee := fun v hv z y hb he => by
      rcases h.deletesSee v hv z y hb he with h1 | h1
      · exact Or.inl h1
      · right
        rw [(hsameAbort _).isAt]
        exact h1
    ownEnds := fun v hv y hb hna => h.ownEnds v hv y hb (by rw [← (hsameAbort _).isAt]; exact hna)
    deleterSaw := fun v hv t' ht' he ha => by
      obtain ⟨hu, -⟩ := hactive t' ht' ha
      rcases h.deleterSaw v hv t' hu he ha with h1 | h1 | ⟨b, hb, hlt⟩
      · exact Or.inl h1
      · exact Or.inr (Or.inl h1)
      · exact Or.inr (Or.inr ⟨b, (hsameAbort _).time.2 hb, hlt⟩)
    writerStamp := fun t' ht' ha hw => by
      obtain ⟨hu, -⟩ := hactive t' ht' ha
      exact h.writerStamp t' hu ha hw
    endAfterBegin := fun v hv b c hb hc =>
      h.endAfterBegin v hv b c ((hsameAbort _).time.1 hb) ((hsameAbort _).time.1 hc)
    rewrittenCommitted := fun t' ht' hrw => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · obtain ⟨e, he⟩ := h.rewrittenCommitted t htm hrw
        rw [hst] at he
        cases he
      · exact h.rewrittenCommitted t' ht' hrw }

open AbortProof in
theorem inv_rollback {s : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .aborted) :
    Inv { s with
      chain := s.chain.map (rollbackVersion x)
      txs := updateTx s.txs x (fun u => { u with state := .terminated }) } := by
  have htm := (findTx_some ht).1
  have htx := (findTx_some ht).2
  have hmem : ∀ {t'}, t' ∈ updateTx s.txs x (fun u => { u with state := .terminated }) ↔
      t' = { t with state := .terminated } ∨ (t' ∈ s.txs ∧ t'.id ≠ x) :=
    mem_upd h.idsNodup ht
  have hnew : { t with state := .terminated } ∈
      updateTx s.txs x (fun u => { u with state := .terminated }) :=
    hmem.2 (Or.inl rfl)
  have hold : ∀ {u}, u ∈ s.txs → u.id ≠ x →
      u ∈ updateTx s.txs x (fun u => { u with state := .terminated }) :=
    fun hu hne => hmem.2 (Or.inr ⟨hu, hne⟩)
  have hut : ∀ {u}, u ∈ s.txs → u.id = x → u = t := by
    intro u hu hux
    have h1 := h.findTx_mem hu
    rw [hux, ht] at h1
    exact (Option.some.inj h1).symm
  have hends : txEnds t = [] := by simp [txEnds, hst]
  have hres := rollback_resolve ht hst
  have hsame : ∀ v, SameResolve s.txs
      (updateTx s.txs x (fun u => { u with state := .terminated })) v :=
    fun v => ⟨hres _, hres _⟩
  have hx : resolve s.txs (some (.id x)) = .dead x := by simp [resolve, ht, hst]
  have hxa : (resolve s.txs (some (.id x))).isAt = false := by rw [hx]; rfl
  have hnx : s.nextTx ≠ x := by have := h.idLt t htm; omega
  have hactive : ∀ r ∈ updateTx s.txs x (fun u => { u with state := .terminated }),
      r.state = .active → r ∈ s.txs ∧ r.id ≠ x := by
    intro r hr ha
    rcases hmem.1 hr with rfl | h'
    · simp at ha
    · exact h'
  have hmat : ∀ v ∈ s.chain.map (rollbackVersion x), v.mat = 0 := by
    intro v hv
    obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
    exact rollback_mat (h.noMat v0 hv0)
  obtain ⟨hunique, hreads, hevidence⟩ := reads_transfer (g := rollbackVersion x) h
    (s := s) (s' := { s with
      chain := s.chain.map (rollbackVersion x)
      txs := updateTx s.txs x (fun u => { u with state := .terminated }) })
    rfl rfl hmat (fun v _ => rollback_val x v) (fun v _ => rollback_resident x v) (by
      intro r hr
      obtain ⟨hr', -⟩ := readers_of (s := s) (s' := { s with
        chain := s.chain.map (rollbackVersion x)
        txs := updateTx s.txs x (fun u => { u with state := .terminated }) })
        rfl hnx hactive hr
      exact ⟨hr', rfl, rfl, rfl, fun v hv => rollback_views h ht hst hr' hv⟩)
  exact {
    idsNodup := by
      show ((updateTx s.txs x (fun u => { u with state := .terminated })).map Tx.id).Nodup
      rw [map_upd (fun _ => rfl)]
      exact h.idsNodup
    idLt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.idLt t htm
      · exact h.idLt t' ht'
    beginGt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginGt t htm
      · exact h.beginGt t' ht'
    beginLt := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginLt t htm
      · exact h.beginLt t' ht'
    readMarkEq := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.readMarkEq t htm
      · exact h.readMarkEq t' ht'
    endBounds := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.endBounds t' ht'
    tsNodup := by
      show ((updateTx s.txs x (fun u => { u with state := .terminated })).map Tx.beginTs ++
        (updateTx s.txs x (fun u => { u with state := .terminated })).flatMap txEnds).Nodup
      rw [map_upd (fun _ => rfl), flatMap_upd h.idsNodup ht (by simp [txEnds, hst])]
      exact h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.histPrepared t' ht'
    histPreparedNone := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · simp [txEnds]
      · exact h.histPreparedNone t' ht'
    histEpoch := fun e he => by
      rcases h.histEpoch e he with h1 | ⟨u, hu, hue⟩
      · exact Or.inl h1
      · by_cases hux : u.id = x
        · rw [hut hu hux, hends] at hue
          simp at hue
        · exact Or.inr ⟨u, hold hu hux, hue⟩
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := fun t' ht' => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · exact h.beginNotHist t htm
      · exact h.beginNotHist t' ht'
    tsStamp := fun v hv c hc => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      exact h.tsStamp v0 hv0 c (mem_stamps_rollback hc).1
    idStamp := fun v hv y hy => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      obtain ⟨hy0, hyx⟩ := mem_stamps_rollback hy
      obtain ⟨u, hu, huy, hut', hur⟩ := h.idStamp v0 hv0 y hy0
      have hux : u.id ≠ x := by
        rw [huy]
        intro e
        exact hyx (by rw [e])
      exact ⟨u, hold hu hux, huy, hut', hur⟩
    idStampWrote := fun v hv y hy => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      exact h.idStampWrote v0 hv0 y (mem_stamps_rollback hy).1
    noMat := hmat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp
      by_cases hux : u.id = x
      · have := hut hu hux
        subst this
        exact ⟨_, hnew, hup⟩
      · exact ⟨u, hold hu hux, hup⟩
    wroteNodup := h.wroteNodup
    walEq := h.walEq
    reads := hreads
    unique := hunique
    evidence := hevidence
    noResident := fun hb v hv => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      rw [rollback_resident]
      exact h.noResident hb v0 hv0
    liveLast := fun l1' c' l2' hs hl w' hw' => by
      obtain ⟨l1, c, l2, hs0, -, rfl, rfl⟩ := map_eq_append_cons hs
      obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hw'
      rw [live_same (hsame _), rollback_live hx] at hl
      right
      rw [settled_same (hsame _), rollback_settled hx]
      rcases h.liveLast l1 c l2 hs0 hl w hw with hg | hs'
      · exact garbage_not_settled hg
      · exact hs'
    pendingOrder := fun y hy ha l1' n' l2' hs hn => by
      obtain ⟨hy', hyx⟩ := hactive y hy ha
      obtain ⟨l1, n, l2, hs0, -, rfl, rfl⟩ := map_eq_append_cons hs
      have hn0 : livePendingOf y.id n = true := by
        rcases rollback_cases x n with ⟨hb, heq⟩ | ⟨hb, he, heq⟩ | ⟨hb, he, heq⟩ <;>
          rw [heq] at hn
        · simp [livePendingOf] at hn
        · exfalso
          simp [livePendingOf] at hn
          rcases h.deletesSee n (by rw [hs0]; simp) y.id x hn he with e | hz
          · exact hyx e
          · simp [resolve, h.findTx_mem hy', ha, RS.isAt] at hz
        · exact hn
      rcases h.pendingOrder y hy' ha l1 n l2 hs0 hn0 with hd | hord
      · left
        rw [doomed_same (fun w _ => hsame w)]
        unfold doomed at hd ⊢
        rw [List.any_map]
        exact (any_congr' (fun w _ => rollback_doomedTerm hx y.beginTs w)).trans hd
      · right
        intro w' hw'
        obtain ⟨w, hw, rfl⟩ := List.mem_map.1 hw'
        rcases hord w hw with hg | ⟨hs', hb⟩
        · left
          rw [rollback_garbage hg]
          exact hg
        · right
          refine ⟨by rw [settled_same (hsame _), rollback_settled hx]; exact hs', ?_⟩
          cases hbt : belongsTo y.id (rollbackVersion x w)
          · rfl
          · rw [rollback_belongsTo hbt] at hb
            exact hb
    deletesSee := fun v hv z y hb he => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      have heq := rollback_of_endAt he
      rw [heq] at hb he
      rw [hres]
      exact h.deletesSee v0 hv0 z y hb he
    ownEnds := fun v hv y hb hna => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      rcases rollback_cases x v0 with ⟨hb0, heq⟩ | ⟨hb0, he0, heq⟩ | ⟨hb0, he0, heq⟩ <;>
        rw [heq] at hb ⊢
      · simp at hb
      · exact Or.inl rfl
      · rw [hres] at hna
        exact h.ownEnds v0 hv0 y hb hna
    deleterSaw := fun v hv t' ht' he ha => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      obtain ⟨hu, -⟩ := hactive t' ht' ha
      have heq := rollback_of_endAt he
      rw [heq] at he ⊢
      rw [hres]
      exact h.deleterSaw v0 hv0 t' hu he ha
    writerStamp := fun t' ht' ha hw => by
      obtain ⟨hu, hux⟩ := hactive t' ht' ha
      obtain ⟨v, hv, hst'⟩ := h.writerStamp t' hu ha hw
      refine ⟨rollbackVersion x v, List.mem_map_of_mem hv, ?_⟩
      exact mem_stamps_rollback_of hst' (fun e => hux (Stamp.id.inj e))
        (fun hb => h.ownEnds v hv x hb hxa)
    endAfterBegin := fun v hv b c hb hc => by
      obtain ⟨v0, hv0, rfl⟩ := List.mem_map.1 hv
      rw [hres] at hb hc
      rcases rollback_cases x v0 with ⟨-, heq⟩ | ⟨-, -, heq⟩ | ⟨-, -, heq⟩ <;>
        rw [heq] at hb hc
      · simp [resolve] at hb
      · simp [resolve] at hc
      · exact h.endAfterBegin v0 hv0 b c hb hc
    rewrittenCommitted := fun t' ht' hrw => by
      rcases hmem.1 ht' with rfl | ⟨ht', -⟩
      · obtain ⟨e, he⟩ := h.rewrittenCommitted t htm hrw
        rw [hst] at he
        cases he
      · exact h.rewrittenCommitted t' ht' hrw }

open AbortProof in
theorem inv_remove {s : St} {x : Nat} {t : Tx} (h : Inv s) (ht : findTx s.txs x = some t)
    (hst : t.state = .terminated) :
    Inv { s with txs := removeTx s.txs x, wrote := dropWrote s.wrote x } := by
  have htm := (findTx_some ht).1
  have htx := (findTx_some ht).2
  have hut : ∀ {u}, u ∈ s.txs → u.id = x → u = t := by
    intro u hu hux
    have h1 := h.findTx_mem hu
    rw [hux, ht] at h1
    exact (Option.some.inj h1).symm
  have hends : txEnds t = [] := by simp [txEnds, hst]
  have hold : ∀ {u}, u ∈ s.txs → u.id ≠ x → u ∈ removeTx s.txs x :=
    fun hu hne => mem_removeTx.2 ⟨hu, hne⟩
  have hnoref : ∀ v ∈ s.chain, ∀ y, Stamp.id y ∈ stamps v → y ≠ x := by
    intro v hv y hy hyx
    obtain ⟨u, hu, huy, hut', -⟩ := h.idStamp v hv y hy
    have := hut hu (by rw [huy, hyx])
    subst this
    exact hut' hst
  have hne_begin : ∀ v ∈ s.chain, v.beginAt ≠ some (.id x) := by
    intro v hv hb
    exact hnoref v hv x (mem_stamps.2 (Or.inl hb)) rfl
  have hne_end : ∀ v ∈ s.chain, v.endAt ≠ some (.id x) := by
    intro v hv he
    exact hnoref v hv x (mem_stamps.2 (Or.inr he)) rfl
  have hsame : ∀ v ∈ s.chain, SameResolve s.txs (removeTx s.txs x) v :=
    fun v hv => ⟨resolve_remove (hne_begin v hv), resolve_remove (hne_end v hv)⟩
  have hresid : ∀ v ∈ s.chain, ∀ z, v.beginAt = some (.id z) ∨ v.endAt = some (.id z) →
      resolve (removeTx s.txs x) (some (.id z)) = resolve s.txs (some (.id z)) := by
    intro v hv z hz
    apply resolve_remove
    intro e
    exact hnoref v hv z (mem_stamps.2 hz) (Stamp.id.inj (Option.some.inj e))
  have hlw : ∀ y, y ≠ x → lookupWrote (dropWrote s.wrote x) y = lookupWrote s.wrote y := by
    intro y hy
    rw [lookupWrote_dropWrote, if_neg hy]
  have hnx : s.nextTx ≠ x := by have := h.idLt t htm; omega
  have hactive : ∀ r ∈ removeTx s.txs x, r.state = .active → r ∈ s.txs ∧ r.id ≠ x :=
    fun r hr _ => mem_removeTx.1 hr
  have hchainMem : ∀ {l1 c l2}, s.chain = l1 ++ c :: l2 →
      c ∈ s.chain ∧ ∀ w ∈ l2, w ∈ s.chain := by
    intro l1 c l2 hs
    rw [hs]
    exact ⟨by simp, fun w hw => by simp [hw]⟩
  obtain ⟨hunique, hreads, hevidence⟩ := reads_transfer (g := id) h
    (s := s) (s' := { s with txs := removeTx s.txs x, wrote := dropWrote s.wrote x })
    (List.map_id _).symm rfl h.noMat (fun _ _ => rfl) (fun _ _ => rfl) (by
      intro r hr
      obtain ⟨hr', hrx⟩ := readers_of (s := s)
        (s' := { s with txs := removeTx s.txs x, wrote := dropWrote s.wrote x })
        rfl hnx hactive hr
      refine ⟨hr', ?_, rfl, ?_, fun v hv => ?_⟩
      · show (match lookupWrote (dropWrote s.wrote x) r.id with
          | some w => w
          | none => valueAt s.init s.hist r.beginTs) = s.expected r
        rw [hlw r.id hrx]
        rfl
      · show (lookupWrote (dropWrote s.wrote x) r.id).isSome = (lookupWrote s.wrote r.id).isSome
        rw [hlw r.id hrx]
      · have hk := h.known hv
        have hro := h.readerOk hr' hv
        have hro' : readerOk (removeTx s.txs x) r v := by
          intro y hy hyr
          rw [hresid v hv y (Or.inr hy)]
          exact hro y hy hyr
        exact ⟨isVisible_same (hsame v hv) hk,
          isBtreeInvalidating_same (hsame v hv) hk hro hro'⟩)
  exact {
    idsNodup := by
      show ((s.txs.filter fun t => t.id ≠ x).map Tx.id).Nodup
      exact (List.filter_sublist.map Tx.id).nodup h.idsNodup
    idLt := fun t' ht' => h.idLt t' (mem_removeTx.1 ht').1
    beginGt := fun t' ht' => h.beginGt t' (mem_removeTx.1 ht').1
    beginLt := fun t' ht' => h.beginLt t' (mem_removeTx.1 ht').1
    readMarkEq := fun t' ht' => h.readMarkEq t' (mem_removeTx.1 ht').1
    endBounds := fun t' ht' => h.endBounds t' (mem_removeTx.1 ht').1
    tsNodup := by
      show ((s.txs.filter fun t => t.id ≠ x).map Tx.beginTs ++
        (s.txs.filter fun t => t.id ≠ x).flatMap txEnds).Nodup
      rw [flatMap_filter_eq (fun a ha hp => by
        have hax : a.id = x := by simpa using hp
        rw [hut ha hax, hends])]
      exact ((List.filter_sublist.map Tx.beginTs).append (List.Sublist.refl _)).nodup h.tsNodup
    histSorted := h.histSorted
    histLt := h.histLt
    histPrepared := fun t' ht' e he w hw => by
      obtain ⟨ht'', hne⟩ := mem_removeTx.1 ht'
      have hw' : lookupWrote s.wrote t'.id = some w := by
        rw [← hlw t'.id hne]
        exact hw
      exact h.histPrepared t' ht'' e he w hw'
    histPreparedNone := fun t' ht' e he hw => by
      obtain ⟨ht'', hne⟩ := mem_removeTx.1 ht'
      have hw' : lookupWrote s.wrote t'.id = none := by
        rw [← hlw t'.id hne]
        exact hw
      exact h.histPreparedNone t' ht'' e he hw'
    histEpoch := fun e he => by
      rcases h.histEpoch e he with h1 | ⟨u, hu, hue⟩
      · exact Or.inl h1
      · by_cases hux : u.id = x
        · rw [hut hu hux, hends] at hue
          simp at hue
        · exact Or.inr ⟨u, hold hu hux, hue⟩
    lastLt := h.lastLt
    ckptLe := h.ckptLe
    beginNotHist := fun t' ht' => h.beginNotHist t' (mem_removeTx.1 ht').1
    tsStamp := h.tsStamp
    idStamp := fun v hv y hy => by
      obtain ⟨u, hu, huy, hut', hur⟩ := h.idStamp v hv y hy
      have hux : u.id ≠ x := by rw [huy]; exact hnoref v hv y hy
      exact ⟨u, hold hu hux, huy, hut', hur⟩
    idStampWrote := fun v hv y hy => by
      show (lookupWrote (dropWrote s.wrote x) y).isSome = true
      rw [hlw y (hnoref v hv y hy)]
      exact h.idStampWrote v hv y hy
    noMat := h.noMat
    btreeOk := h.btreeOk
    wroteKeys := fun p hp => by
      have hp' := List.mem_filter.1 hp
      obtain ⟨u, hu, hup⟩ := h.wroteKeys p hp'.1
      have hux : u.id ≠ x := by
        rw [hup]
        simpa using hp'.2
      exact ⟨u, hold hu hux, hup⟩
    wroteNodup := by
      show ((s.wrote.filter fun p => p.1 ≠ x).map Prod.fst).Nodup
      exact (List.filter_sublist.map Prod.fst).nodup h.wroteNodup
    walEq := h.walEq
    reads := hreads
    unique := hunique
    evidence := hevidence
    noResident := h.noResident
    liveLast := fun l1 c l2 hs hl w hw => by
      obtain ⟨hc, hl2⟩ := hchainMem hs
      rw [live_same (hsame c hc)] at hl
      rcases h.liveLast l1 c l2 hs hl w hw with hg | hs'
      · exact Or.inl hg
      · right
        rw [settled_same (hsame w (hl2 w hw))]
        exact hs'
    pendingOrder := fun y hy ha l1 n l2 hs hn => by
      have hy' := (mem_removeTx.1 hy).1
      obtain ⟨-, hl2⟩ := hchainMem hs
      rw [doomed_same hsame]
      rcases h.pendingOrder y hy' ha l1 n l2 hs hn with hd | hord
      · exact Or.inl hd
      · right
        intro w hw
        rcases hord w hw with hg | ⟨hs', hb⟩
        · exact Or.inl hg
        · right
          rw [settled_same (hsame w (hl2 w hw))]
          exact ⟨hs', hb⟩
    deletesSee := fun v hv z y hb he => by
      rw [hresid v hv z (Or.inl hb)]
      exact h.deletesSee v hv z y hb he
    ownEnds := fun v hv y hb hna => by
      rw [hresid v hv y (Or.inl hb)] at hna
      exact h.ownEnds v hv y hb hna
    deleterSaw := fun v hv t' ht' he ha => by
      rw [(hsame v hv).1]
      exact h.deleterSaw v hv t' (mem_removeTx.1 ht').1 he ha
    writerStamp := fun t' ht' ha hw => by
      obtain ⟨ht'', hne⟩ := mem_removeTx.1 ht'
      have hw' : wroteRow s t'.id = true := by
        show (lookupWrote s.wrote t'.id).isSome = true
        rw [← hlw t'.id hne]
        exact hw
      exact h.writerStamp t' ht'' ha hw'
    endAfterBegin := fun v hv b c hb hc => by
      rw [(hsame v hv).1] at hb
      rw [(hsame v hv).2] at hc
      exact h.endAfterBegin v hv b c hb hc
    rewrittenCommitted := fun t' ht' hrw => h.rewrittenCommitted t' (mem_removeTx.1 ht').1 hrw }

end MvccGc
