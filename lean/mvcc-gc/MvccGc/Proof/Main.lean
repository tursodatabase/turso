import MvccGc.Proof.Steps.Begin
import MvccGc.Proof.Steps.Commit
import MvccGc.Proof.Steps.Abort
import MvccGc.Proof.Steps.Prepare
import MvccGc.Proof.Steps.Checkpoint
import MvccGc.Proof.Steps.Write

/-!
# Main theorems

`Reachable` is every state that the model can reach from an empty version
store over any row value in the B-tree, through any order of steps. The only
restriction is the WAL fact that the Rust engine shows after a Truncate
checkpoint: the backfill boundary is the new materialization position
(`Action.realWal`).
-/

namespace MvccGc

/-- The WAL behavior of the Rust engine at a Truncate checkpoint. -/
def Action.realWal : Action → Bool
  | .checkpoint _ backfillBehind => !backfillBehind
  | _ => true

inductive Reachable : St → Prop where
  | init (i : Option Nat) : Reachable (St.initial i)
  | step {s s' : St} (a : Action) : Reachable s → a.realWal = true → step s a = .ok s' →
      Reachable s'

theorem inv_step {s s' : St} {a : Action} (h : Inv s) (ha : a.realWal = true)
    (hs : step s a = .ok s') : Inv s' := by
  cases a with
  | begin =>
    simp only [step] at hs
    cases hs
    exact inv_begin h
  | write x v =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hact
        exact inv_write h hft hact hs
      · cases hs
    · cases hs
  | delete x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hact
        exact inv_delete h hft hact hs
      · cases hs
    · cases hs
  | prepare x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hact
        exact inv_prepare h hft hact hs
      · cases hs
    · cases hs
  | commit x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i e hst
        cases hs
        exact inv_commit h hft hst
      · cases hs
    · cases hs
  | rewrite x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i e hst
        split at hs
        · cases hs
        · rename_i hrw
          cases hs
          exact inv_rewrite h hft hst (by simpa using hrw)
      · cases hs
    · cases hs
  | finish x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i e hst
        split at hs
        · cases hs
        · rename_i hrw
          cases hs
          exact inv_finish h hft hst (by simpa using hrw)
      · cases hs
    · cases hs
  | abort x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hst
        cases hs
        exact inv_abort h hft hst
      · cases hs
    · cases hs
  | rollback x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hst
        cases hs
        exact inv_rollback h hft hst
      · cases hs
    · cases hs
  | remove x =>
    simp only [step] at hs
    split at hs
    · rename_i t hft
      split at hs
      · rename_i hst
        cases hs
        exact inv_remove h hft hst
      · cases hs
    · cases hs
  | gc lwm =>
    simp only [step] at hs
    split at hs
    · rename_i hal
      cases hs
      exact inv_gc h hal
    · cases hs
  | checkpoint pinned backfillBehind =>
    simp only [Action.realWal, Bool.not_eq_eq_eq_not, Bool.not_true] at ha
    subst ha
    simp only [step] at hs
    exact inv_checkpoint h hs

theorem inv_of_reachable {s : St} (h : Reachable s) : Inv s := by
  induction h with
  | init i => exact inv_initial i
  | step a _ ha hs ih => exact inv_step ih ha hs

/-- Snapshot isolation for reads: every active transaction reads the row as of
its begin timestamp, or its own last write to the row. This holds through any
interleaving of transactions, inline GC passes and Truncate checkpoints. -/
theorem snapshot_reads {s : St} (h : Reachable s) {t : Tx} (ht : t ∈ s.txs)
    (hact : t.state = .active) : s.read t = s.expected t :=
  (inv_of_reachable h).reads t (mem_readers.2 (Or.inr ⟨ht, hact⟩))

/-- A transaction that begins now reads the latest committed value. -/
theorem next_reader_reads {s : St} (h : Reachable s) :
    s.read s.nextReader = valueAt s.init s.hist s.clock := by
  have hi := inv_of_reachable h
  rw [hi.reads s.nextReader hi.nextReader_mem]
  simp only [St.expected, St.nextReader]
  rw [lookupWrote_none_of]
  intro p hp heq
  obtain ⟨u, hu, hup⟩ := hi.wroteKeys p hp
  have := hi.idLt u hu
  omega

/-- The B-tree holds the value of the row at the durable boundary `ckptMax`. -/
theorem btree_durable {s : St} (h : Reachable s) :
    s.btree = valueAt s.init s.hist (s.ckptMax + 1) :=
  (inv_of_reachable h).btreeOk

/-- An inline GC pass never changes what an active transaction reads. -/
theorem gc_keeps_reads {s : St} {lwm : Option Nat} (h : Reachable s)
    (ha : lwmAllowed lwm (computeLwm s.txs)) {t : Tx} (ht : t ∈ s.txs)
    (hact : t.state = .active) :
    readRow s.txs s.fin t
        (gcChain s.chain lwm s.ckptMax false (inlineMinReaderMark s.txs s.backfill) true)
        s.btree s.ckptMax = s.read t := by
  have hi := inv_of_reachable h
  have hg := inv_gc hi ha
  have hr := hg.reads t (mem_readers.2 (Or.inr ⟨ht, hact⟩))
  rw [hi.reads t (mem_readers.2 (Or.inr ⟨ht, hact⟩))]
  exact hr

/-- The model never takes a panic path of the engine. -/
theorem no_panic {s : St} (h : Reachable s) (a : Action) (msg : String) :
    step s a ≠ .panic msg := by
  have hi := inv_of_reachable h
  cases a with
  | begin => simp [step]
  | write x v =>
    simp only [step]
    split
    · rename_i t hft
      split
      · exact write_no_panic hi hft
      · simp
    · simp
  | delete x =>
    simp only [step]
    split
    · rename_i t hft
      split
      · exact delete_no_panic hi
      · simp
    · simp
  | prepare x =>
    simp only [step]
    split
    · split
      · simp only [stepPrepare]; split <;> simp
      · simp
    · simp
  | commit x => simp only [step]; split <;> (try split) <;> simp
  | rewrite x => simp only [step]; split <;> (try split) <;> (try split) <;> simp
  | finish x => simp only [step]; split <;> (try split) <;> (try split) <;> simp
  | abort x => simp only [step]; split <;> (try split) <;> simp
  | rollback x => simp only [step]; split <;> (try split) <;> simp
  | remove x => simp only [step]; split <;> (try split) <;> simp
  | gc lwm => simp only [step]; split <;> simp
  | checkpoint p f => simp only [step, stepCheckpoint]; split <;> simp

/-- The two properties that the search also checks, as Boolean checks. -/
theorem checks_hold {s : St} (h : Reachable s) : readsCorrect s = true ∧ btreeCorrect s = true := by
  have hi := inv_of_reachable h
  constructor
  · unfold readsCorrect
    rw [List.all_eq_true]
    intro t ht
    by_cases hact : t.state = .active
    · simp [hact, snapshot_reads h ht hact]
    · simp [hact]
  · unfold btreeCorrect
    simp [hi.btreeOk]

end MvccGc
