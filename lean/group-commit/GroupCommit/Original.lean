import GroupCommit.Check

/-!
# Counterexamples for the original code

Each theorem replays a trace that the explorer found for `Variant.original`, and shows
that the last state breaks a safety property. The kernel runs the model to check each
trace. Only transactions `0` and `1` take steps, so the other transactions stay idle.
-/

namespace GroupCommit

/-- Reachable states of a system where only transactions `0, …, n - 1` take steps. -/
inductive ReachableN (v : Variant) (n : Nat) : Sys → Prop where
  | init : ReachableN v n init
  | step {s s' : Sys} {c : Nat} {ch : Choice} :
      ReachableN v n s → c < n → next v s c ch = some s' → ReachableN v n s'

theorem ReachableN.reachable {v : Variant} {n : Nat} {s : Sys} (h : ReachableN v n s) :
    Reachable v s := by
  induction h with
  | init => exact Reachable.init
  | step _ _ hs ih => exact Reachable.step ih (Step.thread _ _ _ _ hs)

/-- Run the thread steps of a list in order. -/
def run (v : Variant) : Sys → List (Nat × Choice) → Option Sys
  | s, [] => some s
  | s, (c, ch) :: rest => (next v s c ch).bind (run v · rest)

theorem run_reachableN {v : Variant} {n : Nat} :
    ∀ {s s' : Sys} {l : List (Nat × Choice)}, ReachableN v n s → (∀ p ∈ l, p.1 < n) →
      run v s l = some s' → ReachableN v n s'
  | s, s', [], hs, _, h => by simp [run] at h; subst h; exact hs
  | s, s', (c, ch) :: rest, hs, hl, h => by
    simp only [run] at h
    cases hn : next v s c ch with
    | none => simp [hn] at h
    | some s1 =>
      simp only [hn, Option.bind_some] at h
      exact run_reachableN (ReachableN.step hs (hl (c, ch) (by simp)) hn)
        (fun p hp => hl p (by simp [hp])) h

/-- Bug A: a waiter is dropped after the leader took its record. The leader checks that
the transaction is still in `txs`, and then writes the record of a rolled-back transaction. -/
def traceRolledBack : List (Nat × Choice) :=
  [(0, .main), (0, .main), (0, .drop), (0, .main), (0, .main), (1, .main), (1, .main),
   (1, .alt), (1, .main), (1, .main), (1, .main), (0, .main), (0, .main), (0, .main),
   (0, .main), (1, .main), (1, .main), (1, .main), (1, .main)]

/-- Bug D: a waiter leaves before the dropped leader asks it to retry. The retry hole
stays with no owner, and `take_work` never gives a batch again. -/
def traceHole : List (Nat × Choice) :=
  [(0, .main), (0, .main), (0, .drop), (0, .main), (0, .main), (1, .main), (1, .main),
   (1, .alt), (1, .main), (1, .main), (1, .main), (0, .main), (1, .main), (1, .main),
   (1, .main), (1, .drop), (1, .main), (1, .main), (1, .main), (1, .main)]

/-- Bug E: the leader finishes the commit of an abandoned waiter and does not notify
the transactions that depend on it. -/
def traceNotify : List (Nat × Choice) :=
  [(0, .main), (0, .main), (0, .drop), (0, .main), (0, .main), (1, .main), (1, .main),
   (1, .alt), (1, .main), (1, .main), (1, .main), (0, .main), (0, .main), (1, .main),
   (1, .main), (1, .main), (0, .main), (1, .main), (1, .main), (1, .main), (1, .main)]

/-- A trace of steps of transactions below `n` that ends in a state where `p` holds. -/
theorem reach_of_run {v : Variant} {n : Nat} {l : List (Nat × Choice)} {p : Sys → Bool}
    (hl : ∀ q ∈ l, q.1 < n) (h : (run v init l).map p = some true) :
    ∃ s, ReachableN v n s ∧ p s = true := by
  cases hr : run v init l with
  | none => simp [hr] at h
  | some s =>
    simp only [hr, Option.map_some, Option.some.injEq] at h
    exact ⟨s, run_reachableN ReachableN.init hl hr, h⟩

theorem original_logs_rolled_back_tx :
    ∃ s, ReachableN .original 2 s ∧ ∃ r ∈ s.log, (s.tx r.tx).rolledBack = true := by
  obtain ⟨s, hs, hp⟩ := reach_of_run (v := .original) (n := 2) (l := traceRolledBack)
    (p := fun s => s.log.any fun r => (s.tx r.tx).rolledBack) (by decide) (by decide)
  exact ⟨s, hs, by simpa using hp⟩

theorem original_leaves_hole_without_owner :
    ∃ s, ReachableN .original 2 s ∧ ∃ t ∈ s.g.retry, ∀ c < 2, ownsHole (s.tx c) t = false := by
  obtain ⟨s, hs, hp⟩ := reach_of_run (v := .original) (n := 2) (l := traceHole)
    (p := fun s => s.g.retry.any fun t => (List.range 2).all fun c => !ownsHole (s.tx c) t)
    (by decide) (by decide)
  refine ⟨s, hs, ?_⟩
  simp only [List.any_eq_true, List.all_eq_true, List.mem_range, Bool.not_eq_eq_eq_not,
    Bool.not_true] at hp
  exact hp

theorem original_skips_notify :
    ∃ s, ReachableN .original 2 s ∧ ∃ c, (s.tx c).st = .removed ∧
      (s.tx c).wasCommitted = true ∧ (s.tx c).notified = false := by
  obtain ⟨s, hs, hp⟩ := reach_of_run (v := .original) (n := 2) (l := traceNotify)
    (p := fun s => (s.tx 0).st == .removed && (s.tx 0).wasCommitted && !(s.tx 0).notified)
    (by decide) (by decide)
  refine ⟨s, hs, 0, ?_⟩
  simpa [and_assoc] using hp

end GroupCommit
