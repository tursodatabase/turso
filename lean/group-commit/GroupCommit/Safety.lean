import GroupCommit.Dispatch
import GroupCommit.Check

/-!
# Safety of the fixed protocol

Each theorem holds in every reachable state of `Variant.fixed`, for any number of
transactions. Each one follows from the invariant `Inv`, which `inv_reachable` proves.
-/

namespace GroupCommit
open Sys

variable {s : Sys}

/-- No log record belongs to a transaction that was rolled back. Recovery replays every
record in the log, so such a record would bring back the writes of a rolled-back
transaction. -/
theorem no_rolled_back_record (h : Reachable .fixed s) :
    ∀ r ∈ s.log, (s.tx r.tx).rolledBack = false := by
  intro r hr
  exact (((inv_reachable h).log r.tx).2.2.1 ⟨r, hr, rfl⟩).2

/-- A commit that reached `CommitEnd` has its record in the part of the log that an fsync
made durable. -/
theorem acked_is_durable (h : Reachable .fixed s) (c : Nat) (ha : (s.tx c).acked = true) :
    ∃ r ∈ s.log.take s.synced, r.tx = c :=
  ((inv_reachable h).log c).2.2.2.2.1 ha

/-- A transaction that became Committed has a record in the log. -/
theorem committed_is_logged (h : Reachable .fixed s) (c : Nat)
    (hc : (s.tx c).wasCommitted = true) : ∃ r ∈ s.log, r.tx = c :=
  ((inv_reachable h).log c).2.2.2.1 hc

/-- No transaction has two records in the log. -/
theorem no_duplicate_record (h : Reachable .fixed s) : (s.log.map (·.tx)).Nodup :=
  (inv_reachable h).mark.2.2.2.2

/-- At most one thread holds the commit lock. -/
theorem lock_is_exclusive (h : Reachable .fixed s) (c d : Nat)
    (hc : (s.tx c).holdsLock = true) (hd : (s.tx d).holdsLock = true) : c = d :=
  lock_unique (inv_reachable h) hc hd

/-- The commit lock is taken exactly when a thread holds it. -/
theorem lock_holder (h : Reachable .fixed s) (c : Nat) :
    (s.tx c).holdsLock = true ↔ s.lockHolder = some c :=
  ((inv_reachable h).groupTx c).1

/-- A committed transaction that left `txs` notified the transactions that depend on it. -/
theorem committed_notified (h : Reachable .fixed s) (c : Nat)
    (hr : (s.tx c).st = .removed) (hw : (s.tx c).wasCommitted = true) :
    (s.tx c).notified = true :=
  ((inv_reachable h).log c).2.2.2.2.2.1 hr hw

/-- Every retry hole has an owner: a transaction whose thread waits on the ticket of the
hole, or is in the cleanup of a dropped commit before `leave`, which removes the hole.
A hole without an owner stops `take_work` from giving a batch forever. -/
theorem holes_have_owners (h : Reachable .fixed s) :
    ∀ t ∈ s.g.retry, ∃ c, (s.tx c).ticket = some t :=
  (inv_reachable h).holes

/-- The owner of a retry hole is a waiter on the ticket, or a thread in the cleanup
before `leave`. -/
theorem hole_owner_steps {r : TxRec} {t : Nat} (h : r.ticket = some t) :
    r.pc.waitTicket = some t ∨ (r.pc.beforeLeave = true ∧ r.dropFrom.ticket = some t) := by
  simp only [TxRec.ticket] at h
  split at h
  · rename_i t' ht; left; rw [ht, h]
  · rename_i ht
    split at h
    · right; exact ⟨by assumption, h⟩
    · cases h

end GroupCommit
