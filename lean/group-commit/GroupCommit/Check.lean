import GroupCommit.Protocol

/-!
# Executable safety checks

Boolean versions of the safety properties, for a system with transactions
`0, …, n - 1`. The explorer uses them to search for bad states.
-/

namespace GroupCommit

def txIds (n : Nat) : List Nat := List.range n

def inLog (s : Sys) (c : Nat) : Bool := s.log.any (·.tx == c)

def inSynced (s : Sys) (c : Nat) : Bool := (s.log.take s.synced).any (·.tx == c)

def holdsLock (r : TxRec) : Bool :=
  match r.pc with
  | .awaitLocked _ | .awaitWork _ => true
  | _ => r.held

def releasingPc : Pc → Bool
  | .dR1 | .dR2 | .dR2c | .dRbOtherA _ | .dRbOtherB _ | .dR2d | .dR2e | .dR3 | .dLeave => true
  | _ => false

def ownsHole (r : TxRec) (h : Nat) : Bool :=
  match r.pc with
  | .await t | .awaitLock t | .awaitLocked t | .awaitWork t
  | .syncPrefix t | .prefixSynced t => t == h
  | pc => releasingPc pc && r.dropFrom.ticket == some h

/-- No log record belongs to a transaction that was rolled back. -/
def noRolledBackRecord (s : Sys) : Bool :=
  s.log.all fun r => !(s.tx r.tx).rolledBack

/-- A commit that reached `CommitEnd` without a drop has a durable record. -/
def ackedIsDurable (n : Nat) (s : Sys) : Bool :=
  (txIds n).all fun c => !(s.tx c).acked || inSynced s c

/-- A transaction that became Committed has a record in the log. -/
def committedIsLogged (n : Nat) (s : Sys) : Bool :=
  (txIds n).all fun c => !(s.tx c).wasCommitted || inLog s c

/-- No transaction has two records in the log. -/
def noDuplicateRecord (s : Sys) : Bool :=
  let txs := s.log.map (·.tx)
  txs.all fun c => txs.count c == 1

/-- At most one thread holds the commit lock, and the lock is taken exactly
when one thread holds it. -/
def lockIsExclusive (n : Nat) (s : Sys) : Bool :=
  let holders := (txIds n).filter fun c => holdsLock (s.tx c)
  holders.length ≤ 1 && (s.locked == !holders.isEmpty) && holders.all (fun c => s.lockHolder == some c)

/-- A committed transaction that left `txs` notified its dependents. -/
def committedNotified (n : Nat) (s : Sys) : Bool :=
  (txIds n).all fun c =>
    let r := s.tx c
    !(r.st == .removed && r.wasCommitted) || r.notified

/-- Every retry hole has a waiter that can still take it or remove it. A
hole without one stops `take_work` from giving a batch forever. -/
def holesHaveOwners (n : Nat) (s : Sys) : Bool :=
  s.g.retry.all fun h => (txIds n).any fun c => ownsHole (s.tx c) h

def violations (n : Nat) (s : Sys) : List String :=
  (if noRolledBackRecord s then [] else ["a rolled-back transaction has a record in the log"]) ++
  (if ackedIsDurable n s then [] else ["an acknowledged commit is not durable"]) ++
  (if committedIsLogged n s then [] else ["a committed transaction has no record in the log"]) ++
  (if noDuplicateRecord s then [] else ["a transaction has two records in the log"]) ++
  (if lockIsExclusive n s then [] else ["the commit lock is not exclusive"]) ++
  (if committedNotified n s then [] else ["a committed transaction did not notify its dependents"]) ++
  (if holesHaveOwners n s then [] else ["a retry hole has no owner"])

end GroupCommit
