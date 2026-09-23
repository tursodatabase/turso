/-!
# The group commit coordinator

This file models `CommitCoordinator` in `core/mvcc/database/group_commit.rs`.
Each function here is one call on the coordinator. The Rust code runs each
call under the `group` mutex, so each call is one atomic step.

The `parked` map is not modeled. It only wakes waiters, so it does not change
which states are reachable.

`Variant.original` is the code before the fix. `Variant.fixed` is the code
after the fix. Some calls exist only in one variant. Their docstrings say
which variant uses them.
-/

namespace GroupCommit

inductive Variant where
  | original
  | fixed
deriving DecidableEq, Repr, Hashable

/-- `QueuedCommit` without the bytes of the log record. -/
structure Entry where
  ticket : Nat
  tx : Nat
deriving DecidableEq, Repr, Hashable

def setInsert (x : Nat) (l : List Nat) : List Nat :=
  if x ∈ l then l else x :: l

def setErase (x : Nat) (l : List Nat) : List Nat :=
  l.filter (· != x)

def eraseAll (xs : List Nat) (l : List Nat) : List Nat :=
  l.filter (fun y => !xs.contains y)

/-- `GroupState`. The fields `taken` and `withdrawn` exist only in the fixed code. -/
structure Group where
  nextTicket : Nat := 0
  durableThrough : Nat := 0
  writtenThrough : Nat := 0
  pending : List Entry := []
  retry : List Nat := []
  issued : Option Nat := none
  abandoned : List Nat := []
  taken : List Nat := []
  withdrawn : List Nat := []
deriving DecidableEq, Repr, Hashable

inductive Work where
  | lead (writing : Entry) (rest : List Entry)
  | syncPrefix
  | none
deriving DecidableEq, Repr

namespace Group

/-- `enqueue`. The new entry gets the next ticket. -/
def enqueue (g : Group) (tx : Nat) : Group :=
  { g with nextTicket := g.nextTicket + 1,
           pending := g.pending ++ [⟨g.nextTicket + 1, tx⟩] }

/-- `take_work`. The fixed code also marks the records of the batch as taken. -/
def takeWork (v : Variant) (g : Group) : Group × Work :=
  if !g.retry.isEmpty then
    (g, if g.writtenThrough > g.durableThrough then .syncPrefix else .none)
  else
    match g.pending with
    | e :: rest =>
      let taken := match v with
        | .original => g.taken
        | .fixed => g.taken ++ (e :: rest).map (·.tx)
      ({ g with pending := [], taken }, .lead e rest)
    | [] => (g, if g.writtenThrough > g.durableThrough then .syncPrefix else .none)

/-- `requeue`. The fixed code does not put back the records of withdrawn commits. -/
def requeue (v : Variant) (g : Group) (es : List Entry) : Group :=
  match v with
  | .original => { g with pending := es ++ g.pending }
  | .fixed =>
    let txs := es.map (·.tx)
    { g with pending := es.filter (fun e => !g.withdrawn.contains e.tx) ++ g.pending,
             taken := eraseAll txs g.taken,
             withdrawn := eraseAll txs g.withdrawn }

/-- `dense_prefix_cap`. -/
def densePrefixCap (ticket written : Nat) (retry : List Nat) : Nat :=
  let cap := min ticket written
  match (retry.filter (· ≤ cap)).min? with
  | some hole => hole - 1
  | none => cap

/-- `mark_durable`. -/
def markDurable (g : Group) (ticket : Nat) : Group :=
  let cap := densePrefixCap ticket g.writtenThrough g.retry
  { g with durableThrough := max g.durableThrough cap }

/-- `note_written`. -/
def noteWritten (g : Group) (ticket : Nat) : Group :=
  if g.retry.any (· ≤ ticket) then g
  else { g with writtenThrough := max g.writtenThrough ticket }

/-- Original code: `drop_pending`. -/
def dropPending (g : Group) (ticket : Nat) : Group :=
  { g with retry := setErase ticket g.retry,
           pending := g.pending.filter (·.ticket != ticket) }

def lowerTo (ticket mark : Nat) : Nat :=
  if mark ≥ ticket then ticket - 1 else mark

/-- `request_retry`. -/
def requestRetry (g : Group) (ticket : Nat) : Group :=
  { g with retry := setInsert ticket g.retry,
           writtenThrough := lowerTo ticket g.writtenThrough,
           durableThrough := lowerTo ticket g.durableThrough }

/-- `take_retry`. -/
def takeRetry (g : Group) (ticket : Nat) : Group × Bool :=
  ({ g with retry := setErase ticket g.retry }, g.retry.contains ticket)

/-- Original code: `abandon_if_issued`. -/
def abandonIfIssued (g : Group) (tx : Nat) : Group × Bool :=
  if g.issued = some tx then ({ g with abandoned := setInsert tx g.abandoned }, true)
  else (g, false)

/-- Original code: `take_abandoned`. -/
def takeAbandoned (g : Group) (tx : Nat) : Group × Bool :=
  ({ g with abandoned := setErase tx g.abandoned }, g.abandoned.contains tx)

/-- Fixed code: decide under the mutex whether the leader writes the record
of `tx`. A commit that left the group before this call is skipped. -/
def tryIssue (g : Group) (tx : Nat) : Group × Bool :=
  let g := { g with taken := setErase tx g.taken }
  if g.withdrawn.contains tx then ({ g with withdrawn := setErase tx g.withdrawn }, false)
  else ({ g with issued := some tx }, true)

/-- Fixed code: the write of `tx` is owned. Returns whether `tx` was abandoned. -/
def finishIssue (g : Group) (tx : Nat) : Group × Bool :=
  ({ g with issued := none, abandoned := setErase tx g.abandoned }, g.abandoned.contains tx)

/-- Fixed code: a dropped leader gives up the issued write of another
transaction. Returns whether that transaction was abandoned. If it was not,
its waiter must retry. -/
def releaseIssued (g : Group) (e : Entry) : Group × Bool :=
  let g := { g with issued := none }
  if g.abandoned.contains e.tx then ({ g with abandoned := setErase e.tx g.abandoned }, true)
  else (g.requestRetry e.ticket, false)

/-- Fixed code: a dropped commit leaves the group. Returns whether the commit
was abandoned to the leader that issued its write. -/
def leave (g : Group) (tx : Nat) (ticket : Option Nat) : Group × Bool :=
  if g.issued = some tx then ({ g with abandoned := setInsert tx g.abandoned }, true)
  else
    let retry := match ticket with
      | some t => setErase t g.retry
      | none => g.retry
    let pending := g.pending.filter (·.tx != tx)
    if g.taken.contains tx then
      ({ g with retry, pending, taken := setErase tx g.taken,
                withdrawn := setInsert tx g.withdrawn }, false)
    else
      ({ g with retry, pending }, false)

end Group

end GroupCommit
