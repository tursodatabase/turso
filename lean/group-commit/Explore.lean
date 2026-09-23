import GroupCommit.Check
import GroupCommit.Invariant
import Std.Data.HashMap

/-!
# Explicit-state explorer

Breadth-first search over all reachable states of a system with `n`
transactions. For each bad state, it prints the shortest trace that reaches
it. This is a bounded check that helps to find bugs. The proofs in
`GroupCommit/Safety.lean` cover every number of transactions.
-/

open GroupCommit

structure Key where
  g : Group
  lead : Option (Nat × Batch)
  lockHolder : Option Nat
  enabled : Bool
  log : List Rec
  synced : Nat
  txs : List TxRec
deriving BEq, Hashable, Repr

def keyOf (n : Nat) (s : Sys) : Key :=
  { g := s.g, lead := s.lead, lockHolder := s.lockHolder, enabled := s.enabled, log := s.log, synced := s.synced,
    txs := (txIds n).map s.tx }

def invViolations (n : Nat) (s : Sys) : List String :=
  let perTx (name : String) (p : Nat → Bool) : List String :=
    match (txIds n).find? (fun c => !p c) with
    | some c => [s!"{name} fails for tx {c}"]
    | none => []
  perTx "StatusInv" (fun c => decide (StatusInv s c)) ++
  perTx "LogInv" (fun c => decide (LogInv s c)) ++
  perTx "TicketInv" (fun c => decide (TicketInv s c)) ++
  perTx "GroupTxInv" (fun c => decide (GroupTxInv s c)) ++
  (if decide (LeadInv s) then [] else ["LeadInv fails"]) ++
  (if decide (QueueInv s) then [] else ["QueueInv fails"]) ++
  (if decide (IssueInv s) then [] else ["IssueInv fails"]) ++
  (if decide (MarkInv s) then [] else ["MarkInv fails"]) ++
  (if decide (OrderInv s) then [] else ["OrderInv fails"]) ++
  (if s.g.retry.all (fun h => (txIds n).any (fun c => (s.tx c).ticket == some h)) then []
   else ["HoleOwners fails"]) ++
  (if (txIds n).all (fun c => (txIds n).all (fun d =>
      c == d || (s.tx c).ticket.isNone || (s.tx c).ticket != (s.tx d).ticket)) then []
   else ["TicketsUnique fails"])

def label (c : Nat) (pc : Pc) (ch : Choice) : String :=
  s!"tx {c} {repr ch} at {repr pc}"

def successors (v : Variant) (n : Nat) (allowToggle : Bool) (s : Sys) :
    List (String × Sys) :=
  let threads := (txIds n).flatMap fun c =>
    [Choice.main, .alt, .fail, .drop].filterMap fun ch =>
      (next v s c ch).map fun s' => (label c (s.tx c).pc ch, s')
  if allowToggle then threads ++ [("toggle group commit", s.withEnabled (!s.enabled))]
  else threads

partial def trace (parents : Std.HashMap Key (Key × String)) (k : Key) (acc : List String) :
    List String :=
  match parents.get? k with
  | some (p, l) => trace parents p (l :: acc)
  | none => acc

def explore (v : Variant) (n : Nat) (allowToggle : Bool) (maxStates : Nat) (checkInv : Bool) :
    IO Unit := do
  let start := init
  let k0 := keyOf n start
  let mut parents : Std.HashMap Key (Key × String) := {}
  let mut seen : Std.HashSet Key := Std.HashSet.emptyWithCapacity 1024
  seen := seen.insert k0
  let mut frontier : Array Sys := #[start]
  let mut found : Std.HashSet String := {}
  let mut count := 1
  let mut depth := 0
  while !frontier.isEmpty && count < maxStates do
    let mut nextFrontier : Array Sys := #[]
    for s in frontier do
      let ks := keyOf n s
      for (l, s') in successors v n allowToggle s do
        let k := keyOf n s'
        if !seen.contains k then
          seen := seen.insert k
          parents := parents.insert k (ks, l)
          count := count + 1
          nextFrontier := nextFrontier.push s'
          let msgs := violations n s' ++ (if checkInv then invViolations n s' else [])
          for msg in msgs do
            if !found.contains msg then
              found := found.insert msg
              IO.println s!"VIOLATION: {msg} (depth {depth + 1})"
              for step in trace parents k [] do
                IO.println s!"  {step}"
              IO.println s!"  final log: {repr s'.log}, synced {s'.synced}"
              IO.println s!"  final group: {repr s'.g}"
    frontier := nextFrontier
    depth := depth + 1
  IO.println s!"variant {repr v}, {n} transactions, toggle {allowToggle}: {count} states, depth {depth}, frontier {frontier.size}"
  if found.isEmpty then IO.println "no violation found"

def main (args : List String) : IO Unit := do
  let v := if args.contains "fixed" then Variant.fixed else Variant.original
  let n := (args.find? (·.startsWith "n=")).bind (fun a => (a.drop 2).toNat?) |>.getD 2
  let toggle := args.contains "toggle"
  let maxStates := (args.find? (·.startsWith "max=")).bind (fun a => (a.drop 4).toNat?)
    |>.getD 5000000
  explore v n toggle maxStates (args.contains "inv")
