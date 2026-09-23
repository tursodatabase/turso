import MvccGc
import Std.Data.HashMap

/-!
Breadth-first search over the model with small bounds. It stops at the first
state where a reader sees a wrong row, the B-tree is wrong, or the engine
would panic, and prints the steps that lead there.
-/

open MvccGc

structure Bounds where
  maxTx : Nat
  maxOps : Nat
  maxCkpts : Nat
  values : List Nat
  maxStates : Nat
  pinnedReaders : Bool
  backfillBehind : Bool
  checkInv : Bool

structure Node where
  st : St
  ops : Nat
  ckpts : Nat
  deriving BEq, Hashable

instance : Inhabited Node := ⟨{ st := St.initial none, ops := 0, ckpts := 0 }⟩

def lwmChoices (s : St) : List (Option Nat) :=
  let exact := computeLwm s.txs
  let stale := (List.range (s.clock + 1)).map some
  (exact :: stale).filter fun l => decide (lwmAllowed l exact)

def actions (b : Bounds) (n : Node) : List Action :=
  let s := n.st
  let perTx := s.txs.flatMap fun t =>
    let writes :=
      if n.ops < b.maxOps then (b.values.map fun v => Action.write t.id v) ++ [Action.delete t.id]
      else []
    writes ++ [.prepare t.id, .commit t.id, .rewrite t.id, .finish t.id, .abort t.id,
      .rollback t.id, .remove t.id]
  let begins := if s.nextTx ≤ b.maxTx then [Action.begin] else []
  let gcs := (lwmChoices s).map Action.gc
  let ckpts :=
    if n.ckpts < b.maxCkpts then
      [Action.checkpoint false false, .checkpoint true false, .checkpoint false true,
        .checkpoint true true].filter fun
        | .checkpoint p f => (b.pinnedReaders || !p) && (b.backfillBehind || !f)
        | _ => true
    else []
  begins ++ perTx ++ gcs ++ ckpts

def advance (n : Node) (a : Action) (s : St) : Node :=
  let ops := match a with
    | .write _ _ => n.ops + 1
    | .delete _ => n.ops + 1
    | _ => n.ops
  let ckpts := match a with
    | .checkpoint _ _ => n.ckpts + 1
    | _ => n.ckpts
  { st := s, ops, ckpts }

inductive Problem where
  | wrongRead
  | wrongBtree
  | panic (msg : String)
  | invariant (names : List String)

def Problem.describe : Problem → String
  | .wrongRead => "a reader sees a row that its snapshot does not have"
  | .wrongBtree => "the B-tree does not hold the value at the durable boundary"
  | .panic msg => s!"the engine panics: {msg}"
  | .invariant names => s!"candidate invariant fails: {names}"

def checkState (checkInv : Bool) (s : St) : Option Problem :=
  if !readsCorrect s then some .wrongRead
  else if !btreeCorrect s then some .wrongBtree
  else if checkInv then
    match failedChecks s with
    | [] => none
    | names => some (.invariant names)
  else none

partial def trace (parents : Std.HashMap Node (Option (Node × Action))) (n : Node)
    (acc : List (Action × St)) : List (Action × St) :=
  match parents.get? n with
  | some (some (p, a)) => trace parents p ((a, n.st) :: acc)
  | _ => acc

def showTx (s : St) (t : Tx) : String :=
  let reads := if t.state = .active then
      s!" reads={repr (s.read t)} expected={repr (s.expected t)}"
    else ""
  s!"  tx {t.id}: begin={t.beginTs} state={repr t.state} rewritten={t.rewritten}{reads}"

def showSt (s : St) : String :=
  let txs := String.intercalate "\n" (s.txs.map (showTx s))
  s!"  chain={repr s.chain}\n  btree={repr s.btree} ckptMax={s.ckptMax} clock={s.clock} " ++
  s!"walPos={s.walPos} backfill={s.backfill} fin={repr s.fin}\n" ++
  s!"  hist={repr s.hist} init={repr s.init}\n{txs}"

def report (parents : Std.HashMap Node (Option (Node × Action))) (n : Node) (last : Action)
    (p : Problem) (bad : Option St) : IO Unit := do
  IO.println s!"PROBLEM: {p.describe}"
  let steps := trace parents n []
  IO.println s!"initial init={repr n.st.init}"
  for (a, s) in steps do
    IO.println s!"step {repr a}"
    IO.println (showSt s)
  match bad with
  | some s =>
    IO.println s!"step {repr last}"
    IO.println (showSt s)
  | none => IO.println s!"step {repr last} (panics)"

partial def bfs (b : Bounds) (queue : Array Node) (next : Array Node)
    (parents : Std.HashMap Node (Option (Node × Action))) (count : Nat) : IO Bool := do
  if queue.isEmpty then
    if next.isEmpty then
      IO.println s!"no problem found; {count} states"
      return true
    else
      IO.println s!"... level done, {count} states, next level {next.size}"
      bfs b next #[] parents count
  else if count > b.maxStates then
    IO.println s!"state limit reached; {count} states, no problem found so far"
    return true
  else
    let n := queue.back!
    let queue := queue.pop
    let mut parents := parents
    let mut next := next
    let mut count := count
    for a in actions b n do
      match step n.st a with
      | .disabled => pure ()
      | .panic msg =>
        report parents n a (.panic msg) none
        return false
      | .ok s =>
        let child := advance n a s
        if !parents.contains child then
          parents := parents.insert child (some (n, a))
          count := count + 1
          match checkState b.checkInv s with
          | some p =>
            report parents n a p (some s)
            return false
          | none => next := next.push child
    bfs b queue next parents count

def runWith (b : Bounds) : IO Bool := do
  let roots := [St.initial none, St.initial (some 0)].map fun s => { st := s, ops := 0, ckpts := 0 : Node }
  let parents := roots.foldl (fun m r => m.insert r none) ({} : Std.HashMap Node (Option (Node × Action)))
  bfs b roots.toArray #[] parents roots.length

def main (args : List String) : IO UInt32 := do
  let nums := args.filterMap String.toNat?
  let get (i d : Nat) := nums.getD i d
  let b : Bounds := {
    maxTx := get 0 2, maxOps := get 1 2, maxCkpts := get 2 1, values := [1, 2],
    maxStates := get 3 2000000
    pinnedReaders := !args.contains "--no-pinned"
    backfillBehind := !args.contains "--no-backfill-behind"
    checkInv := args.contains "--check-invariant" }
  IO.println s!"bounds: maxTx={b.maxTx} maxOps={b.maxOps} maxCkpts={b.maxCkpts} maxStates={b.maxStates} pinned={b.pinnedReaders} backfillBehind={b.backfillBehind}"
  let ok ← runWith b
  return if ok then 0 else 1
