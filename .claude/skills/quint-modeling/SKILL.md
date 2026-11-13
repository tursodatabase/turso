---
name: quint-modeling
description: Use when you model, check or simplify a Turso concurrency design with Quint (the MVCC passive checkpoint specs in quint/mvcc-passive-checkpoint/), when you turn a model counterexample into a Rust reproducer, when you replay model traces against the engine, or when you decide whether a special case in the MVCC code is needed. Covers spec structure, Fix/guard constants, variant configurations, make targets, Quint pitfalls, model fidelity, and the simplification process.
---
# Modeling MVCC with Quint

The Quint specs in `quint/mvcc-passive-checkpoint/` state the design of the
MVCC passive checkpoint and check it with TLC. Read `README.md` there for the
tools and the make targets. Read `DESIGN.md` first for the design. Use this
skill to add to the specs, to find bugs with them, and to make the code
simpler without a loss of guarantees.

## Files in the spec folder

| File | Content |
|---|---|
| `Mvcc*.qnt` | One spec for each part: rows and GC, schema and root pages, sequences |
| `Mvcc*Configs.qnt` | One module for each configuration. `main` is the main configuration |
| `DESIGN*.md` | The design in prose, each rule mapped to code with `file:line` |
| `SIMPLIFY.md` | The plan to make the code simpler with the same invariants |
| `MODEL-GAPS.md` | Each code mechanism, its modes, and whether a spec models it |
| `SYNC.md` | What sync expects from table ids |
| `HARNESS.md` | The design of the trace replay test |

## How to model a part of the code

1. Extract the design before you write the spec. Name the shared state, the
   actors and the steps of each actor. A step is the code between two points
   where another actor can run: a yield point, a lock release or an I/O wait.
   Find those points in the state machines (`CommitState`, `CheckpointState`)
   and in `core/mvcc/yield_points.rs`.
2. Write one action for each step. Fewer faithful actions beat many guessed
   ones. If two code steps cannot interleave with other actors, merge them
   into one action and say why in a comment.
3. Write invariants as promises that a user can see: a read returns the value
   of its snapshot (`ReadsCorrect`), recovery gives the committed state
   (`RecoveryCorrect`), no B-tree is owned twice. Do not write invariants
   about internal flags.
4. Model the code as it is today in the main configuration. Then add one
   boolean constant for each special case in the code (`Guard*` or `Fix*`),
   so that a configuration can turn it off or turn a proposed fix on.
5. For each special case, add a variant module that turns it off. The result
   is one of three verdicts. Needed: a variant gives a counterexample. Not
   shown needed at these bounds: TLC finds no error, and you say what the
   model leaves out. Not modeled: you say why. The second verdict is not a
   reason to delete code. See "How to delete a guard".
6. Add reachability checks. A `Never*` value that must fail shows that a state
   is reachable, for example a busy retry or a root page reuse. Without them,
   an action that never fires makes every invariant pass for no reason.
7. For each GitHub issue that the model covers, add a variant `issueNNNN`
   with the code-today constants. It must break the invariant that matches
   the user-visible failure. Add the fix as a `Fix*` constant.

## Configuration comments

The Makefile reads two comment lines above each module in a configs file.

```quint
// Expect CompactionKeepsCurrent to fail: a setval that commits during the
// checkpoint loses its row.
// Invariants: CompactionKeepsCurrent
module setvalRace {
```

A variant that must fail lists only that one invariant, because Quint reports
a violation as `q_inv` and does not name the invariant. A variant that must
pass lists every invariant of the main configuration.

## Commands

Run these in `quint/mvcc-passive-checkpoint/`.

| Command | What it does |
|---|---|
| `make typecheck` | Type checks every spec |
| `make check` | Runs TLC on each `main` module. It must find no error |
| `make variants` | Runs TLC on each variant. Each must give the result in its comment |
| `make simulate` | Random walks of each `main` module with the Quint simulator |
| `make traces` | Writes random ITF traces of each `main` module to `traces/` |
| `make bug-traces` | Writes one failing ITF trace for each failing variant to `traces/bugs/` |

Use `ONLY="mod1 mod2"` to run some modules, and `SPECS=MvccPassiveSchema` to run
one spec. With 4 workers, `make check` took 216 s for rows, 215 s for schema
and 72 s for sequences.

## Quint pitfalls

- Set `JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=<folder under /home>`. TLC writes
  large state files, and `/tmp` can be a small tmpfs with a quota.
- Never pass `-cleanup` to TLC. It deletes the state folders of other runs.
- TLC visits each order of the keys and readers, so state counts grow
  fast. Keep constants small, for example `MaxTs = 4`.
- `quint verify --out-itf` with the TLC backend writes no file. Use
  `quint run --mbt --out-itf` (the simulator) to get ITF traces.
- Every action argument must come from a named `nondet`. Otherwise the trace
  does not record it in `mbt::nondetPicks`, and a replay cannot use it.
- `val` is a keyword. `to` and `keys` are built-in names. Lists start at 0.
  Quint has no `CHOOSE` (use a fold) and no `UNCHANGED` (assign every
  variable in every action).

## How to turn a counterexample into a Rust reproducer

1. Run `make bug-traces ONLY=<variant>` and print the actions with
   `mbt::actionTaken` and `mbt::nondetPicks` of each state.
2. Write a test in `core/mvcc/database/tests.rs`. Use
   `MvccTestDbNoConn::new_with_random_db_passive()`, one connection for each
   actor, and `PRAGMA mvcc_checkpoint_threshold = -1` on the setup connection.
3. To stop the checkpoint between two of its steps, set
   `PRAGMA mvcc_checkpoint_threshold = 0` on a second connection. Give that
   connection a `FixedYieldInjector` with a `CheckpointYieldPoint`, and step
   an INSERT on it until the injector is empty. See the `yield-injections`
   skill.
4. For a crash, call `std::mem::forget` on each connection and then
   `db.restart_result()`.
5. Assert the correct result, so the test fails on main. Run it three
   times. The model leaves out some engine conditions, so the test can need
   more setup than the trace. Example: the `setval` race needs `BEGIN
   CONCURRENT` and a sequence that already has a root page.

## How to make the code simpler

1. Change the spec first. Make the `main` module use the simpler rule. Run
   `make check`, `make variants` and `make bug-traces`. Update the variant
   comments and `DESIGN*.md`.
2. Then change the Rust code to match. Add a Rust test that fails before the
   change and passes after it.
3. To delete a guard, follow every step of "How to delete a guard".
4. Separate design from optimization. If a mechanism gives the same result
   with less work, model the simpler rule and keep the code as a refinement
   of it. `MODEL-GAPS.md` section 4 lists these pairs.

## How to delete a guard

A guard is a special case in the code that blocks or changes a step. A TLC
run with the guard off and no error only says "not shown needed at these
bounds". Replayed traces that pass without the guard prove even less. They
show that the code can follow the paths of the model. A guard protects
against paths that the code has and the model lacks, and a replay never
tests those paths. So a deletion needs an invariant that makes the guard
unable to change a decision, plus runs of the code without the guard.

Do these steps in order. Record the result of each step in the PR.

1. State the harm. Write down what goes wrong without the guard: a wrong
   read, a lost row, a leak, a stuck checkpoint or a crash. Read the commit,
   the issue and the regression test that added the guard. Make sure that an
   invariant of the spec detects that harm. A read invariant does not see a
   leak or a crash.
2. List every effect of the code that the guard skips, not only the effect
   in its comment. Example: `rootpage_gc_protected` skipped the GC of a
   chain, and it also skipped `collect_referenced_txids` for that chain.
3. List every caller of the guarded code in every mode. Match each caller to
   a spec action, and compare the step size of the action with the yield
   points of the code. A spec step that the code splits into several steps
   hides interleavings, so a guard can look removable when it is needed.
   Add each missing caller to `MODEL-GAPS.md`, or add it to the spec.
4. Show that the dangerous state is reachable. With the guard on, add a
   `Never*` check for "the guard fires here". With the guard off, add one for
   "the blocked step runs in that state". Both must fail. Without them, a pass
   can mean that the state never occurred.
5. Run a negative control at the same bounds. Turn off a guard that is known
   to be needed, for example an `issueNNNN` variant. It must fail. If it
   passes, the bounds are too small to judge the guard under test.
6. Run the guard-off variant at growing bounds. Raise one constant at a time:
   keys, readers, timestamps. Record the largest bounds that finished. If a
   run fails, the guard is needed. Write a Rust reproducer from the trace.
7. Write the reason as an inductive invariant. An inductive invariant holds
   in the start state, and every step from any state where it holds keeps it.
   So it holds in every reachable state, at any run length. Check it with
   `quint verify --inductive-invariant IndInv --invariant Safety` (Apalache,
   not TLC). Then check in one step from `IndInv` that the step with the
   guard and the step without it give the same result. If the check finds a
   counterexample to induction, add the missing fact to `IndInv`. If you
   cannot make the invariant pass, you do not yet know why the guard is
   redundant, so keep it.
8. Change the spec first. The main configuration uses the rule without the
   guard. Run `make check`, `make variants` and the trace replay. If the
   replay refuses a trace because the engine merges spec steps, add the
   merged step to the spec as one action and check it. Do not add a rule to
   the replay test that makes the trace pass.
9. Replace the guard with an assertion before you delete it. Assert in Rust
   that the guarded condition never occurs. Replay traces that reach the
   dangerous state from step 4, not only random traces. Run the MVCC tests
   and the concurrent simulator with
   `--enable-mvcc --enable-experimental-mvcc-passive-checkpoint`. Then delete
   the guard, or keep the assertion if it is cheap. Put the bounds, the
   invariant name and the gaps from step 3 in the commit message.

If time is short, steps 1 to 6 and step 9 are the minimum for a guard on a
path with low risk. For a guard in GC, recovery or error handling, also do
step 7.

The evidence for this process:

- MongoDB checked a simpler reconfiguration protocol with a written proof,
  TLC runs of 345 and 812 million states, and a refinement argument
  ([Schultz et al.](https://arxiv.org/abs/2102.11960)).
- Trace validation found differences between spec and code in every system
  that its authors tested. Many were differences in step size
  ([Cirstea et al.](https://arxiv.org/abs/2404.16075)).
- A CockroachDB crash came from a retry path that its spec left out
  ([Cockroach Labs](https://www.cockroachlabs.com/blog/demonic-nondeterminism/)).
- The Quint docs say that model-based testing does not prove the code
  correct ([Quint](https://quint.sh/docs/model-based-testing)).

## How to check that the model is correct

A model can be wrong in two ways: its invariants state the wrong promises, or
its actions do not match the code. Use these checks, strongest first.

1. Predictions that come true in Rust. A model bug that a Rust test shows on
   the engine is evidence that the model matches the code on that path.
2. Trace replay (`HARNESS.md`). A test runs each step of a model trace on a
   real database and compares the state after each step. A difference means
   that the model or the code is wrong. Find out which one before you change
   either.
3. Variants and reachability checks. They show that each invariant can fail
   and that each action can fire.
4. The `file:line` map in `DESIGN*.md`. It is a manual review, and it is the
   weakest check.

## Known modes

The code has three modes. Blocking: the passive flag off. Passive: the flag
on. A third mode: the flag on with an explicit `PRAGMA
wal_checkpoint(TRUNCATE)`. The specs model only passive mode. Read
`MODEL-GAPS.md` before you trust a result for the other two modes.
