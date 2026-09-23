# Group commit: Lean model and proof

This folder has a Lean 4 model of MVCC group commit and a proof of its safety.

The model contains these parts of the code:

- `CommitCoordinator` in `core/mvcc/database/group_commit.rs`.
- The group commit steps of `CommitStateMachine` in `core/mvcc/database/mod.rs`,
  from `BeginCommitLogicalLog` to `FinalizeCommit`.
- The cleanup of a commit that did not finish: `release_group_claim` and
  `cleanup_dropped_commit`. This cleanup runs when a commit statement is dropped,
  and when a commit step returns an error.

The model has two variants. `original` is the code before the fix. `fixed` is
the code after the fix.

## Results

### The fixed code

Lean proves that these properties are true in all reachable states of the `fixed`
variant, for all numbers of transactions. The reachable states include the cleanup
after a dropped statement and the cleanup after an I/O error. The theorems are in
`GroupCommit/Safety.lean`.

| Theorem | Property |
| --- | --- |
| `no_rolled_back_record` | No log record belongs to a rolled-back transaction. |
| `acked_is_durable` | A commit that got to `CommitEnd` has its record in the durable part of the log. |
| `committed_is_logged` | A committed transaction has a record in the log. |
| `no_duplicate_record` | No transaction has two records in the log. |
| `lock_is_exclusive`, `lock_holder` | Only one thread at a time holds the commit lock. |
| `committed_notified` | A committed transaction that left `txs` notified its dependents. |
| `holes_have_owners` | Each retry hole has an owner that can take the hole or remove it. |

A retry hole without an owner is a safety problem and a liveness problem. When
such a hole exists, `take_work` never gives a batch again.

The main theorem is `inv_reachable` in `GroupCommit/Dispatch.lean`. It shows that
the invariant `Inv` in `GroupCommit/Invariant.lean` is true in all reachable
states. The proofs do not use `sorry`. They use only the axioms `propext`,
`Classical.choice` and `Quot.sound`.

### The original code

`GroupCommit/Original.lean` has five counterexamples for the `original` variant.
Each counterexample is a trace of two transactions. The Lean kernel runs the model
on the trace, and then it examines the last state.

| Theorem | Problem |
| --- | --- |
| `original_logs_rolled_back_tx` | A waiter is dropped after the leader takes its record. The waiter rolls back. Then the leader writes the record of the rolled-back transaction. Recovery replays this record. |
| `original_leaves_hole_without_owner` | A waiter leaves the group before a dropped leader asks it to retry. The retry hole has no owner. |
| `original_skips_notify` | The leader completes the commit of an abandoned waiter, but it does not notify the dependents. |
| `original_io_error_logs_rolled_back_tx` | The leader gets an I/O error when it writes the record of a waiter. Its cleanup puts its own record back in the queue, and then it rolls back its transaction. The next leader writes this record. |
| `original_io_errors_leave_hole_without_owner` | After the problem in the row above, the next leader also gets an I/O error. It asks the rolled-back transaction to retry. The retry hole has no owner. |

In the traces of the last two theorems, no caller drops a statement. A commit
stops early only after an I/O error, for example when the disk is full.

For each property, the explorer shows only the shortest trace. Other traces can
break the same property.

## How to use

Install Lean with [elan](https://github.com/leanprover/elan). The file
`lean-toolchain` sets the Lean version. The model uses only the Lean core library.

To check all the proofs, do this command. It takes approximately one minute.

```sh
make check
```

To search all states of small systems, do this command.

```sh
make explore
```

The explorer (`Explore.lean`) finds all reachable states of a system with `n`
transactions. For each bad state, it shows the shortest trace. It accepts these
arguments:

- `original` or `fixed`: the variant of the model.
- `n=N`: the number of transactions.
- `toggle`: also change the `mvcc_group_commit` pragma between steps.
- `inv`: also examine the invariant in each state.
- `errorsonly`: a commit stops early only after an I/O error. The explorer does
  not drop a statement that did not fail.
- `max=M`: stop after `M` states.

The explorer is a bounded check. The proofs are true for all numbers of
transactions.

## The model

Each transaction has one committer thread. A thread does one atomic step at a time.
The steps of different threads occur in all possible orders. One atomic step is one
call on the coordinator, one change of the `txs` map, one lock operation, or one
log operation. The code between two such operations changes only local state, so
it is part of the step before it.

A commit statement can be dropped only when the Rust `step` function returns to
its caller. `Pc.dropPoint` gives these points. A step can also return an I/O
error (`Choice.fail`). Then `Program::abort` drops the commit, so the next step of
the thread is the drop, and the same cleanup runs.

The log contains records, not bytes. A record is in the log when the write offset
moves past it (`advance_logical_log_offset_after_success`). `synced` is the length
of the part of the log that an fsync made durable.

## Assumptions and limits

- A write that the code discards before the offset moves never becomes part of
  the log. The model does not include a discarded write that gets to the file
  after a later write. For example, io_uring can do writes in a different order.
- An fsync makes durable all records that are in the log when the fsync starts.
- The model does not include checkpoints. It does not include the parts of a
  commit that do not use the group or the log.
- The model has no I/O error in `UpgradeLogicalLogHeader`. After such an error,
  the cleanup is the same as after a drop at this step. The proofs include this
  drop.
- The model does not include the `parked` map of the coordinator. This map only
  wakes waiters, so it does not change the reachable states.
- The proofs are about safety. They do not prove that each commit completes.
- The proofs are about the model, not about the Rust code. A person must compare
  the model with the code.

## Files

| File | Contents |
| --- | --- |
| `GroupCommit/Coordinator.lean` | The calls on the coordinator. |
| `GroupCommit/Protocol.lean` | The steps of the committer threads. |
| `GroupCommit/Invariant.lean` | The invariant `Inv`. |
| `GroupCommit/Lemmas.lean`, `Frame.lean`, `Facts.lean` | General lemmas. |
| `GroupCommit/Steps/*.lean` | One lemma for each kind of step. |
| `GroupCommit/Dispatch.lean` | All steps keep the invariant. |
| `GroupCommit/Safety.lean` | The safety theorems for the fixed code. |
| `GroupCommit/Original.lean` | The counterexamples for the original code. |
| `GroupCommit/Check.lean`, `Explore.lean` | The explorer. |
