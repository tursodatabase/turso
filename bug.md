# Bug: MVCC Concurrent Commit Deadlock (Yield Spin)

## Summary

When two MVCC (`BEGIN CONCURRENT`) transactions commit concurrently under
cooperative (round-robin) scheduling, the second commit enters an infinite
spin inside the VDBE interpreter, starving the first commit and deadlocking
both.

## Root Cause

The MVCC commit state machine serializes logical log writes using
`pager_commit_lock`. When a transaction cannot acquire this lock, it returns
`Completion::new_yield()` to signal "try again later":

```rust
// core/mvcc/database/mod.rs
CommitState::BeginCommitLogicalLog { .. } => {
    let locked = self.commit_coordinator.pager_commit_lock.write();
    if !locked {
        return Ok(TransitionResult::Io(IOCompletions::Single(
            Completion::new_yield(),  // inner: None → finished() == true
        )));
    }
    // ...
}
```

The VDBE main loop (`core/vdbe/mod.rs`) had an optimization: if IO is already
finished, skip yielding and continue the loop immediately:

```rust
Ok(InsnFunctionStepResult::IO(io)) => {
    io.set_waker(waker);
    let finished = io.finished();
    state.io_completions = Some(io);
    if !finished {
        return Ok(StepResult::IO);  // yields to caller
    }
    // finished → continues loop WITHOUT yielding
}
```

Since `Completion::new_yield()` has `inner: None`, `finished()` always returns
`true`. The VDBE continues its inner loop, re-executes `AutoCommit`, re-enters
the commit state machine, fails the lock again, gets another yield — infinite
spin inside a single `stmt.step()` call.

## The Deadlock

Under round-robin scheduling (e.g. the concurrent simulator):

1. **commit1.step()** → acquires `pager_commit_lock`, starts WAL write → returns `StepResult::IO`
2. **io.step()** → processes commit1's pending WAL IO
3. **commit2.step()** → tries lock, fails → yield spin → **never returns**

commit1 needs another `step()` call to finish and release the lock. But
commit2 has hijacked the thread. Neither commit ever finishes.

## Fix

In `core/vdbe/mod.rs`, always return `StepResult::IO` when an instruction
returns IO completions, regardless of `finished()`:

```rust
Ok(InsnFunctionStepResult::IO(io)) => {
    io.set_waker(waker);
    state.io_completions = Some(io);
    return Ok(StepResult::IO);  // always yield
}
```

The "continue if finished" optimization assumed finished IO means the
instruction can make forward progress. Yield completions break that
assumption — the lock holder needs a turn to release it first.

## Regression Test

```bash
cargo test -p turso_whopper test_concurrent_commit_no_yield_spin
```

`testing/concurrent-simulator/lib.rs::test_concurrent_commit_no_yield_spin` —
two MVCC connections with non-conflicting inserts, committed via round-robin
stepping. Without the fix the test hangs; with the fix both commits complete.

This bug can only be reproduced under cooperative scheduling. A simple
integration test cannot trigger it: sequential `execute()` serializes commits
(no contention), and multi-threaded tests don't deadlock because the lock
holder finishes on its own thread independently.
