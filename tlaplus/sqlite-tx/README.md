# SQLite WAL Transaction Specification

TLA+ specification for SQLite WAL mode transaction concurrency control.

## Quick Start

```bash
make check
```

## The Specification

`SqliteTx.tla` models SQLite's WAL mode transaction protocol: single-writer, multiple-reader concurrency with snapshot isolation.

**State variables:**
- `txState[c]` - connection state: Idle, Reading, or Writing
- `writeLock` - which connection holds the write lock (or NoWriter)
- `dbVersion` - current committed database version
- `txSnapshot[c]` - version snapshot each transaction sees

**Transaction flow:**

```
Idle ──BeginRead──► Reading ──UpgradeToWrite──► Writing
  │                    │                           │
  │                    │◄────────CommitWrite───────┤
  │                    │◄───────RollbackWrite──────┤
  │                    │
  │◄───CommitRead──────┤
  │◄──RollbackRead─────┤
```

**Safety properties verified:**
- `SingleWriter` - at most one connection in Writing state
- `WriteLockConsistency` - Writing state ⟺ holding write lock
- `SnapshotValidity` - active transactions see valid snapshots
- `NoFutureReads` - no transaction sees uncommitted data

---

## Tutorial: Creating a Refinement Proof

A refinement proof verifies that your implementation correctly implements this abstract specification. Here's how to do it step by step.

### Step 1: Understand the Implementation

First, ask Claude to analyze your implementation and map it to the abstract spec:

> I want to create a TLA+ refinement proof for the abstract specification in tlaplus/sqlite-tx/SqliteTx.tla against the Turso implementation in core/storage/wal.rs and core/storage/pager.rs.
>
> Read the abstract spec and the implementation code. Then:
> 1. Identify the implementation state variables that correspond to abstract state
> 2. Identify the implementation actions that correspond to abstract actions
> 3. Create a refinement mapping

Claude will read both files and produce something like:

```tla
\* Refinement mapping from implementation to abstract spec:
\*
\* txState[c] = "Writing"  ↔  writeLockOwner = c
\* txState[c] = "Reading"  ↔  connReadSlot[c] ≠ 0 ∧ writeLockOwner ≠ c
\* txState[c] = "Idle"     ↔  connReadSlot[c] = 0
\* writeLock               ↔  writeLockOwner
\* dbVersion               ↔  maxFrame
\* txSnapshot[c]           ↔  connSnapshot[c]
```

### Step 2: Create the Implementation Spec

Claude will create `YourImpl.tla` modeling your implementation's state and actions:

```tla
---- MODULE YourImpl ----
EXTENDS Naturals, FiniteSets

CONSTANTS Connections, MaxVersion, NoOwner

VARIABLES
    writeLockOwner,    \* Who holds write lock
    connReadSlot,      \* Per-connection read lock slot (0 = none)
    connSnapshot,      \* Per-connection snapshot
    maxFrame           \* Current WAL frame (version)

\* Actions matching your implementation
BeginReadTx(c) == ...
BeginWriteTx(c) == ...   \* Note: requires read lock first!
CommitWriteTx(c) == ...
...

\* Instantiate abstract spec with refinement mapping
Abs == INSTANCE SqliteTx WITH
    NoWriter <- NoOwner,
    txState <- [c \in Connections |->
        IF writeLockOwner = c THEN "Writing"
        ELSE IF connReadSlot[c] # 0 THEN "Reading"
        ELSE "Idle"],
    writeLock <- writeLockOwner,
    dbVersion <- maxFrame,
    txSnapshot <- connSnapshot

\* Refinement theorem
AbsSpec == Abs!Spec
====
```

### Step 3: Run the Model Checker

```bash
java -XX:+UseParallelGC -jar tla2tools.jar -config YourImpl.cfg YourImpl.tla
```

### Step 4: When Refinement Fails

TLC will likely find violations on the first run. When it does, paste the error trace and ask:

> The refinement proof failed with this trace:
> [paste TLC output showing the violating behavior]
>
> Analyze why the implementation action doesn't refine any abstract action.
> Is this:
> 1. A bug in the implementation?
> 2. A missing action in the abstract spec?
> 3. A wrong refinement mapping?

For example, we discovered that the original abstract spec had `BeginWrite: Idle → Writing`, but the actual implementation requires `BeginReadTx` first, then `BeginWriteTx`. This wasn't a bug—the abstract spec was wrong! We fixed it by adding `UpgradeToWrite: Reading → Writing`.

### Step 5: Validate Against SQLite Source

When you're unsure whether the abstract spec or implementation is wrong, check SQLite's source:

> Check whether the abstract spec correctly models SQLite's behavior by examining the SQLite source at https://github.com/sqlite/sqlite/blob/master/src/wal.c and https://github.com/sqlite/sqlite/blob/master/src/pager.c.
>
> Specifically verify:
> 1. Does SQLite require a read lock before acquiring write lock?
> 2. What state does SQLite transition to after CommitWrite?
> 3. What locks are held/released during each operation?

We found that SQLite's `sqlite3WalBeginWriteTransaction` has:
```c
/* Cannot start a write transaction without first holding a read transaction. */
assert( pWal->readLock>=0 );
```

This confirmed the implementation was correct and the abstract spec needed fixing.

### Step 6: Iterate Until It Passes

Keep fixing issues until TLC reports success:

```
Model checking completed. No error has been found.
31015 states generated, 8734 distinct states found, 0 states left on queue.
```

---

## Limitations: What Refinement Proofs Can Miss

Refinement proofs verify **protocol correctness**, not implementation correctness. Here's a real bug they missed:

### The frame_cache Race Condition

Commit `46134f1` fixed a bug where `rollback_tx()` did:

```rust
fn rollback_tx() {
    wal.end_write_tx();     // Step 1: release write lock
    self.rollback();        // Step 2: cleanup frame_cache  ← BUG!
}
```

**Race condition:**
1. Thread 0 releases write_lock
2. Thread 1 acquires write_lock, adds frames to frame_cache
3. Thread 0 calls rollback(), deletes Thread 1's frames
4. Thread 1's commit succeeds but frames are gone → corruption

**Why the spec missed it:**

1. **Atomic action abstraction** - `RollbackWriteTx` was modeled as one atomic step, hiding the interleaving
2. **Missing state** - `frame_cache` wasn't modeled at all
3. **Missing granularity** - The spec didn't model the sub-steps within rollback

To catch this class of bugs, you'd need to model:

```tla
\* Fine-grained sub-steps
ReleaseWriteLock(c) == ...     \* Sub-step 1
CleanupFrameCache(c) == ...    \* Sub-step 2

AddFramesToCache(c) == ...     \* Commit sub-step 1
UpdateMaxFrame(c) == ...       \* Commit sub-step 2
```

Then TLC would find the bad interleaving.

### When to Add Granularity

Ask Claude:

> The abstract spec models rollback as atomic, but the implementation does multiple steps:
> 1. Release write lock
> 2. Clean up frame_cache based on max_frame
>
> Should we add finer-grained actions to catch interleaving bugs?
> What bugs might we miss with the current granularity?

---

## Files

| File | Description |
|------|-------------|
| `SqliteTx.tla` | Abstract transaction specification |
| `SqliteTx.cfg` | TLC model configuration |
| `tla2tools.jar` | TLA+ model checker |

## References

- [SQLite WAL Mode](https://www.sqlite.org/wal.html)
- [TLA+ Documentation](https://lamport.azurewebsites.net/tla/tla.html)
- SQLite source: [wal.c](https://github.com/sqlite/sqlite/blob/master/src/wal.c), [pager.c](https://github.com/sqlite/sqlite/blob/master/src/pager.c)
