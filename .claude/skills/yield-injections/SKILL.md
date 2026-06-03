---
name: yield-injections
description: Use when working with Turso yield injection or synthetic failure injection machinery for safe resumable state-machine boundaries, including YieldInjector, FailureInjector, YieldPointMarker, inject_transition_yield!, inject_io_yield!, inject_transition_failure!, CommitYieldPoint, CheckpointYieldPoint, CursorYieldPoint, deterministic interleaving tests, abandoned statement/commit tests, and concurrent-simulator yield plans.
---
# Yield Injection Guide

Yield injection is test-only infrastructure for forcing cooperative-yield or failure boundaries in resumable state machines. The implementation currently lives under `core/mvcc/`, but the mechanism is not MVCC semantics.

## Key Files

- `core/mvcc/yield_points.rs`: injector traits and macros.
- `core/mvcc/yield_hooks.rs`: `YieldPointMarker`, `YieldContext`, `ProvidesYieldContext`.
- `core/connection.rs`: per-connection injector slots and `yield_instance_id_counter`.
- `core/mvcc/database/mod.rs`: commit points and commit cleanup.
- `core/mvcc/database/checkpoint_state_machine.rs`: checkpoint points.
- `core/mvcc/cursor.rs`: cursor points.
- `core/mvcc/database/tests.rs`: fixed test injectors and regression examples.
- `testing/concurrent-simulator/yield_injection.rs`: simulator injector.

All hooks are behind `cfg(any(test, injected_yields))`; `injected_yields` means `test_helper` or `simulator`.

## Core Model

`YieldPoint { ordinal, point_count }` identifies one hook in one point family. Families are `#[repr(u8)]` enums implementing `YieldPointMarker`; `ordinal` is source-order `self as u8`, and `point_count` comes from `EnumCount`.

Do not reorder existing yield-point variants. Simulator plans store raw ordinals, not names.

Why reordering matters: `ordinal` is `self as u8`, so source order is part of the deterministic schedule. If a seed planned "yield at ordinal 2", reordering can silently retarget that same seed from one conceptual hook to another. This makes CI seeds, bisects, and simulator coverage hard to reproduce. Appending a variant still changes `point_count` and can perturb future plans, but it preserves the meaning of existing ordinals.

Each yield-capable live object has:

- `instance_id`: distinguishes simultaneous state machines/cursors.
- `selection_key`: stable logical-operation key used by deterministic plans.
- active connection injectors: `yield_injector()` and `failure_injector()`.

## Hook Macros

- `inject_transition_yield!(self, Point)`: for `StateTransition` returning `TransitionResult<T>`.
- `inject_io_yield!(self, Point)`: for cursor/helper functions returning `IOResult<T>`.
- `inject_transition_failure!(self, Point)`: returns `Err(LimboError)` from `StateTransition`.

There is no `inject_io_failure!` today; failure injection only works for `TransitionResult` state machines.

Injected yields return `Completion::new_yield()`. It is already finished, but `is_explicit_yield()` is true, so VDBE must return `StepResult::IO` instead of continuing immediately.

## New Yield-Capable Type

For a new state machine/cursor family, add all of this behind `cfg(any(test, injected_yields))`:

```rust
yield_instance_id: u64,
```

Initialize it from the connection:

```rust
yield_instance_id: connection.next_yield_instance_id(),
```

Implement `ProvidesYieldContext`:

```rust
impl ProvidesYieldContext for MyStateMachine {
    fn yield_context(&self) -> YieldContext {
        YieldContext::new(
            self.connection.yield_injector(),
            self.connection.failure_injector(),
            self.yield_instance_id,
            my_yield_key(self.logical_operation_id),
        )
    }
}
```

Add a family-specific `*_yield_key(...) -> u64` helper. Mix stable logical identity, such as tx id/table id, with a family tag so simulator plans do not collide across families. Do not use wall-clock time, random values, allocation addresses, or incidental counters unrelated to the logical operation.

## Adding A Point

Before adding a hook, prove re-entry is safe. A synthetic yield returns `StepResult::IO`; the same statement/state machine may be stepped again immediately later. On re-entry, it must resume from an explicit state, not repeat non-idempotent work.

Rules:

- Mutate the state machine into the resumable state before yielding.
- Do not put a hook before a `push`, `insert`, counter increment, lock acquisition, or cleanup action unless repeating that action is harmless or explicitly guarded by state.
- If a lock/guard is held across the yield, test both resume and drop-at-yield paths.
- For abandonment tests, dropping the statement at the yield must restore invariants through Drop/abort cleanup.
- When adding a new yield point, always add it at the last position in the enum to preserve existing ordinals.
- Do not reorder variants in existing `YieldPointMarker` enums, unless absolutely required. Reordering changes the meaning of existing ordinals and can break reproducibility of CI seeds and bisects.

Checklist:

1. Append a variant to the appropriate `*YieldPoint` enum. Do not reorder.
2. Place the macro after the transition that makes resume safe, or before a lock acquisition when explicitly testing lock interleavings.
3. Avoid calling macros while borrowing `&mut self.state`; they need `self.yield_context()`.
4. Add a deterministic test with `FixedYieldInjector` or `FixedFailureInjector`.
5. If testing abandoned work, drop the statement at the yield and assert cleanup invariants.

For deeper re-entry/state-machine rules, also use `async-io-model`.

## Fixed Unit-Test Injectors

Use fixed injectors in targeted tests:

```rust
use crate::mvcc::yield_hooks::YieldPointMarker;

conn.set_yield_injector(Some(FixedYieldInjector::new([
    CommitYieldPoint::LogRecordPrepared.point(),
])));
```

`FixedYieldInjector` stores a `HashSet<YieldPoint>`, ignores `instance_id`/`selection_key`, and consumes each configured point once total. If two simultaneous instances hit the same point, the first one consumes it.

`FixedFailureInjector` behaves similarly but maps one point to one `LimboError`.

Clear injectors when reusing the same connection:

```rust
conn.set_yield_injector(None);
conn.set_failure_injector(None);
```

## Common Test Targets

- Commit `LogRecordPrepared`: leave commit in `Preparing`, interleave another writer/checkpoint, or drop the statement.
- Commit `BeforeCommittedTimestampWatermarkUpdate`: test out-of-order completion and monotonic watermarks.
- Commit `BeforeFinishCommittedTx`: test abandonment after committed state but before final cleanup.
- Commit `AfterRemoveTx` failure: verify tx maps, connection tx slots, locks, and exclusive tx atomics are not stranded.
- Checkpoint `BeforeAcquireLock`: interleave before checkpoint boundary sampling.
- Checkpoint `AfterDurableBoundaryAdvanced` failure: test retry/recovery after durable state advanced.
- Cursor `NextStart` or `SeekStart`: test cursor re-entry and dropped-statement cleanup, especially rowid allocator locks.

Abandoned commit tests rely on cleanup paths: `CommitStateMachine::drop` calls `cleanup_unfinished_commit`, and abort-side cleanup runs through `cleanup_abandoned_mvcc_commit`.

## Simulator Use

The concurrent simulator owns randomized deterministic injection. Do not install `FixedYieldInjector` from simulator code.

Runtime:

- Each fiber owns an `Arc<SimulatorYieldInjector>` for the current operation.
- Operation init replaces it with `SimulatorYieldInjector::new(fiber_yield_seed(seed, fiber_idx))`.
- Every `stmt.step()` goes through `step_stmt_with_injected_yield`, which installs the injector, steps once, then clears it via RAII.

Planning:

- Plans are keyed by `(instance_id, selection_key, point_count)`.
- `MAX_YIELDS = 20`.
- For each key, the simulator chooses a random count in `0..=MAX_YIELDS`; zero injected yields is valid.
- It fills that many slots with random ordinals in `0..point_count`.
- Matching slots are consumed once; duplicate ordinals let the same hook fire multiple times.

Simulator reproducibility:

- Same top-level seed and fiber count are required to reproduce a schedule.
- Reordering variants changes ordinal meaning.
- Adding variants changes `point_count`, which can perturb future schedules for the same seed.
- Changing hook placement or fiber assignment can make the same seed explore a different schedule.

## Gotchas

- Do not add test-only ad hoc `Completion::new_yield()` if a yield point can express the interleaving. Runtime lock-contention yields are fine when semantics require them.
- Do not place a hook before state is updated to the resumable state.
- `set_yield_injector(Some(...))` asserts the slot is empty; `set_yield_injector(None)` asserts it is installed.
- Do not rely on exact simulator yield counts outside `SimulatorYieldInjector` tests.
