---
name: yield-injections
description: Use when working with Turso yield injection or synthetic failure injection machinery for safe resumable state-machine boundaries, including YieldInjector, FailureInjector, YieldPointMarker, inject_transition_yield!, inject_io_yield!, inject_transition_failure!, CommitYieldPoint, CheckpointYieldPoint, CursorYieldPoint, deterministic interleaving tests, abandoned statement/commit tests, and concurrent-simulator yield plans.
---
# Yield injection

Test-only code that forces a yield or an error at chosen points in resumable state machines. It lives under `core/mvcc/` but is not MVCC semantics. All hooks are behind `cfg(any(test, injected_yields))`; `injected_yields` is set by the `test_helper` and `simulator` features.

To add a point, read the injector traits and macros, the `YieldPointMarker`, `YieldContext` and `ProvidesYieldContext` definitions, the existing point enums and hooks, the fixed injectors in the MVCC tests, and the injector in `testing/concurrent-simulator/`.

## Model

A `YieldPoint { ordinal, point_count }` names one hook in one family. A family is a `#[repr(u8)]` enum implementing `YieldPointMarker`; `ordinal` is `self as u8` and `point_count` comes from `EnumCount`.

Seeds and simulator plans store raw ordinals. Reordering variants changes which hook an old seed targets, which breaks reproduction of CI seeds and bisects. Add new variants at the end of the enum. Adding a variant changes `point_count`, which can change plans for existing seeds, but keeps the meaning of existing ordinals.

Each yield-capable object has an `instance_id` (tells simultaneous state machines apart), a `selection_key` (stable key of the logical operation, used by plans) and the connection's `yield_injector()` and `failure_injector()`.

Macros:
- `inject_transition_yield!(self, Point)`: in a `StateTransition` returning `TransitionResult<T>`.
- `inject_io_yield!(self, Point)`: in cursor or helper functions returning `IOResult<T>`.
- `inject_transition_failure!(self, Point)`: returns `Err(LimboError)` from a `StateTransition`. Failure injection works only for `TransitionResult` state machines.

An injected yield returns `Completion::new_yield()`, which is already finished but reports `is_explicit_yield()`, so the VDBE returns `StepResult::Yield`. To express a test interleaving, add a yield point instead of calling `Completion::new_yield()` directly. Yields for real lock contention are fine.

## New yield-capable type

Behind `cfg(any(test, injected_yields))`, add a field `yield_instance_id: u64`, initialize it with `connection.next_yield_instance_id()`, and implement:

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

`my_yield_key(...) -> u64` mixes a stable logical identity (tx id, table id) with a tag for the family, so plans of different families do not collide. Derive it only from the logical operation, so the same seed gives the same plan.

## Adding a point

A synthetic yield returns `StepResult::Yield`, and the same statement is stepped again later, so the code after the hook must be safe to enter again.

- Update the state to a resumable state before the hook.
- Put the hook after a `push`, `insert`, counter increment, lock acquisition or cleanup action, or make repeating that action harmless through state. The exception is a test of lock interleavings, where the hook goes before the lock acquisition.
- If a lock is held across the yield, test both resuming and dropping the statement at the yield. Dropping must restore invariants through Drop or abort cleanup.
- The macros call `self.yield_context()`, so place them where `&mut self.state` is not borrowed.
- Add a deterministic test with `FixedYieldInjector` or `FixedFailureInjector`. See `async-io-model` for re-entry rules.

## Fixed injectors in tests

```rust
conn.set_yield_injector(Some(FixedYieldInjector::new([
    CommitYieldPoint::<variant>.point(),
])));
```

`FixedYieldInjector` holds a set of points, ignores `instance_id` and `selection_key`, and fires each point once in total: if two instances reach the same point, the first consumes it. `FixedFailureInjector` maps one point to one `LimboError`.

`set_yield_injector(Some(..))` asserts the slot is empty and `set_yield_injector(None)` asserts it is installed. Reset both injectors before reusing a connection:

```rust
conn.set_yield_injector(None);
conn.set_failure_injector(None);
```

## Simulator

The concurrent simulator installs its own injector; fixed injectors are for unit tests. Each operation gets an injector seeded from the top-level seed and the fiber index, installed around each statement step. A plan per `(instance_id, selection_key, point_count)` has a limited number of random ordinals.

To reproduce a schedule, use the same top-level seed and fiber count. Changing variants, hook placement or fiber assignment changes the schedule for the same seed. Tests outside the simulator injector's own tests cannot rely on exact simulator yield counts.
