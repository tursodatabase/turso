---
name: async-io-model
description: Explanations of common asynchronous patterns used in tursodb. Involves IOResult, state machines, re-entrancy pitfalls, CompletionGroup. Always use these patterns in `core` when doing anything IO
---
# Async I/O Model Guide

Turso uses cooperative yielding with explicit state machines instead of Rust async/await.

## Core Types

```rust
pub enum IOCompletions {
    Single(Completion),
}

#[must_use]
pub enum IOResult<T> {
    Done(T),      // Operation complete, here's the result
    IO(IOCompletions),  // Need I/O, call me again after completions finish
}
```

Functions returning `IOResult` must be called repeatedly until `Done`.

## Completion and CompletionGroup

A `Completion` tracks a single I/O operation:

```rust
pub struct Completion { /* ... */ }

impl Completion {
    pub fn finished(&self) -> bool;
    pub fn succeeded(&self) -> bool;
    pub fn get_error(&self) -> Option<CompletionError>;
}
```

To wait for multiple I/O operations, use `CompletionGroup`:

```rust
let mut group = CompletionGroup::new(|_| {});

// Add individual completions
group.add(&completion1);
group.add(&completion2);

// Build into single completion that finishes when all complete
let combined = group.build();
io_yield_one!(combined);
```

`CompletionGroup` features:
- Aggregates multiple completions into one
- Calls callback when all complete (or any errors)
- Can nest groups (add a group's completion to another group)
- Cancellable via `group.cancel()`

## Helper Macros

### `return_if_io!`
Unwraps `IOResult`, propagates IO variant up the call stack:
```rust
let result = return_if_io!(some_io_operation());
// Only reaches here if operation returned Done
```

### `io_yield_one!`
Yields a single completion:
```rust
io_yield_one!(completion);  // Returns Ok(IOResult::IO(Single(completion)))
```

## State Machine Pattern

Operations that may yield use explicit state enums:

```rust
enum MyOperationState {
    Start,
    WaitingForRead { page: PageRef },
    Processing { data: Vec<u8> },
    Done,
}
```

The function loops, matching on state and transitioning:

```rust
fn my_operation(&mut self) -> Result<IOResult<Output>> {
    loop {
        match &mut self.state {
            MyOperationState::Start => {
                let (page, completion) = start_read();
                self.state = MyOperationState::WaitingForRead { page };
                io_yield_one!(completion);
            }
            MyOperationState::WaitingForRead { page } => {
                let data = page.get_contents();
                self.state = MyOperationState::Processing { data: data.to_vec() };
                // No yield, continue loop
            }
            MyOperationState::Processing { data } => {
                let result = process(data);
                self.state = MyOperationState::Done;
                return Ok(IOResult::Done(result));
            }
            MyOperationState::Done => unreachable!(),
        }
    }
}
```

## Re-Entrancy: The Critical Pitfall

**State mutations before yield points cause bugs on re-entry.**

### Wrong
```rust
fn bad_example(&mut self) -> Result<IOResult<()>> {
    self.counter += 1;  // Mutates state
    return_if_io!(something_that_might_yield());  // If yields, re-entry will increment again!
    Ok(IOResult::Done(()))
}
```

If `something_that_might_yield()` returns `IO`, caller waits for completion, then calls `bad_example()` again. `counter` gets incremented twice (or more).

### Correct: Mutate After Yield
```rust
fn good_example(&mut self) -> Result<IOResult<()>> {
    return_if_io!(something_that_might_yield());
    self.counter += 1;  // Only reached once, after IO completes
    Ok(IOResult::Done(()))
}
```

### Correct: Use State Machine
```rust
enum State { Start, AfterIO }

fn good_example(&mut self) -> Result<IOResult<()>> {
    loop {
        match self.state {
            State::Start => {
                // Don't mutate shared state here
                self.state = State::AfterIO;
                return_if_io!(something_that_might_yield());
            }
            State::AfterIO => {
                self.counter += 1;  // Safe: only entered once
                return Ok(IOResult::Done(()));
            }
        }
    }
}
```

## Common Re-Entrancy Bugs

| Pattern | Problem |
|---------|---------|
| `vec.push(x); return_if_io!(...)` | Vec grows on each re-entry |
| `idx += 1; return_if_io!(...)` | Index advances multiple times |
| `map.insert(k,v); return_if_io!(...)` | Duplicate inserts or overwrites |
| `flag = true; return_if_io!(...)` | Usually ok, but check logic |

## State Enum Design

Encode progress in state variants:

```rust
// Good: index is part of state, preserved across yields
enum ProcessState {
    Start,
    ProcessingItem { idx: usize, items: Vec<Item> },
    Done,
}

// Loop advances idx only when transitioning states
ProcessingItem { idx, items } => {
    return_if_io!(process_item(&items[idx]));
    if idx + 1 < items.len() {
        self.state = ProcessingItem { idx: idx + 1, items };
    } else {
        self.state = Done;
    }
}
```

## Turso Implementation

Key files:
- `core/types.rs` - `IOResult`, `IOCompletions`, `return_if_io!`, `return_and_restore_if_io!`
- `core/io/completions.rs` - `Completion`, `CompletionGroup`
- `core/util.rs` - `io_yield_one!` macro
- `core/state_machine.rs` - Generic `StateMachine` wrapper
- `core/storage/btree.rs` - Many state machine examples
- `core/storage/pager.rs` - `CompletionGroup` usage examples

## Testing Async Code

Re-entrancy bugs often only manifest under specific IO timing. Use:
- Deterministic simulation (`testing/simulator/`)
- Whopper concurrent DST (`testing/concurrent-simulator/`)
- Fault injection to force yields at different points

## References

- `docs/manual.md` section on I/O
