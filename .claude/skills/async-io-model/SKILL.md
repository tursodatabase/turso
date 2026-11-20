---
name: async-io-model
description: Use when writing or changing code that returns IOResult or keeps I/O state across yields - state machines, re-entrancy, CompletionGroup
---
# Async I/O model

Turso uses cooperative yielding with explicit state machines instead of async/await.

## Types

```rust
pub enum IOResult<T> {
    Done(T),            // finished, here is the result
    IO(IOCompletions),  // wait for the completion, then call again
}
pub struct IOCompletions(pub Completion);
pub type IOResultOr<T> = Result<IOResult<T>, Box<LimboError>>;
```

A function returning `IOResult` is called repeatedly until it returns `Done`.

To wait for several completions, add them to a `CompletionGroup` and yield the built completion. It finishes when all complete or one fails, can be nested, and can be cancelled with `group.cancel()`:

```rust
let mut group = CompletionGroup::new(|_| {});
group.add(&completion1);
group.add(&completion2);
io_yield_one!(group.build());
```

## Macros

- `return_if_io!(expr)` unwraps `Done(v)` to `v` and returns the `IO` variant to the caller.
- `io_yield_one!(c)` returns `Ok(IOResult::IO(IOCompletions(c)))`.

## State machines

Keep progress in a state enum, loop on it, and set the next state before each yield:

```rust
loop {
    match &mut self.state {
        State::Start => {
            let (page, completion) = start_read();
            self.state = State::WaitingForRead { page };
            io_yield_one!(completion);
        }
        State::WaitingForRead { page } => {
            let data = page.get_contents().to_vec();
            self.state = State::Processing { data };
        }
        State::Processing { data } => return Ok(IOResult::Done(process(data))),
    }
}
```

If a field is used in only one or two states, put it in those variants. Other states then cannot access it, and the flow is easier to follow. Loop indexes belong in the variant too (`ProcessingItem { idx, items }`), advanced only when changing state.

## Re-entrancy

When a call yields, the caller waits and then calls the function again from the top. Any state changed before the yield is changed again on the second call:

```rust
self.counter += 1;                          // runs on every re-entry
return_if_io!(something_that_might_yield());
```

Do the change after the yield returns, or move to a new state before yielding and make the change in that state:

```rust
return_if_io!(something_that_might_yield());
self.counter += 1;  // reached once
```

Typical bugs: `vec.push(x)`, `idx += 1`, or `map.insert(k, v)` before a `return_if_io!` repeat on each re-entry.
