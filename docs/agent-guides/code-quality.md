---
name: code-quality
description: Correctness rules, Rust patterns, comments, avoiding over-engineering
---
# Code Quality Guide

## Core Principle

Production database. Correctness paramount. Crash > corrupt.

## Correctness Rules

1. **No workarounds or quick hacks.** Handle all errors, check invariants
2. **Assert often.** Never silently fail or swallow edge cases
3. **Crash on invalid state** if it risks data integrity. Don't continue in undefined state
4. **Consider edge cases.** On long enough timeline, all possible bugs will happen

## Rust Patterns

- Make illegal states unrepresentable
- Exhaustive pattern matching
- Prefer enums over strings/sentinels
- Minimize heap allocations
- Write CPU-friendly code (microsecond = long time)
- Don't make identifier visibility any broader than it needs to be.

### Avoid `unwrap()`

Never use bare `unwrap()`. Instead:

**For truly unreachable states** (invariants that should never be violated):
```rust
// Good: documents the invariant
let value = option.expect("value must be set in Init phase");
```

**For recoverable errors** (states that could happen at runtime):
```rust
// Good: proper error handling
let Some(value) = option else {
    return Err(LimboError::InvalidArgument("value not provided".into()));
};
```

The choice depends on whether the None/Err case represents:
- A bug in the code (use `expect` with descriptive message)
- A valid runtime condition (use `let ... else` or `match`)

### No silent defaults in custom-type code

`unwrap_or`, `unwrap_or_else`, `unwrap_or_default`, `map_or`, `map_or_else`
and `Result::ok` turn a missing value or an error into a default. In custom-type
code a wrong default is a wrong answer, e.g. an unknown type that compares as
text. Clippy rejects these methods in files that start with
`#![deny(clippy::disallowed_methods)]` (list in `core/clippy.toml`), currently
`core/schema.rs`, `core/translate/collate.rs`, `core/translate/expr/binding.rs`
and `core/translate/expr/expr_type.rs`. Return an error for the missing case
instead. If the default really is correct, put
`#[expect(clippy::disallowed_methods, reason = "...")]` on the smallest
statement or item, and make the reason point at the code that makes the missing
case impossible or harmless (`path:line`). "Should never happen" is not a
reason. Defaults nobody has justified yet say `UNREVIEWED fallback: <what the
default hides>`.

## If-Statements

Wrong:
```rust
if condition {
    // happy path
} else {
    // "shouldn't happen" - silently ignored
}
```

Right:
```rust
// If only one branch should ever be hit:
assert!(condition, "invariant violated: ...");
// OR
return Err(LimboError::InternalError("unexpected state".into()));
// OR
unreachable!("impossible state: ...");
```

Use if-statements only when both branches are expected paths.

## Comments

- Do not add comments. Instead, focus on making your code expressive.

## Avoid Over-Engineering

- Only changes directly requested or clearly necessary
- Don't add features beyond what's asked
- Don't add docstrings/comments to unchanged code
- Don't add error handling for impossible scenarios
- Don't create abstractions for one-time operations
- Three similar lines > premature abstraction

## Ensure understanding of IO model

- [Async IO model](./async-io-model.md)

## Cleanup

- Delete unused code completely
- No backwards-compat hacks (renamed `_vars`, re-exports, `// removed` comments)
