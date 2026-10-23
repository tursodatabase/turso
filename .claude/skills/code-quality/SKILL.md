---
name: code-quality
description: General Correctness rules, Rust patterns, comments, avoiding over-engineering. When writing code always take these into account
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

- Don't add code comments, except for rust `SAFETY` comments.

## Avoid Over-Engineering

- Only changes directly requested or clearly necessary
- Don't add features beyond what's asked
- Don't add docstrings/comments to unchanged code
- Don't add error handling for impossible scenarios
- Don't create abstractions for one-time operations
- Three similar lines > premature abstraction

## Index Mutations

When code involves index inserts, deletes, or conflict resolution, double-check the ordering against SQLite. Wrong ordering causes index inconsistencies. and easy to miss.

## Ensure understanding of IO model

- [Async IO model](../async-io-model/SKILL.md)

## Cleanup

- Delete unused code completely
- No backwards-compat hacks (renamed `_vars`, re-exports, `// removed` comments)

## Single-use helpers

If a helper function has no meaning outside of a single use case, move it inside the function that uses it:

```rust
fn do_something() {
    
    something_specific();

    fn something_specific() {
    }
}
```