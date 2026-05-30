---
name: allocator-migration
description: Use when migrating Turso core code to crate::alloc, allocator-api2, cfg(nightly) allocator aliases, fallible collection APIs, try_vec!, try_collect, or LimboError::OutOfMemory handling. Covers import style, allocator-aware aliases, stable/nightly boundaries, shuttle Arc compatibility, and how to keep allocator foundation commits separate from module migrations. Read this when changing code that does allocations.
---

# Turso Allocator Migration

Use this skill for Turso allocator migration work in `core`, especially when replacing standard heap collections with `crate::alloc` aliases or adding fallible allocation paths.

## Core Rules

- Use `crate::alloc::*` in migrated modules.
- Keep call-site names stable: `Vec`, `Box`, `HashMap`, `BTreeSet`, `Arc`, and similar standard names.
- Use `Turso` for new allocator-specific names, not `Limbo`.
- Use `cfg(nightly)` for nightly allocator behavior, not Cargo features.
- Do not change `set_allocator` unless the user explicitly asks.
- Keep `Arc` and `Weak` routed through `crate::sync` for shuttle compatibility for now. Leave allocator-aware shared pointers as a TODO.
- Do not migrate tests to fallible allocation unless they block compilation or the user asks.

## Import Pattern

For migrated modules, prefer a single allocator namespace import:

```rust
use crate::alloc::*;
```

Then remove conflicting imports such as:

```rust
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::vec::Vec;
use crate::sync::Arc;
```

Keep explicit `std::...` imports only at narrow boundaries where unmigrated types still require standard-library collections.

## Constructors

After `use crate::alloc::*`, use normal constructor names:

```rust
Vec::new()
Vec::with_capacity(capacity)
HashMap::new()
BTreeSet::new()
Box::new(value)
Arc::new(value)
```

Allocator extension traits provide allocator-aware constructors underneath where needed.

When a method becomes fallible, prefer changing the return type instead of renaming the method:

```rust
pub fn new(...) -> Result<Self>
```

Do not rename a method to `try_new` only because its return type changed, unless the local API already uses that pattern or the user asks for it.

## Fallible Allocation

In migrated production code that returns `Result`, or can reasonably be changed to return `Result`, use fallible allocation APIs:

```rust
try_vec![a, b, c]?
vec.try_push(value)?
vec.try_extend(iter)?
vec.try_reserve(additional)?
Vec::try_with_capacity(capacity)?
iter.try_collect::<Vec<_>>()?
```

Use `?` directly when possible. `TryReserveError` converts into `LimboError`, so avoid repetitive manual error mapping.

Do not add manual capacity overflow checks when `try_with_capacity` or `try_reserve` already accounts for capacity failure. Compare against the previous code before adding checks or restructuring allocation logic.

## Iterator Collection

Use `TursoIteratorExt::try_collect` for allocator-aware collection.

Supported patterns include plain collections, `Option`, and `Result`:

```rust
let values: Vec<_> = iter.try_collect()?;

let maybe_values: Option<Vec<_>> = iter_of_options.try_collect()?;

let result_values: Result<Vec<_>, E> = iter_of_results.try_collect()?;
```

For parsing where an invalid item should produce `None`, prefer:

```rust
let parsed = input
    .split_whitespace()
    .map(|s| s.parse::<u64>().ok())
    .try_collect()?;
```

## String And Std Boundaries

`String` is not fully allocator-parametric on stable. When migrated code still performs string formatting, `to_string`, `format!`, or `join`, do not over-migrate unless the user asks. Add a focused TODO when useful:

```rust
// TODO: make this formatting allocation fallible once String is allocator-aware
// in the Turso allocation namespace.
```

Some central types may still use standard collections until separately migrated, especially:

- `Value::Blob`
- record payloads
- shared pointer internals
- external API surfaces

At these boundaries, explicit `std::vec::Vec` conversions are acceptable. Keep them narrow and avoid spreading standard collection imports through migrated modules.

## Migration Workflow

1. Inspect the module and nearby callers.
2. Compare with the previous code before simplifying checks or changing capacity calculations.
3. Add `use crate::alloc::*`.
4. Remove conflicting standard collection and pointer imports.
5. Convert constructors and mutation paths.
6. Make allocation-bearing functions return `Result` where appropriate.
7. Use fallible APIs consistently in production code.
8. Leave tests infallible unless needed.
9. Check formatting and compilation.
10. Commit each coherent migration slice.

## Commit Boundaries

Keep foundation allocator work separate from migrations.

Foundation commits include:

- aliases in `core::alloc`
- extension traits
- `try_vec!`
- `try_collect`
- `TryReserveError` conversions
- allocator backend setup

Migration commits should be module-focused, for example:

- RowSet
- VDBE arrays
- sorter
- stats
- schema
- statement
- virtual tables

Commit frequently after each coherent slice.

## Verification

For normal migration slices, check formatting and compilation:

```bash
cargo fmt
cargo check -p turso_core
```

Before allocator-sensitive commits, also check the nightly cfg build:

```bash
RUSTFLAGS="--cfg nightly" cargo +nightly check -p turso_core
```

For allocator collection performance work, use the Divan comparison script. It
runs the `alloc_collections` bench by default and prints a sorted std-vs-Turso
comparison table:

```bash
scripts/compare-divan-std-turso.py --run
```

To exercise the nightly allocator paths, run:

```bash
scripts/compare-divan-std-turso.py --run --nightly
```

Useful variants:

```bash
scripts/compare-divan-std-turso.py --run --bench-filter hash_map --filter hash_map
scripts/compare-divan-std-turso.py --run --save-output /tmp/alloc_collections.txt
scripts/compare-divan-std-turso.py /tmp/alloc_collections.txt
```

Run focused tests only when the touched code path is risky, behavior changed, or compilation is not enough to validate the migration.
