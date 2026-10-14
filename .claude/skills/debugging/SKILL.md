---
name: debugging
description: How to debug tursodb using Bytecode comparison, logging, ThreadSanitizer, deterministic simulation, and corruption analysis tools
---
# Debugging Guide

## Bytecode Comparison Flow

Turso aims for SQLite compatibility. When behavior differs:

```
1. EXPLAIN query in sqlite3
2. EXPLAIN query in tursodb
3. Compare bytecode
   ├─ Different → bug in code generation
   └─ Same but results differ → bug in VM or storage layer
```

### Example

```bash
# SQLite
sqlite3 :memory: "EXPLAIN SELECT 1 + 1;"

# Turso
cargo run --bin tursodb :memory: "EXPLAIN SELECT 1 + 1;"
```

## Manual Query Inspection

```bash
cargo run --bin tursodb :memory: 'SELECT * FROM foo;'
cargo run --bin tursodb :memory: 'EXPLAIN SELECT * FROM foo;'
```

## Logging

```bash
# Trace core during tests
RUST_LOG=none,turso_core=trace make test

# Output goes to testing/test.log
# Warning: can be megabytes per test run
```

## Threading Issues

Use stress tests with ThreadSanitizer:

```bash
rustup toolchain install nightly
rustup override set nightly
cargo run -Zbuild-std --target x86_64-unknown-linux-gnu \
  -p turso_stress -- --vfs syscall --nr-threads 4 --nr-iterations 1000
```

## Deterministic Simulation

Reproduce bugs with seed. Note: simulator uses legacy "limbo" naming.

```bash
# Simulator
RUST_LOG=limbo_sim=debug cargo run --bin limbo_sim -- -s <seed>

# Whopper (concurrent DST)
SEED=1234 ./testing/concurrent-simulator/bin/run
```

## Stack Usage

Parsing and translating an expression recurse once per level of nesting, so
the stack frame size of those functions limits how deep an expression can be.
Three scripts in `scripts/stack/` measure stack use. Use a release build
(`cargo build --release --bin tursodb`): debug builds do not reuse stack slots,
so their frames say little about what users run. Run each script with `-h`
for all options.

```bash
# Frame size of each function, read from its prologue. -c compares two binaries.
scripts/stack/frame-sizes.sh 'translate::expr::'
scripts/stack/frame-sizes.sh -c /tmp/tursodb-main 'translate_expr$' 'parse_expr_inner$'

# Smallest stack that runs a SQL script, per binary. -S adds sqlite3.
scripts/stack/min-stack.sh -S -b /tmp/tursodb-main -b target/release/tursodb deep.sql

# Which functions use the stack at the deepest point (runs under lldb).
scripts/stack/stack-profile.sh deep.sql
```

Functions that recurse per level should be small. Move rare and large paths
into `#[inline(never)]` helpers, like SQLite's `SQLITE_NOINLINE`.

## Architecture Reference

- **Parser** → AST from SQL strings
- **Code generator** → bytecode from AST
- **Virtual machine** → executes SQLite-compatible bytecode
- **Storage layer** → B-tree operations, paging

## Corruption Debugging

For WAL corruption and database integrity issues, use the corruption debug tools in [scripts](./scripts).

See [references/CORRUPTION-TOOLS.md](./references/CORRUPTION-TOOLS.md) for detailed usage.
