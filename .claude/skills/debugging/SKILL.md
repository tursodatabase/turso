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

## Architecture Reference

- **Parser** → AST from SQL strings
- **Code generator** → bytecode from AST
- **Virtual machine** → executes SQLite-compatible bytecode
- **Storage layer** → B-tree operations, paging

## Corruption Debugging

For WAL corruption and database integrity issues, use the corruption debug tools in [scripts](./scripts).

See [references/CORRUPTION-TOOLS.md](./references/CORRUPTION-TOOLS.md) for detailed usage.
