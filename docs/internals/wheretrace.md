# WHERETRACE - Query Optimizer Debugging

WHERETRACE provides SQLite-compatible bitmask-filtered trace output for debugging query optimizer decisions.

## Quick Start

```bash
# Build with feature
cargo build --release --bin tursodb --features wheretrace

# Enable all traces
TURSO_WHERETRACE=0xFFFF ./target/release/tursodb db.sqlite < query.sql

# Compare with sqlite - note that you should build sqlite with debug flags required for wheretrace
sqlite3 ./perf/tpc-h/TPC-H.db <<'EOF'
.wheretrace 0xFFFF
.read ./perf/tpc-h/queries/19.sql
EOF
```

## Trace Flags

| Flag | Hex | Description |
|------|-----|-------------|
| SUMMARY | 0x0001 | Optimizer start/finish messages |
| SOLVER | 0x0002 | DP solver join order exploration |
| COST | 0x0004 | Cost calculations and estimates |
| LOOP_INSERT | 0x0008 | Join candidate add/skip decisions |
| ACCESS_METHOD | 0x0010 | Index selection, BEGIN/END addBtreeIdx |
| CONSTRAINT | 0x0020 | WHERE term analysis and selectivity |
| REJECTED | 0x0040 | Why alternative plans were rejected |
| QUERY_STRUCTURE | 0x4000 | FROM/WHERE/ORDERBY structure |
| CODE_GEN | 0x8000 | Coding level, DISABLE-TERM |

**Turso-specific flags:**

| Flag | Hex | Description |
|------|-----|-------------|
| HASH_JOIN | 0x0100 | Hash join build/probe decisions |
| BLOOM_FILTER | 0x0200 | Bloom filter creation/usage |
| EPHEMERAL_IDX | 0x0400 | Temporary index builds |
| ORDER_TARGET | 0x0800 | ORDER BY satisfaction |

**Convenience masks:**
- `0xFFFF` - All traces
- `0xC03F` - SQLite-compatible only
- `0x0F00` - Turso-specific only

## Adding New Traces

1. Choose appropriate flag from `core/translate/optimizer/trace.rs`
2. Use the `wheretrace!` macro:

```rust
#[cfg(feature = "wheretrace")]
crate::wheretrace!(crate::translate::optimizer::trace::flags::COST,
    "cost: table={} total={:.1}", table_name, cost);
```

3. For formatted output, use helpers from `pretty.rs`:

```rust
#[cfg(feature = "wheretrace")]
{
    use crate::translate::optimizer::{pretty, trace::flags};
    crate::wheretrace!(flags::ACCESS_METHOD, "{}",
        pretty::format_btree_idx_begin(&table_name, constraint_count, candidate_count));
}
```

## Output Format

The `TURSO_WHERETRACE_FORMAT` env var controls output format:
- Default: Badge-style with ANSI colors (when stderr is a TTY)
- `json`: JSON Lines for programmatic parsing

```bash
TURSO_WHERETRACE=0xFFFF TURSO_WHERETRACE_FORMAT=json ./target/debug/tursodb ...
```

## Files

- `core/translate/optimizer/trace.rs` - Flags, macros, initialization
- `core/translate/optimizer/pretty.rs` - Formatting utilities
