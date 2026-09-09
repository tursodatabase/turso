---
name: rationalize-deps
description: Analyze Cargo.toml dependencies and attempt to remove unused features to reduce compile times and binary size
---

# Rationalize Dependencies

This skill analyzes Cargo.toml dependencies to identify and remove unused features.

## Overview

Many crates enable features by default that may not be needed. This skill:
1. Identifies dependencies with default features enabled
2. Tests if `default-features = false` works
3. Identifies which specific features are actually needed
4. Verifies compilation after changes

## Step 1: Identify the target

Ask the user which crate(s) to analyze:
- A specific crate name (e.g., "tokio", "serde")
- A specific workspace member (e.g., "quickwit-search")
- "all" to scan the entire workspace

## Step 2: Analyze current dependencies

For the workspace Cargo.toml (`quickwit/Cargo.toml`), list dependencies that:
- Do NOT have `default-features = false`
- Have default features that might be unnecessary

Run: `cargo tree -p <crate> -f "{p} {f}" --edges features` to see what features are actually used.

## Step 3: For each candidate dependency

### 3a: Check the crate's default features

Look up the crate on crates.io or check its Cargo.toml to understand:
- What features are enabled by default
- What each feature provides

Use: `cargo metadata --format-version=1 | jq '.packages[] | select(.name == "<crate>") | .features'`

### 3b: Try disabling default features

Modify the dependency in `quickwit/Cargo.toml`:

From:
```toml
some-crate = { version = "1.0" }
```

To:
```toml
some-crate = { version = "1.0", default-features = false }
```

### 3c: Run cargo check

Run: `cargo check --workspace` (or target specific packages for faster feedback)

If compilation fails:
1. Read the error messages to identify which features are needed
2. Add only the required features explicitly:
   ```toml
   some-crate = { version = "1.0", default-features = false, features = ["needed-feature"] }
   ```
3. Re-run cargo check

### 3d: Binary search for minimal features

If there are many default features, use binary search:
1. Start with no features
2. If it fails, add half the default features
3. Continue until you find the minimal set

## Step 4: Document findings

For each dependency analyzed, report:
- Original configuration
- New configuration (if changed)
- Features that were removed
- Any features that are required

## Step 5: Verify full build

After all changes, run:
```bash
cargo check --workspace --all-targets
cargo test --workspace --no-run
```

## Common Patterns

### Serde
Often only needs `derive`:
```toml
serde = { version = "1.0", default-features = false, features = ["derive", "std"] }
```

### Tokio
Identify which runtime features are actually used:
```toml
tokio = { version = "1.0", default-features = false, features = ["rt-multi-thread", "macros", "sync"] }
```

### Reqwest
Often doesn't need all TLS backends:
```toml
reqwest = { version = "0.11", default-features = false, features = ["rustls-tls", "json"] }
```

## Rollback

If changes cause issues:
```bash
git checkout quickwit/Cargo.toml
cargo check --workspace
```

## Tips

- Start with large crates that have many default features (tokio, reqwest, hyper)
- Use `cargo bloat --crates` to identify large dependencies
- Check `cargo tree -d` for duplicate dependencies that might indicate feature conflicts
- Some features are needed only for tests - consider using `[dev-dependencies]` features
