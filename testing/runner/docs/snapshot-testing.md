# Snapshot Testing Guide

This guide covers how to use snapshot testing in the test-runner to capture and validate SQL query execution plans.

## Overview

Snapshot tests capture the output of `EXPLAIN QUERY PLAN` and `EXPLAIN` (bytecode) for SQL queries. They help detect:

- Query plan regressions
- Unexpected changes to index usage
- Bytecode generation differences

Unlike regular tests that compare query results, snapshot tests validate that the *way* a query executes remains consistent.

> **Note:** Snapshot tests currently only run on the **Rust backend**. Other backends (CLI, JS) skip snapshot tests automatically. This is because EXPLAIN output format can differ between backends.

## Quick Start

### 1. Write a Snapshot Test

```sql
@database :memory:

setup schema {
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    CREATE INDEX idx_users_name ON users(name);
}

@setup schema
snapshot my-query-plan {
    SELECT * FROM users WHERE id = 1;
}
```

### 2. Run Tests to Generate Snapshots

```bash
# First run creates .snap.new files for review
make -C test-runner run-cli

# Or directly:
cargo run --bin test-runner -- run tests/my-test.sqltest
```

### 3. Accept the Snapshots

```bash
# Review and accept all pending snapshots
cargo run --bin test-runner -- run tests/ --snapshot-mode=always
```

### 4. Commit the Snapshot Files

```bash
git add tests/snapshots/
git commit -m "Add query plan snapshots"
```

## Snapshot Update Modes

The `--snapshot-mode` flag controls how snapshots are updated:

| Mode | Description | Use Case |
|------|-------------|----------|
| `auto` | `no` in CI, `new` locally (default) | Normal development |
| `new` | Write `.snap.new` files for review | Manual review before accepting |
| `always` | Write directly to `.snap` files | Accept all changes |
| `no` | Read-only, no files written | CI validation |

### Mode Details

**`auto` (default)**

Automatically detects the environment:
- In CI (GitHub Actions, Travis, CircleCI, etc.): acts as `no`
- Locally: acts as `new`

CI detection checks for environment variables: `CI`, `GITHUB_ACTIONS`, `TRAVIS`, `CIRCLECI`, `GITLAB_CI`, `BUILDKITE`, `JENKINS_URL`, `TF_BUILD`.

**`new`**

Creates `.snap.new` files alongside existing snapshots. This allows you to review changes before accepting them:

```
tests/snapshots/
  my-test__query-plan.snap      # existing
  my-test__query-plan.snap.new  # new/changed
```

Review the diff manually, then run with `--snapshot-mode=always` to accept.

**`always`**

Directly updates `.snap` files without creating `.snap.new` intermediates. Use this when you've reviewed the changes and want to accept them.

**`no`**

Read-only mode. No snapshot files are written. Tests will fail if:
- A snapshot doesn't exist
- A snapshot doesn't match

This is the mode used in CI to ensure all snapshots are committed.

## Snapshot File Format

Snapshot files use YAML frontmatter followed by the captured output:

```yaml
---
source: my-test.sqltest
expression: SELECT * FROM users WHERE id = 1;
info:
  statement_type: SELECT
  tables:
  - users
  setup_blocks:
  - schema
  database: ':memory:'
---
QUERY PLAN
`--SEARCH users USING INTEGER PRIMARY KEY (rowid=?)

BYTECODE
addr  opcode       p1  p2  p3  p4          p5  comment
   0  Init          0   8   0               0  Start at 8
   1  OpenRead      0   2   0  k(3,B,B,B)   0  table=users, root=2, iDb=0
   ...
```

### Metadata Fields

| Field | Description |
|-------|-------------|
| `source` | Test file name |
| `expression` | The SQL query |
| `info.statement_type` | Auto-detected: SELECT, INSERT, UPDATE, DELETE, etc. |
| `info.tables` | Auto-extracted table names from the query |
| `info.setup_blocks` | Setup blocks applied before the snapshot |
| `info.database` | Database type used |

## File Organization

Snapshot files are stored in a `snapshots/` directory adjacent to the test file:

```
tests/
  queries.sqltest
  aggregates.sqltest
  snapshots/
    queries__select-by-id.snap
    queries__select-by-name.snap
    aggregates__count-all.snap
```

Naming convention: `{test-file-stem}__{snapshot-name}.snap`

## CLI Commands

### Run Tests with Snapshots

```bash
# Default mode (auto)
cargo run --bin test-runner -- run tests/

# Accept all snapshot changes
cargo run --bin test-runner -- run tests/ --snapshot-mode=always

# Review mode (create .snap.new files)
cargo run --bin test-runner -- run tests/ --snapshot-mode=new

# Read-only mode (CI)
cargo run --bin test-runner -- run tests/ --snapshot-mode=no

# Filter specific snapshots
cargo run --bin test-runner -- run tests/ --snapshot-filter="query-plan*"
```

### Check for Pending Snapshots

```bash
cargo run --bin test-runner -- check tests/
```

This command:
1. Validates test file syntax
2. Detects pending `.snap.new` files
3. Fails if any pending snapshots exist (useful for CI)

### Using the Makefile

```bash
# Run all tests including snapshots
make -C test-runner run-cli

# Run examples (includes snapshot examples)
make -C test-runner run-examples

# Check syntax and pending snapshots
make -C test-runner check
```

## CI Integration

### Recommended CI Configuration

```yaml
# .github/workflows/test.yml
- name: Run SQL tests
  run: |
    cargo run --bin test-runner -- run tests/ --snapshot-mode=no

- name: Check for pending snapshots
  run: |
    cargo run --bin test-runner -- check tests/
```

The `check` command will fail if any `.snap.new` files exist, ensuring all snapshot changes are committed.

### Workflow for Updating Snapshots

1. Make changes that affect query plans
2. Run tests locally (creates `.snap.new` files)
3. Review the changes: `diff tests/snapshots/*.snap tests/snapshots/*.snap.new`
4. Accept changes: `--snapshot-mode=always`
5. Commit the updated `.snap` files
6. Push to CI

## Snapshot Test Syntax

### Basic Snapshot

```sql
@database :memory:

snapshot query-plan {
    SELECT * FROM users;
}
```

### With Setup Blocks

```sql
@database :memory:

setup schema {
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
}

setup data {
    INSERT INTO users VALUES (1, 'Alice');
}

@setup schema
@setup data
snapshot query-plan-with-data {
    SELECT * FROM users WHERE id = 1;
}
```

### Skipping Snapshots

```sql
# Unconditional skip
@skip "query plan not stable yet"
snapshot unstable-plan {
    SELECT * FROM complex_view;
}

# Conditional skip (MVCC mode)
@skip-if mvcc "different plan in MVCC mode"
snapshot standard-plan {
    SELECT * FROM users;
}
```

### Backend-Specific Snapshots

Since snapshot tests only run on the Rust backend, the `@backend` decorator is typically not needed for snapshots. However, you can still use it to explicitly require the Rust backend:

```sql
# Explicitly require Rust backend (optional, since it's the only backend that runs snapshots)
@backend rust
snapshot turso-query-plan {
    SELECT * FROM users WHERE id = 1;
}
```

### Capability Requirements

```sql
# Snapshot requires trigger support
@requires trigger "query plan involves triggers"
@setup schema-with-triggers
snapshot trigger-query-plan {
    INSERT INTO audit_log SELECT * FROM events;
}
```

### Supported Decorators

Snapshots support all test decorators:

| Decorator | Description |
|-----------|-------------|
| `@setup <name>` | Apply a setup block before the snapshot |
| `@skip "reason"` | Skip this snapshot unconditionally |
| `@skip-if <cond> "reason"` | Skip conditionally (e.g., `mvcc`, `sqlite`) |
| `@backend <name>` | Only run on specified backend (snapshots only run on `rust`) |
| `@requires <cap> "reason"` | Only run if backend supports capability |

File-level directives (`@skip-file`, `@skip-file-if`, `@requires-file`) also apply to snapshots.

## Output Format

Snapshots capture two sections:

### QUERY PLAN

The output of `EXPLAIN QUERY PLAN`, formatted as a tree:

```
QUERY PLAN
`--SEARCH users USING INTEGER PRIMARY KEY (rowid=?)
```

For complex queries with subqueries or joins:

```
QUERY PLAN
|--SCAN users
`--SEARCH orders USING INDEX idx_orders_user (user_id=?)
```

### BYTECODE

The output of `EXPLAIN`, formatted as an aligned table:

```
BYTECODE
addr  opcode       p1  p2  p3  p4          p5  comment
   0  Init          0   8   0               0  Start at 8
   1  OpenRead      0   2   0  k(3,B,B,B)   0  table=users, root=2, iDb=0
   2  SeekRowid     0   4   7               0  if (r[4]!=cursor 0...) goto 7
```

## Differences from cargo-insta

While the workflow is similar to [cargo-insta](https://insta.rs/), test-runner uses a custom snapshot implementation:

| Feature | test-runner | cargo-insta |
|---------|-------------------|-------------|
| File format | YAML frontmatter + content | YAML frontmatter + content |
| Review tool | Manual diff / `--snapshot-mode` | `cargo insta review` |
| CI mode | `--snapshot-mode=no` | `--check` |
| Accept all | `--snapshot-mode=always` | `cargo insta accept` |
| Metadata | SQL-specific (tables, statement type) | Generic |

## Troubleshooting

### "Snapshot mismatch" in CI

1. Run tests locally to generate `.snap.new` files
2. Review the differences
3. Accept with `--snapshot-mode=always`
4. Commit the updated `.snap` files

### "Found pending snapshot files"

The `check` command found `.snap.new` files. Either:
- Accept them: run with `--snapshot-mode=always`
- Delete them: `rm tests/snapshots/*.snap.new`

### Different plans between runs

Query plans can vary based on:
- Database statistics
- Index availability
- SQLite/Turso version

Ensure your setup blocks create consistent schema and data.

### Snapshot not updating

Make sure you're using `--snapshot-mode=always` or `--snapshot-mode=new`. The default `auto` mode acts as `no` in CI environments.

## Best Practices

1. **Use descriptive snapshot names** - `query-plan-user-by-id` is better than `test1`

2. **Group related snapshots** - Keep snapshots for similar functionality in the same test file

3. **Include necessary setup** - Snapshots need indexes and data to produce meaningful plans

4. **Review before accepting** - Don't blindly accept snapshot changes; understand why the plan changed

5. **Commit snapshots with code changes** - When changing query logic, update snapshots in the same commit

6. **Use skip for unstable plans** - If a plan varies between environments, use `@skip` until stabilized
