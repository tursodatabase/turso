# SQL Test DSL Specification

This document specifies the `.sqltest` DSL (Domain-Specific Language) for writing SQL tests.

## Overview

The `.sqltest` format is designed to be:
- **Readable**: Human-friendly syntax with clear structure
- **Composable**: Named setups that can be combined per-test
- **Parallel-safe**: All tests are isolated and run in parallel by default
- **Flexible**: Supports multiple databases and comparison modes

## File Structure

A `.sqltest` file consists of:
1. Database declarations (`@database`)
2. Named setup blocks (`setup name { ... }`)
3. Test cases (`test name { ... } expect { ... }`)
4. Snapshot cases (`snapshot name { ... }`) - for capturing EXPLAIN output

```
@database <db-spec>
@database <db-spec>

setup <name> {
    <sql>
}

@setup <name>
@setup <name>
@skip "reason"
test <name> {
    <sql>
}
expect [modifier] {
    <expected-output>
}

# Snapshot tests (for EXPLAIN output)
@setup <name>
snapshot <name> {
    <sql>
}
```

## Database Declarations

Every file must have at least one `@database` declaration.

### Syntax

```
@database :memory:
@database :temp:
@database :default:
@database :default-no-rowidalias:
@database path/to/file.db readonly
```

### Database Types

| Type | Description |
|------|-------------|
| `:memory:` | Fresh in-memory database for each test |
| `:temp:` | Fresh temporary file database for each test |
| `:default:` | Pre-generated database with `INTEGER PRIMARY KEY` (rowid alias) |
| `:default-no-rowidalias:` | Pre-generated database with `INT PRIMARY KEY` (no rowid alias) |
| `path readonly` | Existing database opened in read-only mode |

### Default Databases

The `:default:` and `:default-no-rowidalias:` database types use pre-generated databases containing fake user and product data. These databases must be generated before running tests.

- `:default:` - Uses `INTEGER PRIMARY KEY`, which creates a rowid alias (column is an alias for the internal rowid)
- `:default-no-rowidalias:` - Uses `INT PRIMARY KEY`, which does NOT create a rowid alias (column is a regular integer with a unique constraint)

Both databases contain:
- `users` table: id, first_name, last_name, email, phone_number, address, city, state, zipcode, age
- `products` table: id, name, price

Database file locations:
- `:default:` → `testing/database.db`
- `:default-no-rowidalias:` → `testing/database-no-rowidalias.db`

### Rules

1. **All databases in a file must be the same type:**
   - Writable (`:memory:`, `:temp:`) - can be mixed together
   - Readonly (`path readonly`, `:default:`, `:default-no-rowidalias:`) - cannot mix with writable

2. **Multiple databases**: When multiple databases are declared, all tests run against each database.

### Examples

```sql
# Writable file - both memory and temp allowed
@database :memory:
@database :temp:

# Readonly file - only readonly paths allowed
@database testing/testing.db readonly
@database testing/testing_small.db readonly

# Default databases - pre-generated with fake data
@database :default:

# Default database without rowid alias
@database :default-no-rowidalias:
```

## Setup Blocks

Named setup blocks define SQL that can be composed and applied to tests.

### Syntax

```
setup <name> {
    <sql-statements>
}
```

### Rules

1. Setup names must be unique within a file
2. Setup names must be valid identifiers (alphanumeric + underscore/hyphen, starting with letter)
3. Setup blocks are **only allowed in writable database files**
4. Readonly database files cannot have setup blocks

### Examples

```sql
setup users {
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
    INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);
}

setup products {
    CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
    INSERT INTO products VALUES (1, 'Widget', 9.99);
}
```

## File-Level Directives

These directives apply to all tests and snapshots in the file.

### Syntax

```
@skip-file "reason"
@skip-file-if <condition> "reason"
@requires-file <capability> "reason"
```

### Directives

| Directive | Description |
|-----------|-------------|
| `@skip-file "reason"` | Skip all tests in the file unconditionally |
| `@skip-file-if <condition> "reason"` | Skip all tests conditionally (e.g., `mvcc`) |
| `@requires-file <capability> "reason"` | Require capability for all tests (e.g., `trigger`) |

### Example

```sql
@database :memory:
@skip-file-if mvcc "MVCC not supported for this file"
@requires-file trigger "all tests need trigger support"

test example {
    SELECT 1;
}
expect {
    1
}
```

## Test Cases

### Basic Syntax

```
test <name> {
    <sql>
}
expect {
    <expected-output>
}
```

### Test Decorators

Decorators appear before the `test` keyword:

| Decorator | Description |
|-----------|-------------|
| `@setup <name>` | Apply a named setup before the test (can be repeated) |
| `@skip "reason"` | Skip this test unconditionally with the given reason |
| `@skip-if <condition> "reason"` | Skip this test conditionally based on runtime configuration |
| `@backend <name>` | Only run this test on the specified backend (`rust`, `cli`, `js`) |
| `@requires <capability> "reason"` | Only run if the backend supports the capability |

#### Skip Conditions

The `@skip-if` decorator supports the following conditions:

| Condition | Description |
|-----------|-------------|
| `mvcc` | Skip when MVCC mode is enabled (`--mvcc` flag) |

#### Capabilities

The `@requires` decorator supports the following capabilities:

| Capability | Description |
|------------|-------------|
| `trigger` | Backend supports `CREATE TRIGGER` |
| `strict` | Backend supports `STRICT` tables |
| `materialized_views` | Backend supports materialized views (experimental) |

### Expect Modifiers

The `expect` block can have modifiers:

| Modifier | Description |
|----------|-------------|
| (none) | Exact row-by-row match |
| `error` | Expect an error (optional pattern match) |
| `pattern` | Match output against regex pattern |
| `unordered` | Compare as sets (order doesn't matter) |

### Output Format

Expected output uses pipe-separated values for columns:

```
column1|column2|column3
value1|value2|value3
```

Single-column output omits the pipe:

```
value1
value2
```

### Complete Example

```sql
# Test with no setup
test select-constant {
    SELECT 42;
}
expect {
    42
}

# Test with single setup
@setup users
test select-users {
    SELECT id, name FROM users ORDER BY id;
}
expect {
    1|Alice
    2|Bob
}

# Test composing multiple setups
@setup users
@setup products
test select-join {
    SELECT u.name, p.name, p.price
    FROM users u, products p
    WHERE u.id = 1 LIMIT 1;
}
expect {
    Alice|Widget|9.99
}

# Test expecting error
test select-missing-table {
    SELECT * FROM nonexistent;
}
expect error {
    no such table
}

# Test with regex pattern
test select-random {
    SELECT random();
}
expect pattern {
    ^-?\d+$
}

# Test with unordered comparison
@setup users
test select-unordered {
    SELECT name FROM users;
}
expect unordered {
    Bob
    Alice
}

# Skipped test (unconditional)
@skip "known bug #123"
test select-buggy-feature {
    SELECT buggy();
}
expect {
    result
}

# Conditionally skipped test (only skipped in MVCC mode)
@skip-if mvcc "total_changes not supported in MVCC mode"
test total-changes {
    CREATE TABLE t (id INTEGER PRIMARY KEY);
    INSERT INTO t VALUES (1), (2), (3);
    SELECT total_changes();
}
expect {
    3
}

# Backend-specific test (only runs with CLI backend)
@backend cli
test cli-specific-feature {
    SELECT sqlite_version();
}
expect pattern {
    ^3\.\d+\.\d+$
}

# Backend-specific test (only runs with Rust backend)
@backend rust
test rust-specific-feature {
    SELECT 'rust-only';
}
expect {
    rust-only
}

# Test requiring a specific capability
@requires trigger "this test uses triggers"
test trigger-test {
    CREATE TABLE t (id INTEGER PRIMARY KEY);
    CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; END;
    INSERT INTO t VALUES (1);
    SELECT 'triggered';
}
expect {
    triggered
}
```

## Snapshot Cases

Snapshot tests capture the `EXPLAIN QUERY PLAN` and `EXPLAIN` (bytecode) output of a SQL query. They are used to detect changes in query execution plans over time.

### Basic Syntax

```
snapshot <name> {
    <sql>
}
```

Unlike regular tests, snapshot cases do not have an `expect` block. Instead, the expected output is stored in a `.snap` file that is automatically managed.

### Snapshot Decorators

Snapshots support all the same decorators as tests:

| Decorator | Description |
|-----------|-------------|
| `@setup <name>` | Apply a named setup before the snapshot (can be repeated) |
| `@skip "reason"` | Skip this snapshot unconditionally |
| `@skip-if <condition> "reason"` | Skip this snapshot conditionally |
| `@backend <name>` | Only run this snapshot on the specified backend (`rust`, `cli`, `js`) |
| `@requires <capability> "reason"` | Only run if the backend supports the capability |

### Snapshot File Location

Snapshot files are stored in a `snapshots/` directory adjacent to the test file:

```
tests/
  my-tests.sqltest
  snapshots/
    my-tests__query-plan-name.snap
```

The naming convention is: `{test-file-stem}__{snapshot-name}.snap`

### Example

```sql
@database :memory:

setup schema {
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    CREATE INDEX idx_users_name ON users(name);
}

setup data {
    INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
}

# Snapshot captures EXPLAIN QUERY PLAN + EXPLAIN output
@setup schema
@setup data
snapshot query-plan-by-id {
    SELECT * FROM users WHERE id = 1;
}

@setup schema
@setup data
snapshot query-plan-by-name {
    SELECT * FROM users WHERE name = 'Alice';
}
```

For detailed information about working with snapshots (update modes, commands, CI integration), see [Snapshot Testing Guide](./snapshot-testing.md).

## Grammar (EBNF-like)

```ebnf
file            = { file_directive | database_decl | setup_block | test_case | snapshot_case }

file_directive  = "@skip-file" STRING NEWLINE
                | "@skip-file-if" skip_condition STRING NEWLINE
                | "@requires-file" capability STRING NEWLINE

database_decl   = "@database" database_spec NEWLINE
database_spec   = ":memory:" | ":temp:" | ":default:" | ":default-no-rowidalias:" | PATH ["readonly"]

setup_block     = "setup" IDENTIFIER block
test_case       = { decorator } "test" IDENTIFIER block expect_block
snapshot_case   = { decorator } "snapshot" IDENTIFIER block

decorator       = "@setup" IDENTIFIER NEWLINE
                | "@skip" STRING NEWLINE
                | "@skip-if" skip_condition STRING NEWLINE
                | "@backend" IDENTIFIER NEWLINE
                | "@requires" capability STRING NEWLINE

skip_condition  = "mvcc"

capability      = "trigger" | "strict" | "materialized_views"

expect_block    = "expect" [expect_modifier] block

expect_modifier = "error" | "pattern" | "unordered"

block           = "{" { any_content } "}"

IDENTIFIER      = [a-zA-Z_][a-zA-Z0-9_-]*
STRING          = '"' { any_char } '"'
PATH            = [^\s]+
NEWLINE         = '\n'
```

## Validation Rules

### File-Level Validation

1. At least one `@database` declaration required
2. Cannot mix readonly and writable databases in the same file
3. Setup blocks not allowed in readonly database files
4. All referenced setup names must exist

### Test-Level Validation

1. Test names must be unique within a file
2. Setup names in `@setup` decorators must reference defined setups
3. SQL must end with semicolon

### Snapshot-Level Validation

1. Snapshot names must be unique within a file
2. Snapshot names must not conflict with test names
3. Setup names in `@setup` decorators must reference defined setups
4. SQL must end with semicolon

## Execution Model

### Parallel Execution

All tests are isolated and run in parallel:

```
For each @database:
    For each test (in parallel):
        1. Create fresh database connection
        2. Execute composed setups in order
        3. Execute test SQL
        4. Compare result with expectation
        5. Close connection
```

### Setup Composition

When a test has multiple `@setup` decorators, they are executed in order:

```sql
@setup users      # Executed first
@setup products   # Executed second
@setup orders     # Executed third
test my-test {
    ...
}
```

## Special Values

| Value | Representation |
|-------|----------------|
| NULL | `NULL` (literal string) |
| Empty string | (empty between pipes, e.g., `a||b`) |
| BLOB | Hex representation or raw bytes |

## Comments

Lines starting with `#` are comments:

```sql
# This is a comment
@database :memory:

# Another comment
test example {
    SELECT 1;
}
expect {
    1
}
```

## File Extension

Test files should use the `.sqltest` extension.

---

## Implementation Notes

### Lexer

The lexer uses the [Logos](https://docs.rs/logos) crate (v0.16) for tokenization. Key implementation details:

- **Block content extraction**: When `{` is encountered, a custom callback extracts all content until the matching `}`, handling nested braces. This allows arbitrary SQL and expected output without special escaping.

- **Tokens**: The lexer produces these token types:
  - Keywords: `@database`, `@setup`, `@skip`, `@skip-if`, `@skip-file`, `@skip-file-if`, `@requires`, `@requires-file`, `@backend`, `setup`, `test`, `expect`, `snapshot`
  - Modifiers: `error`, `pattern`, `unordered`, `readonly`, `raw`
  - Skip conditions: `mvcc`
  - Capabilities: `trigger`, `strict`, `materialized_views`
  - Database types: `:memory:`, `:temp:`, `:default:`, `:default-no-rowidalias:`
  - Block content: `{...}` (content between braces)
  - Identifiers, strings, paths, comments, newlines

### Parser

The parser is a simple recursive descent parser that:

1. Tokenizes the input using Logos
2. Parses top-level constructs (databases, setups, tests)
3. Validates the resulting AST against the rules

### AST Types

```rust
// Core types in src/parser/ast.rs
pub struct TestFile {
    pub databases: Vec<DatabaseConfig>,
    pub setups: HashMap<String, String>,
    pub tests: Vec<TestCase>,
    pub snapshots: Vec<SnapshotCase>,
    pub global_skip: Option<Skip>,        // @skip-file / @skip-file-if
    pub global_requires: Vec<Requirement>, // @requires-file
}

pub struct TestCase {
    pub name: String,
    pub sql: String,
    pub expectations: Expectations,
    pub modifiers: CaseModifiers,
}

pub struct SnapshotCase {
    pub name: String,
    pub sql: String,
    pub modifiers: CaseModifiers,
}

pub struct CaseModifiers {
    pub setups: Vec<SetupRef>,
    pub skip: Option<Skip>,
    pub backend: Option<Backend>,
    pub requires: Vec<Requirement>,
}

pub struct Skip {
    pub reason: String,
    pub condition: Option<SkipCondition>,
}

pub enum SkipCondition {
    Mvcc,  // Skip when MVCC mode is enabled
}

pub enum Capability {
    Trigger,           // CREATE TRIGGER support
    Strict,            // STRICT tables support
    MaterializedViews, // Materialized views (experimental)
}

pub struct Requirement {
    pub capability: Capability,
    pub reason: String,
}

pub enum Backend {
    Rust,
    Cli,
    Js,
}

pub enum Expectation {
    Exact(Vec<String>),
    Pattern(String),
    Unordered(Vec<String>),
    Error(Option<String>),
}
```

### Validation

The parser validates:

1. At least one `@database` declaration exists
2. Readonly and writable databases are not mixed
3. Setup blocks are not present in readonly database files
4. All `@setup` references point to defined setups
5. Test names are unique within a file
