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
| `@skip "reason"` | Skip this test with the given reason |

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

# Skipped test
@skip "known bug #123"
test select-buggy-feature {
    SELECT buggy();
}
expect {
    result
}
```

## Grammar (EBNF-like)

```ebnf
file            = { database_decl | setup_block | test_case }

database_decl   = "@database" database_spec NEWLINE
database_spec   = ":memory:" | ":temp:" | ":default:" | ":default-no-rowidalias:" | PATH ["readonly"]

setup_block     = "setup" IDENTIFIER block
test_case       = { decorator } "test" IDENTIFIER block expect_block

decorator       = "@setup" IDENTIFIER NEWLINE
                | "@skip" STRING NEWLINE

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
  - Keywords: `@database`, `@setup`, `@skip`, `setup`, `test`, `expect`
  - Modifiers: `error`, `pattern`, `unordered`, `readonly`
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
}

pub struct TestCase {
    pub name: String,
    pub sql: String,
    pub expectation: Expectation,
    pub setups: Vec<String>,
    pub skip: Option<String>,
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
