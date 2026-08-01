# SQLTest Syntax Highlighting for VS Code

Syntax highlighting for Turso's `.sqltest` test files.

## Installation

1. Open VS Code (or your preferred VS Code fork)
2. Open Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
3. Run `Developer: Install Extension from Location...`
4. Select the `testing/runner/syntax-highlighter/vscode` directory

## Supported Syntax

### Database Declarations

```sqltest
@database :memory:
@database :temp:
@database :default:
@database :default-no-rowidalias:
@database path/to/file.db readonly
```

### File-Level Directives

```sqltest
@skip-file "reason to skip all tests"
@skip-file-if mvcc "reason to skip in MVCC mode"
@skip-file-if sqlite "reason to skip on sqlite backend"
@requires-file trigger "all tests need trigger support"
```

### Setup Blocks

```sqltest
setup schema {
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
}
```

### Test Blocks with Decorators

```sqltest
@setup schema
@skip "known bug"
@skip-if mvcc "not supported in MVCC"
@skip-if sqlite "sqlite has different behavior"
@backend rust
@requires trigger "needs trigger support"
test my-test {
    SELECT * FROM users;
}
expect {
    1|Alice
}
```

### Snapshot Blocks

```sqltest
@setup schema
@backend cli
@requires strict "needs strict tables"
snapshot query-plan {
    SELECT * FROM users WHERE id = 1;
}
```

### Expect Block Modifiers

```sqltest
expect { ... }           # Exact match
expect error { ... }     # Expect error
expect pattern { ... }   # Regex pattern match
expect unordered { ... } # Set comparison
expect raw { ... }       # Raw output
expect @rust { ... }     # Backend-specific expectation
```

## Features

- Full SQL syntax highlighting inside `setup`, `test`, and `snapshot` blocks (via embedded SQL grammar)
- Keyword highlighting for `@database`, `@setup`, `@skip`, `@skip-if`, `@backend`, `@requires`, `test`, `expect`, `snapshot`
- Constant highlighting for database types (`:memory:`, `:temp:`, etc.)
- Constant highlighting for skip conditions (`mvcc`, `sqlite`) and capabilities (`trigger`, `strict`, `materialized_views`)
- String highlighting for skip/requires reasons
- Comment highlighting (`# comment`)
