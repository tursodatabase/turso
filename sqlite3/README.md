# SQLite3 Implementation for Turso

This directory contains a Rust implementation of the SQLite3 C API. The implementation serves as a compatibility layer between SQLite's C API and Turso's native Rust database implementation.

## Purpose

This implementation provides SQLite3 API compatibility for Turso, allowing existing applications that use SQLite to work with Turso without modification. The code:

1. Implements the SQLite3 C API functions in Rust
2. Translates between C and Rust data structures
3. Maps SQLite operations to equivalent Turso operations
4. Maintains API compatibility with SQLite version 3.42.0

## Testing Strategy

We employ a dual-testing approach to ensure complete compatibility with SQLite:

### Test Database Setup

Before running tests, you need to set up a test database:

```bash
# Create testing directory
mkdir -p ../../testing

# Create and initialize test database
sqlite3 ../../testing/testing.db ".databases"
```

This creates an empty SQLite database that both test suites will use.

### 1. C Test Suite (`/tests`)
- Written in C to test the exact same API that real applications use
- Can be compiled and run against both:
  - Official SQLite library (for verification)
  - Our Rust implementation (for validation)
- Serves as the "source of truth" for correct behavior

To run C tests against official SQLite:
```bash
cd tests
make clean
make LIBS="-lsqlite3"
./sqlite3-tests
```

To run C tests against our implementation:
```bash
cd tests
make clean
make LIBS="-L../../target/debug -lturso_sqlite3"
LD_LIBRARY_PATH=../../target/debug ./sqlite3-tests
```

### 2. Rust Tests (`src/lib.rs`)
- Unit tests written in Rust
- Test the same functionality as C tests
- Provide better debugging capabilities
- Help with development and implementation

To run Rust tests:
```bash
cargo test
```

### Why Two Test Suites?

1. **Behavior Verification**: C tests ensure our implementation matches SQLite's behavior exactly by running the same tests against both
2. **Development Efficiency**: Rust tests provide better debugging and development experience
3. **Complete Coverage**: Both test suites together provide comprehensive testing from both C and Rust perspectives

### Common Test Issues

1. **Missing Test Database**
   - Error: `SQLITE_CANTOPEN (14)` in tests
   - Solution: Create test database as shown in "Test Database Setup"

2. **Wrong Database Path**
   - Tests expect database at `../../testing/testing.db`
   - Verify path relative to where tests are run

3. **Permission Issues**
   - Ensure test database is readable/writable
   - Check directory permissions

## Implementation Notes

- All public functions are marked with `#[no_mangle]` and follow SQLite's C API naming convention
- Uses `unsafe` blocks for C API compatibility
- Implements error handling similar to SQLite
- Maintains thread safety guarantees of SQLite

## Contributing

When adding new features or fixing bugs:

1. Add C tests that can run against both implementations
2. Add corresponding Rust tests
3. Verify behavior matches SQLite by running C tests against both implementations
4. Ensure all existing tests pass in both suites
5. Make sure test database exists and is accessible

## Status

This is an ongoing implementation. Some functions are marked with `stub!()` macro, indicating they're not yet implemented. Check individual function documentation for implementation status. 