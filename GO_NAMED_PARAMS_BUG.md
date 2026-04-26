# Bug: Local Go driver fails with named parameters

## Summary

`TestParametersNamed/turso` fails because the local Go driver passes bare parameter names (e.g. `"a"`) to `turso_statement_named_position`, which requires the SQL prefix (e.g. `":a"`).

## Error

```
turso: unknown named parameter "a"
```

## Reproduce

```bash
cargo build -p turso_sync_sdk_kit
ln -sf $PWD/target/debug/libturso_sync_sdk_kit.dylib testing/go-drivers/
cd testing/go-drivers && DYLD_LIBRARY_PATH=. go test -v -count=1 -run TestParametersNamed ./...
```

## Root Cause

`bindings/go/driver_db.go`, function `bindArgs`, line 759:

```go
np := int(turso_statement_named_position(stmt, nv.Name))
```

Go's `database/sql` strips the prefix — `sql.Named("a", "one")` produces `Name = "a"`. But `turso_statement_named_position` (backed by `sdk-kit/src/rsapi.rs` `named_position`) requires the SQL placeholder prefix (`:`, `@`, or `$`). Without it, it returns -1 (not found).

The serverless driver is unaffected because it sends names over the Hrana HTTP protocol where the server resolves them without requiring a prefix.

## Fix

In `bindings/go/driver_db.go`, `bindArgs` function — prepend `":"` to the name before calling `turso_statement_named_position` when it doesn't already have a prefix:

```go
if nv.Name != "" {
    name := nv.Name
    // Go's database/sql provides names without the SQL prefix.
    // turso_statement_named_position requires the prefix (e.g. ":name").
    if !strings.HasPrefix(name, ":") && !strings.HasPrefix(name, "@") && !strings.HasPrefix(name, "$") {
        name = ":" + name
    }
    np := int(turso_statement_named_position(stmt, name))
    if np <= 0 {
        return fmt.Errorf("turso: unknown named parameter %q", nv.Name)
    }
    pos = np
}
```

This matches how `go-sqlite3` and `modernc.org/sqlite` handle the same situation — both prepend `:` before calling `sqlite3_bind_parameter_index`.

## Verification

After the fix, re-run:

```bash
cd testing/go-drivers && DYLD_LIBRARY_PATH=. go test -v -count=1 -run TestParametersNamed ./...
```

Both `TestParametersNamed/turso` and `TestParametersNamed/turso-serverless` should pass.
