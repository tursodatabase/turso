# Turso.Data.Native

This is an implementation package used by Turso .NET providers. Application
projects should reference a provider package such as
`Turso.Data.Sqlite.Provider` instead of referencing this package directly.

The package contains the combined `turso_sdk_kit` native runtime and the transitive
build support needed to load it on supported platforms.

`TursoBindings.Interrupt`, `SetQueryTimeout`, and `GetQueryTimeout` expose native
statement cancellation and execution deadlines to managed providers. Query timeout
stops active SQL execution and is not the lock-wait busy timeout.
