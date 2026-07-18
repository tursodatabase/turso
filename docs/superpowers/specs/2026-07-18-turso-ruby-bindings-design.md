# Ruby bindings for Turso

## Status

Design approved. Ready for implementation planning.

## Goal

Provide Turso with a Ruby native-extension binding. The binding follows the
same layered pattern used by the JavaScript and Python bindings:

- A thin Rust crate in `bindings/ruby/` that exposes low-level
  `Database` / `Connection` / `Statement` objects using the `magnus` crate.
- A pure-Ruby gem named `turso` that provides idiomatic Ruby semantics on top
  of the Rust extension.

The result must be usable directly, and it must provide a clean seam for an
ActiveRecord adapter and a `squel` integration.

## Scope

- **Local database only** for the initial implementation.
- **Synchronous API first**. The Rust engine is internally asynchronous, but
  `turso_sdk_kit::rsapi` can be configured with `async_io: false`, which causes
  the engine to drive its own I/O loop internally and return a final result.
  The Ruby binding uses this mode so the public API is synchronous.
- **Turso Cloud sync-engine support** is explicitly out of scope for this
  phase. It can be added later as a second crate, analogous to
  `bindings/javascript/sync`.
- **ActiveRecord adapter** is not part of this phase; the design leaves a
  stable seam for one.
- **Ruby implementation target: MRI only**. `magnus` builds on `rb-sys`, which
  targets MRI. JRuby and TruffleRuby support are non-goals for the first
  version.

## Reference architecture

Turso already has three relevant binding styles:

- `bindings/javascript/src/lib.rs` — direct NAPI wrapper around `turso_core`.
  It duplicates the connection/prepare/step/state-machine lifecycle.
- `bindings/python/src/turso.rs` — wrapper around the higher-level
  `turso_sdk_kit::rsapi` API (`TursoDatabase`, `TursoConnection`,
  `TursoStatement`).
- `sdk-kit/src/rsapi.rs` — the SDK abstraction that Python and the Rust
  high-level API consume.

The Ruby binding should follow Python and wrap `turso_sdk_kit::rsapi`, not
reimplement the low-level JavaScript/NAPI state machine. This keeps the Rust
crate small, stable, and aligned with the rest of the workspace.

## Two-layer design

### Layer 1: Rust native extension (`bindings/ruby/`)

A workspace crate `turso_ruby` compiles to a `cdylib` and uses `magnus` to
expose Ruby classes under the `Turso` namespace.

`Turso::Database` owns the underlying `turso_core::Database` and its default
`Connection`. It is a 1:1 handle: opening a database also creates one
connection to it. `Turso::Connection` is exposed as a thin wrapper so an
ActiveRecord adapter can later model the connection separately if needed, but
for local DB usage the gem's `Turso::DB` will use the single connection owned
by the `Database` object.

| Ruby class | Wraps | Key methods |
|---|---|---|
| `Turso::Database` | `Arc<TursoDatabase>` | `new`, `connect`, `close`, `last_insert_rowid`, `changes`, `total_changes`, `in_transaction?`, `readonly?`, `memory?`, `path`, `open?` |
| `Turso::Connection` | `Arc<TursoConnection>` | `prepare`, `execute`, `query`, `interrupt`, `set_busy_timeout`, `set_query_timeout`, `get_auto_commit`, `close` |
| `Turso::Statement` | `Box<TursoStatement>` | `bind_positional`, `bind_named`, `parameter_count`, `parameter_name`, `step`, `row`, `execute`, `reset`, `finalize`, `columns`, `column_count`, `column_name`, `column_decltype` |
| `Turso::Status` | `TursoStatusCode` | constants `DONE`, `ROW`, `IO` |
| Error classes | `TursoError` variants | `Turso::Error`, `Turso::BusyError`, `Turso::InterruptError`, `Turso::ConstraintError`, etc. |

### Layer 2: Pure-Ruby gem (`turso`)

`lib/turso.rb` loads the native extension and provides the high-level surface:

```ruby
db = Turso::DB.new(":memory:")
db.execute("CREATE TABLE users (name TEXT)")
db.execute("INSERT INTO users VALUES (?)", ["Alice"])
db.query("SELECT * FROM users").each { |row| p row.to_h }
```

Main Ruby classes:

- `Turso::DB` — connection-like object with `execute`, `query`, `transaction`,
  `close`.
- `Turso::ResultSet` — `Enumerable`, streams rows lazily from the underlying
  statement as you iterate, supports `each`, `to_a`, `columns`, `column_types`.
- `Turso::Row` — supports `[]` by index/name, `to_h`, `fetch`, `values`, `keys`.
- `Turso::Transaction` — helper for `BEGIN` / `COMMIT` / `ROLLBACK` blocks.
  Nested `db.transaction` blocks in the first version either raise or flatten
  to a single transaction; savepoint semantics are left for the ActiveRecord
  adapter phase.

## Crate layout

```
bindings/ruby/
├── Cargo.toml
├── build.rs          # rb-sys-env setup if needed
├── src/
│   ├── lib.rs        # #[magnus::init], module registration
│   ├── database.rs   # TursoDatabase wrapper
│   ├── connection.rs # TursoConnection wrapper
│   ├── statement.rs  # TursoStatement wrapper
│   ├── value.rs      # Ruby <-> turso_core::Value conversion
│   └── error.rs      # TursoError -> Ruby exception mapping
└── tests/            # Rust-side unit/integration tests
```

The gem itself lives in its own tree (conventional Ruby gem layout):

```
gem/
├── lib/
│   └── turso.rb
├── lib/turso/
│   ├── db.rb
│   ├── result_set.rb
│   ├── row.rb
│   └── transaction.rb
├── ext/
│   └── turso_ruby/
│       ├── extconf.rb
│       └── Cargo.toml -> ../../Cargo.toml (or copies workspace deps)
├── test/
│   └── ...
├── Rakefile
├── turso.gemspec
└── Gemfile
```

For development inside the monorepo, the gem can load the extension from the
Rust build output directly.

## Workspace updates

- Add `bindings/ruby` to `[workspace]` `members` and `default-members` in the
  root `Cargo.toml`.
- Add `turso_ruby = { path = "bindings/ruby" }` to
  `[workspace.dependencies]`.

## Dependencies

```toml
[package]
name = "turso_ruby"
version.workspace = true
edition.workspace = true
license.workspace = true
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
magnus = { version = "0.8", features = ["rb-sys"] }
turso_sdk_kit = { workspace = true, features = ["fts"] }
turso_core = { workspace = true }
```

For gem packaging, the pure-Ruby gem depends on `rb_sys ~> 0.9` and uses
`rb_sys/mkmf` with `rake-compiler`.

## Value mapping

Ruby to Rust:

| Ruby type | Rust `turso_core::Value` |
|---|---|
| `nil` | `Value::Null` |
| `Integer` (`Fixnum`/`Bignum`) | `Value::Numeric(Numeric::Integer(i64))` if it fits in `i64`; otherwise raises `Turso::Error` |
| `Float` | `Value::Numeric(Numeric::Float(f64))` |
| `String` (UTF-8) | `Value::Text` |
| `String` (ASCII-8BIT / BINARY) | `Value::Blob` |
| unsupported | raises `TypeError` |

Rust to Ruby:

| `turso_core::Value` | Ruby type |
|---|---|
| `Null` | `nil` |
| `Integer(i)` | `Integer` |
| `Float(f)` | `Float` |
| `Text(s)` | `String` (UTF-8) |
| `Blob(b)` | `String` (ASCII-8BIT / BINARY) |

Blob strings use `ASCII-8BIT` encoding. This matches the `sqlite3` Ruby gem
convention and is what ActiveRecord and `squel` expect for binary columns.

## Error mapping

Map each `TursoError` variant to a distinct Ruby exception class under
`Turso::Error`:

```ruby
Turso::Error < StandardError
Turso::BusyError < Turso::Error
Turso::BusySnapshotError < Turso::Error
Turso::InterruptError < Turso::Error
Turso::ConstraintError < Turso::Error
Turso::ReadonlyError < Turso::Error
Turso::MisuseError < Turso::Error
Turso::DatabaseFullError < Turso::Error
Turso::NotADatabaseError < Turso::Error
Turso::CorruptError < Turso::Error
Turso::IoError < Turso::Error
```

Every Rust method returns `Result<T, magnus::Error>` and converts the
`TursoError` variant to the matching Ruby exception class with the original
message. The conversion uses an exhaustive `match` on `TursoError` so that
new upstream variants become compile errors rather than silently falling
through.

When possible, the original Rust error message and any underlying I/O cause
are preserved in the Ruby exception's message. `Turso::Error` is the
catch-all base class.

## Concurrency contract

- **MRI only**. JRuby and TruffleRuby are not targeted.
- **A single `Turso::Database`/`Connection`/`Statement` must not be used
  concurrently from multiple Ruby threads.** The binding releases the GVL
  during engine calls so other Ruby threads can run, but the underlying
  `TursoConnection` and `TursoStatement` are not thread-safe. The gem layer
  (`Turso::DB`) may serialize access with a `Mutex` in the future, but the
  initial low-level binding documents this as a caller responsibility.
- **Re-entrancy within the same thread is also invalid**: calling a method on
  a `Statement` while another method on the same object is still executing
  (for example, from an `interrupt` handler) raises `Turso::MisuseError`.

## GVL handling

Turso engine calls can block on I/O. Methods that call into the engine
(open, connect, prepare, step, execute, finalize, reset) release the Ruby
GVL around the work. Magnus does not expose a high-level
`thread_call_without_gvl` helper, so the implementation uses the lower-level
Ruby C API (`rb_thread_call_without_gvl2` / `rb_thread_call_with_gvl2`)
through `rb-sys` for engine entry points.

Invariant before releasing the GVL:
- All Ruby inputs (SQL strings, bound parameter values, option hashes) must
  be fully converted to owned Rust data (`String`, `Vec<u8>`,
  `turso_core::Value`).
- No Ruby `Value` objects are held across the GVL boundary.

After the engine call completes and the GVL is reacquired, all results are
converted to Ruby objects.

Panic safety: the closure passed to the without-GVL C API is wrapped in
`std::panic::catch_unwind`. Any panic is converted to a `Turso::Error` before
control returns to Ruby, preventing unwinding across the FFI boundary.

Because `sdk-kit` is used with `async_io: false`, the engine loops on I/O
internally and returns a final result, so the Ruby API remains synchronous and
no `Status::IO` codes leak out to Ruby code.

## Statement lifecycle

- `Turso::Database#prepare(sql)` creates a `Turso::Statement`.
- Each `Statement` wrapper uses a "busy" flag to reject concurrent use from
  another Ruby thread or re-entrant call on the same thread, raising
  `Turso::MisuseError` instead of a Rust panic.
- Statements are finalized automatically on garbage collection via Magnus'
  `DataTypeFunctions::free` / `Drop`.
- Explicit `finalize` is also exposed.
- `Database#close` finalizes any live statements, mirroring the JavaScript
  binding cleanup.
- GC finalizers must never block on I/O. If a `Statement` or `Database` is
  garbage-collected while the engine is busy, finalization drops Rust
  references only; explicit `close` / `finalize` remains the preferred
  cleanup path.

## Configuration / options

The Rust `Database` constructor accepts a Ruby Hash or keyword arguments:

```ruby
Turso::Database.new(path, {
  readonly: false,
  timeout: 5_000,              # busy timeout ms
  default_query_timeout: 30_000,
  file_must_exist: false,
  tracing: "info",
  experimental: ["views", "vacuum"],
  encryption: { cipher: :aes256gcm, hexkey: "..." }
})
```

The Ruby wrapper gem can offer a nicer builder:

```ruby
Turso::DB.new(":memory:", experimental: [:views, :vacuum])
```

## Testing strategy

1. **Rust unit tests in `bindings/ruby/src/`** for value conversion and error
   mapping.
2. **Rust integration tests** in `bindings/ruby/tests/` that exercise the
   wrappers without Ruby.
3. **Ruby tests** in the `turso` gem (`gem/test/`):
   - `test_database.rb` — open/close, `:memory:`, persistence.
   - `test_execute.rb` — DDL, DML, parameter binding, changes count.
   - `test_query.rb` — SELECT, rows, columns, types.
   - `test_transaction.rb` — BEGIN / COMMIT / ROLLBACK.
   - `test_interrupt.rb` — `Connection#interrupt` while another thread is
     blocked on I/O.

Because the project test harness (`cargo test`, `make test`) may not always
have Ruby available, Ruby binding tests should be skippable in CI when Ruby
is missing. Long-term CI should also test against the last 2–3 MRI minor
versions, since `rb-sys`/GVL APIs can be version-sensitive.

## Build / packaging

Local Rust development:

```bash
cargo build -p turso_ruby
```

Ruby gem development:

```bash
cd gem
bundle install
rake compile
rake test
```

The gem’s `extconf.rb` uses `rb_sys/mkmf` and
`create_rust_makefile("turso_ruby/turso_ruby")`.

For the packaged gem, `ext/turso_ruby/Cargo.toml` is a generated/vendored file
that contains the necessary dependencies so that `gem build`/`gem push` works
without relying on workspace symlinks (which do not survive packaging reliably
across platforms). Inside the monorepo the gem can load the extension from the
workspace build output for development.

## Implementation phases

1. **Bootstrap** — create `bindings/ruby/Cargo.toml`, add to workspace,
   `#[magnus::init]` that defines the `Turso` module and a smoke global
   function.
2. **Database** — wrap `TursoDatabase::new` / `open` / `connect` / `close`,
   expose `path`, `open?`, `memory?`.
3. **Statement + value conversion** — `prepare`, `bind_positional`, `execute`,
   `step`, `row`, `finalize`. Implement value mapping both ways.
4. **Connection helpers** — `execute` / `query` convenience methods on
   `Connection`, `last_insert_rowid`, `changes`, `total_changes`,
   `in_transaction`.
5. **Errors** — full `TursoError` -> Ruby exception mapping.
6. **Ruby wrapper gem** — `Turso::DB`, `ResultSet`, `Row`, `Transaction`.
7. **Tests + packaging** — Rust unit tests, Ruby gem tests, `extconf.rb`,
   `Rakefile`, gemspec.

## ActiveRecord adapter seam

An ActiveRecord adapter will be built on top of `Turso::DB` later. The current
Layer 1/Layer 2 surface already provides:

- Connection lifecycle and SQL execution.
- Positional and named parameter binding.
- Row metadata (column names and declared types).
- Transaction state (`in_transaction?`, `get_auto_commit`).

Still required for a full AR adapter (future work):

- Savepoint support.
- Schema introspection helpers around `PRAGMA table_info` / `PRAGMA index_list`.
- Quoting/escaping rules specific to SQLite.
- Transaction isolation hooks (SQLite isolation is limited, but AR expects
  the API shape).

These will be added without breaking the existing low-level API.

## Sync-engine forward note

The Turso Cloud sync engine will be added as a second crate later, analogous to
`bindings/javascript/sync`. At that time the public `Turso::DB` API can remain
unchanged by introducing a separate construction path (for example,
`Turso::DB.new` with a sync mode flag, or a distinct `Turso::Cloud` namespace
that still returns `Turso::DB`-compatible objects). The low-level
`Turso::Database`/`Connection` classes will be extended, not replaced.

## Versioning policy

The Ruby gem version always matches the workspace `turso_ruby` crate version.
Layer 1 and Layer 2 ship together in one gem, so they cannot drift.

## Decisions

| Topic | Decision |
|---|---|
| Gem name | `turso` |
| Ruby implementations | MRI only; JRuby/TruffleRuby are non-goals |
| Safe-integers toggle | Not needed; Ruby automatically promotes to `Bignum` |
| Blob representation | `String` with `ASCII-8BIT` encoding |
| Unsupported value coercion | Raise `TypeError` in the Rust layer |
| ResultSet semantics | Lazy streaming over `Statement#step` |
| Binding layer | Wrap `turso_sdk_kit::rsapi`, like Python |
| Sync/async | Synchronous API first using `async_io: false` |
| Sync engine | Out of scope for this phase |
| Database/Connection model | 1:1 — `Database` owns one `Connection` |

## Risks

- GVL release requires careful use of `rb-sys` low-level APIs. The
  implementation must not hold Ruby `Value` objects across the GVL boundary.
  All inputs are converted to owned Rust data before the GVL is released; all
  outputs are converted to Ruby objects only after the GVL is reacquired.
- `TursoStatement` is not `Send`/`Sync`. Magnus methods receive `&self`, so
  interior mutability via `RefCell` is required, plus a per-statement busy flag
  to reject concurrent or re-entrant use.
- Statement finalization on GC must be safe and must release the
  `Arc<Connection>` chain to avoid keeping the database alive after close.
- `Connection#interrupt` while another Ruby thread is inside a without-GVL
  engine call must be exercised in tests; this is the primary use case for
  releasing the GVL at all.

## See also

- `bindings/javascript/src/lib.rs` — JavaScript binding reference.
- `bindings/python/src/turso.rs` — Python binding reference.
- `sdk-kit/src/rsapi.rs` — SDK API to wrap.
- `docs/agent-guides/testing.md` — project testing conventions.
- `docs/agent-guides/code-quality.md` — project correctness rules.
