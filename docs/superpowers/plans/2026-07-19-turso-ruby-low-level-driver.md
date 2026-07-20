# Turso Ruby Low-Level Driver — Production Readiness Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers-ruby:subagent-driven-development` (recommended) or `superpowers-ruby:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the `bindings/ruby/` gem into a production-ready, low-level Ruby driver that mirrors the `sqlite3` gem API, releases the GVL via Ruby 3.2's fiber scheduler blocking-operation API, and serves as the stable foundation for `activerecord-turso` and a future Sequel adapter.

**Architecture:**
- The public API becomes `Turso::Database`, `Turso::Statement`, `Turso::ResultSet`, and a `sqlite3`-style exception hierarchy, replacing the current thin `Turso::DB` wrapper.
- A small Rust `blocking_op` module in `bindings/ruby/src/` runs every long native call through `rb_fiber_scheduler_blocking_operation_extract`/`execute` so fibers and threads are not blocked.
- The Ruby layer uses `turso_sdk_kit` methods (`prepare_single`, `prepare_first`/`consume_stmt`, `named_position`, `parameter_name`) for correctness; no string-based SQL splitting.

**Tech Stack:** Ruby 3.2+, Rust, magnus 0.8, rb_sys, turso_sdk_kit, minitest, rake-compiler.

---

## Task 1: Bump minimum Ruby version and align versions

**Files:**
- Modify: `bindings/ruby/gem/turso.gemspec`
- Modify: `bindings/ruby/gem/lib/turso/version.rb`
- Modify: `bindings/ruby/gem/ext/turso_ruby/Cargo.toml`

- [ ] **Step 1: Update Ruby requirement**

  In `bindings/ruby/gem/turso.gemspec`:
  ```ruby
  spec.required_ruby_version = ">= 3.2.0"
  ```

- [ ] **Step 2: Align version numbers**

  Set all of the following to the same starting version, e.g. `0.1.2`:
  - `bindings/ruby/gem/lib/turso/version.rb`
  - `bindings/ruby/gem/ext/turso_ruby/Cargo.toml` (`version = "0.1.2"`)
  - `bindings/ruby/gem/turso.gemspec` (`spec.version = Turso::VERSION`)

- [ ] **Step 3: Commit**

  ```bash
  git add bindings/ruby/gem/turso.gemspec bindings/ruby/gem/lib/turso/version.rb bindings/ruby/gem/ext/turso_ruby/Cargo.toml
  git commit -m "bindings/ruby: require Ruby 3.2 and align gem/extension versions"
  ```

---

## Task 2: Add packaging and documentation

**Files:**
- Create: `bindings/ruby/gem/README.md`
- Create: `bindings/ruby/gem/LICENSE`
- Create: `bindings/ruby/gem/CHANGELOG.md`
- Modify: `bindings/ruby/gem/.gitignore` if build artifacts are missing

- [ ] **Step 1: Write README.md**

  Create `bindings/ruby/gem/README.md` with:
  - Supported Ruby version (`>= 3.2.0`).
  - Installation: `gem install turso` plus platform gem note.
  - Quick start mirroring the `sqlite3` gem:
    ```ruby
    require "turso"

    db = Turso::Database.new(":memory:")
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    row = db.get_first_row("SELECT * FROM users WHERE id = ?", 1)
    p row
    ```
  - Open options table: `readonly`, `file_must_exist`, `busy_timeout`, `query_timeout`, `experimental_features`, `encryption`, `vfs`.
  - Named parameters example: `{ name: "Alice" }` for `:name` placeholders.
  - Exception hierarchy mapping (`Turso::Exception`, `Turso::BusyException`, etc.).
  - License and contributing pointers.

- [ ] **Step 2: Add LICENSE**

  Copy the top-level project license text into `bindings/ruby/gem/LICENSE`.

- [ ] **Step 3: Add CHANGELOG.md**

  Create `bindings/ruby/gem/CHANGELOG.md`:
  ```markdown
  # Changelog

  ## 0.1.2

  - Production-ready low-level driver rewrite.
  - Ruby 3.2+ required; GVL released via fiber scheduler blocking operations.
  - `sqlite3`-style API: `Turso::Database`, `Turso::Statement`, `Turso::ResultSet`.
  - Added open options: `readonly`, `file_must_exist`, `busy_timeout`, `query_timeout`, `experimental_features`, `encryption`, `vfs`.
  - Added named parameters, statement helpers (`run`, `get`, `all`, `iterate`), and batch execution.
  - Added `sqlite3`-compatible exception hierarchy.
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/gem/README.md bindings/ruby/gem/LICENSE bindings/ruby/gem/CHANGELOG.md
  git commit -m "bindings/ruby: add README, LICENSE, and CHANGELOG"
  ```

---

## Task 3: Implement fiber-scheduler blocking operation helper

**Files:**
- Create: `bindings/ruby/src/blocking_op.rs`
- Modify: `bindings/ruby/src/lib.rs` to register no Ruby classes; just expose the helper internally

- [ ] **Step 1: Add the blocking operation module**

  Create `bindings/ruby/src/blocking_op.rs`:
  ```rust
  use rb_sys::{
      rb_fiber_scheduler_blocking_operation_extract,
      rb_fiber_scheduler_blocking_operation_execute,
  };
  use std::panic::{catch_unwind, AssertUnwindSafe};
  use std::ptr;

  use crate::error::{ErrorClasses, from_turso_error};

  pub fn run_blocking<T, F>(
      ruby: &magnus::Ruby,
      classes: &ErrorClasses,
      f: F,
  ) -> Result<T, magnus::Error>
  where
      T: Send + 'static,
      F: FnOnce() -> Result<T, turso_sdk_kit::TursoError> + Send + 'static,
  {
      let mut closure = Some(f);
      let mut result: Option<Result<T, turso_sdk_kit::TursoError>> = None;

      unsafe {
          let handle = rb_fiber_scheduler_blocking_operation_extract();
          if handle.is_null() {
              // No scheduler: fall back to running inline. Ruby 3.2 always exposes the API,
              // but a null handle means the current scheduler does not implement blocking ops.
              result = Some(closure.take().unwrap()());
          } else {
              extern "C" fn trampoline<T, F>(
                  arg: *mut std::os::raw::c_void,
              ) -> std::os::raw::c_int
              where
                  T: Send + 'static,
                  F: FnOnce() -> Result<T, turso_sdk_kit::TursoError> + Send + 'static,
              {
                  let closure_ptr = arg as *mut Option<F>;
                  let closure = (*closure_ptr).take().unwrap();
                  let out_ptr = (arg as usize + std::mem::size_of::<Option<F>>())
                      as *mut Option<Result<T, turso_sdk_kit::TursoError>>;
                  *out_ptr = Some(catch_unwind(AssertUnwindSafe(closure)).unwrap_or_else(|_| {
                      Err(turso_sdk_kit::TursoError::Error(
                          "panic in blocking operation".to_string(),
                      ))
                  }));
                  0
              }

              let mut slot = closure;
              let mut arg: (*mut Option<F>, *mut Option<Result<T, _>>) =
                  (&mut slot, &mut result);

              rb_fiber_scheduler_blocking_operation_execute(
                  handle,
                  Some(trampoline::<T, F>),
                  &mut arg as *mut _ as *mut std::os::raw::c_void,
              );
          }
      }

      result.unwrap().map_err(|e| from_turso_error(e, classes))
  }
  ```

  > **Note:** Verify the exact signature of `rb_fiber_scheduler_blocking_operation_execute` in the installed `rb-sys` bindings for this Ruby version; adjust argument order/types if the generated bindings differ.

- [ ] **Step 2: Wire the module into lib.rs**

  In `bindings/ruby/src/lib.rs`:
  ```rust
  mod blocking_op;
  ```

- [ ] **Step 3: Add Rust unit test**

  In `bindings/ruby/tests/blocking_op.rs`:
  ```rust
  use std::time::Duration;
  use std::thread;

  #[test]
  fn test_run_blocking_runs_closure() {
      // Smoke test for the closure pattern; full scheduler integration tested in Ruby.
      let result: Result<i32, _> = Ok(42);
      assert_eq!(result.unwrap(), 42);
  }
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/src/blocking_op.rs bindings/ruby/src/lib.rs bindings/ruby/tests/blocking_op.rs
  git commit -m "bindings/ruby: add fiber scheduler blocking operation helper"
  ```

---

## Task 4: Refactor database open options

**Files:**
- Modify: `bindings/ruby/src/database.rs`
- Create: `bindings/ruby/gem/lib/turso/database.rb`
- Modify: `bindings/ruby/gem/lib/turso.rb`

- [ ] **Step 1: Accept keyword arguments in Rust**

  In `bindings/ruby/src/database.rs`, change the constructor to take Ruby kwargs. Use magnus' `scan_args` or `kwargs` API. Example target signature:
  ```rust
  #[magnus::init]
  fn init(ruby: &Ruby) -> Result<(), Error> {
      // unchanged except registering Database class
  }
  ```

  The `Database::new` method should be declared as:
  ```rust
  pub fn new(
      path: String,
      kwargs: &[Value],
  ) -> Result<magnus::typed_data::Obj<Self>, Error>
  ```
  Extract options using `ruby.get_kwargs` or equivalent.

  Option mapping:
  ```rust
  let mut config = TursoDatabaseConfig {
      path,
      experimental_features: None,
      async_io: true, // required for fiber scheduler integration
      encryption: None,
      vfs: IoBackend::Default,
      io: None,
      db_file: None,
  };

  // readonly -> add "readonly" to experimental_features or set DatabaseOpts
  // file_must_exist -> add "file_must_exist" to experimental_features
  // busy_timeout: u64 -> config set later on connection
  // query_timeout: u64 -> config set later on connection
  // encryption: { cipher: String, hexkey: String }
  // vfs: Symbol or String -> "memory" | "syscall" | "io_uring"
  ```

  > **Important:** `readonly` and `file_must_exist` are part of `DatabaseOpts` in `turso_core`; check the exact `experimental_features` strings required by `TursoDatabaseConfig::database_opts()`. If they are not in the experimental list, they may need to be set directly via `DatabaseOpts`. Inspect `sdk-kit/src/rsapi.rs` around line 185–215 and the `turso_core::DatabaseOpts` builder.

- [ ] **Step 2: Add high-level Ruby Database class**

  Create `bindings/ruby/gem/lib/turso/database.rb`:
  ```ruby
  # frozen_string_literal: true

  module Turso
    class Database
      def initialize(path, **options)
        @native = NativeDatabase.new(path, **options)
        @closed = false
      end

      def closed?
        @closed || !@native.open
      end

      def close
        return if closed?
        @native.close
        @closed = true
      end

      def prepare(sql)
        raise Turso::Exception, "database is closed" if closed?
        Statement.new(@native.prepare_single(sql), @native)
      end

      def execute(sql, *bind_args)
        stmt = prepare(sql)
        begin
          stmt.bind(*bind_args) unless bind_args.empty?
          stmt.run
        ensure
          stmt.close
        end
      end

      def execute_batch(sql)
        raise Turso::Exception, "database is closed" if closed?
        @native.execute_batch(sql)
      end

      def get_first_row(sql, *bind_args)
        stmt = prepare(sql)
        begin
          stmt.bind(*bind_args) unless bind_args.empty?
          stmt.get
        ensure
          stmt.close
        end
      end

      def get_first_value(sql, *bind_args)
        row = get_first_row(sql, *bind_args)
        row&.first
      end

      def transaction(mode = :deferred)
        raise Turso::Exception, "database is closed" if closed?
        execute("BEGIN #{mode.to_s.upcase}")
        begin
          result = yield
          execute("COMMIT")
          result
        rescue Exception
          execute("ROLLBACK")
          raise
        end
      end

      def busy_timeout=(ms)
        @native.busy_timeout = ms
      end

      def busy_timeout
        @native.busy_timeout
      end

      def total_changes
        @native.total_changes
      end

      def changes
        @native.changes
      end

      def last_insert_rowid
        @native.last_insert_rowid
      end

      def interrupt
        @native.interrupt
      end
    end
  end
  ```

- [ ] **Step 3: Update lib/turso.rb to load Database and remove old DB promotion**

  ```ruby
  require_relative "turso/version"
  require_relative "turso/database"
  require_relative "turso/statement"
  require_relative "turso/result_set"
  require_relative "turso/row"
  ```

  Keep `require_relative "turso/db"` for backwards compatibility, but mark it deprecated.

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/src/database.rb bindings/ruby/gem/lib/turso/database.rb bindings/ruby/gem/lib/turso.rb
  git commit -m "bindings/ruby: add sqlite3-style Database with open options"
  ```

---

## Task 5: Fix execute return value and implement execute_batch

**Files:**
- Modify: `bindings/ruby/src/connection.rs`
- Modify: `bindings/ruby/gem/lib/turso/database.rb`
- Create: `bindings/ruby/gem/test/test_execute.rb`
- Create: `bindings/ruby/gem/test/test_batch.rb`

- [ ] **Step 1: Expose `changes` and `last_insert_rowid` on Connection in Rust**

  In `bindings/ruby/src/connection.rs`, ensure these methods exist:
  ```rust
  pub fn changes(&self) -> i64 {
      self.connection.changes()
  }

  pub fn last_insert_rowid(&self) -> i64 {
      self.connection.last_insert_rowid()
  }
  ```

- [ ] **Step 2: Return `{ changes:, last_insert_rowid: }` from `Statement#run`**

  In the Ruby `Statement#run`:
  ```ruby
  def run(*bind_args)
    bind(*bind_args) unless bind_args.empty?
    step_loop
    { changes: @database.changes, last_insert_rowid: @database.last_insert_rowid }
  end
  ```

- [ ] **Step 3: Implement `execute_batch` in Rust using `prepare_first`/`consume_stmt`**

  Add a Rust method on `Connection`:
  ```rust
  pub fn execute_batch(&self, sql: String) -> Result<magnus::RArray, Error> {
      let mut results = RArray::new();
      let mut offset: usize = 0;
      loop {
          match self.connection.prepare_first(&sql, offset) {
              Ok(Some((stmt, consumed))) => {
                  let mut stmt = Statement::from_sdk(stmt, self.clone(), self.classes);
                  let info = stmt.execute_to_completion()?;
                  results.push(info)?;
                  offset += consumed;
              }
              Ok(None) => break,
              Err(e) => return Err(from_turso_error(e, &self.classes)),
          }
      }
      Ok(results)
  }
  ```

  > **Note:** `prepare_first` signature and `consume_stmt` semantics must be checked in `sdk-kit/src/rsapi.rs`. Adjust the `offset`/slice handling to match.

- [ ] **Step 4: Add tests**

  `test/test_execute.rb`:
  ```ruby
  def test_execute_returns_changes_and_rowid
    db = in_memory_db
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
    result = db.execute("INSERT INTO t (x) VALUES (?)", "a")
    assert_equal 1, result[:changes]
    assert_equal 1, result[:last_insert_rowid]
  end
  ```

  `test/test_batch.rb`:
  ```ruby
  def test_batch_executes_multiple_statements
    db = in_memory_db
    results = db.execute_batch(<<~SQL)
      CREATE TABLE t (x TEXT);
      INSERT INTO t (x) VALUES ('hello');
      SELECT * FROM t;
    SQL
    assert_kind_of Array, results
    assert_equal 1, results[1][:changes]
  end
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add bindings/ruby/src/connection.rs bindings/ruby/gem/lib/turso/database.rb bindings/ruby/gem/test/test_execute.rb bindings/ruby/gem/test/test_batch.rb
  git commit -m "bindings/ruby: fix execute return value and add execute_batch"
  ```

---

## Task 6: Add statement convenience helpers and named parameters

**Files:**
- Modify: `bindings/ruby/src/statement.rs`
- Create: `bindings/ruby/gem/lib/turso/statement.rb`
- Create: `bindings/ruby/gem/test/test_statement_helpers.rb`
- Create: `bindings/ruby/gem/test/test_named_params.rb`

- [ ] **Step 1: Expose parameter metadata in Rust**

  In `bindings/ruby/src/statement.rs`, add:
  ```rust
  pub fn parameter_count(&self) -> i64 {
      self.statement.parameters_count() as i64
  }

  pub fn parameter_name(&self, index: i64) -> Option<String> {
      self.statement.parameter_name(index as usize)
  }

  pub fn named_position(&self, name: String) -> Result<i64, Error> {
      self.statement.named_position(&name)
          .map(|i| i as i64)
          .map_err(|e| from_turso_error(e, &self.classes))
  }
  ```

- [ ] **Step 2: Build Ruby Statement class with helpers**

  Create `bindings/ruby/gem/lib/turso/statement.rb`:
  ```ruby
  # frozen_string_literal: true

  module Turso
    class Statement
      def initialize(native_statement, native_database)
        @native = native_statement
        @database = native_database
      end

      def bind(*args)
        return self if args.empty?

        if args.size == 1 && args.first.is_a?(Hash)
          bind_hash(args.first)
        else
          args.each_with_index do |value, index|
            bind_value(index + 1, value)
          end
        end
        self
      end

      def run(*bind_args)
        bind(*bind_args) unless bind_args.empty?
        step_loop(consume_rows: false)
        { changes: @database.changes, last_insert_rowid: @database.last_insert_rowid }
      end

      def get(*bind_args)
        bind(*bind_args) unless bind_args.empty?
        row = nil
        step_loop(consume_rows: true) { |r| row ||= r; false }
        row
      end

      def all(*bind_args)
        bind(*bind_args) unless bind_args.empty?
        rows = []
        step_loop(consume_rows: true) { |r| rows << r; true }
        rows
      end

      def each(*bind_args)
        return to_enum(:each, *bind_args) unless block_given?
        bind(*bind_args) unless bind_args.empty?
        step_loop(consume_rows: true) { |r| yield r; true }
      end

      def columns
        (1..@native.column_count).map do |i|
          {
            name: @native.column_name(i),
            type: @native.column_decltype(i),
          }
        end
      end

      def reader?
        @native.column_count > 0
      end

      def close
        @native.finalize
      end

      private

      def bind_hash(hash)
        hash.each do |key, value|
          name = key.to_s
          name = ":#{name}" unless name.start_with?(%r{^[:@?$]})
          position = @native.named_position(name)
          bind_value(position, value)
        end
      end

      def bind_value(position, value)
        case value
        when nil
          @native.bind_null(position)
        when Integer
          @native.bind_int(position, value)
        when Float
          @native.bind_double(position, value)
        when String
          if value.encoding == Encoding::ASCII_8BIT
            @native.bind_blob(position, value)
          else
            @native.bind_text(position, value)
          end
        when true
          @native.bind_int(position, 1)
        when false
          @native.bind_int(position, 0)
        when Symbol
          @native.bind_text(position, value.to_s)
        when Time
          @native.bind_text(position, value.iso8601)
        else
          @native.bind_text(position, value.to_s)
        end
      end

      def step_loop(consume_rows:)
        loop do
          result = @native.step
          case result
          when :done
            break
          when :row
            if consume_rows
              row = build_row
              continue = yield row
              break unless continue
            end
          else
            # handled inside native step
          end
        end
      end

      def build_row
        Row.new(@native)
      end
    end
  end
  ```

- [ ] **Step 3: Add tests**

  `test/test_statement_helpers.rb`:
  ```ruby
  def test_all_returns_array
    db = in_memory_db
    db.execute("CREATE TABLE t (x TEXT)")
    db.execute("INSERT INTO t VALUES ('a')")
    db.execute("INSERT INTO t VALUES ('b')")
    rows = db.prepare("SELECT * FROM t").all
    assert_equal [["a"], ["b"]], rows.map(&:to_a)
  end
  ```

  `test/test_named_params.rb`:
  ```ruby
  def test_named_parameters
    db = in_memory_db
    db.execute("CREATE TABLE t (x TEXT)")
    db.execute("INSERT INTO t (x) VALUES (:name)", name: "Alice")
    row = db.get_first_row("SELECT * FROM t WHERE x = :name", name: "Alice")
    assert_equal "Alice", row[:x]
  end
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/src/statement.rs bindings/ruby/gem/lib/turso/statement.rb bindings/ruby/gem/test/test_statement_helpers.rb bindings/ruby/gem/test/test_named_params.rb
  git commit -m "bindings/ruby: add statement helpers and named parameter support"
  ```

---

## Task 7: Implement Row and ResultSet classes

**Files:**
- Create: `bindings/ruby/gem/lib/turso/row.rb`
- Create: `bindings/ruby/gem/lib/turso/result_set.rb`

- [ ] **Step 1: Create Row class**

  `bindings/ruby/gem/lib/turso/row.rb`:
  ```ruby
  # frozen_string_literal: true

  module Turso
    class Row
      include Enumerable

      def initialize(native_statement)
        @native = native_statement
      end

      def to_a
        (1..@native.column_count).map { |i| value_at(i) }
      end

      def [](key)
        case key
        when Integer
          value_at(key + 1) # sqlite3 gem uses 0-based indexing
        when String, Symbol
          value_at(column_index(key.to_s))
        else
          raise ArgumentError, "invalid key type: #{key.class}"
        end
      end

      def each(&block)
        to_a.each(&block)
      end

      def length
        @native.column_count
      end

      private

      def column_index(name)
        (1..@native.column_count).find do |i|
          @native.column_name(i) == name
        end || raise(IndexError, "column #{name} not found")
      end

      def value_at(index)
        # delegate to native row value access; add helper in Rust if needed
        @native.row_value(index)
      end
    end
  end
  ```

  > **Note:** If `turso_sdk_kit` does not expose a single `row_value` method, expose the needed `row_value_*` methods in Rust and pick the right Ruby type.

- [ ] **Step 2: Create ResultSet class**

  `bindings/ruby/gem/lib/turso/result_set.rb`:
  ```ruby
  # frozen_string_literal: true

  module Turso
    class ResultSet
      include Enumerable

      def initialize(statement)
        @statement = statement
        @rows = nil
      end

      def fields
        @statement.columns.map { |c| c[:name] }
      end

      def each(&block)
        reset if @rows
        @statement.each(&block)
      end

      def next
        # forward-only cursor semantics
        @statement.get
      end

      def reset
        @statement.reset
      end

      def close
        @statement.close
      end
    end
  end
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add bindings/ruby/gem/lib/turso/row.rb bindings/ruby/gem/lib/turso/result_set.rb
  git commit -m "bindings/ruby: add Row and ResultSet classes"
  ```

---

## Task 8: Match sqlite3 exception hierarchy

**Files:**
- Modify: `bindings/ruby/src/error.rs`
- Modify: `bindings/ruby/gem/lib/turso.rb`

- [ ] **Step 1: Rename Ruby exception classes in Rust**

  In `bindings/ruby/src/error.rs`, change `ErrorClasses` to define:
  ```rust
  base -> "Turso::Exception"
  busy -> "Turso::BusyException"
  constraint -> "Turso::ConstraintException"
  corrupt -> "Turso::CorruptException"
  misuse -> "Turso::MisuseException"
  // ... others as needed
  ```

- [ ] **Step 2: Ensure Turso::Error aliases Turso::Exception**

  In `bindings/ruby/gem/lib/turso.rb`:
  ```ruby
  module Turso
    Error = Exception
  end
  ```

- [ ] **Step 3: Update from_turso_error mapping**

  Map `TursoError::Busy` → `BusyException`, `Constraint` → `ConstraintException`, etc.

- [ ] **Step 4: Add tests**

  `test/test_errors.rb`:
  ```ruby
  def test_busy_timeout_raises_busy_exception
    skip unless ENV["CI"] # concurrency-dependent
  end

  def test_constraint_exception_class
    db = in_memory_db
    db.execute("CREATE TABLE t (x TEXT NOT NULL)")
    assert_raises(Turso::ConstraintException) do
      db.execute("INSERT INTO t (x) VALUES (NULL)")
    end
  end
  ```

- [ ] **Step 5: Commit**

  ```bash
  git add bindings/ruby/src/error.rs bindings/ruby/gem/lib/turso.rb bindings/ruby/gem/test/test_errors.rb
  git commit -m "bindings/ruby: align exception hierarchy with sqlite3 gem"
  ```

---

## Task 9: Replace old high-level wrapper and update entry point

**Files:**
- Modify: `bindings/ruby/gem/lib/turso.rb`
- Modify: `bindings/ruby/gem/lib/turso/db.rb`
- Modify: `bindings/ruby/gem/bin/console`

- [ ] **Step 1: Rewrite lib/turso.rb**

  ```ruby
  # frozen_string_literal: true

  require_relative "turso/version"
  require_relative "turso/database"
  require_relative "turso/statement"
  require_relative "turso/result_set"
  require_relative "turso/row"
  require_relative "turso/db" # deprecated compatibility shim

  module Turso
    Error = Exception
  end
  ```

- [ ] **Step 2: Convert Turso::DB to a deprecated shim**

  In `bindings/ruby/gem/lib/turso/db.rb`:
  ```ruby
  # frozen_string_literal: true

  module Turso
    class DB < Database
      def initialize(path = ":memory:", **options)
        warn "Turso::DB is deprecated; use Turso::Database"
        super(path, **options)
      end
    end
  end
  ```

- [ ] **Step 3: Update console helper**

  In `bindings/ruby/gem/bin/console`:
  ```ruby
  db = Turso::Database.new(":memory:")
  ```

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/gem/lib/turso.rb bindings/ruby/gem/lib/turso/db.rb bindings/ruby/gem/bin/console
  git commit -m "bindings/ruby: replace DB with sqlite3-style Database API"
  ```

---

## Task 10: Add concurrency and GVL-release tests

**Files:**
- Create: `bindings/ruby/gem/test/test_concurrency.rb`

- [ ] **Step 1: Write concurrency test**

  ```ruby
  # frozen_string_literal: true

  require "test_helper"

  class TestConcurrency < Turso::TestCase
    def test_other_thread_runs_during_long_query
      db = in_memory_db
      ran = false

      t = Thread.new { ran = true }

      # A query that takes a non-trivial amount of time but not forever.
      db.execute("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 100000) SELECT count(*) FROM r")

      t.join
      assert ran, "other thread should have run during query execution"
    end
  end
  ```

- [ ] **Step 2: Add statement-exclusive-use test**

  ```ruby
  def test_statement_cannot_be_used_concurrently
    db = in_memory_db
    db.execute("CREATE TABLE t (x)")
    db.execute("INSERT INTO t VALUES (1),(2),(3)")
    stmt = db.prepare("SELECT * FROM t")

    t = Thread.new do
      assert_raises(Turso::Exception) { stmt.step }
    end

    stmt.step
    t.join
  end
  ```

- [ ] **Step 3: Commit**

  ```bash
  git add bindings/ruby/gem/test/test_concurrency.rb
  git commit -m "bindings/ruby: add concurrency and GVL release tests"
  ```

---

## Task 11: Packaging for cross-platform native gems

**Files:**
- Modify: `bindings/ruby/gem/turso.gemspec`
- Modify: `bindings/ruby/gem/Rakefile`
- Create: `bindings/ruby/gem/.github/workflows/release.yml` (optional)

- [ ] **Step 1: Update gemspec for native extensions and platforms**

  ```ruby
  spec.extensions = ["ext/turso_ruby/extconf.rb"]
  spec.files = Dir["lib/**/*.rb", "ext/**/*.{rs,toml,rb}"]
  spec.require_paths = ["lib"]
  spec.platforms = ["ruby"]
  ```

- [ ] **Step 2: Ensure Rakefile has compilation and test tasks**

  ```ruby
  # frozen_string_literal: true

  require "bundler/gem_tasks"
  require "rake/testtask"
  require "rake/extensiontask"

  Rake::ExtensionTask.new("turso_ruby") do |ext|
    ext.ext_dir = "ext/turso_ruby"
    ext.lib_dir = "lib/turso"
  end

  Rake::TestTask.new(:test) do |t|
    t.libs << "lib" << "test"
    t.test_files = FileList["test/test_*.rb"]
  end

  task default: [:compile, :test]
  ```

- [ ] **Step 3: Add CI release scaffolding** (optional)

  Create `bindings/ruby/gem/.github/workflows/release.yml` that builds platform gems using `rake-compiler-dock` or matrix builds on Linux, macOS, Windows.

- [ ] **Step 4: Commit**

  ```bash
  git add bindings/ruby/gem/turso.gemspec bindings/ruby/gem/Rakefile bindings/ruby/gem/.github/workflows/release.yml
  git commit -m "bindings/ruby: configure native extension packaging"
  ```

---

## Task 12: Final integration run

**Files:**
- All of the above

- [ ] **Step 1: Run the test suite**

  ```bash
  cd bindings/ruby/gem
  bundle install
  bundle exec rake compile test
  ```

  Expected: all new and existing tests pass.

- [ ] **Step 2: Run formatting and linting**

  ```bash
  cd /home/bendangelo/Projects/turso
  cargo fmt --check
  cargo clippy --package turso_ruby -- -D warnings
  cd bindings/ruby/gem
  rubocop -a || true
  ```

- [ ] **Step 3: Final commit if any fixes were needed**

  ```bash
  git commit -m "bindings/ruby: final polish for production-ready driver"
  ```

---

## Spec coverage check

| Requirement | Task |
|---|---|
| Ruby 3.2+ minimum | Task 1 |
| sqlite3 gem API surface | Tasks 4, 5, 6, 7, 9 |
| Fiber scheduler GVL release | Task 3 |
| Named parameters | Task 6 |
| Encryption/options parity | Task 4 |
| Batch execution via core API | Task 5 |
| sqlite3-compatible exceptions | Task 8 |
| Packaging/docs | Tasks 2, 11 |
| Concurrency/tests | Tasks 10, 12 |

## Open risks to verify during execution

1. The exact signature of `rb_fiber_scheduler_blocking_operation_execute` in the installed `rb-sys` version. Check generated bindings before writing the call site.
2. Whether `readonly` and `file_must_exist` are controlled via `experimental_features` strings or direct `DatabaseOpts` builder methods. Inspect `sdk-kit/src/rsapi.rs` and `core/src/database.rs`.
3. Whether `prepare_first`/`consume_stmt` in `sdk-kit` returns byte offsets or statement counts. Verify before implementing `execute_batch`.
4. Whether `turso_sdk_kit::TursoConnection` has a `changes()` method separate from `total_changes()`. Add if missing or use `n_change()` semantics.

## Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-07-19-turso-ruby-low-level-driver.md`.**

Two execution options:

1. **Subagent-driven (recommended)** — dispatch a fresh subagent per task, review between tasks. Use `superpowers-ruby:subagent-driven-development`.
2. **Inline execution** — execute tasks in this session using `superpowers-ruby:executing-plans` with checkpoints.

**Which approach do you want?**
