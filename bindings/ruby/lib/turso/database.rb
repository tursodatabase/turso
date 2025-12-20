# frozen_string_literal: true

require_relative "row"
require_relative "result_set"

module Turso
  # Database wraps a Turso database connection with a sqlite3-ruby compatible API.
  #
  # Example:
  #   Turso::Database.open("mydb.db") do |db|
  #     db.execute("SELECT * FROM users") { |row| puts row[:name] }
  #   end
  #
  class Database
    # @return [Boolean] Whether to return rows as hashes (default: false, returns Row objects)
    attr_accessor :results_as_hash

    # @return [Symbol] Default transaction mode (:deferred, :immediate, :exclusive)
    attr_accessor :default_transaction_mode

    class << self
      # Open a database file and return a Database instance.
      # If a block is given, the database is yielded and auto-closed.
      #
      # @param path [String] Path to database file or ":memory:"
      # @yield [Database] If block given
      # @return [Database, Object] Database instance or block result
      def open(path, &block)
        # Use native RbDatabase class (Rb prefix avoids collision with this Ruby class)
        native_db = Turso::RbDatabase.open(path)
        native_conn = native_db.connect
        database = new(native_conn)

        if block
          begin
            yield database
          ensure
            database.close
          end
        else
          database
        end
      end
    end

    # @param native_connection [Turso::Connection] Native connection from Rust
    def initialize(native_connection)
      @conn = native_connection
      @results_as_hash = false
      @default_transaction_mode = :deferred
    end

    # Execute SQL and iterate over results.
    # If no block given, returns a frozen array of rows.
    #
    # @param sql [String] SQL statement
    # @param bind_vars [Array] Parameters to bind
    # @yield [Row] Each row
    # @return [Array<Row>] Results if no block given
    def execute(sql, *bind_vars, &block)
      prepare(sql) do |stmt|
        stmt.bind_params(*bind_vars) unless bind_vars.empty?
        result = build_result_set(stmt)

        if block
          result.each(&block)
        else
          result.to_a.freeze
        end
      end
    end

    # Execute SQL and return first row only.
    #
    # @param sql [String] SQL statement
    # @param bind_vars [Array] Parameters to bind
    # @return [Row, nil]
    def get_first_row(sql, *bind_vars)
      execute(sql, *bind_vars).first
    end

    # Execute SQL and return first value of first row.
    #
    # @param sql [String] SQL statement
    # @param bind_vars [Array] Parameters to bind
    # @return [Object, nil]
    def get_first_value(sql, *bind_vars)
      row = get_first_row(sql, *bind_vars)
      row ? row[0] : nil
    end

    # Execute multiple SQL statements separated by semicolons.
    #
    # @param sql [String] SQL script
    # @return [nil]
    def execute_batch(sql)
      sql = sql.strip
      offset = 0

      while offset < sql.length
        result = @conn.prepare_first(sql[offset..])
        break if result.nil?

        stmt, tail_idx = result
        begin
          # Step through all results
          loop do
            status = stmt.step
            break if status == :done
          end
        ensure
          stmt.finalize!
        end

        offset += tail_idx
      end

      nil
    end

    # Prepare a statement. If block given, yields and auto-closes.
    #
    # @param sql [String] SQL statement
    # @yield [Statement] If block given
    # @return [Statement]
    def prepare(sql)
      stmt = Statement.new(@conn, sql)

      if block_given?
        begin
          yield stmt
        ensure
          stmt.close unless stmt.closed?
        end
      else
        stmt
      end
    end

    # Prepare the first statement from a multi-statement SQL string.
    # Returns [Statement, tail_offset] or nil if empty.
    #
    # @param sql [String] SQL containing one or more statements
    # @return [Array(Statement, Integer), nil] [statement, offset] or nil
    def prepare_first(sql)
      result = @conn.prepare_first(sql)
      return nil if result.nil?

      native_stmt, tail_offset = result
      [Statement.new_from_native(@conn, native_stmt), tail_offset]
    end

    # Execute SQL and return a live ResultSet cursor (for lazy iteration).
    # Unlike `execute` which returns Array, this returns an Enumerable cursor.
    #
    # @param sql [String] SQL statement
    # @param bind_vars [Array] Parameters to bind
    # @yield [Row] Each row if block given
    # @return [ResultSet] Live cursor
    def query(sql, *bind_vars, &block)
      stmt = Statement.new(@conn, sql)
      stmt.bind_params(*bind_vars) unless bind_vars.empty?
      result = build_result_set(stmt)

      if block
        begin
          result.each(&block)
        ensure
          stmt.close unless stmt.closed?
        end
      else
        result
      end
    end

    # Execute a transaction. Commits on success, rolls back on exception.
    #
    # @param mode [Symbol] :deferred, :immediate, or :exclusive
    # @yield [Database] self
    # @return [Object] Block result
    def transaction(mode = nil)
      mode ||= @default_transaction_mode

      if in_transaction?
        raise NotSupportedError, "cannot start a transaction within a transaction"
      end

      execute("BEGIN #{mode.to_s.upcase} TRANSACTION")

      if block_given?
        begin
          result = yield self
          commit
          result
        rescue => e
          rollback rescue nil
          raise
        end
      else
        true
      end
    end

    # Commit current transaction.
    # @return [true]
    def commit
      execute("COMMIT TRANSACTION")
      true
    end

    # Rollback current transaction.
    # @return [true]
    def rollback
      execute("ROLLBACK TRANSACTION")
      true
    end

    # @return [Boolean] Whether currently in a transaction
    def in_transaction?
      @conn.in_transaction?
    end

    # @return [Integer] Number of rows changed by last statement
    def changes
      @conn.changes
    end

    # @return [Integer] Last inserted row ID
    def last_insert_row_id
      @conn.last_insert_row_id
    end

    # Close the database connection.
    def close
      @conn.close
    end

    # @return [Boolean]
    def closed?
      @conn.closed?
    end

    private

    def build_result_set(stmt)
      ResultSet.new(stmt, stmt.columns)
    end
  end

  # Statement wraps a prepared SQL statement.
  class Statement
    include Enumerable

    # @return [Array<String>] Column names
    attr_reader :columns

    # @param connection [Turso::Connection] Native connection
    # @param sql [String] SQL to prepare
    def initialize(connection, sql)
      @conn = connection
      @native_stmt = connection.prepare(sql)
      @columns = @native_stmt.columns
      @closed = false
      ObjectSpace.define_finalizer(self, self.class.finalize(@native_stmt))
    end

    # Create Statement from an already-prepared native statement.
    # Used by prepare_first.
    #
    # @param native_stmt [Turso::RbStatement] Native prepared statement
    # @return [Statement]
    def self.new_from_native(connection, native_stmt)
      stmt = allocate
      stmt.instance_variable_set(:@conn, connection)
      stmt.instance_variable_set(:@native_stmt, native_stmt)
      stmt.instance_variable_set(:@columns, native_stmt.columns)
      stmt.instance_variable_set(:@closed, false)
      ObjectSpace.define_finalizer(stmt, finalize(native_stmt))
      stmt
    end

    def self.finalize(native_stmt)
      proc { native_stmt.finalize! unless native_stmt.closed? }
    end

    # Bind parameters to the statement.
    #
    # @param bind_vars [Array] Values to bind
    def bind_params(*bind_vars)
      # Native bind expects all values as array, binds positionally
      @native_stmt.bind(*bind_vars) unless bind_vars.empty?
    end

    # Execute the statement and return a ResultSet.
    #
    # @param bind_vars [Array] Optional parameters
    # @return [ResultSet]
    def execute(*bind_vars)
      if @executed
        reset!
      end
      bind_params(*bind_vars) unless bind_vars.empty?
      @executed = true
      ResultSet.new(self, @columns)
    end

    # Step once and return row data or nil if done.
    # @return [Array, nil]
    def step_row
      result = @native_stmt.step
      return nil if result == :done

      @native_stmt.row
    end

    # Step the statement
    # @return [Symbol] :row or :done
    def step
      @native_stmt.step
    end

    # Get current row values
    # @return [Array]
    def row
      @native_stmt.row
    end

    # Reset statement for re-execution.
    def reset!
      @native_stmt.reset!
    end

    # @return [Integer] Number of rows changed by last execution.
    def rows_changed
      @native_stmt.rows_changed
    end

    # Finalize (close) the statement.
    def finalize!
      return if @closed
      @native_stmt.finalize!
      @closed = true
    end
    alias_method :close, :finalize!

    # @return [Boolean]
    def closed?
      @closed
    end

    # Iterate over result rows.
    def each
      return to_enum(:each) unless block_given?

      loop do
        result = step
        break if result == :done
        yield Row.new(@columns, row)
      end
    end
  end
end
