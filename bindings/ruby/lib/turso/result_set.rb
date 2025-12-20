# frozen_string_literal: true

module Turso
  # ResultSet encapsulates the enumerability of a query's output.
  # It is a cursor over the data that the query returns.
  #
  # ResultSet is Enumerable, supporting:
  #   results.each { |row| ... }
  #   results.map { |row| row[:name] }
  #   results.to_a
  #
  # Follows sqlite3-ruby conventions.
  class ResultSet
    include Enumerable

    # @return [Array<String>] Column names
    attr_reader :columns

    # @param statement [Turso::Statement] The underlying statement
    # @param columns [Array<String>] Column names
    def initialize(statement, columns)
      @statement = statement
      @columns = columns.freeze
      @eof = false
    end

    # Reset the cursor to iterate again
    def reset
      @statement.reset!
      @eof = false
    end

    # @return [Boolean] Whether cursor has reached end
    def eof?
      @eof
    end

    # Fetch the next row
    # @return [Row, nil] Next row or nil if no more rows
    def next
      return nil if @eof

      row_data = @statement.step_row
      if row_data.nil?
        @eof = true
        return nil
      end

      Row.new(@columns, row_data)
    end

    # Iterate over all rows
    # @yield [Row] Each row
    def each
      return to_enum(:each) unless block_given?

      while (row = self.next)
        yield row
      end
    end

    # Fetch the next row as a Hash
    # @return [Hash, nil]
    def next_hash
      row = self.next
      row&.to_h
    end

    # Iterate yielding hashes
    def each_hash
      return to_enum(:each_hash) unless block_given?

      while (row = next_hash)
        yield row
      end
    end

    # @return [Integer] Number of rows changed by last execution.
    def rows_changed
      @statement.rows_changed
    end

    # Close the underlying statement
    def close
      @statement.finalize!
    end

    # @return [Boolean]
    def closed?
      @statement.closed?
    end
  end
end
