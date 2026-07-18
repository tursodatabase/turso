# frozen_string_literal: true

module Turso
  class ResultSet
    include Enumerable

    def initialize(statement)
      @statement = statement
    end

    def each
      return to_enum unless block_given?

      columns = column_names
      loop do
        case @statement.step
        when 1 # ROW
          yield Row.new(columns, @statement.row)
        when 0 # DONE
          break
        else
          raise Turso::Error, "unexpected step status"
        end
      end
      self
    ensure
      @statement.finalize
    end

    def column_names
      count = @statement.column_count
      (0...count).map { |i| @statement.column_name(i) }
    end
  end
end
