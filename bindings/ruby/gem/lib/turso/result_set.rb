# frozen_string_literal: true

module Turso
  class ResultSet
    include Enumerable

    def initialize(statement)
      @statement = statement
    end

    def fields
      @statement.columns.map { |c| c[:name] }
    end

    def each(&block)
      @statement.each(&block)
    end

    def next
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
