# frozen_string_literal: true

module Turso
  class Row
    include Enumerable

    def initialize(values, column_names)
      @values = values
      @column_names = column_names
    end

    def to_a
      @values.dup
    end

    def [](key)
      case key
      when Integer
        @values[key]
      when String, Symbol
        @values[column_index(key.to_s)]
      else
        raise ArgumentError, "invalid key type: #{key.class}"
      end
    end

    def each(&block)
      @values.each(&block)
    end

    def length
      @values.length
    end
    alias size length

    private

    def column_index(name)
      @column_names.index(name) || raise(IndexError, "column #{name} not found")
    end
  end
end
