# frozen_string_literal: true

module Turso
  class Row
    include Enumerable

    def initialize(native_statement)
      @native = native_statement
    end

    def to_a
      (0...@native.column_count).map { |i| @native.row_value(i) }
    end

    def [](key)
      case key
      when Integer
        @native.row_value(key)
      when String, Symbol
        @native.row_value(column_index(key.to_s))
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
    alias size length

    private

    def column_index(name)
      (0...@native.column_count).find do |i|
        @native.column_name(i) == name
      end || raise(IndexError, "column #{name} not found")
    end
  end
end
