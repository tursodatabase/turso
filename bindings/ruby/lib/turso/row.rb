# frozen_string_literal: true

module Turso
  # Row represents a single row from a result set.
  #   row[:name]   - Symbol access (idiomatic Ruby)
  #   row["name"]  - String access
  #   row[0]       - Positional access
  #   row.to_h     - Convert to Hash
  #   row.keys     - Column names as symbols
  #
  # Row is Enumerable, so you can use map, each, etc.
  class Row
    include Enumerable

    # @param columns [Array<String>] Column names
    # @param values [Array] Row values
    def initialize(columns, values)
      @columns = columns.map(&:to_sym).freeze
      @values = values.freeze
      
      index = {}
      @columns.each_with_index do |col, i|
        index[col] = i
        index[col.to_s] = i
      end
      @index = index.freeze
    end

    # Access value by symbol, string, or integer index
    # @param key [Symbol, String, Integer]
    # @return [Object] The value
    def [](key)
      case key
      when Integer
        @values[key]
      else
        idx = @index[key]
        idx ? @values[idx] : nil
      end
    end

    # Iterate over values
    def each(&block)
      @values.each(&block)
    end

    # @return [Array<Symbol>] Column names
    def keys
      @columns.dup
    end

    # @return [Array] Values
    def values
      @values.dup
    end

    # Convert to Hash with symbol keys
    # @return [Hash{Symbol => Object}]
    def to_h
      @columns.zip(@values).to_h
    end

    # Convert to Array
    # @return [Array]
    def to_a
      @values.dup
    end

    # @return [Integer] Number of columns
    def length
      @values.length
    end
    alias_method :size, :length

    def inspect
      "#<Turso::Row #{to_h.inspect}>"
    end

    def ==(other)
      return false unless other.respond_to?(:to_h)
      to_h == other.to_h
    end
    alias_method :eql?, :==

    def hash
      [@columns, @values].hash
    end
  end
end
