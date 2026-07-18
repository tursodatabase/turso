# frozen_string_literal: true

module Turso
  class Row
    def initialize(columns, values)
      @columns = columns
      @values = values
    end

    def [](key)
      case key
      when Integer then @values[key]
      when String, Symbol then @values[@columns.index(key.to_s)]
      end
    end

    def to_h
      @columns.zip(@values).to_h
    end

    def keys = @columns
    def values = @values

    def fetch(key, &fallback)
      value = self[key]
      return value unless value.nil?
      return fallback.call(key) if fallback

      raise KeyError, "key not found: #{key.inspect}"
    end
  end
end
