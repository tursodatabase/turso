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
        { name: @native.column_name(i - 1), type: @native.column_decltype(i - 1) }
      end
    end

    def reader?
      @native.column_count > 0
    end

    def step
      @native.step
    end

    def reset
      @native.reset
    end

    def close
      @native.finalize
    end

    private

    def bind_hash(hash)
      hash.each do |key, value|
        name = key.to_s
        name = ":#{name}" unless name.start_with?(/^[:@?$]/)
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
          @native.bind_blob(position, value.b)
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
        when 0 # done
          break
        when 1 # row
          if consume_rows
            row = build_row
            continue = yield row
            break unless continue
          end
        end
      end
    end

    def build_row
      Row.new(@native)
    end
  end
end
