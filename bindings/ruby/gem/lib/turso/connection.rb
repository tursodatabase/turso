# frozen_string_literal: true

module Turso
  class Connection
    def initialize(native_connection)
      @native = native_connection
      @closed = false
    end

    def closed?
      @closed
    end

    def close
      return if closed?
      @native.close
      @closed = true
    end
  end
end
