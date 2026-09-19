# frozen_string_literal: true

module Turso
  VALID_TRANSACTION_MODES = %i[deferred immediate exclusive concurrent].freeze

  class Transaction
    def initialize(db, mode = :immediate)
      unless VALID_TRANSACTION_MODES.include?(mode)
        fail ArgumentError, "Invalid transaction mode: #{mode.inspect}. Valid modes: #{VALID_TRANSACTION_MODES.inspect}"
      end
      @db = db
      @mode = mode
      @active = false
    end

    def begin
      @db.execute("BEGIN #{@mode.to_s.upcase}")
      @active = true
    end

    def commit
      @db.execute("COMMIT")
      @active = false
    end

    def rollback
      @db.execute("ROLLBACK")
      @active = false
    end
  end
end
