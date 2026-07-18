# frozen_string_literal: true

module Turso
  class Transaction
    def initialize(db)
      @db = db
      @active = false
    end

    def begin
      @db.execute("BEGIN")
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
