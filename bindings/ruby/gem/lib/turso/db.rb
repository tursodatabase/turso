# frozen_string_literal: true

module Turso
  class DB
    def initialize(path, **opts)
      @database = Database.new(path, opts)
    end

    def execute(sql, params = [])
      stmt = @database.connection.prepare(sql)
      stmt.bind_positional(params) unless params.empty?
      case stmt.execute
      when Integer
        @database.last_insert_rowid
      when 0
        @database.changes
      end
    ensure
      stmt&.finalize
    end

    def query(sql, params = [])
      stmt = @database.connection.prepare(sql)
      stmt.bind_positional(params) unless params.empty?
      ResultSet.new(stmt)
    end

    def transaction(&block)
      if block
        execute("BEGIN")
        begin
          result = block.call(self)
          execute("COMMIT")
          result
        rescue StandardError
          execute("ROLLBACK")
          raise
        end
      else
        Transaction.new(self)
      end
    end

    def total_changes
      @database.total_changes
    end

    def changes
      query("SELECT changes()").first&.values&.first.to_i
    end

    def close
      @database.close
    end

    def closed?
      !@database.open?
    end
  end
end
