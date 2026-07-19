# frozen_string_literal: true

module Turso
  class DB
    def initialize(path, **opts)
      opts[:experimental_features] = normalize_experimental_features(opts[:experimental_features])
      @database = Database.new(path, opts.transform_keys(&:to_s))
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

    def prepare(sql)
      @database.connection.prepare(sql)
    end

    VALID_TRANSACTION_MODES = %i[deferred immediate exclusive concurrent].freeze

    def transaction(mode = :immediate, &block)
      unless VALID_TRANSACTION_MODES.include?(mode)
        fail ArgumentError, "Invalid transaction mode: #{mode.inspect}. Valid modes: #{VALID_TRANSACTION_MODES.inspect}"
      end

      if block
        execute("BEGIN #{mode.to_s.upcase}")
        begin
          result = block.call(self)
          execute("COMMIT")
          result
        rescue StandardError
          execute("ROLLBACK")
          raise
        end
      else
        Transaction.new(self, mode)
      end
    end

    def execute_batch(sql)
      transaction do
        sql.split(/;\s*$/).each do |statement|
          stripped = statement.strip
          execute(stripped) unless stripped.empty?
        end
      end
      nil
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

    def busy_timeout=(ms)
      @database.connection.busy_timeout = ms
    end

    def query_timeout=(ms)
      @database.connection.query_timeout = ms
    end

    def interrupt
      @database.connection.interrupt
    end

    private

    def normalize_experimental_features(features)
      return nil if features.nil? || features.empty?

      Array(features).join(",")
    end
  end
end
