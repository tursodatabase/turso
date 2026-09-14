# frozen_string_literal: true

require "forwardable"

module Turso
  class Database
    extend Forwardable

    def_delegators :connection, :prepare, :execute, :execute_batch, :query,
                   :get_first_row, :get_first_value, :busy_timeout, :busy_timeout=,
                   :query_timeout, :query_timeout=, :interrupt, :total_changes,
                   :changes, :last_insert_rowid

    def initialize(path = ":memory:", **options)
      options[:experimental_features] = normalize_experimental_features(options[:experimental_features])
      native_opts = options.each_with_object({}) { |(k, v), h| h[k.to_s] = v }
      @native = NativeDatabase.new(path, native_opts)
      @closed = false
    end

    private

    def normalize_experimental_features(features)
      return nil if features.nil? || features.empty?

      Array(features).join(",")
    end

    public

    def closed?
      @closed || !@native.open?
    end

    def close
      return if closed?
      @native.close
      @closed = true
    end

    def connection
      Connection.new(@native.connection)
    end

    def transaction(mode = :deferred)
      raise Turso::Exception, "database is closed" if closed?
      execute("BEGIN #{mode.to_s.upcase}")
      begin
        result = yield self
        execute("COMMIT")
        result
      rescue ::Exception
        execute("ROLLBACK")
        raise
      end
    end
  end
end
