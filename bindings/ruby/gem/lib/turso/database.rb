# frozen_string_literal: true

module Turso
  class Database
    def initialize(path, **options)
      @native = NativeDatabase.new(path, **options)
      @closed = false
    end

    def closed?
      @closed || !@native.open?
    end

    def close
      return if closed?
      @native.close
      @closed = true
    end

    def prepare(sql)
      raise Turso::Exception, "database is closed" if closed?
      Statement.new(@native.connection.prepare(sql), @native)
    end

    def execute(sql, *bind_args)
      stmt = prepare(sql)
      begin
        stmt.bind(*bind_args) unless bind_args.empty?
        stmt.run
      ensure
        stmt.close
      end
    end

    def execute_batch(sql)
      raise Turso::Exception, "database is closed" if closed?
      @native.connection.execute_batch(sql)
    end

    def get_first_row(sql, *bind_args)
      stmt = prepare(sql)
      begin
        stmt.bind(*bind_args) unless bind_args.empty?
        stmt.get
      ensure
        stmt.close
      end
    end

    def get_first_value(sql, *bind_args)
      row = get_first_row(sql, *bind_args)
      row&.first
    end

    def transaction(mode = :deferred)
      raise Turso::Exception, "database is closed" if closed?
      execute("BEGIN #{mode.to_s.upcase}")
      begin
        result = yield self
        execute("COMMIT")
        result
      rescue Exception
        execute("ROLLBACK")
        raise
      end
    end

    def busy_timeout=(ms)
      @native.connection.busy_timeout = ms
    end

    def busy_timeout
      @native.connection.busy_timeout
    end

    def total_changes
      @native.total_changes
    end

    def changes
      @native.connection.changes
    end

    def last_insert_rowid
      @native.last_insert_rowid
    end

    def interrupt
      @native.connection.interrupt
    end
  end
end
