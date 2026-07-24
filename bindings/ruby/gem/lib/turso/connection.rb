# frozen_string_literal: true

module Turso
  class Connection
    def initialize(native_connection)
      @native = native_connection
      @closed = false
      @owner = owner_token
    end

    def closed?
      @closed
    end

    def close
      return if closed?
      @native.close
      @closed = true
    end

    def prepare(sql)
      check_owner!
      Statement.new(@native.prepare(sql), self)
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
      check_owner!
      @native.execute_batch(sql)
    end

    def query(sql, *bind_args)
      stmt = prepare(sql)
      stmt.bind(*bind_args) unless bind_args.empty?
      ResultSet.new(stmt)
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

    def busy_timeout=(ms)
      check_owner!
      @native.busy_timeout = ms.to_i
    end

    def query_timeout=(ms)
      check_owner!
      @native.query_timeout = ms.to_i
    end

    def query_timeout
      check_owner!
      @native.query_timeout
    end

    def interrupt
      check_owner!
      @native.interrupt
    end

    def total_changes
      check_owner!
      @native.total_changes
    end

    def changes
      check_owner!
      @native.changes
    end

    def last_insert_rowid
      check_owner!
      @native.last_insert_rowid
    end

    private

    def owner_token
      [Thread.current.object_id, Fiber.current.object_id]
    end

    def check_owner!
      unless owner_token == @owner
        raise Turso::Exception, "connection is already in use by another thread or fiber"
      end
    end
  end
end
