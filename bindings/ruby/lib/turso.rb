# frozen_string_literal: true

require_relative "turso/version"
require_relative "turso/turso" # Native extension

module Turso
  # Open a database file and return a connection.
  #
  # @param path [String] Path to database file, or ":memory:" for in-memory
  # @yield [conn] If block given, yields connection and auto-closes
  # @return [Connection] Database connection
  def self.open(path, &block)
    db = Database.open(path)
    conn = db.connect

    if block
      begin
        yield conn
      ensure
        conn.close
      end
    else
      conn
    end
  end
end
