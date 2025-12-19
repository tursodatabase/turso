# frozen_string_literal: true

require_relative "turso/version"
require_relative "turso/turso" # Native extension

module Turso
  # Configure Turso global settings like logging.
  #
  # @param level [Symbol, String] Log level (:trace, :debug, :info, :warn, :error)
  # @param logger [Object] Ruby Logger-compatible object
  # @yield [log] Yields Turso::Log for custom processing
  def self.setup(level: :info, logger: nil, &block)
    @setup_called ||= false
    if @setup_called
      warn "Turso.setup has already been called. Global settings are already locked."
    end

    @logger_callback = if block
      block
    elsif logger
      proc { |log| logger.add(map_severity(log.level), log.message, "Turso") }
    else
      nil
    end

    _native_setup(level.to_s.downcase, @logger_callback)
    @setup_called = true
  end

  private

  def self.map_severity(level_sym)
    case level_sym
    when :trace, :debug then 0 # Logger::DEBUG
    when :info  then 1 # Logger::INFO
    when :warn  then 2 # Logger::WARN
    when :error then 3 # Logger::ERROR
    else 4 # Logger::FATAL
    end
  end

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
