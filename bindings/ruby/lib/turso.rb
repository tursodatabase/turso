# frozen_string_literal: true

require_relative "turso/version"
require_relative "turso/turso" # Native extension

module Turso
  # Configure Turso global settings like logging.
  #
  # @param level [Symbol, String] Log level (:trace, :debug, :info, :warn, :error)
  # @param logger [Object] Ruby Logger-compatible object
  # @yield [log] Yields Turso::Log for custom processing
  # @return [true] if setup was successful
  # @raise [Turso::Error] if configuration fails
  def self.setup(level: :info, logger: nil, &block)
    return true if @setup_called

    @logger_callback = if block
      block
    elsif logger
      proc { |log| logger.add(map_severity(log.level), log.message, "Turso") }
    else
      nil
    end

    _native_setup(level.to_s.downcase, @logger_callback)
    @setup_called = true

    # Start a background thread to consume logs from the native queue
    @log_thread = Thread.new do
      Thread.current.name = "turso-logger" if Thread.current.respond_to?(:name=)
      loop do
        _native_poll_logs
        sleep 0.05
      end
    rescue StandardError => e
      warn "[Turso] Internal logger thread failed: #{e.message}"
    end

    # Clear the callback and stop the thread before Ruby exits 
    at_exit do
      _clear_logger
      @log_thread&.kill
    end
    true
  end

  private

  # Maps Turso log levels to Ruby Logger severity constants.
  # :trace and :debug both map to DEBUG (0).
  def self.map_severity(level_sym)
    case level_sym
    when :trace, :debug then 0 
    when :info          then 1 
    when :warn          then 2 
    when :error         then 3 
    when :fatal         then 4 
    else 1 # defaults info
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

# this is just for reference/api
module Turso
  # Represents a log entry from the Turso engine.
  class Log
    # @!method level
    #   @return [Symbol] Log level (:trace, :debug, :info, :warn, :error, :fatal)

    # @!method message
    #   @return [String] Log message

    # @!method target
    #   @return [String] Log target (e.g., "turso::ruby", "turso_core::wal")

    # @!method timestamp
    #   @return [Integer] Unix timestamp in microseconds

    # @!method file
    #   @return [String] Source file name

    # @!method line
    #   @return [Integer] Source line number
  end
end
