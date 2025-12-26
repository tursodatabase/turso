# frozen_string_literal: true

require_relative "turso/version"
require_relative "turso/errors"
require_relative "turso/pragmas"
require_relative "turso/turso"

require_relative "turso/row"
require_relative "turso/result_set"
require_relative "turso/database"

module Turso
  class << self
    # Open a database file and return a Database instance.
    # If a block is given, the database is yielded and auto-closed.
    #
    # @example Block form (recommended)
    #   Turso.open("mydb.db") do |db|
    #     db.execute("SELECT * FROM users") { |row| puts row[:name] }
    #   end
    #
    # @example Manual form
    #   db = Turso.open("mydb.db")
    #   db.execute("SELECT * FROM users")
    #   db.close
    #
    # @param path [String] Path to database file, or ":memory:" for in-memory
    # @yield [Database] If block given, yields database and auto-closes
    # @return [Database] Database instance
    def open(path, &block)
      Database.open(path, &block)
    end

    # Configure Turso global settings like logging.
    #
    # @param level [Symbol, String] Log level (:trace, :debug, :info, :warn, :error)
    # @param logger [Object] Ruby Logger-compatible object
    # @yield [log] Yields Turso::Log for custom processing
    # @return [true] if setup was successful
    def setup(level: :info, logger: nil, &block)
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
        Thread.current.name = "turso-logger"
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

    def map_severity(level_sym)
      case level_sym
      when :trace, :debug then 0
      when :info          then 1
      when :warn          then 2
      when :error         then 3
      when :fatal         then 4
      else 1
      end
    end
  end

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
