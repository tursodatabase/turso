# frozen_string_literal: true

module Turso
  class Error < StandardError; end
  class BusyError < Error; end
  class InterruptError < Error; end
  class MisuseError < Error; end
  class ConstraintError < Error; end
  class ReadonlyError < Error; end
  class DatabaseFullError < Error; end
  class NotAdbError < Error; end
  class CorruptError < Error; end
  class StatementClosedError < Error; end
  class NotSupportedError < RuntimeError; end
end
