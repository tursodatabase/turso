# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require "minitest/autorun"
require "turso"

module Turso
  class TestCase < Minitest::Test
    def in_memory_db
      DB.new(":memory:")
    end
  end
end
