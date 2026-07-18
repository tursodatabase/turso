# frozen_string_literal: true

require_relative "test_helper"

class TestDatabase < Turso::TestCase
  def test_open_memory_database
    db = in_memory_db
    refute db.closed?
    db.close
    assert db.closed?
  end
end
