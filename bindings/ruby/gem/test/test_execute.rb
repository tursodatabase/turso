# frozen_string_literal: true

require_relative "test_helper"

class TestExecute < Turso::TestCase
  def test_execute_creates_table
    db = in_memory_db
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO users (name) VALUES (?)", ["Alice"])
    count = db.query("SELECT COUNT(*) FROM users").first[0]
    assert_equal 1, count
  end
end
