# frozen_string_literal: true

require_relative "test_helper"

class TestConnectionExecution < Turso::TestCase
  def test_connection_execute
    db = in_memory_db
    conn = db.connection
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    assert_equal 1, conn.changes
  end

  def test_connection_query
    db = in_memory_db
    conn = db.connection
    conn.execute("CREATE TABLE users (name TEXT)")
    conn.execute("INSERT INTO users VALUES (?)", "Alice")
    rows = conn.query("SELECT name FROM users").to_a
    assert_equal ["Alice"], rows.map { |r| r["name"] }
  end
end
