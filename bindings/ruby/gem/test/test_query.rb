# frozen_string_literal: true

require_relative "test_helper"

class TestQuery < Turso::TestCase
  def test_query_returns_rows
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.execute("INSERT INTO users VALUES (?)", "Alice")
    rows = db.query("SELECT name FROM users").to_a
    assert_equal 1, rows.length
    assert_equal "Alice", rows.first["name"]
  end

  def test_query_with_no_results
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    rows = db.query("SELECT name FROM users").to_a
    assert_empty rows
  end

  def test_statement_can_be_closed
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    stmt = db.prepare("SELECT * FROM users")
    assert_respond_to stmt, :close
    stmt.close
    assert_raises(Turso::Exception) { stmt.get }
  end
end
