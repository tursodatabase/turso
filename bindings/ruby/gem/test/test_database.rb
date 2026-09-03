# frozen_string_literal: true

require_relative "test_helper"

class TestDatabase < Turso::TestCase
  def test_open_memory_database
    db = in_memory_db
    refute db.closed?
    db.close
    assert db.closed?
  end

  def test_total_changes_tracks_all_writes
    db = in_memory_db
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    db.execute("INSERT INTO users (name) VALUES (?)", "Bob")
    assert_equal 2, db.total_changes
    db.execute("DELETE FROM users")
    assert_equal 4, db.total_changes
  end

  def test_changes_returns_last_statement_count
    db = in_memory_db
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    db.execute("INSERT INTO users (name) VALUES (?)", "Bob")
    assert_equal 1, db.changes
  end

  def test_database_exposes_connection
    db = in_memory_db
    conn = db.connection
    assert_kind_of Turso::Connection, conn
    assert_respond_to conn, :closed?
    assert_respond_to conn, :close
  end
end
