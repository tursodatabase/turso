# frozen_string_literal: true

require_relative "test_helper"

class TestTransaction < Turso::TestCase
  def test_transaction_commits
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction do |d|
      d.execute("INSERT INTO users VALUES (?)", "Alice")
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_rolls_back_on_error
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    assert_raises(StandardError) do
      db.transaction do |d|
        d.execute("INSERT INTO users VALUES (?)", "Alice")
        raise "boom"
      end
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_empty rows
  end

  def test_transaction_mode_default_is_deferred
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction do |d|
      d.execute("INSERT INTO users VALUES (?)", "Alice")
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_accepts_deferred_mode
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction(:deferred) do |d|
      d.execute("INSERT INTO users VALUES (?)", "Alice")
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_savepoint_commit
    conn = in_memory_db.connection
    conn.execute("CREATE TABLE users (name TEXT)")
    conn.execute("BEGIN")
    conn.execute("INSERT INTO users VALUES ('outer')")
    conn.savepoint("sp1")
    conn.execute("INSERT INTO users VALUES ('inner')")
    conn.release_savepoint("sp1")
    conn.execute("COMMIT")
    rows = conn.query("SELECT name FROM users ORDER BY name").to_a
    assert_equal ["inner", "outer"], rows.map { |r| r["name"] }
  end

  def test_savepoint_rollback
    conn = in_memory_db.connection
    conn.execute("CREATE TABLE users (name TEXT)")
    conn.execute("BEGIN")
    conn.execute("INSERT INTO users VALUES ('outer')")
    conn.savepoint("sp1")
    conn.execute("INSERT INTO users VALUES ('inner')")
    conn.rollback_to_savepoint("sp1")
    conn.execute("COMMIT")
    rows = conn.query("SELECT name FROM users").to_a
    assert_equal ["outer"], rows.map { |r| r["name"] }
  end
end
