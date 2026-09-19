# frozen_string_literal: true

require_relative "test_helper"

class TestExecute < Turso::TestCase
  def test_execute_creates_table
    db = in_memory_db
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO users (name) VALUES (?)", "Alice")
    count = db.query("SELECT COUNT(*) FROM users").first[0]
    assert_equal 1, count
  end

  def test_execute_batch_runs_multiple_statements
    db = in_memory_db
    db.execute_batch(<<~SQL)
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
      INSERT INTO users (name) VALUES ('Alice');
      INSERT INTO users (name) VALUES ('Bob');
    SQL
    rows = db.query("SELECT name FROM users ORDER BY name").to_a
    assert_equal ["Alice", "Bob"], rows.map { |r| r["name"] }
  end

  def test_boolean_params_are_bound_as_integers
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT, active INTEGER)")
    db.execute("INSERT INTO users (name, active) VALUES (?, ?)", "Alice", true)
    db.execute("INSERT INTO users (name, active) VALUES (?, ?)", "Bob", false)
    rows = db.query("SELECT name, active FROM users ORDER BY name").to_a
    assert_equal [["Alice", 1], ["Bob", 0]], rows.map { |r| [r["name"], r["active"]] }
  end
end
