# frozen_string_literal: true

require_relative "test_helper"

class TestTransaction < Turso::TestCase
  def test_transaction_commits
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction do |d|
      d.execute("INSERT INTO users VALUES (?)", ["Alice"])
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_rolls_back_on_error
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    assert_raises(StandardError) do
      db.transaction do |d|
        d.execute("INSERT INTO users VALUES (?)", ["Alice"])
        raise "boom"
      end
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_empty rows
  end
end
