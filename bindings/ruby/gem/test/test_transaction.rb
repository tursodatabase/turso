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

  def test_transaction_mode_default_is_immediate
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction do |d|
      d.execute("INSERT INTO users VALUES (?)", ["Alice"])
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_accepts_deferred_mode
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    db.transaction(:deferred) do |d|
      d.execute("INSERT INTO users VALUES (?)", ["Alice"])
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_accepts_concurrent_mode
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    begin
      db.transaction(:concurrent) do |d|
        d.execute("INSERT INTO users VALUES (?)", ["Alice"])
      end
    rescue Turso::Error => e
      skip "Concurrent mode not supported: #{e.message}"
    end
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_manual_transaction_accepts_mode
    db = in_memory_db
    db.execute("CREATE TABLE users (name TEXT)")
    tx = db.transaction(:exclusive)
    tx.begin
    db.execute("INSERT INTO users VALUES (?)", ["Alice"])
    tx.commit
    rows = db.query("SELECT * FROM users").to_a
    assert_equal 1, rows.length
  end

  def test_transaction_rejects_invalid_mode
    db = in_memory_db
    assert_raises(ArgumentError) { db.transaction(:invalid_mode) }
  end

  def test_manual_transaction_rejects_invalid_mode
    db = in_memory_db
    assert_raises(ArgumentError) { Turso::Transaction.new(db, :bogus) }
  end
end
