# frozen_string_literal: true

require "fileutils"
require "tmpdir"
require_relative "test_helper"

class TestDatabase < Turso::TestCase
  def test_open_memory_database
    db = in_memory_db
    refute db.closed?
    db.close
    assert db.closed?
  end

  def test_readonly_open_reads_existing_data
    Dir.mktmpdir do |dir|
      path = File.join(dir, "data.db")
      db = Turso::Database.new(path)
      db.execute("CREATE TABLE t (x TEXT)")
      db.execute("INSERT INTO t (x) VALUES (?)", "a")
      db.close

      readonly_path = File.join(dir, "readonly.db")
      FileUtils.cp(path, readonly_path)
      FileUtils.cp("#{path}-wal", "#{readonly_path}-wal")

      ro = Turso::Database.new(readonly_path, readonly: true)
      assert_equal "a", ro.get_first_value("SELECT x FROM t")
      assert_raises(Turso::Exception) { ro.execute("INSERT INTO t (x) VALUES (?)", "b") }
      ro.close
    end
  end

  def test_file_must_exist_raises_for_missing_file
    Dir.mktmpdir do |dir|
      assert_raises(Turso::Exception) do
        Turso::Database.new(File.join(dir, "missing.db"), file_must_exist: true)
      end
    end
  end

  def test_default_open_creates_file
    Dir.mktmpdir do |dir|
      path = File.join(dir, "created.db")
      Turso::Database.new(path).close
      assert File.exist?(path)
    end
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
