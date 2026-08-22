# frozen_string_literal: true

require_relative "test_helper"

class TestConcurrency < Turso::TestCase
  def test_statement_cannot_be_used_concurrently
    db = in_memory_db
    db.execute("CREATE TABLE t (x)")
    db.execute("INSERT INTO t VALUES (1),(2),(3)")
    stmt = db.prepare("SELECT * FROM t")

    t = Thread.new do
      assert_raises(Turso::Exception) { stmt.run }
    end

    stmt.step
    t.join
  end

  def test_statement_cannot_be_used_from_another_fiber
    db = in_memory_db
    db.execute("CREATE TABLE t (x)")
    db.execute("INSERT INTO t VALUES (1),(2),(3)")
    stmt = db.prepare("SELECT * FROM t")

    fiber = Fiber.new do
      assert_raises(Turso::Exception) { stmt.run }
    end
    fiber.resume
  end
end
