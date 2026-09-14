# frozen_string_literal: true

require_relative "test_helper"

class TestErrors < Turso::TestCase
  def test_constraint_exception_class
    db = in_memory_db
    db.execute("CREATE TABLE t (x TEXT NOT NULL)")
    assert_raises(Turso::ConstraintException) do
      db.execute("INSERT INTO t (x) VALUES (NULL)")
    end
  end

  def test_error_is_alias_for_exception
    assert_equal Turso::Exception, Turso::Error
  end

  def test_exception_is_base_class
    assert Turso::BusyException < Turso::Exception
    assert Turso::ConstraintException < Turso::Exception
    assert Turso::MisuseException < Turso::Exception
  end
end
