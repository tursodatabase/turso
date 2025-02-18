require 'minitest/autorun'
require 'rutie'
require_relative '../target/release/liblimbo'

module Limbo
  class Connection
    def initialize(path)
      @connection = RubyConnection.new(path)
    end

    def execute(sql)
      @connection.ruby_execute(sql)
    end
  end
end

class TestRubyBinding < Minitest::Test
  def setup
    @connection = Limbo::Connection.new(":memory:")
    @connection.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT)")
    @connection.execute("INSERT INTO users VALUES (1, 'alice')")
    @connection.execute("INSERT INTO users VALUES (2, 'bob')")
  end

  def test_select_all_users
    result = @connection.execute("SELECT * FROM users")
    assert_equal "1 alice\n2 bob\n", result
  end

  def test_select_user_ids
    result = @connection.execute("SELECT id FROM users")
    assert_equal "1 \n2 \n", result
  end

  def test_select_max_user_id
    result = @connection.execute("SELECT MAX(id) FROM users")
    assert_equal "2 \n", result
  end
end
