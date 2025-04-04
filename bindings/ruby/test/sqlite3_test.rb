require "minitest/autorun"
require "limbo"

puts "limbo version = #{Limbo::CORE_VERSION}"

SQLite3 = Limbo::Compat::SQLite3


describe Limbo::Compat::SQLite3 do
  before do
    @db = SQLite3::Database.new(":memory:")
    @db.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT)")
    @db.execute("INSERT INTO users VALUES (1, 'alice')")
    @db.execute("INSERT INTO users VALUES (2, 'bob')")
  end
  describe "select all users" do
    it "returns all users" do
      result = @db.execute("SELECT * FROM users")
      assert_equal [[1, "alice"], [2, "bob"]], result
    end
  end
  describe "select user ids" do
    it "returns user ids" do
      result = @db.execute("SELECT id FROM users")
      assert_equal [[1], [2]], result
    end
  end
end
