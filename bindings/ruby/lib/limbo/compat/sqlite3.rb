module Limbo::Compat::SQLite3
  class Database
    def initialize(path)
      @db = ::Limbo::Builder.new(path).build
      @conn = @db.connect
    end

    def execute(sql)
      @conn.prepare(sql).execute
    end
  end
end
