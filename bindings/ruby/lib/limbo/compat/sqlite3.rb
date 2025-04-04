module Limbo::Compat::SQLite3
  class Database
    def initialize(path)
      @db = ::Limbo::Builder.new(path).build
      @conn = @db.connect
    end

    def execute(sql)
      case sql
      when /\ASELECT\s+.*\s+FROM\s+.*\z/i
        execute_query(sql)
      else
        @conn.prepare(sql).execute([])
      end
    end

    private

    def execute_query(sql)
      rows = @conn.prepare(sql).query([])
      result = []
      loop do
        row = rows.next
        break unless row
        vals = []
        idx = 0
        loop do
          val = row.get_value(idx)
          break unless val
          vals << val
          idx += 1
        end
        result << vals
      end
      result
    end
  end
end
