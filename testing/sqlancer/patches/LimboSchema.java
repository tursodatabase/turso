package sqlancer.limbo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;

/**
 * Schema implementation for Limbo.
 *
 * This is a simplified version of SQLite3Schema that doesn't query
 * sqlite_temp_master (which Limbo doesn't support).
 */
public class LimboSchema extends AbstractSchema<SQLite3GlobalState, SQLite3Table> {

    private final List<String> indexNames;

    public LimboSchema(List<SQLite3Table> databaseTables, List<String> indexNames) {
        super(databaseTables);
        this.indexNames = indexNames;
    }

    public List<String> getIndexNames() {
        return indexNames;
    }

    public static LimboSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<SQLite3Table> databaseTables = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();

        // Query only sqlite_master (not sqlite_temp_master)
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT name, 'table' as category, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                    + "UNION SELECT name, 'view' as category, sql FROM sqlite_master WHERE type='view' "
                    + "GROUP BY name;")) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    String tableType = rs.getString("category");
                    String sql = rs.getString("sql");
                    boolean isView = tableType.equals("view");
                    boolean isReadOnly = isView;
                    boolean isVirtual = false;

                    List<SQLite3Column> columns = getColumns(con, tableName);
                    SQLite3Table table = new SQLite3Table(tableName, columns, null, isView, isReadOnly, isVirtual);
                    for (SQLite3Column c : columns) {
                        c.setTable(table);
                    }
                    databaseTables.add(table);
                }
            }
        }

        // Get indexes from sqlite_master only
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT name FROM sqlite_master WHERE type = 'index'")) {
                while (rs.next()) {
                    indexNames.add(rs.getString("name"));
                }
            }
        }

        return new LimboSchema(databaseTables, indexNames);
    }

    private static List<SQLite3Column> getColumns(SQLConnection con, String tableName) throws SQLException {
        List<SQLite3Column> columns = new ArrayList<>();

        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("PRAGMA table_info('" + tableName + "')")) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    boolean isNotNull = rs.getInt("notnull") == 1;
                    boolean isPrimaryKey = rs.getInt("pk") == 1;

                    SQLite3DataType type = parseDataType(dataType);
                    SQLite3Column column = new SQLite3Column(columnName, type,
                            isNotNull, isPrimaryKey, SQLite3CollateSequence.BINARY);
                    columns.add(column);
                }
            }
        }

        return columns;
    }

    private static SQLite3DataType parseDataType(String dataType) {
        if (dataType == null || dataType.isEmpty()) {
            return SQLite3DataType.NULL;
        }
        String upper = dataType.toUpperCase();
        if (upper.contains("INT")) {
            return SQLite3DataType.INT;
        } else if (upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT")) {
            return SQLite3DataType.TEXT;
        } else if (upper.contains("BLOB")) {
            return SQLite3DataType.BINARY;
        } else if (upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB")) {
            return SQLite3DataType.REAL;
        } else {
            return SQLite3DataType.NULL;
        }
    }

    public SQLite3Tables getRandomTableNonEmptyTables() {
        return new SQLite3Tables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public List<SQLite3Table> getDatabaseTablesWithoutViews() {
        return getDatabaseTables().stream()
                .filter(t -> !t.isView())
                .collect(java.util.stream.Collectors.toList());
    }
}
