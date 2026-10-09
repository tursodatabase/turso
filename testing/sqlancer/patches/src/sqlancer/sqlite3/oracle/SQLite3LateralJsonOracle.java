package sqlancer.sqlite3.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.common.oracle.lateral.LateralColumnType;
import sqlancer.common.oracle.lateral.LateralJsonOracle;
import sqlancer.common.oracle.lateral.LateralQuery;
import sqlancer.common.oracle.lateral.LateralQuery.ComparisonOperator;
import sqlancer.common.oracle.lateral.LateralQuery.Join;
import sqlancer.common.oracle.lateral.LateralQuery.Subquery;
import sqlancer.common.oracle.lateral.LateralTable;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

/**
 * Compares LATERAL joins with their JSON form, {@code json_each} over a subquery that collects the rows with
 * {@code json_group_array}, as the LateralMatchesJsonEach property of the Turso simulator. SQLite does not support
 * LATERAL joins, so the oracle is for a DBMS with the SQLite dialect that supports them, such as Turso.
 *
 * JSON keeps an integer or a text without a change, but it does not keep the affinity and the collation of a column,
 * and it changes other values. Thus:
 * <ul>
 * <li>A subquery returns a column as it is only if the column has INTEGER affinity and only integers, or TEXT affinity
 * and only text. It returns the other columns with {@code quote()}, as the Turso property does.</li>
 * <li>A reference to a column of a LATERAL join has {@code COLLATE BINARY}, so that the collation of a comparison does
 * not depend on the form.</li>
 * <li>A column of a LATERAL join is compared only with columns whose affinity gives the same result in the two
 * forms.</li>
 * <li>The outer query returns each value with {@code quote()}, so that the comparison of the rows sees their
 * types.</li>
 * </ul>
 * The JSON form reads its tables with {@code NOT INDEXED}. See {@link LateralJsonOracle}.
 */
public class SQLite3LateralJsonOracle extends LateralJsonOracle<SQLite3GlobalState> {

    static final String LATERAL = "lateral";
    static final String EXACT_INTEGER = "integer";
    static final String EXACT_TEXT = "text";
    static final String QUOTED = "quoted";
    private static final String TEXT_AFFINITY = "TEXT";
    private static final List<String> NUMERIC_AFFINITIES = Arrays.asList("INTEGER", "REAL", "NUMERIC");
    private static final String TABLE_HINT = " NOT INDEXED";
    private static final Pattern FIRST_INDEX_COLUMN = Pattern.compile(
            "(?is)\\bON\\s+[\"`\\[]?\\w+[\"`\\]]?\\s*\\(\\s*[\"`\\[]?(\\w+)[\"`\\]]?\\s*(?:[,)]|COLLATE|ASC|DESC)");
    private static final String TURSO_ISSUE_9051 = "insufficient registers allocated for expression vector write";
    private static final String TURSO_BARE_COLLATE_CONDITION = "Collate in WHERE clause is not supported";
    private static final String TURSO_INTEGER_OVERFLOW = "integer overflow";
    private static final List<String> UNSUPPORTED_FEATURE_ERRORS = Arrays.asList("Parse error", "not yet implemented",
            "not implemented", "not supported", "unsupported", "no such function", "COLLATE", "INDEXED BY",
            "NOT INDEXED");

    private final ExpectedErrors predicateErrors;
    private final Map<String, SQLite3Table> tablesByName = new HashMap<>();
    private List<LateralTable> tables;
    private List<Integer> jsonJoins = Collections.emptyList();

    public SQLite3LateralJsonOracle(SQLite3GlobalState state) {
        super(state, ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                .with(TURSO_INTEGER_OVERFLOW, TURSO_ISSUE_9051, TURSO_BARE_COLLATE_CONDITION).build());
        predicateErrors = ExpectedErrors.newErrors().with(SQLite3Errors.getExpectedExpressionErrors())
                .with(SQLite3Errors.getMatchQueryErrors()).with(SQLite3Errors.getQueryErrors())
                .with(UNSUPPORTED_FEATURE_ERRORS).build();
    }

    @Override
    protected List<LateralTable> getTables() throws SQLException {
        if (tables == null) {
            List<LateralTable> newTables = new ArrayList<>();
            for (SQLite3Table table : state.getSchema().getDatabaseTablesWithoutViewsWithoutVirtualTables()) {
                if (table.isSystemTable()) {
                    continue;
                }
                try {
                    newTables.add(lateralTable(table));
                    tablesByName.put(table.getName(), table);
                } catch (IgnoreMeException e) {
                    // the table cannot be read
                }
            }
            tables = newTables;
        }
        return tables;
    }

    private LateralTable lateralTable(SQLite3Table table) throws SQLException {
        Map<String, String> declaredTypes = new HashMap<>();
        Set<String> indexedColumns = new HashSet<>();
        List<String> columnNames = table.getColumns().stream().map(SQLite3Column::getName).collect(Collectors.toList());
        List<Long> notIntegers = new ArrayList<>();
        List<Long> notTexts = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement()) {
            try (ResultSet rs = statement.executeQuery("PRAGMA table_xinfo(" + table.getName() + ")")) {
                while (rs.next()) {
                    declaredTypes.put(rs.getString("name"), rs.getString("type"));
                }
            }
            try (ResultSet rs = statement
                    .executeQuery("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = '"
                            + table.getName() + "' AND sql IS NOT NULL")) {
                while (rs.next()) {
                    String column = firstIndexColumn(rs.getString(1));
                    if (column != null) {
                        indexedColumns.add(column);
                    }
                }
            }
            List<String> counts = new ArrayList<>();
            for (String column : columnNames) {
                counts.add("COALESCE(SUM(typeof(" + column + ") NOT IN ('integer', 'null')), 0)");
                counts.add("COALESCE(SUM(typeof(" + column + ") NOT IN ('text', 'null')), 0)");
            }
            try (ResultSet rs = statement
                    .executeQuery("SELECT " + String.join(", ", counts) + " FROM " + table.getName())) {
                if (!rs.next()) {
                    throw new IgnoreMeException();
                }
                for (int column = 0; column < columnNames.size(); column++) {
                    notIntegers.add(rs.getLong(2 * column + 1));
                    notTexts.add(rs.getLong(2 * column + 2));
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        List<LateralTable.Column> columns = new ArrayList<>();
        for (int column = 0; column < columnNames.size(); column++) {
            String name = columnNames.get(column);
            if (!declaredTypes.containsKey(name)) {
                throw new IgnoreMeException();
            }
            String affinity = affinity(declaredTypes.get(name));
            LateralColumnType type = new LateralColumnType(
                    jsonKind(affinity, notIntegers.get(column) == 0, notTexts.get(column) == 0), null, affinity);
            boolean leadsAnIndex = indexedColumns.contains(name) || table.getColumns().get(column).isOnlyPrimaryKey();
            columns.add(new LateralTable.Column(name, type, leadsAnIndex, true));
        }
        return new LateralTable(table.getName(), table.getNrRows(state), columns);
    }

    static String firstIndexColumn(String createIndex) {
        Matcher matcher = FIRST_INDEX_COLUMN.matcher(createIndex);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1);
    }

    static String affinity(String declaredType) {
        String type = declaredType.toUpperCase(Locale.ROOT).replace(" GENERATED ALWAYS", "").trim();
        if (type.contains("INT")) {
            return "INTEGER";
        }
        if (type.contains("CHAR") || type.contains("CLOB") || type.contains("TEXT")) {
            return TEXT_AFFINITY;
        }
        if (type.contains("BLOB") || type.isEmpty()) {
            return "BLOB";
        }
        if (type.contains("REAL") || type.contains("FLOA") || type.contains("DOUB")) {
            return "REAL";
        }
        return "NUMERIC";
    }

    static String jsonKind(String affinity, boolean onlyIntegers, boolean onlyTexts) {
        if (affinity.equals("INTEGER") && onlyIntegers) {
            return EXACT_INTEGER;
        }
        if (affinity.equals(TEXT_AFFINITY) && onlyTexts) {
            return EXACT_TEXT;
        }
        return QUOTED;
    }

    @Override
    protected String predicate(LateralTable table, String alias) {
        return predicateThatSelectsARow(table, alias, () -> randomPredicate(table, alias));
    }

    private String randomPredicate(LateralTable table, String alias) {
        SQLite3Table original = tablesByName.get(table.getName());
        List<SQLite3Column> columns = original.getColumns().stream().map(column -> new SQLite3Column(column.getName(),
                column.getType(), false, column.isPrimaryKey(), column.getCollateSequence()))
                .collect(Collectors.toList());
        SQLite3Table aliasedTable = new SQLite3Table(alias, columns, original.getTableType(),
                original.hasWithoutRowid(), false, false, original.isReadOnly());
        columns.forEach(column -> column.setTable(aliasedTable));
        return SQLite3Visitor.asString(new SQLite3ExpressionGenerator(state).setColumns(columns).generateExpression());
    }

    @Override
    protected ExpectedErrors predicateErrors() {
        return predicateErrors;
    }

    @Override
    protected boolean canCompare(LateralColumnType first, LateralColumnType second) {
        boolean firstIsLateral = first.getGroup().equals(LATERAL);
        boolean secondIsLateral = second.getGroup().equals(LATERAL);
        if (firstIsLateral == secondIsLateral) {
            return !firstIsLateral;
        }
        LateralColumnType lateral = firstIsLateral ? first : second;
        String affinity = firstIsLateral ? second.getGroup() : first.getGroup();
        switch (lateral.getName()) {
        case EXACT_INTEGER:
            return NUMERIC_AFFINITIES.contains(affinity);
        case EXACT_TEXT:
            return affinity.equals(TEXT_AFFINITY);
        default:
            return true;
        }
    }

    @Override
    protected boolean alwaysSelectsCanonicalForms() {
        return true;
    }

    @Override
    protected LateralColumnType canonicalType(LateralColumnType type) {
        if (type.getGroup().equals(LATERAL)) {
            return type;
        }
        return new LateralColumnType(type.getName(), null, LATERAL);
    }

    @Override
    protected String canonicalExpression(String expression, LateralColumnType type) {
        if (type.getGroup().equals(LATERAL)) {
            return expression;
        }
        switch (type.getName()) {
        case EXACT_INTEGER:
            return expression;
        case EXACT_TEXT:
            return "(" + expression + " COLLATE BINARY)";
        default:
            return "quote(" + expression + ")";
        }
    }

    @Override
    protected String comparison(String left, ComparisonOperator operator, String right) {
        switch (operator) {
        case EQUALS:
            return left + " = " + right;
        case NOT_EQUALS:
            return left + " <> " + right;
        case LESS:
            return left + " < " + right;
        case LESS_EQUALS:
            return left + " <= " + right;
        case GREATER:
            return left + " > " + right;
        case GREATER_EQUALS:
            return left + " >= " + right;
        case IS_NOT_DISTINCT_FROM:
            return left + " IS " + right;
        case IS_DISTINCT_FROM:
            return left + " IS NOT " + right;
        default:
            throw new AssertionError(operator);
        }
    }

    @Override
    protected String outputColumn(String expression) {
        return "quote(" + expression + ")";
    }

    @Override
    protected String joinColumnRef(int join, int column) {
        if (jsonJoins.contains(join)) {
            return "((" + joinAlias(join) + ".value ->> " + column + ") COLLATE BINARY)";
        }
        return "(" + super.joinColumnRef(join, column) + " COLLATE BINARY)";
    }

    @Override
    protected String jsonQuery(LateralQuery query, List<Integer> jsonJoins) {
        this.jsonJoins = jsonJoins;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ").append(selectList(query)).append(" FROM ").append(query.getTable()).append(" AS ")
                    .append(query.getAlias());
            for (int position = 0; position < query.getJoins().size(); position++) {
                Join join = query.getJoins().get(position);
                if (jsonJoins.contains(position)) {
                    sb.append(joinClause(join,
                            "json_each((" + jsonArray(join.getSubquery()) + ")) AS " + join.getAlias()));
                } else {
                    sb.append(lateralJoin(join));
                }
            }
            sb.append(whereClause(query));
            return sb.toString();
        } finally {
            this.jsonJoins = Collections.emptyList();
        }
    }

    private String jsonArray(Subquery subquery) {
        if (subquery.hasDistinctOrderByOrLimit()) {
            List<String> values = new ArrayList<>();
            for (int column = 0; column < subquery.getColumns().size(); column++) {
                values.add("q." + columnName(column));
            }
            return "SELECT json_group_array(json_array(" + String.join(", ", values) + ")) FROM ("
                    + subquery(subquery, "", TABLE_HINT) + ") AS q";
        }
        List<String> values = subquery.getColumns().stream().map(this::selectedColumn).collect(Collectors.toList());
        return "SELECT json_group_array(json_array(" + String.join(", ", values) + "))"
                + fromAndWhere(subquery, TABLE_HINT);
    }
}
