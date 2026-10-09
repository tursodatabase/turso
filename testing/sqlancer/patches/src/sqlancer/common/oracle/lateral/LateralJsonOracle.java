package sqlancer.common.oracle.lateral;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLGlobalState;
import sqlancer.common.oracle.AbstractComparisonReproducer;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.TestOracleUtils;
import sqlancer.common.oracle.UnexpectedErrorReproducer;
import sqlancer.common.oracle.lateral.LateralQuery.ColumnRef;
import sqlancer.common.oracle.lateral.LateralQuery.Comparison;
import sqlancer.common.oracle.lateral.LateralQuery.ComparisonOperator;
import sqlancer.common.oracle.lateral.LateralQuery.Join;
import sqlancer.common.oracle.lateral.LateralQuery.OrderTerm;
import sqlancer.common.oracle.lateral.LateralQuery.SelectedColumn;
import sqlancer.common.oracle.lateral.LateralQuery.Subquery;
import sqlancer.common.oracle.lateral.LateralQuery.SubqueryTable;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

/**
 * Compares LATERAL joins with their JSON form. The oracle makes a query with one to three LATERAL joins. The second
 * query changes a random non-empty set of these joins into a JSON form: a subquery collects the rows of the LATERAL
 * subquery into one JSON array, and a table function changes the array back into rows. The two queries must return the
 * same rows, in any order:
 *
 * <pre>
 * SELECT o.c0, s0.v0 FROM t0 AS o
 * LEFT JOIN LATERAL (SELECT i0.c1 AS v0 FROM t1 AS i0 WHERE (i0.c0 &lt; o.c0) AND (TRUE)) AS s0 ON TRUE WHERE TRUE;
 *
 * SELECT o.c0, s0.v0 FROM t0 AS o
 * LEFT JOIN json_to_recordset((SELECT json_agg(json_build_object('v0', i0.c1)) FROM t1 AS i0
 *   WHERE (i0.c0 &lt; o.c0) AND (TRUE))) AS s0(v0 integer) ON TRUE WHERE TRUE;
 * </pre>
 *
 * The JSON form declares the exact type of each column, so that the values do not change. If a subquery has DISTINCT or
 * LIMIT, it returns each column in a canonical form, so that the two forms select the same values. This oracle is a
 * port of the LateralMatchesJsonEach property of the Turso simulator.
 *
 * @param <G>
 *            the DBMS-specific global state class
 */
public abstract class LateralJsonOracle<G extends SQLGlobalState<?, ?>> implements TestOracle<G> {

    private static final int MAX_REPORTED_ROWS = 10;
    private static final int MAX_PREDICATE_ATTEMPTS = 3;

    protected final G state;
    protected final ExpectedErrors errors;

    private Reproducer<G> reproducer;
    private String lastQueryString;

    protected LateralJsonOracle(G state, ExpectedErrors errors) {
        this.state = state;
        this.errors = errors;
    }

    @Override
    public void check() throws Exception {
        reproducer = null;
        List<LateralTable> tables = getTables();
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        LateralQuery query = new LateralQueryGenerator(this, tables).generate();
        List<Integer> jsonJoins = pickJsonJoins(query.getJoins().size());
        String lateralQuery = lateralQuery(query);
        String jsonQuery = jsonQuery(query, jsonJoins);
        String jsonQueryDescription = describeJsonQuery(jsonQuery);
        lastQueryString = lateralQuery;
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(lateralQuery);
            state.getLogger().writeCurrent(jsonQueryDescription);
        }
        int columnCount = columnCount(query);

        List<String> lateralRows;
        List<String> jsonRows;
        try {
            lateralRows = fetchRows(state, lateralQuery, columnCount);
            jsonRows = runJsonQuery(state, jsonQuery, columnCount);
        } catch (AssertionError unexpectedError) {
            reproducer = errorReproducer(lateralQuery, jsonQuery, jsonQueryDescription, columnCount,
                    TestOracleUtils.getUnexpectedErrorMessage(unexpectedError));
            throw unexpectedError;
        }
        if (!sameRows(lateralRows, jsonRows)) {
            reproducer = new LateralJsonReproducer(lateralQuery, jsonQuery, jsonQueryDescription, columnCount);
            String report = mismatchReport(lateralQuery, jsonQueryDescription, lateralRows, jsonRows);
            state.getState().getLocalState().log(report);
            throw new AssertionError(report);
        }
    }

    @Override
    public Reproducer<G> getLastReproducer() {
        return reproducer;
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }

    /**
     * Returns the tables of the database, with the types of their columns.
     *
     * @return the tables that the oracle can read
     *
     * @throws SQLException
     *             if a query on the catalog fails
     */
    protected abstract List<LateralTable> getTables() throws SQLException;

    /**
     * Generates a random predicate on the columns of a table. Use {@link #predicateThatSelectsARow} to get a predicate
     * that selects at least one row.
     *
     * @param table
     *            the table
     * @param alias
     *            the alias of the table in the query
     *
     * @return the predicate as SQL text
     */
    protected abstract String predicate(LateralTable table, String alias);

    /**
     * Returns true if a comparison of the two types gives no error and gives the same result in the two forms.
     *
     * @param first
     *            the type of the first column
     * @param second
     *            the type of the second column
     *
     * @return true if the oracle can compare columns of these types
     */
    protected abstract boolean canCompare(LateralColumnType first, LateralColumnType second);

    /**
     * Returns the type of the canonical form of a value. Two equal values have the same canonical form, so that
     * DISTINCT and LIMIT select the same values in the two forms.
     *
     * @param type
     *            the type of the value
     *
     * @return the type of the canonical form, or null if equal values of the type always look the same
     */
    protected abstract LateralColumnType canonicalType(LateralColumnType type);

    /**
     * Returns the canonical form of a value.
     *
     * @param expression
     *            the value
     * @param type
     *            the type of the value
     *
     * @return the SQL expression of the canonical form
     */
    protected abstract String canonicalExpression(String expression, LateralColumnType type);

    /**
     * Returns true if a LEFT JOIN LATERAL subquery can return a column of the outer query or of an earlier join.
     *
     * @return false to select only columns of the tables of the subquery in a LEFT JOIN
     */
    protected boolean leftJoinCanSelectOuterColumns() {
        return true;
    }

    /**
     * Returns true if a LATERAL subquery can read the columns of an earlier LEFT JOIN whose ON clause is not TRUE.
     *
     * @return false to hide the columns of such a join from the joins on its right
     */
    protected boolean laterJoinsCanReadLeftJoinWithOnPredicate() {
        return true;
    }

    /**
     * Returns true if the subquery of a LEFT JOIN LATERAL can read more than one table.
     *
     * @return false to read only one table in the subquery of a LEFT JOIN
     */
    protected boolean leftJoinCanReadManyTables() {
        return true;
    }

    /**
     * Returns true if the WHERE clause of a LEFT JOIN LATERAL subquery can combine its correlation and its filter with
     * OR.
     *
     * @return false to combine them only with AND in a LEFT JOIN
     */
    protected boolean leftJoinCanFilterWithOr() {
        return true;
    }

    /**
     * Returns true if a LATERAL subquery returns each column in its canonical form, also without DISTINCT and LIMIT.
     *
     * @return true if the JSON form changes the values of a column that is not in its canonical form
     */
    protected boolean alwaysSelectsCanonicalForms() {
        return false;
    }

    /**
     * Returns the errors that a random predicate can cause in the query that makes sure that the predicate selects a
     * row.
     *
     * @return the expected errors
     */
    protected ExpectedErrors predicateErrors() {
        return errors;
    }

    /**
     * Returns the operators that the oracle can use to compare two columns.
     *
     * @return the comparison operators
     */
    protected List<ComparisonOperator> comparisonOperators() {
        return Arrays.asList(ComparisonOperator.values());
    }

    /**
     * Returns the comparison of two values as SQL text.
     *
     * @param left
     *            the left value
     * @param operator
     *            the operator
     * @param right
     *            the right value
     *
     * @return the comparison
     */
    protected abstract String comparison(String left, ComparisonOperator operator, String right);

    /**
     * Returns the query with the given joins in the JSON form.
     *
     * @param query
     *            the query
     * @param jsonJoins
     *            the positions of the joins to write in the JSON form
     *
     * @return the SQL text of the query
     */
    protected abstract String jsonQuery(LateralQuery query, List<Integer> jsonJoins);

    /**
     * Runs the query with the JSON form. A DBMS can change its settings for this query, so that the two forms do not
     * use the same plan.
     *
     * @param globalState
     *            the state with the connection to use
     * @param query
     *            the SQL text of the query
     * @param columnCount
     *            the number of columns of the query
     *
     * @return the rows of the query
     *
     * @throws SQLException
     *             if the DBMS fails
     */
    protected List<String> runJsonQuery(G globalState, String query, int columnCount) throws SQLException {
        return fetchRows(globalState, query, columnCount);
    }

    /**
     * Returns the statements that run the query with the JSON form, for the bug report.
     *
     * @param query
     *            the SQL text of the query
     *
     * @return the statements that {@link #runJsonQuery} runs
     */
    protected String describeJsonQuery(String query) {
        return query;
    }

    protected final String lateralQuery(LateralQuery query) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(selectList(query)).append(" FROM ").append(query.getTable()).append(" AS ")
                .append(query.getAlias());
        for (Join join : query.getJoins()) {
            sb.append(lateralJoin(join));
        }
        sb.append(whereClause(query));
        return sb.toString();
    }

    protected final String selectList(LateralQuery query) {
        List<String> columns = new ArrayList<>();
        for (String column : query.getColumns()) {
            columns.add(outputColumn(query.getAlias() + "." + column));
        }
        for (int join = 0; join < query.getJoins().size(); join++) {
            for (int column = 0; column < query.getJoins().get(join).getSubquery().getColumns().size(); column++) {
                columns.add(outputColumn(joinColumnRef(join, column)));
            }
        }
        return String.join(", ", columns);
    }

    /**
     * Returns the SQL text of a column in the select list of the outer query.
     *
     * @param expression
     *            the SQL text of the column
     *
     * @return the SQL text to select
     */
    protected String outputColumn(String expression) {
        return expression;
    }

    protected final String lateralJoin(Join join) {
        return joinClause(join, "LATERAL (" + subquery(join.getSubquery(), "", "") + ") AS " + join.getAlias());
    }

    protected final String joinClause(Join join, String table) {
        switch (join.getType()) {
        case COMMA:
            return ", " + table;
        case CROSS:
            return " CROSS JOIN " + table;
        case INNER:
            return " INNER JOIN " + table + " ON " + join.getOnClause();
        case LEFT:
            return " LEFT JOIN " + table + " ON " + join.getOnClause();
        default:
            throw new AssertionError(join.getType());
        }
    }

    protected final String whereClause(LateralQuery query) {
        return " WHERE " + query.getWhereClause();
    }

    /**
     * Returns the SQL text of a LATERAL subquery.
     *
     * @param subquery
     *            the subquery
     * @param selectHint
     *            the text to put after SELECT, for example an optimizer hint
     * @param tableHint
     *            the text to put after each table, for example an index hint
     *
     * @return the SQL text of the subquery
     */
    protected final String subquery(Subquery subquery, String selectHint, String tableHint) {
        List<String> columns = new ArrayList<>();
        for (int column = 0; column < subquery.getColumns().size(); column++) {
            columns.add(selectedColumn(subquery.getColumns().get(column)) + " AS " + columnName(column));
        }
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(selectHint);
        if (subquery.isDistinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(String.join(", ", columns));
        sb.append(fromAndWhere(subquery, tableHint));
        if (!subquery.getOrderBy().isEmpty()) {
            List<String> terms = new ArrayList<>();
            for (OrderTerm term : subquery.getOrderBy()) {
                terms.add(columnName(term.getColumn()) + (term.isDescending() ? " DESC" : " ASC"));
            }
            sb.append(" ORDER BY ").append(String.join(", ", terms));
        }
        if (subquery.getLimit() != null) {
            sb.append(" LIMIT ").append(subquery.getLimit());
        }
        return sb.toString();
    }

    /**
     * Returns the FROM and WHERE clauses of a LATERAL subquery.
     *
     * @param subquery
     *            the subquery
     * @param tableHint
     *            the text to put after each table, for example an index hint
     *
     * @return the SQL text, which starts with " FROM "
     */
    protected final String fromAndWhere(Subquery subquery, String tableHint) {
        StringBuilder sb = new StringBuilder(" FROM ");
        for (SubqueryTable table : subquery.getTables()) {
            if (!table.getOn().isEmpty()) {
                sb.append(" JOIN ");
            }
            sb.append(table.getTable()).append(" AS ").append(table.getAlias()).append(tableHint);
            if (!table.getOn().isEmpty()) {
                sb.append(" ON ").append(table.getOn().stream().map(this::comparison)
                        .map(comparison -> "(" + comparison + ")").collect(Collectors.joining(" AND ")));
            }
        }
        sb.append(" WHERE (").append(comparison(subquery.getCorrelation())).append(")");
        sb.append(subquery.isFilterWithOr() ? " OR " : " AND ");
        sb.append("(").append(subquery.getFilter()).append(")");
        return sb.toString();
    }

    protected final String selectedColumn(SelectedColumn column) {
        String expression = columnRef(column.getSource());
        if (column.isCanonical()) {
            return canonicalExpression(expression, column.getSourceType());
        }
        return expression;
    }

    protected final String columnRef(ColumnRef column) {
        if (column.isTableColumn()) {
            return column.getTableAlias() + "." + column.getColumnName();
        }
        return joinColumnRef(column.getJoin(), column.getColumn());
    }

    /**
     * Returns the SQL text of a column of a LATERAL join.
     *
     * @param join
     *            the position of the join
     * @param column
     *            the position of the column in the select list of the subquery
     *
     * @return the SQL text of the column
     */
    protected String joinColumnRef(int join, int column) {
        return joinAlias(join) + "." + columnName(column);
    }

    private String comparison(Comparison comparison) {
        return comparison(columnRef(comparison.getLeft()), comparison.getOperator(), columnRef(comparison.getRight()));
    }

    protected static String columnName(int position) {
        return "v" + position;
    }

    protected static String joinAlias(int position) {
        return "s" + position;
    }

    /**
     * Returns the first of a few random predicates that selects at least one row of the table without an error. If no
     * predicate does, it returns TRUE. A predicate that selects no row makes most comparisons compare two empty
     * results.
     *
     * @param table
     *            the table
     * @param alias
     *            the alias of the table in the query
     * @param randomPredicate
     *            makes a random predicate on the columns of the table
     *
     * @return the predicate as SQL text
     */
    protected final String predicateThatSelectsARow(LateralTable table, String alias,
            Supplier<String> randomPredicate) {
        for (int attempt = 0; attempt < MAX_PREDICATE_ATTEMPTS; attempt++) {
            String predicate = randomPredicate.get();
            if (countRows(table, alias, predicate) > 0) {
                return predicate;
            }
        }
        return "TRUE";
    }

    /**
     * Returns the number of rows of the table that the predicate selects, or 0 if the predicate causes an expected
     * error. A driver can report an error when it reads a row, not when it runs the query.
     *
     * @param table
     *            the table
     * @param alias
     *            the alias of the table in the predicate
     * @param predicate
     *            the predicate as SQL text
     *
     * @return the number of rows
     */
    protected long countRows(LateralTable table, String alias, String predicate) {
        String query = "SELECT COUNT(*) FROM " + table.getName() + " AS " + alias + " WHERE " + predicate;
        try (SQLancerResultSet result = new SQLQueryAdapter(query, predicateErrors()).executeAndGet(state)) {
            if (result == null || !result.next()) {
                return 0;
            }
            return result.getLong(1);
        } catch (SQLException e) {
            if (predicateErrors().errorIsExpected(e.getMessage())) {
                return 0;
            }
            throw new AssertionError(query, e);
        }
    }

    protected final List<String> fetchRows(G globalState, String query, int columnCount) throws SQLException {
        SQLQueryAdapter adapter = new SQLQueryAdapter(query, errors);
        List<String> rows = new ArrayList<>();
        try (SQLancerResultSet result = adapter.executeAndGet(globalState)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            while (result.next()) {
                List<String> values = new ArrayList<>();
                for (int column = 1; column <= columnCount; column++) {
                    String value = result.getString(column);
                    values.add(value == null ? "NULL" : "'" + value.replace("'", "''") + "'");
                }
                rows.add("(" + String.join(", ", values) + ")");
            }
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(query, e);
        }
        return rows;
    }

    private static List<Integer> pickJsonJoins(int joinCount) {
        List<Integer> joins = new ArrayList<>();
        for (int join = 0; join < joinCount; join++) {
            if (Randomly.getBoolean()) {
                joins.add(join);
            }
        }
        if (joins.isEmpty()) {
            joins.add((int) Randomly.getNotCachedInteger(0, joinCount));
        }
        return joins;
    }

    private static int columnCount(LateralQuery query) {
        int count = query.getColumns().size();
        for (Join join : query.getJoins()) {
            count += join.getSubquery().getColumns().size();
        }
        return count;
    }

    static boolean sameRows(List<String> first, List<String> second) {
        List<String> sortedFirst = new ArrayList<>(first);
        List<String> sortedSecond = new ArrayList<>(second);
        Collections.sort(sortedFirst);
        Collections.sort(sortedSecond);
        return sortedFirst.equals(sortedSecond);
    }

    private static String mismatchReport(String lateralQuery, String jsonQuery, List<String> lateralRows,
            List<String> jsonRows) {
        Map<String, Integer> difference = new TreeMap<>();
        for (String row : lateralRows) {
            difference.merge(row, 1, Integer::sum);
        }
        for (String row : jsonRows) {
            difference.merge(row, -1, Integer::sum);
        }
        List<String> onlyLateral = new ArrayList<>();
        List<String> onlyJson = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : difference.entrySet()) {
            for (int i = 0; i < Math.abs(entry.getValue()); i++) {
                (entry.getValue() > 0 ? onlyLateral : onlyJson).add(entry.getKey());
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("The LATERAL form returned ").append(lateralRows.size()).append(" rows and the JSON form returned ")
                .append(jsonRows.size()).append(" rows, and the rows are not the same.").append(System.lineSeparator());
        sb.append("-- LATERAL form: ").append(lateralQuery).append(';').append(System.lineSeparator());
        sb.append("-- JSON form: ").append(jsonQuery).append(';').append(System.lineSeparator());
        appendRows(sb, "only the LATERAL form returned", onlyLateral);
        appendRows(sb, "only the JSON form returned", onlyJson);
        return sb.toString();
    }

    private static void appendRows(StringBuilder sb, String description, List<String> rows) {
        sb.append("-- ").append(rows.size()).append(" rows that ").append(description);
        if (rows.isEmpty()) {
            sb.append('.').append(System.lineSeparator());
            return;
        }
        sb.append(':').append(System.lineSeparator());
        for (String row : rows.subList(0, Math.min(rows.size(), MAX_REPORTED_ROWS))) {
            sb.append("--   ").append(row).append(System.lineSeparator());
        }
        if (rows.size() > MAX_REPORTED_ROWS) {
            sb.append("--   ...").append(System.lineSeparator());
        }
    }

    private UnexpectedErrorReproducer<G> errorReproducer(String lateralQuery, String jsonQuery,
            String jsonQueryDescription, int columnCount, String expectedErrorMessage) {
        UnexpectedErrorReproducer.Execution<G> execution = globalState -> {
            fetchRows(globalState, lateralQuery, columnCount);
            runJsonQuery(globalState, jsonQuery, columnCount);
        };
        return new UnexpectedErrorReproducer<>(execution, expectedErrorMessage,
                queryLines(lateralQuery, jsonQueryDescription));
    }

    private static String queryLines(String lateralQuery, String jsonQueryDescription) {
        return "-- LATERAL form: " + lateralQuery + ';' + System.lineSeparator() + "-- JSON form: "
                + jsonQueryDescription + ';' + System.lineSeparator();
    }

    private final class LateralJsonReproducer extends AbstractComparisonReproducer<G, List<String>> {

        private final String lateralQuery;
        private final String jsonQuery;
        private final String jsonQueryDescription;
        private final int columnCount;

        LateralJsonReproducer(String lateralQuery, String jsonQuery, String jsonQueryDescription, int columnCount) {
            this.lateralQuery = lateralQuery;
            this.jsonQuery = jsonQuery;
            this.jsonQueryDescription = jsonQueryDescription;
            this.columnCount = columnCount;
        }

        @Override
        protected List<String> evaluateOriginal(G globalState) throws SQLException {
            return fetchRows(globalState, lateralQuery, columnCount);
        }

        @Override
        protected List<String> evaluateTransformed(G globalState) throws SQLException {
            return runJsonQuery(globalState, jsonQuery, columnCount);
        }

        @Override
        protected boolean sidesDiffer(List<String> lateralRows, List<String> jsonRows, G globalState) {
            return !sameRows(lateralRows, jsonRows);
        }

        @Override
        protected String mismatchHeaderLine() {
            return "-- On the database set up by the statements above, the following queries return different rows:";
        }

        @Override
        protected void appendQueryLines(StringBuilder sb) {
            sb.append(queryLines(lateralQuery, jsonQueryDescription));
        }
    }
}
