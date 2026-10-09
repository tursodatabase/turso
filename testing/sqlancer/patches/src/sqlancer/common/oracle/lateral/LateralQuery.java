package sqlancer.common.oracle.lateral;

import java.util.Collections;
import java.util.List;

/**
 * A query that reads a table and joins one to three LATERAL subqueries:
 *
 * <pre>
 * SELECT o.c0, s0.v0 FROM t0 AS o
 * LEFT JOIN LATERAL (SELECT i0.c1 AS v0 FROM t1 AS i0 WHERE (i0.c0 &lt; o.c0) AND (TRUE)) AS s0 ON TRUE
 * WHERE TRUE
 * </pre>
 *
 * A subquery can read the table and the LATERAL joins on its left, and it can join up to two more tables.
 */
public final class LateralQuery {

    private final String table;
    private final String alias;
    private final List<String> columns;
    private final List<Join> joins;
    private final String whereClause;

    LateralQuery(String table, String alias, List<String> columns, List<Join> joins, String whereClause) {
        this.table = table;
        this.alias = alias;
        this.columns = Collections.unmodifiableList(columns);
        this.joins = Collections.unmodifiableList(joins);
        this.whereClause = whereClause;
    }

    public String getTable() {
        return table;
    }

    public String getAlias() {
        return alias;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Join> getJoins() {
        return joins;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public enum JoinType {
        COMMA, CROSS, INNER, LEFT
    }

    public enum ComparisonOperator {
        EQUALS, NOT_EQUALS, LESS, LESS_EQUALS, GREATER, GREATER_EQUALS, IS_NOT_DISTINCT_FROM, IS_DISTINCT_FROM
    }

    /**
     * A column of a table, or a column that an earlier LATERAL join returns.
     */
    public static final class ColumnRef {

        private final String tableAlias;
        private final String columnName;
        private final int join;
        private final int column;

        private ColumnRef(String tableAlias, String columnName, int join, int column) {
            this.tableAlias = tableAlias;
            this.columnName = columnName;
            this.join = join;
            this.column = column;
        }

        static ColumnRef tableColumn(String tableAlias, String columnName) {
            return new ColumnRef(tableAlias, columnName, -1, -1);
        }

        static ColumnRef joinColumn(int join, int column) {
            return new ColumnRef(null, null, join, column);
        }

        public boolean isTableColumn() {
            return tableAlias != null;
        }

        public String getTableAlias() {
            return tableAlias;
        }

        public String getColumnName() {
            return columnName;
        }

        public int getJoin() {
            return join;
        }

        public int getColumn() {
            return column;
        }
    }

    /**
     * A comparison of two columns.
     */
    public static final class Comparison {

        private final ColumnRef left;
        private final ComparisonOperator operator;
        private final ColumnRef right;

        Comparison(ColumnRef left, ComparisonOperator operator, ColumnRef right) {
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        public ColumnRef getLeft() {
            return left;
        }

        public ComparisonOperator getOperator() {
            return operator;
        }

        public ColumnRef getRight() {
            return right;
        }
    }

    /**
     * A table that a LATERAL subquery reads. The ON comparisons of the first table are empty.
     */
    public static final class SubqueryTable {

        private final String table;
        private final String alias;
        private final List<Comparison> on;

        SubqueryTable(String table, String alias, List<Comparison> on) {
            this.table = table;
            this.alias = alias;
            this.on = Collections.unmodifiableList(on);
        }

        public String getTable() {
            return table;
        }

        public String getAlias() {
            return alias;
        }

        public List<Comparison> getOn() {
            return on;
        }
    }

    /**
     * A column that a LATERAL subquery returns. If the subquery has DISTINCT or LIMIT, the subquery can return the
     * column in its canonical form, so that equal values always look the same.
     */
    public static final class SelectedColumn {

        private final ColumnRef source;
        private final LateralColumnType sourceType;
        private final boolean canonical;
        private final LateralColumnType type;

        SelectedColumn(ColumnRef source, LateralColumnType sourceType, boolean canonical, LateralColumnType type) {
            this.source = source;
            this.sourceType = sourceType;
            this.canonical = canonical;
            this.type = type;
        }

        public ColumnRef getSource() {
            return source;
        }

        public LateralColumnType getSourceType() {
            return sourceType;
        }

        public boolean isCanonical() {
            return canonical;
        }

        public LateralColumnType getType() {
            return type;
        }
    }

    /**
     * A term of the ORDER BY clause of a LATERAL subquery.
     */
    public static final class OrderTerm {

        private final int column;
        private final boolean descending;

        OrderTerm(int column, boolean descending) {
            this.column = column;
            this.descending = descending;
        }

        public int getColumn() {
            return column;
        }

        public boolean isDescending() {
            return descending;
        }
    }

    /**
     * A LATERAL join: the join type, the ON clause, and the subquery.
     */
    public static final class Join {

        private final JoinType type;
        private final String onClause;
        private final String alias;
        private final Subquery subquery;

        Join(JoinType type, String onClause, String alias, Subquery subquery) {
            this.type = type;
            this.onClause = onClause;
            this.alias = alias;
            this.subquery = subquery;
        }

        Join withType(JoinType newType) {
            return new Join(newType, null, alias, subquery);
        }

        public JoinType getType() {
            return type;
        }

        public String getOnClause() {
            return onClause;
        }

        public String getAlias() {
            return alias;
        }

        public Subquery getSubquery() {
            return subquery;
        }
    }

    /**
     * The subquery of a LATERAL join.
     */
    public static final class Subquery {

        private final List<SubqueryTable> tables;
        private final boolean distinct;
        private final List<SelectedColumn> columns;
        private final Comparison correlation;
        private final boolean filterWithOr;
        private final String filter;
        private final List<OrderTerm> orderBy;
        private final Integer limit;

        Subquery(List<SubqueryTable> tables, boolean distinct, List<SelectedColumn> columns, Comparison correlation,
                boolean filterWithOr, String filter, List<OrderTerm> orderBy, Integer limit) {
            this.tables = Collections.unmodifiableList(tables);
            this.distinct = distinct;
            this.columns = Collections.unmodifiableList(columns);
            this.correlation = correlation;
            this.filterWithOr = filterWithOr;
            this.filter = filter;
            this.orderBy = Collections.unmodifiableList(orderBy);
            this.limit = limit;
        }

        public List<SubqueryTable> getTables() {
            return tables;
        }

        public boolean isDistinct() {
            return distinct;
        }

        public List<SelectedColumn> getColumns() {
            return columns;
        }

        public Comparison getCorrelation() {
            return correlation;
        }

        public boolean isFilterWithOr() {
            return filterWithOr;
        }

        public String getFilter() {
            return filter;
        }

        public List<OrderTerm> getOrderBy() {
            return orderBy;
        }

        public Integer getLimit() {
            return limit;
        }

        /**
         * Returns true if DISTINCT, ORDER BY or LIMIT can change the rows of the subquery. The JSON form then collects
         * the rows of the unchanged subquery.
         *
         * @return true if the subquery has DISTINCT, ORDER BY or LIMIT
         */
        public boolean hasDistinctOrderByOrLimit() {
            return distinct || !orderBy.isEmpty() || limit != null;
        }

        /**
         * Returns true if the subquery reads only the outer table and its own tables, and no earlier LATERAL join.
         *
         * @return true if no comparison and no selected column reads an earlier LATERAL join
         */
        public boolean readsOnlyTables() {
            if (!correlation.getLeft().isTableColumn() || !correlation.getRight().isTableColumn()) {
                return false;
            }
            for (SubqueryTable table : tables) {
                for (Comparison comparison : table.getOn()) {
                    if (!comparison.getLeft().isTableColumn() || !comparison.getRight().isTableColumn()) {
                        return false;
                    }
                }
            }
            return columns.stream().allMatch(column -> column.getSource().isTableColumn());
        }
    }
}
