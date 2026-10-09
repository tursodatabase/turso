package sqlancer.common.oracle.lateral;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.lateral.LateralQuery.ColumnRef;
import sqlancer.common.oracle.lateral.LateralQuery.Comparison;
import sqlancer.common.oracle.lateral.LateralQuery.ComparisonOperator;
import sqlancer.common.oracle.lateral.LateralQuery.Join;
import sqlancer.common.oracle.lateral.LateralQuery.JoinType;
import sqlancer.common.oracle.lateral.LateralQuery.OrderTerm;
import sqlancer.common.oracle.lateral.LateralQuery.SelectedColumn;
import sqlancer.common.oracle.lateral.LateralQuery.Subquery;
import sqlancer.common.oracle.lateral.LateralQuery.SubqueryTable;

/**
 * Generates a random {@link LateralQuery}. The largest possible result of the query has 10,000 rows or fewer.
 */
final class LateralQueryGenerator {

    private static final String TABLE_ALIAS = "o";
    private static final String TRUE = "TRUE";
    private static final int MAX_JOINS = 3;
    private static final int MAX_JOINED_TABLES = 2;
    private static final long MAX_RESULT_ROWS = 10_000;
    private static final int MAX_SELECTED_COLUMNS = 3;
    private static final int MAX_LIMIT = 3;

    private final LateralJsonOracle<?> oracle;
    private final List<LateralTable> tables;

    LateralQueryGenerator(LateralJsonOracle<?> oracle, List<LateralTable> tables) {
        this.oracle = oracle;
        this.tables = tables;
    }

    LateralQuery generate() {
        LateralTable smallestTable = tables.stream().min(Comparator.comparingLong(LateralTable::getRowCount)).get();
        long rowLimit = MAX_RESULT_ROWS / rows(smallestTable);
        LateralTable table = pickTable(tables, 2, rowLimit);
        if (table == null) {
            table = smallestTable;
        }
        List<VisibleColumn> visibleColumns = tableColumns(table, TABLE_ALIAS);
        long maxRows = rows(table);
        List<LateralTable> usedTables = new ArrayList<>();
        usedTables.add(table);
        List<Join> joins = new ArrayList<>();
        int joinCount = 1 + (int) Randomly.getNotCachedInteger(0, MAX_JOINS);
        for (int position = 0; position < joinCount; position++) {
            LateralTable innerTable = pickSubqueryTable(usedTables, MAX_RESULT_ROWS / maxRows);
            if (innerTable == null) {
                if (!joins.isEmpty()) {
                    break;
                }
                innerTable = smallestTable;
            }
            maxRows *= rows(innerTable);
            usedTables.add(innerTable);
            List<LateralTable> subqueryTables = new ArrayList<>();
            subqueryTables.add(innerTable);
            JoinType joinType = Randomly.fromOptions(JoinType.values());
            int joinedTableCount = joinType == JoinType.LEFT && !oracle.leftJoinCanReadManyTables() ? 0
                    : (int) Randomly.getNotCachedInteger(0, MAX_JOINED_TABLES + 1);
            for (int i = 0; i < joinedTableCount; i++) {
                LateralTable joinedTable = pickSubqueryTable(usedTables, MAX_RESULT_ROWS / maxRows);
                if (joinedTable == null) {
                    break;
                }
                maxRows *= rows(joinedTable);
                usedTables.add(joinedTable);
                subqueryTables.add(joinedTable);
            }
            Join join = join(joinType, table, subqueryTables, visibleColumns, position);
            if (laterJoinsCanRead(join)) {
                List<SelectedColumn> columns = join.getSubquery().getColumns();
                for (int column = 0; column < columns.size(); column++) {
                    visibleColumns.add(
                            new VisibleColumn(ColumnRef.joinColumn(position, column), columns.get(column).getType()));
                }
            }
            joins.add(join);
        }
        int columnCount = 1
                + (int) Randomly.getNotCachedInteger(0, Math.min(table.getColumns().size(), MAX_SELECTED_COLUMNS));
        List<String> columns = Randomly.extractNrRandomColumns(table.getColumns(), columnCount).stream()
                .map(LateralTable.Column::getName).collect(Collectors.toList());
        return new LateralQuery(table.getName(), TABLE_ALIAS, columns, keepCommasAtTheEnd(joins),
                trueOrPredicate(table, TABLE_ALIAS));
    }

    private boolean laterJoinsCanRead(Join join) {
        return join.getType() != JoinType.LEFT || TRUE.equals(join.getOnClause())
                || oracle.laterJoinsCanReadLeftJoinWithOnPredicate();
    }

    private static long rows(LateralTable table) {
        return Math.max(1, table.getRowCount());
    }

    private LateralTable pickSubqueryTable(List<LateralTable> usedTables, long rowLimit) {
        if (Randomly.getBoolean()) {
            LateralTable table = pickTable(usedTables, 1, rowLimit);
            if (table != null) {
                return table;
            }
        }
        return pickTable(tables, 1, rowLimit);
    }

    private static LateralTable pickTable(List<LateralTable> candidates, long preferredMinRows, long rowLimit) {
        List<LateralTable> smallEnoughTables = candidates.stream().filter(table -> rows(table) <= rowLimit)
                .collect(Collectors.toList());
        List<LateralTable> tablesWithEnoughRows = smallEnoughTables.stream()
                .filter(table -> table.getRowCount() >= preferredMinRows).collect(Collectors.toList());
        if (!tablesWithEnoughRows.isEmpty()) {
            return Randomly.fromList(tablesWithEnoughRows);
        }
        if (smallEnoughTables.isEmpty()) {
            return null;
        }
        return Randomly.fromList(smallEnoughTables);
    }

    private Join join(JoinType joinType, LateralTable outerTable, List<LateralTable> subqueryTables,
            List<VisibleColumn> visibleColumns, int position) {
        List<String> aliases = new ArrayList<>();
        List<List<LocalColumn>> columnsPerTable = new ArrayList<>();
        for (int index = 0; index < subqueryTables.size(); index++) {
            String alias = index == 0 ? "i" + position : "i" + position + "_" + index;
            aliases.add(alias);
            columnsPerTable.add(localColumns(subqueryTables.get(index), alias));
        }
        List<LocalColumn> localColumns = columnsPerTable.stream().flatMap(List::stream).collect(Collectors.toList());

        boolean distinct = Randomly.getPercentage() < 0.2;
        Integer limit = Randomly.getPercentage() < 0.3 ? 1 + (int) Randomly.getNotCachedInteger(0, MAX_LIMIT) : null;
        boolean canSelectOuterColumns = joinType != JoinType.LEFT || oracle.leftJoinCanSelectOuterColumns();
        List<SelectedColumn> columns = selectedColumns(localColumns,
                canSelectOuterColumns ? visibleColumns : new ArrayList<>(),
                distinct || limit != null || oracle.alwaysSelectsCanonicalForms());

        List<SubqueryTable> tablesOfJoin = new ArrayList<>();
        tablesOfJoin.add(new SubqueryTable(subqueryTables.get(0).getName(), aliases.get(0), new ArrayList<>()));
        for (int index = 1; index < subqueryTables.size(); index++) {
            List<LocalColumn> earlierColumns = columnsPerTable.subList(0, index).stream().flatMap(List::stream)
                    .collect(Collectors.toList());
            List<Comparison> on = new ArrayList<>();
            if (Randomly.getPercentage() < 0.6) {
                Comparison equiJoin = equiJoin(columnsPerTable.get(index), earlierColumns);
                if (equiJoin != null) {
                    on.add(equiJoin);
                }
            }
            if (on.isEmpty() || Randomly.getPercentage() < 0.6) {
                LocalColumn column = pickCorrelatedColumn(columnsPerTable.get(index), visibleColumns);
                if (column != null) {
                    on.add(correlatedComparison(column, visibleColumns, correlationOperators()));
                }
            }
            if (on.isEmpty()) {
                throw new IgnoreMeException();
            }
            tablesOfJoin.add(new SubqueryTable(subqueryTables.get(index).getName(), aliases.get(index), on));
        }
        LocalColumn correlatedColumn = pickCorrelatedColumn(localColumns, visibleColumns);
        if (correlatedColumn == null) {
            throw new IgnoreMeException();
        }
        Comparison correlation = correlatedComparison(correlatedColumn, visibleColumns, correlationOperators());

        String onClause = null;
        if (joinType == JoinType.INNER || joinType == JoinType.LEFT) {
            onClause = trueOrPredicate(outerTable, TABLE_ALIAS);
        }
        boolean filterWithOr = Randomly.getPercentage() >= 0.7
                && (joinType != JoinType.LEFT || oracle.leftJoinCanFilterWithOr());
        List<OrderTerm> orderBy;
        if (limit != null) {
            orderBy = orderBy(columns.size(), columns.size());
        } else if (Randomly.getPercentage() < 0.2) {
            orderBy = orderBy(columns.size(), 1 + (int) Randomly.getNotCachedInteger(0, columns.size()));
        } else {
            orderBy = new ArrayList<>();
        }
        int filteredTable = (int) Randomly.getNotCachedInteger(0, subqueryTables.size());
        String filter = trueOrPredicate(subqueryTables.get(filteredTable), aliases.get(filteredTable));
        return new Join(joinType, onClause, "s" + position,
                new Subquery(tablesOfJoin, distinct, columns, correlation, filterWithOr, filter, orderBy, limit));
    }

    private List<SelectedColumn> selectedColumns(List<LocalColumn> localColumns, List<VisibleColumn> visibleColumns,
            boolean canonical) {
        List<VisibleColumn> selectableLocalColumns = localColumns.stream().map(column -> column.visible)
                .filter(column -> column.selectable).collect(Collectors.toList());
        if (selectableLocalColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        int columnCount = 1
                + (int) Randomly.getNotCachedInteger(0, Math.min(selectableLocalColumns.size(), MAX_SELECTED_COLUMNS));
        List<VisibleColumn> selected = new ArrayList<>(
                Randomly.extractNrRandomColumns(selectableLocalColumns, columnCount));
        List<VisibleColumn> selectableVisibleColumns = visibleColumns.stream().filter(column -> column.selectable)
                .collect(Collectors.toList());
        if (!selectableVisibleColumns.isEmpty() && Randomly.getPercentage() < 0.3) {
            selected.add(Randomly.fromList(selectableVisibleColumns));
        }
        return selected.stream().map(column -> column.select(oracle, canonical)).collect(Collectors.toList());
    }

    private Comparison equiJoin(List<LocalColumn> columns, List<LocalColumn> earlierColumns) {
        List<LocalColumn> candidates = columns.stream()
                .filter(column -> earlierColumns.stream().anyMatch(earlier -> canCompare(earlier, column)))
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return null;
        }
        LocalColumn column = Randomly.fromList(candidates);
        List<LocalColumn> comparableColumns = earlierColumns.stream().filter(earlier -> canCompare(earlier, column))
                .collect(Collectors.toList());
        List<LocalColumn> sameName = comparableColumns.stream().filter(earlier -> earlier.name.equals(column.name))
                .collect(Collectors.toList());
        List<LocalColumn> sameGroup = comparableColumns.stream()
                .filter(earlier -> earlier.visible.type.getGroup().equals(column.visible.type.getGroup()))
                .collect(Collectors.toList());
        LocalColumn other;
        if (!sameName.isEmpty()) {
            other = Randomly.fromList(sameName);
        } else if (!sameGroup.isEmpty()) {
            other = Randomly.fromList(sameGroup);
        } else {
            other = Randomly.fromList(comparableColumns);
        }
        return eitherOrder(column.visible.source, ComparisonOperator.EQUALS, other.visible.source);
    }

    private boolean canCompare(LocalColumn first, LocalColumn second) {
        return oracle.canCompare(first.visible.type, second.visible.type);
    }

    private LocalColumn pickCorrelatedColumn(List<LocalColumn> columns, List<VisibleColumn> visibleColumns) {
        List<LocalColumn> candidates = columns.stream()
                .filter(column -> visibleColumns.stream()
                        .anyMatch(visible -> oracle.canCompare(column.visible.type, visible.type)))
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return null;
        }
        List<LocalColumn> indexedColumns = candidates.stream().filter(column -> column.leadsAnIndex)
                .collect(Collectors.toList());
        if (!indexedColumns.isEmpty() && Randomly.getBoolean()) {
            return Randomly.fromList(indexedColumns);
        }
        return Randomly.fromList(candidates);
    }

    private List<ComparisonOperator> correlationOperators() {
        if (Randomly.getBoolean()) {
            return Collections.singletonList(ComparisonOperator.EQUALS);
        }
        return oracle.comparisonOperators();
    }

    private Comparison correlatedComparison(LocalColumn column, List<VisibleColumn> visibleColumns,
            List<ComparisonOperator> operators) {
        List<VisibleColumn> comparableColumns = visibleColumns.stream()
                .filter(visible -> oracle.canCompare(column.visible.type, visible.type)).collect(Collectors.toList());
        List<VisibleColumn> sameColumns = comparableColumns.stream()
                .filter(visible -> visible.source.isTableColumn() && visible.source.getColumnName().equals(column.name))
                .collect(Collectors.toList());
        VisibleColumn other;
        if (!sameColumns.isEmpty() && Randomly.getPercentage() < 0.6) {
            other = Randomly.fromList(sameColumns);
        } else {
            other = Randomly.fromList(comparableColumns);
        }
        return eitherOrder(column.visible.source, Randomly.fromList(operators), other.source);
    }

    private static Comparison eitherOrder(ColumnRef first, ComparisonOperator operator, ColumnRef second) {
        if (Randomly.getBoolean()) {
            return new Comparison(first, operator, second);
        }
        return new Comparison(second, operator, first);
    }

    private static List<OrderTerm> orderBy(int columnCount, int sortKeyCount) {
        List<Integer> columns = new ArrayList<>();
        for (int column = 0; column < columnCount; column++) {
            columns.add(column);
        }
        return Randomly.extractNrRandomColumns(columns, sortKeyCount).stream()
                .map(column -> new OrderTerm(column, Randomly.getBoolean())).collect(Collectors.toList());
    }

    private String trueOrPredicate(LateralTable table, String alias) {
        if (Randomly.getBoolean()) {
            return TRUE;
        }
        return oracle.predicate(table, alias);
    }

    private static List<Join> keepCommasAtTheEnd(List<Join> joins) {
        List<Join> result = new ArrayList<>(joins);
        boolean laterJoinIsNotComma = false;
        for (int position = result.size() - 1; position >= 0; position--) {
            Join join = result.get(position);
            if (join.getType() == JoinType.COMMA && laterJoinIsNotComma) {
                result.set(position, join.withType(JoinType.CROSS));
            } else if (join.getType() != JoinType.COMMA) {
                laterJoinIsNotComma = true;
            }
        }
        return result;
    }

    private static List<LocalColumn> localColumns(LateralTable table, String alias) {
        return table.getColumns().stream().map(column -> new LocalColumn(alias, column)).collect(Collectors.toList());
    }

    private static List<VisibleColumn> tableColumns(LateralTable table, String alias) {
        return localColumns(table, alias).stream().map(column -> column.visible).collect(Collectors.toList());
    }

    private static final class VisibleColumn {

        private final ColumnRef source;
        private final LateralColumnType type;
        private final boolean selectable;

        VisibleColumn(ColumnRef source, LateralColumnType type) {
            this(source, type, true);
        }

        VisibleColumn(ColumnRef source, LateralColumnType type, boolean selectable) {
            this.source = source;
            this.type = type;
            this.selectable = selectable;
        }

        SelectedColumn select(LateralJsonOracle<?> oracle, boolean canonical) {
            LateralColumnType canonicalType = canonical ? oracle.canonicalType(type) : null;
            if (canonicalType == null) {
                return new SelectedColumn(source, type, false, type);
            }
            return new SelectedColumn(source, type, true, canonicalType);
        }
    }

    private static final class LocalColumn {

        private final VisibleColumn visible;
        private final String name;
        private final boolean leadsAnIndex;

        LocalColumn(String alias, LateralTable.Column column) {
            this.visible = new VisibleColumn(ColumnRef.tableColumn(alias, column.getName()), column.getType(),
                    column.isSelectable());
            this.name = column.getName();
            this.leadsAnIndex = column.leadsAnIndex();
        }
    }
}
