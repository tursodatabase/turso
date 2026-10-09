package sqlancer.common.oracle.lateral;

import java.util.Collections;
import java.util.List;

/**
 * A table that the LATERAL oracle can read, with the number of rows and the types of its columns.
 */
public final class LateralTable {

    private final String name;
    private final long rowCount;
    private final List<Column> columns;

    public LateralTable(String name, long rowCount, List<Column> columns) {
        this.name = name;
        this.rowCount = rowCount;
        this.columns = Collections.unmodifiableList(columns);
    }

    public String getName() {
        return name;
    }

    public long getRowCount() {
        return rowCount;
    }

    public List<Column> getColumns() {
        return columns;
    }

    /**
     * A column of a table.
     */
    public static final class Column {

        private final String name;
        private final LateralColumnType type;
        private final boolean leadsAnIndex;
        private final boolean selectable;

        /**
         * Creates a column.
         *
         * @param name
         *            the name of the column
         * @param type
         *            the type of the column
         * @param leadsAnIndex
         *            true if the column is the first column of an index
         * @param selectable
         *            false if the JSON form cannot return the exact values of the column
         */
        public Column(String name, LateralColumnType type, boolean leadsAnIndex, boolean selectable) {
            this.name = name;
            this.type = type;
            this.leadsAnIndex = leadsAnIndex;
            this.selectable = selectable;
        }

        public String getName() {
            return name;
        }

        public LateralColumnType getType() {
            return type;
        }

        public boolean leadsAnIndex() {
            return leadsAnIndex;
        }

        public boolean isSelectable() {
            return selectable;
        }
    }
}
