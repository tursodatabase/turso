package sqlancer.common.oracle.lateral;

import java.util.Objects;

/**
 * The type of a column, as the LATERAL oracle needs it. The JSON form of a LATERAL join declares this type for each
 * column, so that the two forms return the same values.
 */
public final class LateralColumnType {

    private final String name;
    private final String collation;
    private final String group;

    /**
     * Creates a column type.
     *
     * @param name
     *            the SQL type, for example "integer" or "varchar(500) CHARACTER SET utf8mb4"
     * @param collation
     *            the collation of the column, or null if the type has no collation
     * @param group
     *            the group of types whose values compare without conversions, for example "number" or "text"
     */
    public LateralColumnType(String name, String collation, String group) {
        this.name = Objects.requireNonNull(name);
        this.collation = collation;
        this.group = Objects.requireNonNull(group);
    }

    public String getName() {
        return name;
    }

    public String getCollation() {
        return collation;
    }

    public String getGroup() {
        return group;
    }

    public boolean hasCollation() {
        return collation != null;
    }

    public boolean hasSameCollation(LateralColumnType other) {
        return Objects.equals(collation, other.collation);
    }
}
