package org.github.tursodatabase.limbo;

import java.util.Optional;

/**
 * Represents the description of a database cursor.
 */
public class Description {
    public final String columnName;
    public final String columnType;
    public final Optional<String> columnSize;
    public final Optional<String> decimalDigits;
    public final Optional<String> numPrecRadix;
    public final Optional<String> nullable;
    public final Optional<String> remarks;

    public Description(String columnName, String columnType, Optional<String> columnSize, Optional<String> decimalDigits, Optional<String> numPrecRadix, Optional<String> nullable, Optional<String> remarks) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
        this.numPrecRadix = numPrecRadix;
        this.nullable = nullable;
        this.remarks = remarks;
    }
}
