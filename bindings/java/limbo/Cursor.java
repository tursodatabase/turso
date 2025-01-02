package limbo;

import java.util.List;
import java.util.Optional;

/**
 * Represents a database cursor.
 */
public class Cursor {
    private long cursorId;

    public Cursor(long cursorId) {
        this.cursorId = cursorId;
    }

    public native Cursor execute(String sql, Optional<Tuple<Object, Object>> parameters);
}
