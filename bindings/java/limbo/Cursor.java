package limbo;

import java.util.Optional;

/**
 * Represents a database cursor.
 */
public class Cursor {
    private long cursorPtr;

    public Cursor(long cursorPtr) {
        this.cursorPtr = cursorPtr;
    }

    // TODO: support parameters
    public Cursor execute(String sql) {
        execute(cursorPtr, sql);
        return this;
    }

    private static native void execute(long cursorPtr, String sql);

    public void fetchOne() {
        fetchOne(cursorPtr);
    }

    private static native void fetchOne(long cursorPtr);
}
