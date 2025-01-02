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

//    public Cursor execute(String sql, List<Tuple<Object, Object>> parameters) {
//        System.out.println("sql: " + sql);
//        execute(cursorPtr, sql, parameters);
//        return this;
//    }
//
//    private static native void execute(long cursorPtr, String sql, List<Tuple<Object, Object>> parameters);
}
