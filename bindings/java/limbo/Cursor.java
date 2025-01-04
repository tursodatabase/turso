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

    public Object[] fetchOne() throws Exception {
        Object result = fetchOne(cursorPtr);
        if (result instanceof Object[]) {
            // The result is an instance of an array
            System.out.println("The result is an array.");
            Object[] array = (Object[]) result;
            for (Object element : array) {
                if (element instanceof String) {
                    System.out.println("String: " + element);
                } else if (element instanceof Integer) {
                    System.out.println("Integer: " + element);
                } else if (element instanceof Double) {
                    System.out.println("Double: " + element);
                } else if (element instanceof Boolean) {
                    System.out.println("Boolean: " + element);
                } else if (element instanceof Long) {
                    System.out.println("Long: " + element);
                } else if (element instanceof byte[]) {
                    System.out.print("byte[]: ");
                    for (byte b : (byte[]) element) {
                        System.out.print(b + " ");
                    }
                    System.out.println();
                } else {
                    System.out.println("Unknown type: " + element);
                }
            }
            return array;
        } else {
            throw new Exception("result should be of type Object[]. Maybe internal logic has error.");
        }
    }

    private static native Object fetchOne(long cursorPtr);
}
