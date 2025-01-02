package limbo;

import java.lang.Exception;

/**
 * Represents a connection to the database.
 */
public class Connection {

    private long connectionId;

    public Connection(long connectionId) {
        this.connectionId = connectionId;
    }

    public native void test();

    /**
     * Creates a new cursor object using this connection.
     *
     * @return A new Cursor object.
     * @throws Exception If the cursor cannot be created.
     */
    public Cursor cursor() throws Exception {
        return cursor(connectionId);
    }

    private native Cursor cursor(long connectionId);

    /**
     * Closes the connection to the database.
     *
     * @throws Exception If there is an error closing the connection.
     */
    public void close() throws Exception {
        close(connectionId);
    }

    private native void close(long connectionId);

    /**
     * Commits the current transaction.
     *
     * @throws Exception If there is an error during commit.
     */
    public void commit() throws Exception {
        try {
            commit(connectionId);
        } catch (Exception e) {
            System.out.println("caught exception: " + e);
        }
    }

    private native void commit(long connectionId) throws Exception;

    /**
     * Rolls back the current transaction.
     *
     * @throws Exception If there is an error during rollback.
     */
    public void rollback() throws Exception {
        rollback(connectionId);
    }

    private native void rollback(long connectionId) throws Exception;
}
