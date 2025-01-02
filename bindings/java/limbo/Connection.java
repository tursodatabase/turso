package limbo;

import limbo.exceptions.InterfaceError;
import limbo.exceptions.OperationalError;

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
     * @throws InterfaceError If the cursor cannot be created.
     */
    public Cursor cursor() throws InterfaceError {
        return cursor(connectionId);
    }

    private native Cursor cursor(long connectionId);

    /**
     * Closes the connection to the database.
     *
     * @throws OperationalError If there is an error closing the connection.
     */
    public void close() throws OperationalError {
        close(connectionId);
    }

    private native void close(long connectionId);

    /**
     * Commits the current transaction.
     *
     * @throws OperationalError If there is an error during commit.
     */
    public void commit() throws OperationalError {
        commit(connectionId);
    }

    private native void commit(long connectionId);

    /**
     * Rolls back the current transaction.
     *
     * @throws OperationalError If there is an error during rollback.
     */
    public void rollback() throws OperationalError {
        rollback(connectionId);
    }

    private native void rollback(long connectionId);
}
