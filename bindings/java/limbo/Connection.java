package limbo;

import java.util.List;
import java.util.Optional;

/**
 * Represents a connection to the database.
 */
public class Connection {

    /**
     * Creates a new cursor object using this connection.
     *
     * @return A new Cursor object.
     * @throws InterfaceError If the cursor cannot be created.
     */
    public Cursor cursor() throws InterfaceError {
        return new Cursor();
    }

    /**
     * Closes the connection to the database.
     *
     * @throws OperationalError If there is an error closing the connection.
     */
    public void close() throws OperationalError {
        // Implementation here
    }

    /**
     * Commits the current transaction.
     *
     * @throws OperationalError If there is an error during commit.
     */
    public void commit() throws OperationalError {
        // Implementation here
    }

    /**
     * Rolls back the current transaction.
     *
     * @throws OperationalError If there is an error during rollback.
     */
    public void rollback() throws OperationalError {
        // Implementation here
    }
}
