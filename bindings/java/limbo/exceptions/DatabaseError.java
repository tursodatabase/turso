package limbo.exceptions;

/**
 * Exception raised for errors that are related to the database.
 */
public class DatabaseError extends Error {
    public DatabaseError(String message) {
        super(message);
    }
}
