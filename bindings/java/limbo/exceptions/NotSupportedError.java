package limbo.exceptions;

/**
 * Exception raised when a method or database API is used which is not supported by the database.
 */
public class NotSupportedError extends DatabaseError {
    public NotSupportedError(String message) {
        super(message);
    }
}
