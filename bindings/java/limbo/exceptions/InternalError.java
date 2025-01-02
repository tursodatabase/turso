package limbo.exceptions;

/**
 * Exception raised when the database encounters an internal error, e.g., cursor is not valid anymore, transaction out of sync.
 */
public class InternalError extends DatabaseError {
    public InternalError(String message) {
        super(message);
    }
}
