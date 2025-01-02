package limbo.exceptions;

/**
 * Exception raised for errors related to the database’s operation, not necessarily under the programmer's control.
 */
public class OperationalError extends DatabaseError {
    public OperationalError(String message) {
        super(message);
    }
}
