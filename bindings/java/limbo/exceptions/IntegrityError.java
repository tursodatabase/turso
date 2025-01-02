package limbo.exceptions;

/**
 * Exception raised when the relational integrity of the database is affected, e.g., a foreign key check fails.
 */
public class IntegrityError extends DatabaseError {
    public IntegrityError(String message) {
        super(message);
    }
}
