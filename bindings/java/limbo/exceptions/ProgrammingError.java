package limbo.exceptions;

/**
 * Exception raised for programming errors, e.g., table not found, syntax error in SQL, wrong number of parameters specified.
 */
public class ProgrammingError extends DatabaseError {
    public ProgrammingError(String message) {
        super(message);
    }
}
