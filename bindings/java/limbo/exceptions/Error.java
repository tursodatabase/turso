package limbo.exceptions;

/**
 * Base class for all other error exceptions. Catch all database-related errors using this class.
 */
public class Error extends Exception {
    public Error(String message) {
        super(message);
    }
}
