package limbo.exceptions;

/**
 * Exception raised for errors related to the database interface rather than the database itself.
 */
public class InterfaceError extends Error {
    public InterfaceError(String message) {
        super(message);
    }
}
