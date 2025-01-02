package limbo.exceptions;

/**
 * Exception raised for important warnings like data truncations while inserting.
 */
public class Warning extends Exception {
    public Warning(String message) {
        super(message);
    }
}
