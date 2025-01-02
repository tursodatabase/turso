package limbo.exceptions;

/**
 * Exception raised for errors due to problems with the processed data like division by zero, numeric value out of range, etc.
 */
public class DataError extends DatabaseError {
    public DataError(String message) {
        super(message);
    }
}
