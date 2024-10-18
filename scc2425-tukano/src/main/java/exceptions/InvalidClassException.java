package exceptions;

public class InvalidClassException extends RuntimeException {

    public InvalidClassException(String message) {
        super(message);
    }

    public InvalidClassException(String message, Throwable error) {
        super(message, error);
    }
}
