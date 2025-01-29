package io.pwrlabs.newerror;

// Custom exception class
public class ValidationException extends RuntimeException  {
    public ValidationException(String message ) {
        super(message);
    }
}
