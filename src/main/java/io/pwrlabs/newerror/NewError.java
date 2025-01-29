package io.pwrlabs.newerror;

public class NewError {

    public static void errorIf(boolean condition, String message) throws ValidationException {
        if (condition) { 

            throw new ValidationException(message);
        }
    }
}

