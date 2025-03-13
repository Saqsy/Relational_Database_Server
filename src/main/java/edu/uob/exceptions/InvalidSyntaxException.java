package edu.uob.exceptions;

import java.io.Serial;

public class InvalidSyntaxException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidSyntaxException(String message) {
        super(message);
    }
}
