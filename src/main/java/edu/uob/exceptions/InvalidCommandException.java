package edu.uob.exceptions;

import java.io.Serial;

public class InvalidCommandException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidCommandException(String message) {
        super(message);
    }
}
