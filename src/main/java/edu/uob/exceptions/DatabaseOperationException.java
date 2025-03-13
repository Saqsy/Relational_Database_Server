package edu.uob.exceptions;

import java.io.Serial;

public class DatabaseOperationException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    public DatabaseOperationException(String message) {
        super(message);
    }
}
