package org.kiwiproject.curator.exception;

/**
 * Exception class indicating there
 */
public class CuratorStartupFailureException extends RuntimeException {

    public CuratorStartupFailureException() {
    }

    public CuratorStartupFailureException(String message) {
        super(message);
    }

    public CuratorStartupFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public CuratorStartupFailureException(Throwable cause) {
        super(cause);
    }
}
