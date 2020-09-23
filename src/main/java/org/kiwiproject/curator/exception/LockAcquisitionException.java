package org.kiwiproject.curator.exception;

/**
 * Exception class indicating a failure to obtain a ZooKeeper lock.
 */
public class LockAcquisitionException extends RuntimeException {

    public LockAcquisitionException() {
    }

    public LockAcquisitionException(String message) {
        super(message);
    }

    public LockAcquisitionException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockAcquisitionException(Throwable cause) {
        super(cause);
    }
}
