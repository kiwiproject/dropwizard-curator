package org.kiwiproject.curator.exception;

/**
 * Exception class indicating that a timeout occurred trying to obtain a ZooKeeper lock.
 */
public class LockAcquisitionTimeoutException extends LockAcquisitionException {

    public LockAcquisitionTimeoutException() {
    }

    public LockAcquisitionTimeoutException(String message) {
        super(message);
    }

    public LockAcquisitionTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockAcquisitionTimeoutException(Throwable cause) {
        super(cause);
    }
}
