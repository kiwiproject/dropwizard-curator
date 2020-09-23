package org.kiwiproject.curator.exception;

/**
 * Exception class indicating that an exception was thrown by Curator while trying to obtain a ZooKeeper lock.
 */
public class LockAcquisitionFailureException extends LockAcquisitionException {

    public LockAcquisitionFailureException() {
    }

    public LockAcquisitionFailureException(String message) {
        super(message);
    }

    public LockAcquisitionFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockAcquisitionFailureException(Throwable cause) {
        super(cause);
    }
}
