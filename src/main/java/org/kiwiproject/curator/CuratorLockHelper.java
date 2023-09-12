package org.kiwiproject.curator;

import static java.util.Objects.isNull;
import static org.kiwiproject.base.KiwiStrings.f;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.kiwiproject.curator.exception.LockAcquisitionException;
import org.kiwiproject.curator.exception.LockAcquisitionFailureException;
import org.kiwiproject.curator.exception.LockAcquisitionTimeoutException;

import java.util.concurrent.TimeUnit;

/**
 * Helper class for creating and managing Curator locks, and converting timeouts and exceptions thrown by Curator
 * into {@link LockAcquisitionException}s.
 */
@Slf4j
public class CuratorLockHelper {

    /**
     * Creates a Curator lock instance for the given path.
     * <p>
     * Use this when you need a re-entrant mutex that can only be released by the acquiring thread.
     *
     * @param client   Curator client
     * @param lockPath the ZooKeeper base lock path
     * @return a Curator lock instance ({@link InterProcessMutex})
     * @see InterProcessMutex
     */
    public InterProcessMutex createInterProcessMutex(CuratorFramework client, String lockPath) {
        return new InterProcessMutex(client, lockPath);
    }

    /**
     * Creates a Curator lock instance for the given path.
     * <p>
     * Use this when you need a non re-entrant mutex which can be released by any thread, not just
     * the acquiring thread.
     *
     * @param client   Curator client
     * @param lockPath the ZooKeeper base lock path
     * @return a Curator lock instance ({@link InterProcessSemaphoreMutex})
     * @see InterProcessSemaphoreMutex
     */
    public InterProcessSemaphoreMutex createInterProcessSemaphoreMutex(CuratorFramework client, String lockPath) {
        return new InterProcessSemaphoreMutex(client, lockPath);
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period. If Curator throws any
     * exception, or if the timeout expires, the appropriate exception is thrown.
     *
     * @param lock the lock to acquire
     * @param time the timeout quantity
     * @param unit the timeout unit
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public void acquire(InterProcessLock lock, long time, TimeUnit unit) {
        boolean acquired;
        try {
            acquired = lock.acquire(time, unit);
        } catch (Exception e) {
            throw new LockAcquisitionFailureException("Failed to acquire lock", e);
        }

        if (!acquired) {
            var msg = f("Failed to acquire lock; timed out after {} {}", time, unit);
            LOG.warn(msg);
            throw new LockAcquisitionTimeoutException(msg);
        }
    }

    /**
     * Release the given lock, ignoring if {@code null} or if any exception occurs releasing the lock.
     *
     * @param lock to release
     */
    public void releaseQuietly(InterProcessLock lock) {
        if (isNull(lock)) {
            return;
        }

        try {
            lock.release();
        } catch (Exception e) {
            // ignore
            LOG.warn("Unable to release lock {}", lock, e);
        }
    }

    /**
     * Release the given lock quietly (ignoring exceptions) <em>if the current process holds it</em>.
     *
     * @param lock to release, if held
     */
    public void releaseLockQuietlyIfHeld(InterProcessLock lock) {
        if (isNull(lock)) {
            return;
        }

        if (lock.isAcquiredInThisProcess()) {
            LOG.trace("Releasing lock [{}]", lock);
            releaseQuietly(lock);
        } else {
            LOG.trace("This process does not own lock [{}]. Nothing to do.", lock);
        }
    }
}
