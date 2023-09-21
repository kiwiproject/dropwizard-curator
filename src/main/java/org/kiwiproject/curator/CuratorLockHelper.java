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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

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
     * @param lock    the lock to acquire
     * @param timeout the timeout duration
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public void acquire(InterProcessLock lock, Duration timeout) {
        var nanos = timeout.toNanos();
        acquire(lock, nanos, TimeUnit.NANOSECONDS);
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

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, executes the specified {@code action}, then releases the lock.
     * <p>
     * If the action throws an exception, the lock is released, and the action's exception
     * is propagated to the caller.
     * <p>
     * If Curator throws any exception, or if the timeout expires, the appropriate exception is thrown.
     *
     * @param lock    the distributed lock to acquire
     * @param timeout the timeout duration
     * @param action  the action to execute while holding the lock
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public void useLock(InterProcessLock lock, Duration timeout, Runnable action) {
        var nanos = timeout.toNanos();
        useLock(lock, nanos, TimeUnit.NANOSECONDS, action);
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, executes the specified {@code action}, then releases the lock.
     * <p>
     * If the action throws an exception, the lock is released, and the action's exception
     * is propagated to the caller.
     * <p>
     * If Curator throws any exception, or if the timeout expires, the appropriate exception is thrown.
     *
     * @param lock   the distributed lock to acquire
     * @param time   the timeout quantity
     * @param unit   the timeout unit
     * @param action the action to execute while holding the lock
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public void useLock(InterProcessLock lock, long time, TimeUnit unit, Runnable action) {
        try {
            acquire(lock, time, unit);
            action.run();
        } finally {
            releaseQuietly(lock);
        }
    }

    /**
     * Enumeration representing the type of error related to using a lock or returning a value
     * while holding a lock.
     */
    public enum ErrorType {
        /**
         * Represents an error related to lock acquisition.
         */
        LOCK_ACQUISITION,

        /**
         * Represents an error related to a executing function or operation, e.g.
         * executing a {@link Runnable} or getting a value from a {@link Supplier}.
         */
        OPERATION
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, executes the specified {@code action}, then releases the lock.
     * <p>
     * If the lock cannot be obtained for any reason, or the action throws an exception, the lock is released, and
     * the {@code errorHandler} is called. The {@link ErrorType} can be used to determine the cause, to permit different
     * handling for lock acquisition failures and action execution failures.
     *
     * @param lock         the distributed lock to acquire
     * @param timeout      the timeout duration
     * @param action       the action to execute while holding the lock
     * @param errorHandler the action to take if the lock cannot be obtained, or if the action throws any exception
     */
    public void useLock(InterProcessLock lock,
                        Duration timeout,
                        Runnable action,
                        BiConsumer<ErrorType, RuntimeException> errorHandler) {
        var nanos = timeout.toNanos();
        useLock(lock, nanos, TimeUnit.NANOSECONDS, action, errorHandler);
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, executes the specified {@code action}, then releases the lock.
     * <p>
     * If the lock cannot be obtained for any reason, or the action throws an exception, the lock is released, and
     * the {@code errorHandler} is called. The {@link ErrorType} can be used to determine the cause, to permit different
     * handling for lock acquisition failures and action execution failures.
     *
     * @param lock         the distributed lock to acquire
     * @param time         the timeout quantity
     * @param unit         the timeout unit
     * @param action       the action to execute while holding the lock
     * @param errorHandler the action to take if the lock cannot be obtained, or if the action throws any exception
     */
    public void useLock(InterProcessLock lock,
                        long time, TimeUnit unit,
                        Runnable action,
                        BiConsumer<ErrorType, RuntimeException> errorHandler) {
        try {
            useLock(lock, time, unit, action);
        } catch(LockAcquisitionException e) {
            errorHandler.accept(ErrorType.LOCK_ACQUISITION, e);
        } catch (RuntimeException e) {
            errorHandler.accept(ErrorType.OPERATION, e);
        }
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, calls the {@code supplier} and returns the result of the its computation,
     * then releases the lock.
     * <p>
     * If the supplier throws an exception, the lock is released, and the supplier's exception
     * is propagated to the caller.
     * <p>
     * If Curator throws any exception, or if the timeout expires, the appropriate exception is thrown.
     *
     * @param <R>      the type of the result produced by the supplier
     * @param lock     the distributed lock to acquire
     * @param timeout  the timeout duration
     * @param supplier The supplier providing the computation to be executed while holding the lock
     * @return The result of the computation provided by the supplier
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public <R> R withLock(InterProcessLock lock, Duration timeout, Supplier<R> supplier) {
        var nanos = timeout.toNanos();
        return withLock(lock, nanos, TimeUnit.NANOSECONDS, supplier);
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, calls the {@code supplier} and returns the result of the its computation,
     * then releases the lock.
     * <p>
     * If the supplier throws an exception, the lock is released, and the supplier's exception
     * is propagated to the caller.
     * <p>
     * If Curator throws any exception, or if the timeout expires, the appropriate exception is thrown.
     *
     * @param <R>      the type of the result produced by the supplier
     * @param lock     the distributed lock to acquire
     * @param time     the timeout quantity
     * @param unit     the timeout unit
     * @param supplier The supplier providing the computation to be executed while holding the lock
     * @return The result of the computation provided by the supplier
     * @throws LockAcquisitionFailureException if the lock throws any exception during acquisition
     * @throws LockAcquisitionTimeoutException if the lock acquisition times out
     */
    public <R> R withLock(InterProcessLock lock, long time, TimeUnit unit, Supplier<R> supplier) {
        try {
            acquire(lock, time, unit);
            return supplier.get();
        } finally {
            releaseQuietly(lock);
        }
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, calls the {@code supplier} and returns the result of the its computation,
     * then releases the lock.
     * <p>
     * If the lock cannot be obtained for any reason, or the action throws an exception, the lock is released, and
     * the {@code errorHandler} is called. The {@link ErrorType} can be used to determine the cause, to permit different
     * handling for lock acquisition failures and action execution failures. Note that because this method
     * requires a return value, the error handler must provide one, though it could be null.
     *
     * @param <R>          the type of the result produced by the supplier
     * @param lock         the distributed lock to acquire
     * @param timeout      the timeout duration
     * @param supplier     the supplier providing the computation to be executed while holding the lock
     * @param errorHandler the action to take if the lock cannot be obtained, or if the supplier throws any exception
     * @return The result of the computation provided by the supplier
     */
    public <R> R withLock(InterProcessLock lock,
                          Duration timeout,
                          Supplier<R> supplier,
                          BiFunction<ErrorType, RuntimeException, R> errorHandler) {
        var nanos = timeout.toNanos();
        return withLock(lock, nanos, TimeUnit.NANOSECONDS, supplier, errorHandler);
    }

    /**
     * Tries to acquire the specified {@code lock}, waiting up to the specified timeout period.
     * Once the lock is acquired, calls the {@code supplier} and returns the result of the its computation,
     * then releases the lock.
     * <p>
     * If the lock cannot be obtained for any reason, or the action throws an exception, the lock is released, and
     * the {@code errorHandler} is called. The {@link ErrorType} can be used to determine the cause, to permit different
     * handling for lock acquisition failures and action execution failures. Note that because this method
     * requires a return value, the error handler must provide one, though it could be null.
     *
     * @param <R>          the type of the result produced by the supplier
     * @param lock         the distributed lock to acquire
     * @param time         the timeout quantity
     * @param unit         the timeout unit
     * @param supplier     the supplier providing the computation to be executed while holding the lock
     * @param errorHandler the action to take if the lock cannot be obtained, or if the supplier throws any exception
     * @return The result of the computation provided by the supplier
     */
    public <R> R withLock(InterProcessLock lock,
                          long time, TimeUnit unit,
                          Supplier<R> supplier,
                          BiFunction<ErrorType, RuntimeException, R> errorHandler) {
        try {
            return withLock(lock, time, unit, supplier) ;
        } catch(LockAcquisitionException e) {
            return errorHandler.apply(ErrorType.LOCK_ACQUISITION, e);
        } catch (RuntimeException e) {
            return errorHandler.apply(ErrorType.OPERATION, e);
        }
    }
}
