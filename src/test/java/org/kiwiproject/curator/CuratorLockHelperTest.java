package org.kiwiproject.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Supplier;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.curator.CuratorLockHelper.ErrorType;
import org.kiwiproject.curator.exception.LockAcquisitionFailureException;
import org.kiwiproject.curator.exception.LockAcquisitionTimeoutException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

@DisplayName("CuratorLockHelper")
class CuratorLockHelperTest {

    private CuratorLockHelper lockHelper;
    private CuratorFramework client;
    private InterProcessLock lock;

    @BeforeEach
    void setUp() {
        client = mock(CuratorFramework.class);
        lock = mock(InterProcessMutex.class);
        lockHelper = new CuratorLockHelper();
    }

    @Test
    void shouldCreateInterProcessMutex() {
        lock = lockHelper.createInterProcessMutex(client, "/lock-path");
        assertThat(lock).isNotNull();
    }

    @Test
    void shouldCreateInterProcessSemaphoreMutex() {
        // this is unfortunately a bit too "clear box" in that it knows about the implementation
        // details of creating InterProcessSemaphoreMutex instances
        when(client.newWatcherRemoveCuratorFramework())
                .thenReturn(mock(WatcherRemoveCuratorFramework.class));

        lock = lockHelper.createInterProcessSemaphoreMutex(client, "/lock-path");
        assertThat(lock).isNotNull();
    }

    @Nested
    class Acquire {

        @Test
        void shouldNotThrow_WhenAcquiresLock() throws Exception {
            when(lock.acquire(10, TimeUnit.SECONDS)).thenReturn(true);

            assertThatCode(() -> lockHelper.acquire(lock, 10, TimeUnit.SECONDS))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldThrowLockAcquisitionFailureException_WhenFails_DueToException() throws Exception {
            doThrow(new CannotAcquireLockException("oops, can't touch this lock")).when(lock).acquire(10, TimeUnit.SECONDS);

            var thrown = catchThrowable(() -> lockHelper.acquire(lock, 10, TimeUnit.SECONDS));

            assertThat(thrown).isInstanceOf(LockAcquisitionFailureException.class);
        }

        @Test
        void shouldThrowLockAcquisitionTimeoutException_WhenFails_DueToTimeout() throws Exception {
            when(lock.acquire(10, TimeUnit.SECONDS)).thenReturn(false);

            var thrown = catchThrowable(() -> lockHelper.acquire(lock, 10, TimeUnit.SECONDS));

            assertThat(thrown).isInstanceOf(LockAcquisitionTimeoutException.class);
        }

        @Test
        void shouldAcceptDuration() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var timeout = Duration.ofSeconds(1);
            assertThatCode(() -> lockHelper.acquire(lock, timeout))
                    .doesNotThrowAnyException();

            verify(lock).acquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Nested
    class ReleaseQuietly {

        @Test
        void shouldIgnore_NullLock() {
            assertThatCode(() -> lockHelper.releaseQuietly(null))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldUnlock_WhenSuccessfullyReleases() throws Exception {
            assertThatCode(() -> lockHelper.releaseQuietly(lock))
                    .doesNotThrowAnyException();

            verify(lock).release();
        }

        @Test
        void shouldNotThrow_WhenFailsToRelease() throws Exception {
            doThrow(new Exception("crapola")).when(lock).release();

            assertThatCode(() -> lockHelper.releaseQuietly(lock))
                    .doesNotThrowAnyException();

            verify(lock).release();
        }
    }

    @Nested
    class ReleaseLockQuietlyIfHeld {

        @Test
        void shouldIgnore_NullLock() {
            assertThatCode(() -> lockHelper.releaseLockQuietlyIfHeld(null))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldRelease_WhenWeOwnTheLock() throws Exception {
            when(lock.isAcquiredInThisProcess()).thenReturn(true);

            lockHelper.releaseLockQuietlyIfHeld(lock);

            verify(lock).release();
        }

        @Test
        void shouldNotRelease_WhenWeDoNotOwnTheLock() throws Exception {
            when(lock.isAcquiredInThisProcess()).thenReturn(false);

            lockHelper.releaseLockQuietlyIfHeld(lock);

            verify(lock, never()).release();
        }
    }

    private static class CannotAcquireLockException extends RuntimeException {
        CannotAcquireLockException(String message) {
            super(message);
        }
    }

    @Nested
    class UseLock {

        @Test
        void shouldCallRunnable_WhenAcquiresLock() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var action = new TrackingRunnable();
            lockHelper.useLock(lock, 3, TimeUnit.SECONDS, action);

            assertThat(action.wasCalled).isTrue();

            verify(lock).acquire(3, TimeUnit.SECONDS);
        }

        @Test
        void shouldThrowLockAcquisitionFailureException_WhenFails_DueToException() throws Exception {
            doThrow(new CannotAcquireLockException("oops, can't touch this lock"))
                    .when(lock)
                    .acquire(anyLong(), any(TimeUnit.class));

            var action = new TrackingRunnable();
            assertThatThrownBy(() -> lockHelper.useLock(lock, 2, TimeUnit.SECONDS, action))
                    .isExactlyInstanceOf(LockAcquisitionFailureException.class);
            assertThat(action.wasCalled).isFalse();

            verify(lock).acquire(2, TimeUnit.SECONDS);
        }

        @Test
        void shouldThrowLockAcquisitionTimeoutException_WhenFails_DueToTimeout() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(false);

            var action = new TrackingRunnable();
            assertThatThrownBy(() -> lockHelper.useLock(lock, 1, TimeUnit.SECONDS, action))
                    .isExactlyInstanceOf(LockAcquisitionTimeoutException.class);
            assertThat(action.wasCalled).isFalse();

            verify(lock).acquire(1, TimeUnit.SECONDS);
        }

        @Test
        void shouldReleaseLock_IfActionThrowsAnException() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var action = new ThrowingRunnable();
            assertThatThrownBy(() -> lockHelper.useLock(lock, 1, TimeUnit.SECONDS, action))
                    .isExactlyInstanceOf(UncheckedIOException.class);

            verify(lock).acquire(1, TimeUnit.SECONDS);
            verify(lock).release();
        }

        @Test
        void shouldAcceptDuration() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var timeout = Duration.ofSeconds(3);
            lockHelper.useLock(lock, timeout, new TrackingRunnable());

            verify(lock).acquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Nested
    class UseLockWithErrorHandler {

        @Test
        void shouldCallRunnable_WhenAcquiresLock() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var action = new TrackingRunnable();
            var errorConsumer = new ErrorConsumer();
            lockHelper.useLock(lock, 5, TimeUnit.SECONDS, action, errorConsumer);

            assertThat(action.wasCalled).isTrue();
            assertThat(errorConsumer.wasCalled).isFalse();

            verify(lock).acquire(5, TimeUnit.SECONDS);
        }

        @Test
        void shouldCallErrorHandler_WhenLockAcquisitionFails() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(false);

            var action = new TrackingRunnable();
            var errorConsumer = new ErrorConsumer();
            lockHelper.useLock(lock, 5, TimeUnit.SECONDS, action, errorConsumer);

            assertThat(action.wasCalled).isFalse();
            assertThat(errorConsumer.wasCalled).isTrue();
            assertThat(errorConsumer.errorType).isEqualTo(ErrorType.LOCK_ACQUISITION);
            assertThat(errorConsumer.e).isExactlyInstanceOf(LockAcquisitionTimeoutException.class);

            verify(lock).acquire(5, TimeUnit.SECONDS);
        }

        @Test
        void shouldCallErrorHandler_WhenActionThrowsException() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var action = new ThrowingRunnable();
            var errorConsumer = new ErrorConsumer();
            lockHelper.useLock(lock, 3, TimeUnit.SECONDS, action, errorConsumer);

            assertThat(errorConsumer.wasCalled).isTrue();
            assertThat(errorConsumer.errorType).isEqualTo(ErrorType.OPERATION);
            assertThat(errorConsumer.e).isExactlyInstanceOf(UncheckedIOException.class);

            verify(lock).acquire(3, TimeUnit.SECONDS);
        }

        @Test
        void shouldAcceptDuration() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var timeout = Duration.ofSeconds(5);
            lockHelper.useLock(lock, timeout, new TrackingRunnable(), new ErrorConsumer());

            verify(lock).acquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Nested
    class WithLock {

        @Test
        void shouldReturnResultOfSupplier_WhenAcquiresLock() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var number = 84L;
            var supplier = new TrackingSupplier(number);
            var result = lockHelper.withLock(lock, 2, TimeUnit.SECONDS, supplier);

            assertThat(result).isEqualTo(number);
            assertThat(supplier.wasCalled).isTrue();

            verify(lock).acquire(2, TimeUnit.SECONDS);
        }

        @Test
        void shouldThrowLockAcquisitionFailureException_WhenFails_DueToException() throws Exception {
            doThrow(new CannotAcquireLockException("oops, can't touch this lock"))
                    .when(lock)
                    .acquire(anyLong(), any(TimeUnit.class));

            var supplier = new TrackingSupplier();
            assertThatThrownBy(() -> lockHelper.withLock(lock, 5, TimeUnit.SECONDS, supplier))
                    .isExactlyInstanceOf(LockAcquisitionFailureException.class);
            assertThat(supplier.wasCalled).isFalse();

            verify(lock).acquire(5, TimeUnit.SECONDS);
        }

        @Test
        void shouldThrowLockAcquisitionTimeoutException_WhenFails_DueToTimeout() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(false);

            var supplier = new TrackingSupplier();
            assertThatThrownBy(() -> lockHelper.withLock(lock, 2, TimeUnit.SECONDS, supplier))
                    .isExactlyInstanceOf(LockAcquisitionTimeoutException.class);
            assertThat(supplier.wasCalled).isFalse();

            verify(lock).acquire(2, TimeUnit.SECONDS);
        }

        @Test
        void shouldReleaseLock_IfSupplierThrowsAnException() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var supplier = new ThrowingSupplier();
            assertThatThrownBy(() -> lockHelper.withLock(lock, 5, TimeUnit.SECONDS, supplier))
                    .isExactlyInstanceOf(UncheckedIOException.class);

            verify(lock).acquire(5, TimeUnit.SECONDS);
            verify(lock).release();
        }

        @Test
        void shouldAcceptDuration() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var timeout = Duration.ofSeconds(2);
            lockHelper.withLock(lock, timeout, new TrackingSupplier(42L));

            verify(lock).acquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Nested
    class WithLockWithErrorHandler {

        @Test
        void shouldReturnResultOfSupplier_WhenAcquiresLock() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var number = 84L;
            var supplier = new TrackingSupplier(number);
            var errorHandler = new ErrorHandlerFn<>(21L);
            var result = lockHelper.withLock(lock, 2, TimeUnit.SECONDS, supplier, errorHandler);

            assertThat(result).isEqualTo(number);
            assertThat(supplier.wasCalled).isTrue();
            assertThat(errorHandler.wasCalled).isFalse();

            verify(lock).acquire(2, TimeUnit.SECONDS);
        }

        @Test
        void shouldCallErrorHandlerFunction_WhenLockAcquisitionFails() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(false);

            var supplier = new TrackingSupplier();
            var fallback = 42L;
            var errorHandler = new ErrorHandlerFn<>(fallback);
            var result = lockHelper.withLock(lock, 2, TimeUnit.SECONDS, supplier, errorHandler);

            assertThat(supplier.wasCalled).isFalse();
            assertThat(result).isEqualTo(fallback);
            assertThat(errorHandler.wasCalled).isTrue();
            assertThat(errorHandler.errorType).isEqualTo(ErrorType.LOCK_ACQUISITION);
            assertThat(errorHandler.e).isExactlyInstanceOf(LockAcquisitionTimeoutException.class);
        }

        @Test
        void shouldCallErrorHandlerFunction_WhenActionThrowsException() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var supplier = new ThrowingSupplier();
            var fallback = 84;
            var errorHandler = new ErrorHandlerFn<>(fallback);
            var result = lockHelper.withLock(lock, 3, TimeUnit.SECONDS, supplier, errorHandler);

            assertThat(result).isEqualTo(fallback);
            assertThat(errorHandler.wasCalled).isTrue();
            assertThat(errorHandler.errorType).isEqualTo(ErrorType.OPERATION);
            assertThat(errorHandler.e).isExactlyInstanceOf(UncheckedIOException.class);
        }

        @Test
        void shouldAcceptDuration() throws Exception {
            when(lock.acquire(anyLong(), any(TimeUnit.class))).thenReturn(true);

            var timeout = Duration.ofSeconds(3);
            lockHelper.withLock(lock, timeout, new TrackingSupplier(42L), new ErrorHandlerFn<>(84L));

            verify(lock).acquire(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    @Getter
    static class TrackingRunnable implements Runnable {

        boolean wasCalled;

        @Override
        public void run() {
            wasCalled = true;
        }
    }

    static class ThrowingRunnable implements Runnable {

        @Override
        public void run() {
            throw new UncheckedIOException(new IOException("I/O error"));
        }
    }

    @Getter
    static class TrackingSupplier implements Supplier<Long> {

        TrackingSupplier() {
            this(42L);
        }

        TrackingSupplier(Long result) {
            this.result = result;
        }

        final Long result;
        boolean wasCalled;

        @Override
        public Long get() {
            wasCalled = true;
            return result;
        }
    }

    static class ThrowingSupplier implements Supplier<Integer> {

        @Override
        public Integer get() {
            throw new UncheckedIOException(new IOException("I/O error"));
        }
    }

    static class ErrorConsumer implements BiConsumer<ErrorType, RuntimeException> {

        boolean wasCalled;
        ErrorType errorType;
        RuntimeException e;

        @Override
        public void accept(ErrorType errorType, RuntimeException e) {
            wasCalled = true;
            this.errorType = errorType;
            this.e = e;
        }
    }

    static class ErrorHandlerFn<R> implements BiFunction<ErrorType, RuntimeException, R> {

        final R fallback;
        boolean wasCalled;
        ErrorType errorType;
        RuntimeException e;

        public ErrorHandlerFn(R fallback) {
            this.fallback = fallback;
        }

        @Override
        public R apply(ErrorType errorType, RuntimeException e) {
            wasCalled = true;
            this.errorType = errorType;
            this.e = e;
            return fallback;
        }
    }
}
