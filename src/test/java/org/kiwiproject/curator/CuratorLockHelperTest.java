package org.kiwiproject.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.curator.exception.LockAcquisitionFailureException;
import org.kiwiproject.curator.exception.LockAcquisitionTimeoutException;

import java.util.concurrent.TimeUnit;

@DisplayName("CuratorLockHelper")
class CuratorLockHelperTest {

    private CuratorLockHelper lockHelper;
    private CuratorFramework client;
    private InterProcessMutex lock;

    @BeforeEach
    void setUp() {
        client = mock(CuratorFramework.class);
        lock = mock(InterProcessMutex.class);
        lockHelper = new CuratorLockHelper();
    }

    @Test
    void shouldCreateInterProcessMutex() {
        var lock = lockHelper.createInterProcessMutex(client, "/lock-path");
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
}
