package org.kiwiproject.curator.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.curator.config.CuratorConfig;
import org.kiwiproject.net.SocketChecker;

@DisplayName("ZooKeeperAvailabilityChecker")
class ZooKeeperAvailabilityCheckerTest {

    private ZooKeeperAvailabilityChecker checker;
    private SocketChecker socketChecker;

    @BeforeEach
    void setUp() {
        socketChecker = mock(SocketChecker.class);
        checker = new ZooKeeperAvailabilityChecker(socketChecker);
    }

    @Test
    void shouldBeAbleToUseRealSocketChecker() {
        checker = new ZooKeeperAvailabilityChecker();

        assertThatCode(() -> checker.anyZooKeepersAvailable("localhost:12345"))
                .describedAs("Using real SocketChecker should not throw any exceptions")
                .doesNotThrowAnyException();
    }

    @Nested
    class AnyZooKeepersAvailable {

        private CuratorConfig config;

        @BeforeEach
        void setUp() {
            config = new CuratorConfig();
        }

        @Test
        void shouldBeTrue_WhenFirstOneIsAvailable() {
            config.setZkConnectString("zk1:2181,zk2:2181,zk3:2181");

            when(socketChecker.canConnectViaSocket(any())).thenReturn(true);

            assertThat(checker.anyZooKeepersAvailable(config)).isTrue();

            verify(socketChecker).canConnectViaSocket(Pair.of("zk1", 2181));
            verify(socketChecker, never()).canConnectViaSocket(Pair.of("zk2", 2181));
            verify(socketChecker, never()).canConnectViaSocket(Pair.of("zk3", 2181));
        }

        @Test
        void shouldBeTrue_WhenFirstOneIsNotAvailable_ButSecondOneIsAvailable() {
            config.setZkConnectString("zk1:2181,zk2:2181,zk3:2181");

            when(socketChecker.canConnectViaSocket(any()))
                    .thenReturn(false)
                    .thenReturn(true);

            assertThat(checker.anyZooKeepersAvailable(config)).isTrue();

            verify(socketChecker).canConnectViaSocket(Pair.of("zk1", 2181));
            verify(socketChecker).canConnectViaSocket(Pair.of("zk2", 2181));
            verify(socketChecker, never()).canConnectViaSocket(Pair.of("zk3", 2181));
        }

        @Test
        void shouldBeFalse_WhenNoneAreAvailable() {
            config.setZkConnectString("zk1:2181,zk2:2181,zk3:2181");

            when(socketChecker.canConnectViaSocket(any())).thenReturn(false);

            assertThat(checker.anyZooKeepersAvailable(config)).isFalse();

            verify(socketChecker).canConnectViaSocket(Pair.of("zk1", 2181));
            verify(socketChecker).canConnectViaSocket(Pair.of("zk2", 2181));
            verify(socketChecker).canConnectViaSocket(Pair.of("zk3", 2181));
        }

        @Test
        void shouldIgnoreWhitespaceInConnectString() {
            when(socketChecker.canConnectViaSocket(any())).thenReturn(false);

            assertThat(checker.anyZooKeepersAvailable("  zk1:2181  ,  zk2:2181  ,  zk3:2181  ")).isFalse();

            verify(socketChecker).canConnectViaSocket(Pair.of("zk1", 2181));
            verify(socketChecker).canConnectViaSocket(Pair.of("zk2", 2181));
            verify(socketChecker).canConnectViaSocket(Pair.of("zk3", 2181));
        }
    }
}
