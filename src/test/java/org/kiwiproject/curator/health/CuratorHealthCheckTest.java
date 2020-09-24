package org.kiwiproject.curator.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.primitives.Ints;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.ZooKeeper;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;

import java.util.concurrent.TimeUnit;

/**
 * Tests run against a test ZooKeeper server.
 */
@DisplayName("CuratorHealthCheck")
@ExtendWith(SoftAssertionsExtension.class)
class CuratorHealthCheckTest {

    private static final int SESSION_TIMEOUT_MILLIS = Ints.checkedCast(TimeUnit.SECONDS.toMillis(2));
    private static final int CONNECTION_TIMEOUT_MILLIS = Ints.checkedCast(TimeUnit.SECONDS.toMillis(1));
    private static final int RETRY_SLEEP_MILLIS = 500;

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorHealthCheck healthCheck;
    private CuratorFramework client;
    private String zkConnectString;

    @BeforeEach
    void setUp() {
        zkConnectString = ZK_TEST_SERVER.getConnectString();
        client = CuratorFrameworkFactory.newClient(zkConnectString,
                SESSION_TIMEOUT_MILLIS,
                CONNECTION_TIMEOUT_MILLIS,
                new RetryOneTime(RETRY_SLEEP_MILLIS));
        healthCheck = new CuratorHealthCheck(client, zkConnectString);
    }

    @AfterEach
    void tearDown() {
        if (client.getState() == CuratorFrameworkState.STARTED) {
            client.close();
        }
    }

    @Test
    void shouldBeHealthy_WhenCuratorStateIs_STARTED() throws Exception {
        startAndWaitUntilConnected();

        assertThat(healthCheck)
                .isHealthy()
                .hasMessage("Curator [ {} ] is healthy", zkConnectString);
    }

    @Test
    void shouldBeUnhealthy_WhenCuratorStateIs_LATENT() {
        // Do not start client to remain in latent state
        assertThat(healthCheck)
                .isUnhealthy()
                .hasMessage("Curator [ {} ] has not been started - start() has not been called", zkConnectString);
    }

    @Test
    void shouldBeUnhealthy_WhenCuratorStateIs_STOPPED() throws Exception {
        startAndWaitUntilConnected();
        client.close();

        assertThat(healthCheck)
                .isUnhealthy()
                .hasMessage("Curator [ {} ] is stopped", zkConnectString);
    }

    /**
     * Warning: there is lots of mocking here since I don't know how to simulate these errors with a real Curator
     * client connecting to a real ZooKeeper server (even if it's one specifically for testing).
     */
    @Nested
    class UnhealthyEdgeCases {

        private CuratorFramework mockClient;
        private CuratorZookeeperClient mockZookeeperClient;

        @BeforeEach
        void setUp() {
            mockClient = mock(CuratorFramework.class);
            mockZookeeperClient = mock(CuratorZookeeperClient.class);
            when(mockClient.getZookeeperClient()).thenReturn(mockZookeeperClient);

            healthCheck = new CuratorHealthCheck(mockClient, zkConnectString);
        }

        @Test
        void whenCuratorFrameworkStateIsUnknown() {
            // This is the best we can do to simulate a new enum constant being added to CuratorFrameworkState.
            // It is obviously not perfect, but will work.
            when(mockClient.getState()).thenReturn(null);

            assertThat(healthCheck)
                    .isUnhealthy()
                    .hasMessage("Curator [ {} ] has unknown state: null", zkConnectString);
        }

        @Test
        void whenCuratorClientReportsNotConnected() {
            when(mockClient.getState()).thenReturn(CuratorFrameworkState.STARTED);
            when(mockZookeeperClient.isConnected()).thenReturn(false);

            assertThat(healthCheck)
                    .isUnhealthy()
                    .hasMessage("Curator [ {} ] is started but is not connected", zkConnectString);
        }

        @Test
        void whenZooKeeperState_IsConnected_ReadOnly() throws Exception {
            when(mockClient.getState()).thenReturn(CuratorFrameworkState.STARTED);
            when(mockZookeeperClient.isConnected()).thenReturn(true);

            var mockZooKeeper = mock(ZooKeeper.class);
            when(mockZookeeperClient.getZooKeeper()).thenReturn(mockZooKeeper);
            when(mockZooKeeper.getState()).thenReturn(ZooKeeper.States.CONNECTEDREADONLY);

            assertThat(healthCheck)
                    .isUnhealthy()
                    .hasMessage("ZooKeeperState [ {} ] is connected but is read-only", zkConnectString);
        }

        @Test
        void whenErrorListingZnodes() throws Exception {
            when(mockClient.getState()).thenReturn(CuratorFrameworkState.STARTED);
            when(mockZookeeperClient.isConnected()).thenReturn(true);

            var mockZooKeeper = mock(ZooKeeper.class);
            when(mockZookeeperClient.getZooKeeper()).thenReturn(mockZooKeeper);
            when(mockZooKeeper.getState()).thenReturn(ZooKeeper.States.CONNECTED);

            // pretend we got here and then it got un-started...somehow
            when(mockClient.getChildren())
                    .thenThrow(new IllegalStateException("instance must be started before calling this method"));

            assertThat(healthCheck)
                    .isUnhealthy()
                    .hasMessage("Curator [ {} ] - unable to read znodes at root path '/'", zkConnectString);
        }
    }

    private void startAndWaitUntilConnected() throws InterruptedException {
        client.start();
        var connectionEstablished = client.blockUntilConnected(5, TimeUnit.SECONDS);
        assertThat(connectionEstablished).describedAs("Halting the test; we did not get connected!").isTrue();
    }
}
