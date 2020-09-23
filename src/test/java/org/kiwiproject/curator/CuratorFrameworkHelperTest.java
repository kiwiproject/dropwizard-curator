package org.kiwiproject.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.dropwizard.util.Duration;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryOneTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.curator.config.CuratorConfig;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;

@DisplayName("CuratorFrameworkHelper")
class CuratorFrameworkHelperTest {

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorFrameworkHelper frameworkHelper;
    private CuratorConfig curatorConfig;

    @BeforeEach
    void setUp() {
        curatorConfig = new CuratorConfig();
        curatorConfig.setConnectionTimeout(Duration.seconds(5));
        curatorConfig.setSessionTimeout(Duration.seconds(45));
        curatorConfig.setZkConnectString(ZK_TEST_SERVER.getConnectString());
        frameworkHelper = new CuratorFrameworkHelper();
    }

    @Test
    void shouldCreateCuratorFramework_InState_LATENT() {
        var client = frameworkHelper.createCuratorFramework(curatorConfig);

        assertThat(client).isNotNull();
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.LATENT);
        assertThat(client.getZookeeperClient().getConnectionTimeoutMs())
                .isEqualTo((int) curatorConfig.getConnectionTimeout().toMilliseconds());
    }

    @Test
    void shouldStartCuratorFramework() {
        try (var client = frameworkHelper.startCuratorFramework(curatorConfig)) {
            assertThat(client).isNotNull();
            assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);
        }
    }

    @Test
    void shouldNotClose_WhenLatent() {
        var client = CuratorFrameworkFactory.newClient(
                curatorConfig.getZkConnectString(),
                new RetryOneTime(250));

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.LATENT);

        frameworkHelper.closeIfStarted(client);

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.LATENT);
    }

    @Test
    void shouldClose_WhenStarted() {
        var client = CuratorFrameworkFactory.newClient(
                curatorConfig.getZkConnectString(),
                new RetryOneTime(250));

        client.start();
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);

        frameworkHelper.closeIfStarted(client);

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);
    }

    @Test
    void shouldNotClose_WhenStopped() {
        var client = CuratorFrameworkFactory.newClient(
                curatorConfig.getZkConnectString(),
                new RetryOneTime(250));

        client.start();
        client.close();
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);

        frameworkHelper.closeIfStarted(client);

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);
    }

    @Test
    void shouldDoNothing_ClosingQuietly_WhenNullArgument() {
        assertThatCode(() -> frameworkHelper.closeQuietly(null)).doesNotThrowAnyException();
    }

    @Test
    void shouldCloseQuietly_WhenStarted() {
        var client = CuratorFrameworkFactory.newClient(
                curatorConfig.getZkConnectString(),
                new RetryOneTime(250));

        client.start();
        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STARTED);

        frameworkHelper.closeQuietly(client);

        assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);
    }
}
