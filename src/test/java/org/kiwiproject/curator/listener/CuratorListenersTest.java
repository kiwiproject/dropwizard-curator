package org.kiwiproject.curator.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.curator.CuratorFrameworkHelper;
import org.kiwiproject.curator.config.CuratorConfig;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;

@DisplayName("CuratorListeners")
class CuratorListenersTest {

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorFramework client;

    @BeforeEach
    void setUp() {
        var config = new CuratorConfig();
        config.setZkConnectString(ZK_TEST_SERVER.getConnectString());

        client = new CuratorFrameworkHelper().createCuratorFramework(config);
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    void shouldAddLoggingCuratorListener() {
        var listenable = client.getCuratorListenable();
        var listenerContainer = getListenerContainer(listenable);
        var initialCount = listenerContainer.size();

        CuratorListeners.addLoggingCuratorListener(client, "testCuratorListener");
        assertThat(listenerContainer.size()).isEqualTo(initialCount + 1);

        startAndSleepQuietly();
    }

    private StandardListenerManager<?> getListenerContainer(Listenable<?> listenable) {
        assertThat(listenable).isInstanceOf(StandardListenerManager.class);
        return (StandardListenerManager<?>) listenable;
    }

    @Test
    void shouldAddLoggingConnectionStateListener() {
        var listenable = client.getConnectionStateListenable();
        var listenerContainer = getListenerContainer(listenable);
        var initialCount = listenerContainer.size();

        CuratorListeners.addLoggingConnectionStateListener(client, "testConnectionStateListener");
        assertThat(listenerContainer.size()).isEqualTo(initialCount + 1);

        startAndSleepQuietly();
    }

    // The only reason to do this is that you can manually inspect to verify events were logged.
    // We cannot verify automatically without hooking into the logging framework, etc. which is not worth doing now.
    private void startAndSleepQuietly() {
        client.start();
        new DefaultEnvironment().sleepQuietly(150);
    }

    @Test
    void shouldAddLoggingUnhandledErrorListener() {
        var listenable = client.getUnhandledErrorListenable();
        var listenerContainer = getListenerContainer(listenable);
        var initialCount = listenerContainer.size();

        CuratorListeners.addLoggingUnhandledErrorListener(client, "testUnhandledErrorListener");
        assertThat(listenerContainer.size()).isEqualTo(initialCount + 1);
    }

    @Test
    void shouldDescribeCuratorEvent() {
        var event = mock(CuratorEvent.class);
        var description = CuratorListeners.descriptionOf(event);

        assertThat(description)
                .contains("type=")
                .contains("resultCode=")
                .contains("path=")
                .contains("context=")
                .contains("stat=")
                .contains("data=")
                .contains("name=")
                .contains("children=")
                .contains("ACLList=")
                .contains("watchedEvent=");
    }
}
