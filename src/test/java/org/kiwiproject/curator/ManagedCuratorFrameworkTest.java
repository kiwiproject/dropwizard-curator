package org.kiwiproject.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ManagedCuratorFramework")
class ManagedCuratorFrameworkTest {

    private ManagedCuratorFramework managedClient;
    private CuratorFramework client;

    @BeforeEach
    void setUp() {
        client = mock(CuratorFramework.class);
        managedClient = new ManagedCuratorFramework(client);
    }

    @Test
    void shouldNotAllowNullClient() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ManagedCuratorFramework(null));
    }

    @Test
    void shouldStartCuratorClient() {
        managedClient.start();

        verify(client).start();
    }

    @Test
    void shouldStart_OnlyOneTime() {
        managedClient.start();
        managedClient.start();
        managedClient.start();
        managedClient.start();

        verify(client, times(1)).start();
    }

    @Test
    void testStop() {
        managedClient.stop();

        verify(client).close();
    }

    @Test
    void shouldGetUnderlyingClient() {
        assertThat(managedClient.getUnderlyingClient()).isSameAs(client);
    }

    @Test
    void shouldHaveToString() {
        when(client.getState()).thenReturn(CuratorFrameworkState.LATENT);
        when(client.toString()).thenReturn("org.apache.curator.framework.imps.CuratorFrameworkImpl@39a8312f");

        var description = managedClient.toString();

        System.out.println("description = " + description);

        assertThat(description)
                .contains("started=false")
                .contains("client.state=LATENT")
                .contains("client=org.apache.curator.framework.imps.CuratorFrameworkImpl@39a8312f");
    }
}
