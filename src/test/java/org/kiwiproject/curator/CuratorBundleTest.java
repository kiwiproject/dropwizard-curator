package org.kiwiproject.curator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.Configuration;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.kiwiproject.curator.config.CuratorConfig;
import org.kiwiproject.curator.config.CuratorConfigured;
import org.kiwiproject.curator.exception.CuratorStartupFailureException;
import org.kiwiproject.curator.health.CuratorHealthCheck;
import org.kiwiproject.test.curator.CuratorTestingServerExtension;
import org.kiwiproject.test.dropwizard.mockito.DropwizardMockitoMocks;

@DisplayName("CuratorBundle")
class CuratorBundleTest {

    @RegisterExtension
    static final CuratorTestingServerExtension ZK_TEST_SERVER = new CuratorTestingServerExtension();

    private CuratorBundle<SampleConfig> bundle;
    private SampleConfig config;
    private Environment environment;
    private LifecycleEnvironment lifecycle;
    private HealthCheckRegistry healthChecks;

    @BeforeEach
    void setUp() {
        config = new SampleConfig();

        var dropwizardMockitoContext = DropwizardMockitoMocks.mockDropwizard();
        lifecycle = dropwizardMockitoContext.lifecycle();
        healthChecks = dropwizardMockitoContext.healthChecks();
        environment = dropwizardMockitoContext.environment();

        bundle = new CuratorBundle<>();
    }

    @Test
    void shouldReturnNullManagedClient_WhenBundleHasNotRun() {
        assertThat(bundle.getManagedClient()).isNull();
    }

    @Test
    void shouldReturnNullUnderlyingClient_WhenBundleHasNotRun() {
        assertThat(bundle.getClient()).isNull();
    }

    @Test
    void shouldCreateManagedCuratorFramework() {
        bundle.run(config, environment);

        assertThat(bundle.getManagedClient()).isNotNull();
    }

    @Test
    void shouldCreateCuratorFramework() {
        bundle.run(config, environment);

        assertThat(bundle.getClient()).isNotNull();
    }

    @Test
    void shouldManageCuratorFramework() {
        bundle.run(config, environment);

        verify(lifecycle).manage(any(ManagedCuratorFramework.class));
    }

    @Test
    void shouldStartCuratorFrameworkImmediately() {
        assertThat(bundle.getManagedClient())
                .describedAs("test precondition violated")
                .isNull();

        bundle.run(config, environment);

        assertThat(bundle.getManagedClient().getUnderlyingClient()).isSameAs(bundle.getClient());
        assertThat(bundle.getClient().getState()).isEqualTo(CuratorFrameworkState.STARTED);
    }

    @Test
    void shouldRegistersCuratorHealthCheck() {
        bundle.run(config, environment);

        verify(healthChecks).register(eq("curator"), any(CuratorHealthCheck.class));
    }

    @Test
    void shouldThrowWhenCuratorDoesNotStart() {
        var mockCuratorHelper = mock(CuratorFrameworkHelper.class);

        var mockCuratorClient = mock(CuratorFramework.class);
        when(mockCuratorHelper.createCuratorFramework(any(CuratorConfig.class))).thenReturn(mockCuratorClient);
        var cause = new IllegalStateException("Cannot be started more than once");
        doThrow(cause).when(mockCuratorClient).start();

        assertThatThrownBy(() -> bundle.runInternal(config, environment, mockCuratorHelper))
                .isExactlyInstanceOf(CuratorStartupFailureException.class)
                .hasCause(cause);
    }

    @Getter
    @Setter
    private static class SampleConfig extends Configuration implements CuratorConfigured {
        private final CuratorConfig curatorConfig;

        SampleConfig() {
            curatorConfig = new CuratorConfig();
            curatorConfig.setZkConnectString(ZK_TEST_SERVER.getConnectString());
        }
    }
}
