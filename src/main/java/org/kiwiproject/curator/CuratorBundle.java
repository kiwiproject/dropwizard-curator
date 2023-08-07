package org.kiwiproject.curator;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.ConfiguredBundle;
import io.dropwizard.core.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.kiwiproject.curator.config.CuratorConfigured;
import org.kiwiproject.curator.exception.CuratorStartupFailureException;
import org.kiwiproject.curator.health.CuratorHealthCheck;

import java.util.Optional;

/**
 * A Dropwizard bundle that creates a {@link CuratorFramework} instance, wrapped in a Dropwizard
 * {@link io.dropwizard.lifecycle.Managed} instance.
 * <p>
 * Starts the {@link CuratorFramework} immediately, i.e. does not wait for Dropwizard to do it. Also, adds a
 * health check ({@link CuratorHealthCheck}).
 *
 * @param <C> configuration class type
 */
@Slf4j
public class CuratorBundle<C extends Configuration & CuratorConfigured> implements ConfiguredBundle<C> {

    private final CuratorFrameworkHelper curatorFrameworkHelper = new CuratorFrameworkHelper();
    private ManagedCuratorFramework managedClient;

    @Override
    public void run(C configuration, Environment environment) {
        runInternal(configuration, environment, curatorFrameworkHelper);
    }

    @VisibleForTesting
    void runInternal(C configuration, Environment environment, CuratorFrameworkHelper curatorFrameworkHelper) {
        var curatorConfig = configuration.getCuratorConfig();
        var client = curatorFrameworkHelper.createCuratorFramework(curatorConfig);
        managedClient = new ManagedCuratorFramework(client);

        environment.lifecycle().manage(managedClient);
        tryStartCurator();

        environment.healthChecks().register(
                curatorConfig.getHealthCheckName(),
                new CuratorHealthCheck(client, curatorConfig.getZkConnectString()));

        LOG.info("Started Curator, registered managed Curator client [ {} ], and registered health check with name '{}'",
                managedClient, curatorConfig.getHealthCheckName());
    }

    private void tryStartCurator() {
        try {
            managedClient.start();
        } catch (Exception e) {
            throw new CuratorStartupFailureException("Error starting Curator", e);
        }
    }

    /**
     * Once the bundle has been run, this will return the {@link ManagedCuratorFramework}.
     *
     * @return the {@link ManagedCuratorFramework} if run has been called, otherwise {@code null}
     */
    public ManagedCuratorFramework getManagedClient() {
        return managedClient;
    }

    /**
     * Once the bundle has been run, this will return the {@link CuratorFramework}.
     *
     * @return the {@link CuratorFramework} if run has been called, otherwise {@code null}
     */
    public CuratorFramework getClient() {
        return Optional.ofNullable(managedClient)
                .map(ManagedCuratorFramework::getUnderlyingClient)
                .orElse(null);
    }
}
