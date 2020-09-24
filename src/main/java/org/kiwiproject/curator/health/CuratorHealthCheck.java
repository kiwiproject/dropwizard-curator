package org.kiwiproject.curator.health;

import com.codahale.metrics.health.HealthCheck;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * A Dropwizard Metrics health check for a {@link CuratorFramework} instance.
 */
@Slf4j
@AllArgsConstructor
public class CuratorHealthCheck extends HealthCheck {

    private final CuratorFramework client;
    private final String connectString;

    /**
     * Check health of a {@link CuratorFramework}.
     * <p>
     * The {@link CuratorFramework} is considered healthy if its state is {@link CuratorFrameworkState#STARTED}, is
     * connected in read/write state, and can list top-level znodes. Otherwise it is considered unhealthy, even if the
     * state is {@link CuratorFrameworkState#LATENT}.
     *
     * @return the {@link Result}
     * @throws Exception if anything is really wrong
     */
    @Override
    protected Result check() throws Exception {
        var state = client.getState();
        if (state == CuratorFrameworkState.LATENT) {
            return Result.unhealthy("Curator [ %s ] has not been started - start() has not been called", connectString);
        } else if (state == CuratorFrameworkState.STOPPED) {
            return Result.unhealthy("Curator [ %s ] is stopped", connectString);
        } else if (state != CuratorFrameworkState.STARTED) {
            return Result.unhealthy("Curator [ %s ] has unknown state: %s", connectString, state);
        }

        var zookeeperClient = client.getZookeeperClient();
        boolean connected = zookeeperClient.isConnected();
        LOG.trace("CuratorZookeeperClient is connected? {}", connected);
        if (!connected) {
            return Result.unhealthy("Curator [ %s ] is started but is not connected", connectString);
        }

        var zooKeeperState = zookeeperClient.getZooKeeper().getState();
        LOG.trace("ZK state: {}", zooKeeperState);
        if (zooKeeperState == ZooKeeper.States.CONNECTEDREADONLY) {
            return Result.unhealthy("ZooKeeperState [ %s ] is connected but is read-only", connectString);
        }

        try {
            List<String> znodes = client.getChildren().forPath("/");
            LOG.trace("Found znodes at root: {}", znodes);
            return Result.healthy("Curator [ %s ] is healthy", connectString);
        } catch (Exception e) {
            LOG.trace("Error getting root-level znodes", e);
            return Result.unhealthy("Curator [ %s ] - unable to read znodes at root path '/'", connectString);
        }
    }
}
