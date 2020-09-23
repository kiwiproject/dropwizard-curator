package org.kiwiproject.curator;

import static java.util.Objects.isNull;

import com.google.common.primitives.Ints;
import io.dropwizard.util.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.kiwiproject.curator.config.CuratorConfig;

/**
 * Helper class for managing the lifecycle of {@link CuratorFramework} instances.
 */
@Slf4j
public class CuratorFrameworkHelper {

    /**
     * Creates a new {@link CuratorFramework} instance. It is in {@link CuratorFrameworkState#LATENT} state, meaning
     * not yet started.
     *
     * @param curatorConfig Curator configuration
     * @return a Curator client
     */
    public CuratorFramework createCuratorFramework(CuratorConfig curatorConfig) {
        LOG.trace("Create Curator client with {} using configuration: {}",
                BoundedExponentialBackoffRetry.class.getSimpleName(),
                curatorConfig);

        var retryPolicy = new BoundedExponentialBackoffRetry(
                toMilliseconds(curatorConfig.getBaseSleepTime()),
                toMilliseconds(curatorConfig.getMaxSleepTime()),
                curatorConfig.getMaxRetries());

        return CuratorFrameworkFactory.newClient(curatorConfig.getZkConnectString(),
                toMilliseconds(curatorConfig.getSessionTimeout()),
                toMilliseconds(curatorConfig.getConnectionTimeout()),
                retryPolicy);
    }

    private static int toMilliseconds(Duration duration) {
        return Ints.checkedCast(duration.toMilliseconds());
    }

    /**
     * Creates <em>and starts</em> a new {Kink CuratorFramework} instance.
     * <p>
     * It will be in the {@link CuratorFrameworkState#STARTED} state.
     *
     * @param curatorConfig Curator configuration
     * @return a started Curator client
     */
    public CuratorFramework startCuratorFramework(CuratorConfig curatorConfig) {
        var client = createCuratorFramework(curatorConfig);
        client.start();
        return client;
    }

    /**
     * Closes the specified Curator client, <em>if it is in the STARTED state</em>. Otherwise does nothing.
     *
     * @param client the Curator client to close
     */
    public void closeIfStarted(CuratorFramework client) {
        var clientState = client.getState();

        if (clientState == CuratorFrameworkState.STARTED) {
            LOG.trace("Closing Curator client [{}1", client);
            client.close();
        } else {
            LOG.trace("Curator client [{}] is not STARTED, so cannot stop (state is [{}])", client, clientState);
        }
    }

    /**
     * Closes the specified Curator client, <em>if it is non-null and in the STARTED state</em>. Otherwise does nothing.
     *
     * @param client the Curator client to close, or {@code null}
     * @see #closeIfStarted(CuratorFramework)
     */
    public void closeQuietly(CuratorFramework client) {
        if (isNull(client)) {
            return;
        }

        closeIfStarted(client);
    }

}
