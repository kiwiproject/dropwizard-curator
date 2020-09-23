package org.kiwiproject.curator;

import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import com.google.common.base.MoreObjects;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Dropwizard {@link Managed} that wraps a {@link CuratorFramework} instance, starting and stopping it automatically
 * with the application.
 */
@Slf4j
public class ManagedCuratorFramework implements Managed {

    private final CuratorFramework client;
    private final AtomicBoolean started;

    /**
     * Create new instance that will manage the given client.
     *
     * @param client the {@link CuratorFramework} to manage
     */
    public ManagedCuratorFramework(CuratorFramework client) {
        this.client = requireNotNull(client);
        this.started = new AtomicBoolean();
    }

    /**
     * @return the managed {@link CuratorFramework} client
     */
    public CuratorFramework getUnderlyingClient() {
        return client;
    }

    /**
     * Starts the managed {@link CuratorFramework} client if it is not started.
     */
    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOG.info("Starting CuratorFramework {}, currently in state {}", client, client.getState());

            client.start();

            LOG.info("CuratorFramework {} now in state {}", client, client.getState());
        }
    }

    /**
     * Stops the managed {@link CuratorFramework} client if it is started.
     */
    @Override
    public void stop() {
        LOG.info("Stopping CuratorFramework {}", client);
        client.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("started", started.get())
                .add("client.state", client.getState())
                .add("client", client)
                .toString();
    }
}
