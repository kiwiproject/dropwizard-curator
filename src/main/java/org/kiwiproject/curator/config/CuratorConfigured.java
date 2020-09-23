package org.kiwiproject.curator.config;

/**
 * This interface is intended to be implemented by a Dropwizard {@link io.dropwizard.Configuration} class, in order
 * to make the intent clear that an application requires a Curator configuration and for consistency across
 * applications.
 */
public interface CuratorConfigured {

    /**
     * @return the Curator configuration
     */
    CuratorConfig getCuratorConfig();
}
