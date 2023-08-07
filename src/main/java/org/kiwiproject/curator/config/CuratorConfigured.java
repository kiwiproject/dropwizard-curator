package org.kiwiproject.curator.config;

import io.dropwizard.core.Configuration;

/**
 * This interface is intended to be implemented by a Dropwizard {@link Configuration} class, in order
 * to make the intent clear that an application requires a Curator configuration and for consistency across
 * applications. It is used by the Curator bundle provided in this library.
 *
 * @see org.kiwiproject.curator.CuratorBundle
 */
public interface CuratorConfigured {

    /**
     * @return the Curator configuration
     */
    CuratorConfig getCuratorConfig();
}
