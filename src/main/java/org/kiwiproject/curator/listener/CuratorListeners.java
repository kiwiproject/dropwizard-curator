package org.kiwiproject.curator.listener;

import com.google.common.base.MoreObjects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;

/**
 * Utilities related to Curator listener classes.
 */
@UtilityClass
@Slf4j
public class CuratorListeners {

    /**
     * Add a {@link org.apache.curator.framework.api.CuratorListener} to the given client that logs receipt of
     * Curator events at INFO level.
     *
     * @param client the Curator client
     * @param myName the name for the logging listener, used in log messages
     * @see org.apache.curator.framework.api.CuratorListener
     * @see CuratorEvent
     */
    public static void addLoggingCuratorListener(CuratorFramework client, String myName) {
        var curatorListenable = client.getCuratorListenable();
        curatorListenable.addListener((client1, event) ->
                LOG.info("{} received Curator event: {}", myName, CuratorListeners.descriptionOf(event)));
    }

    /**
     * Add a {@link org.apache.curator.framework.state.ConnectionStateListener} to the  given client that logs state
     * changes at INFO level.
     *
     * @param client the Curator client
     * @param myName the name for the logging listener, used in log messages
     * @see org.apache.curator.framework.state.ConnectionStateListener
     */
    public static void addLoggingConnectionStateListener(CuratorFramework client, String myName) {
        var stateListenable = client.getConnectionStateListenable();
        stateListenable.addListener((client1, newState) ->
                LOG.info("{} received new connection state: {} (connected? {})", myName, newState, newState.isConnected()));
    }

    /**
     * Add an {@link org.apache.curator.framework.api.UnhandledErrorListener} to the given client that logs
     * unhandled errors at WARN level.
     *
     * @param client the Curator client
     * @param myName the name for the logging listener, used in log messages
     * @see org.apache.curator.framework.api.UnhandledErrorListener
     */
    public static void addLoggingUnhandledErrorListener(CuratorFramework client, String myName) {
        var unhandledErrorListenable = client.getUnhandledErrorListenable();
        unhandledErrorListenable.addListener((message, e) ->
                LOG.warn("{} received new unhandled error: {}", myName, message, e));
    }

    /**
     * Provide a human-readable description of a Curator event.
     *
     * @param event the Curator event to describe
     * @return event description
     */
    public static String descriptionOf(CuratorEvent event) {
        return MoreObjects.toStringHelper(event)
                .add("type", event.getType())
                .add("resultCode", event.getResultCode())
                .add("path", event.getPath())
                .add("context", event.getContext())
                .add("stat", event.getStat())
                .add("data", event.getData())
                .add("name", event.getName())
                .add("children", event.getChildren())
                .add("ACLList", event.getACLList())
                .add("watchedEvent", event.getWatchedEvent())
                .toString();
    }
}
