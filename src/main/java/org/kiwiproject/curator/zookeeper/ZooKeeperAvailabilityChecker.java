package org.kiwiproject.curator.zookeeper;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.tuple.Pair;
import org.kiwiproject.curator.config.CuratorConfig;
import org.kiwiproject.net.SocketChecker;

/**
 * Utility for checking whether ZooKeeper servers are available.
 */
public class ZooKeeperAvailabilityChecker {

    private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final SocketChecker socketChecker;

    /**
     * Create new instance using a default {@link SocketChecker}.
     */
    public ZooKeeperAvailabilityChecker() {
        this(new SocketChecker());
    }

    /**
     * Create new instance using the given {@link SocketChecker}.
     *
     * @param socketChecker the checker to use
     */
    public ZooKeeperAvailabilityChecker(SocketChecker socketChecker) {
        this.socketChecker = socketChecker;
    }

    /**
     * Check if any ZooKeepers are available using the connection string in the {@link CuratorConfig}.
     */
    public boolean anyZooKeepersAvailable(CuratorConfig curatorConfig) {
        return anyZooKeepersAvailable(curatorConfig.getZkConnectString());
    }

    /**
     * Check if any ZooKeepers are available using the given connection string.
     */
    public boolean anyZooKeepersAvailable(String zkConnectString) {
        return COMMA_SPLITTER.splitToList(zkConnectString)
                .stream()
                .map(ZooKeeperAvailabilityChecker::toHostPortPair)
                .anyMatch(socketChecker::canConnectViaSocket);
    }

    private static Pair<String, Integer> toHostPortPair(String hostAndPort) {
        String[] splat = hostAndPort.split(":");
        return Pair.of(splat[0], Integer.parseInt(splat[1]));
    }
}
