package org.kiwiproject.curator.zookeeper;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

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
        this.socketChecker = requireNotNull(socketChecker, "socketChecker must not be null");
    }

    /**
     * Check if any ZooKeepers are available using the connection string in the {@link CuratorConfig}.
     *
     * @param curatorConfig the CuratorConfig containing a ZooKeeper connection string
     * @return true if a ZooKeeper is available at the given connection string, otherwise false
     */
    public boolean anyZooKeepersAvailable(CuratorConfig curatorConfig) {
        checkArgumentNotNull(curatorConfig, "curatorConfig must not be null");
        return anyZooKeepersAvailable(curatorConfig.getZkConnectString());
    }

    /**
     * Check if any ZooKeepers are available using the given connection string.
     *
     * @param zkConnectString the ZooKeeper connection string
     * @return true if a ZooKeeper is available at the given connection string, otherwise false
     */
    public boolean anyZooKeepersAvailable(String zkConnectString) {
        checkArgumentNotBlank(zkConnectString, "ZooKeeper connect string must not be blank");
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
