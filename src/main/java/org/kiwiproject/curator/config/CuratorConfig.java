package org.kiwiproject.curator.config;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import com.google.common.primitives.Ints;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MinDuration;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.kiwiproject.config.provider.FieldResolverStrategy;
import org.kiwiproject.config.provider.ZooKeeperConfigProvider;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for Curator, which is mainly the ZooKeeper connection string along with some properties to configure
 * the retry policy.
 * <p>
 * Currently the only supported retry policy is {@link org.apache.curator.retry.BoundedExponentialBackoffRetry}.
 */
@Getter
@Setter
@ToString(exclude = "zkConfigProvider")
public class CuratorConfig {

    /**
     * We default to localhost:2181 but clearly this will not be useful in most production environments. It might
     * be OK in some unit testing scenarios.
     */
    public static final String DEFAULT_ZK_CONNECT_STRING = "localhost:2181";

    /**
     * Default session timeout when creating new {@link org.apache.curator.framework.CuratorFramework} instances.
     *
     * @implNote Set to the value of {@code DEFAULT_SESSION_TIMEOUT_MS} in
     * {@link org.apache.curator.framework.CuratorFrameworkFactory} which is, unfortunately, private so we cannot
     * simply reference it directly.
     */
    public static final int DEFAULT_SESSION_TIMEOUT_MS = Ints.checkedCast(TimeUnit.SECONDS.toMillis(60));

    /**
     * Default connection timeout when creating new {@link org.apache.curator.framework.CuratorFramework} instances.
     *
     * @implNote Set to the value of {@code DEFAULT_CONNECTION_TIMEOUT_MS}
     * {@link org.apache.curator.framework.CuratorFrameworkFactory} which is, unfortunately, private so we cannot
     * simply reference it directly.
     */
    public static final int DEFAULT_CONNECTION_TIMEOUT_MS = Ints.checkedCast(TimeUnit.SECONDS.toMillis(15));

    /**
     * Default is 29, because (private) max value in {@link org.apache.curator.retry.ExponentialBackoffRetry} is 29.
     */
    public static final int DEFAULT_MAX_RETRIES = 29;

    /**
     * Default for base sleep milliseconds. Required by {@link org.apache.curator.retry.BoundedExponentialBackoffRetry}.
     */
    public static final int DEFAULT_BASE_SLEEP_MS = 500;

    /**
     * Default for max sleep milliseconds. Required by {@link org.apache.curator.retry.BoundedExponentialBackoffRetry}.
     */
    public static final int DEFAULT_MAX_SLEEP_SECONDS = 10;

    /**
     * Default health check name.
     */
    public static final String DEFAULT_HEALTH_CHECK_NAME = "curator";

    @Getter(AccessLevel.NONE)
    private final ZooKeeperConfigProvider zkConfigProvider;

    /**
     * The ZooKeeper connection string, e.g. {@code host1:2181,host2:2181,host3:2181}.
     */
    @NotBlank
    @Getter(AccessLevel.NONE)
    private String zkConnectString;

    /**
     * The ZooKeeper session timeout.
     */
    @NotNull
    private Duration sessionTimeout = Duration.milliseconds(DEFAULT_SESSION_TIMEOUT_MS);

    /**
     * The ZooKeeper connection timeout.
     */
    @NotNull
    private Duration connectionTimeout = Duration.milliseconds(DEFAULT_CONNECTION_TIMEOUT_MS);

    /**
     * Base sleep time for retry policy.
     *
     * @see org.apache.curator.retry.BoundedExponentialBackoffRetry
     */
    @NotNull
    @MinDuration(value = 1, unit = TimeUnit.MILLISECONDS)
    private Duration baseSleepTime = Duration.milliseconds(DEFAULT_BASE_SLEEP_MS);

    /**
     * Maximum sleep time for retry policy.
     *
     * @see org.apache.curator.retry.BoundedExponentialBackoffRetry
     */
    @NotNull
    @MinDuration(value = 10, unit = TimeUnit.MILLISECONDS)
    private Duration maxSleepTime = Duration.seconds(DEFAULT_MAX_SLEEP_SECONDS);

    /**
     * Maximum number of retries for retry policy.
     *
     * @see org.apache.curator.retry.BoundedExponentialBackoffRetry
     */
    @NotNull
    @Min(1)
    private Integer maxRetries = DEFAULT_MAX_RETRIES;

    @NotBlank
    private String healthCheckName = DEFAULT_HEALTH_CHECK_NAME;

    /**
     * Create new instance using a default {@link ZooKeeperConfigProvider} configured with
     * {@link #DEFAULT_ZK_CONNECT_STRING} as the connect string.
     */
    public CuratorConfig() {
        this(defaultZooKeeperConfigProvider());
    }

    /**
     * Create new instance using the given {@link ZooKeeperConfigProvider}.
     *
     * @param zkConfigProvider the config provider
     */
    public CuratorConfig(ZooKeeperConfigProvider zkConfigProvider) {
        this.zkConfigProvider = zkConfigProvider;
    }

    private static ZooKeeperConfigProvider defaultZooKeeperConfigProvider() {
        var fieldResolverStrategy = FieldResolverStrategy.<String>builder()
                .explicitValue(DEFAULT_ZK_CONNECT_STRING)
                .build();

        return ZooKeeperConfigProvider.builder()
                .resolverStrategy(fieldResolverStrategy)
                .build();
    }

    /**
     * Create a copy of the original CuratorConfig.
     *
     * @param original the config to copy
     * @return a new instance with the same values
     */
    public static CuratorConfig copyOf(CuratorConfig original) {
        checkArgumentNotNull(original);
        var copy = new CuratorConfig();
        copy.setZkConnectString(original.getZkConnectString());
        copy.setSessionTimeout(original.getSessionTimeout());
        copy.setConnectionTimeout(original.getConnectionTimeout());
        copy.setBaseSleepTime(original.getBaseSleepTime());
        copy.setMaxSleepTime(original.getMaxSleepTime());
        copy.setMaxRetries(original.getMaxRetries());
        copy.setHealthCheckName(original.getHealthCheckName());
        return copy;
    }

    /**
     * Create a copy of the original CuratorConfig except with {@code newZkConnectString} as the ZooKeeper connect
     * string.
     *
     * @param original           the config to copy
     * @param newZkConnectString the ZooKeeper connect string to set in the returned "copy"
     * @return a new instance with the same values except the connect string
     */
    public static CuratorConfig copyOfWithZkConnectString(CuratorConfig original, String newZkConnectString) {
        checkArgumentNotBlank(newZkConnectString);
        var copy = copyOf(original);
        copy.setZkConnectString(newZkConnectString);
        return copy;
    }

    /**
     * Return the ZooKeeper connect string using an explicitly configured value, or using the resolution of
     * the {@link ZooKeeperConfigProvider} in this instance.
     *
     * @return the ZooKeeper connect string
     * @throws IllegalStateException if there is no explicit connect string value or it cannot be resolved by
     *                               the {@link ZooKeeperConfigProvider}
     */
    public String getZkConnectString() {
        if (isNotBlank(zkConnectString)) {
            return zkConnectString;
        } else if (zkConfigProvider.canProvide()) {
            return zkConfigProvider.getConnectString();
        }

        throw new IllegalStateException(
                "No explicit connect string was given, and the ZooKeeperConfigProvider cannot provide a value");
    }
}
