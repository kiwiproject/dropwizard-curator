package org.kiwiproject.curator.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertOnePropertyViolation;

import io.dropwizard.util.Duration;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.config.provider.FieldResolverStrategy;
import org.kiwiproject.config.provider.ZooKeeperConfigProvider;
import org.kiwiproject.validation.KiwiValidations;

import javax.validation.Validator;

@DisplayName("CuratorConfig")
@ExtendWith(SoftAssertionsExtension.class)
class CuratorConfigTest {

    private CuratorConfig config;
    private Validator validator;

    @BeforeEach
    void setUp() {
        config = new CuratorConfig();
        validator = KiwiValidations.getValidator();
    }

    @Test
    void shouldHaveDefaultValues(SoftAssertions softly) {
        softly.assertThat(config.getZkConnectString()).isEqualTo(CuratorConfig.DEFAULT_ZK_CONNECT_STRING);
        softly.assertThat(config.getBaseSleepTime()).isEqualTo(Duration.milliseconds(CuratorConfig.DEFAULT_BASE_SLEEP_MS));
        softly.assertThat(config.getMaxSleepTime()).isEqualTo(Duration.seconds(CuratorConfig.DEFAULT_MAX_SLEEP_SECONDS));
        softly.assertThat(config.getMaxRetries()).isEqualTo(CuratorConfig.DEFAULT_MAX_RETRIES);
        softly.assertThat(config.getSessionTimeout().toMilliseconds()).isEqualTo(CuratorConfig.DEFAULT_SESSION_TIMEOUT_MS);
        softly.assertThat(config.getConnectionTimeout().toMilliseconds()).isEqualTo(CuratorConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
    }

    @Test
    void shouldExcludeZkConfigProviderFromToString() {
        var config = newCuratorConfigForTests();
        assertThat(config.toString()).doesNotContain("zkConfigProvider");
    }

    @Nested
    class GetZkConnectString {

        @Test
        void shouldReturnExplicitlyConfiguredValue() {
            var zkConnectString = "zk1.test:2181,zk2.test:2181,zk3.test:2181";
            config.setZkConnectString(zkConnectString);

            assertThat(config.getZkConnectString()).isEqualTo(zkConnectString);
        }

        @Test
        void shouldReturnValeProvidedByConfigProvider() {
            var providedValue = "zk4.test:2181,zk5.test:2181,zk6.test:2181";
            var fieldResolverStrategy = FieldResolverStrategy.<String>builder()
                    .explicitValue(providedValue)
                    .build();

            var configProvider = ZooKeeperConfigProvider.builder()
                    .resolverStrategy(fieldResolverStrategy)
                    .build();

            config = new CuratorConfig(configProvider);
            assertThat(config.getZkConnectString()).isEqualTo(providedValue);
        }

        @Test
        void shouldThrow_WhenNoExplicitValue_AndConfigProvider_CannotProvide() {
            var configProvider = ZooKeeperConfigProvider.builder().build();
            config = new CuratorConfig(configProvider);

            assertThatIllegalStateException()
                    .isThrownBy(() -> config.getZkConnectString())
                    .withMessage("No explicit connect string was given, and the ZooKeeperConfigProvider cannot provide a value");
        }
    }

    @Nested
    class Validation {

        @Test
        void shouldNotAllowNull_ZkConnectString() {
            config.setZkConnectString(null);
            assertOnePropertyViolation(validator, config, "zkConnectString");
        }

        @ParameterizedTest
        @ValueSource(strings = {"", " "})
        void shouldNotAllowBlank_ZkConnectString(String value) {
            config.setZkConnectString(value);
            assertOnePropertyViolation(validator, config, "zkConnectString");
        }

        @Test
        void shouldRequireBaseSleepTime() {
            config.setBaseSleepTime(null);
            assertOnePropertyViolation(validator, config, "baseSleepTime");
        }

        @Test
        void shouldRequireMaxSleepTime() {
            config.setMaxSleepTime(null);
            assertOnePropertyViolation(validator, config, "maxSleepTime");
        }

        @Test
        void shouldRequireMaxRetries() {
            config.setMaxRetries(null);
            assertOnePropertyViolation(validator, config, "maxRetries");
        }
    }

    @Nested
    class CopyOf {

        @Test
        void shouldThrow_WhenNullArgument() {
            assertThatThrownBy(() -> CuratorConfig.copyOf(null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldCreateCopy() {
            var original = newCuratorConfigForTests();

            var copy = CuratorConfig.copyOf(original);

            assertThat(copy)
                    .isNotSameAs(original)
                    .isEqualToIgnoringGivenFields(original, "zkConfigProvider");
        }
    }

    @Nested
    class CopyOfWithZkConnectString {

        @Test
        void shouldThrow_WhenNullConfigArgument() {
            assertThatThrownBy(() -> CuratorConfig.copyOfWithZkConnectString(null, "localhost:2181"))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldThrow_WhenBlankConnectStringArgument() {
            var config = newCuratorConfigForTests();

            assertThatThrownBy(() -> CuratorConfig.copyOfWithZkConnectString(config, ""))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldCreateCopy_WithGivenZkConnectString() {
            var original = newCuratorConfigForTests();

            var newZkConnectString = "zk4.test:2181,zk5.test:2181,zk6.test:2181";
            var copy = CuratorConfig.copyOfWithZkConnectString(original, newZkConnectString);

            assertThat(copy)
                    .isNotSameAs(original)
                    .isEqualToIgnoringGivenFields(original, "zkConnectString", "zkConfigProvider");

            assertThat(copy.getZkConnectString()).isEqualTo(newZkConnectString);
        }
    }

    private static CuratorConfig newCuratorConfigForTests() {
        var original = new CuratorConfig();
        original.setZkConnectString("zk1.test:2181,zk2.test:2181,zk3.test:2181");
        original.setSessionTimeout(Duration.seconds(30));
        original.setConnectionTimeout(Duration.seconds(5));
        original.setBaseSleepTime(Duration.milliseconds(750));
        original.setMaxSleepTime(Duration.seconds(15));
        original.setMaxRetries(10);
        return original;
    }

}
