// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.compat;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.peppe_git.redisstreamsworker.autoconfigure.AutoConfigurationScenarios;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;

/** The auto-configuration scenarios on Spring Boot 4 with Lettuce 7. */
class SpringBoot4AutoConfigurationIT extends AutoConfigurationScenarios {

    @Override
    protected Class<?> springBootRedisAutoConfiguration() {
        return DataRedisAutoConfiguration.class;
    }

    @Test
    void reallyRunsOnSpringBoot4AndLettuce7() {
        assertThat(SpringBootVersion.getVersion()).startsWith("4.");
        assertThat(RedisClient.class.getPackage().getImplementationVersion()).startsWith("7.");
    }
}
