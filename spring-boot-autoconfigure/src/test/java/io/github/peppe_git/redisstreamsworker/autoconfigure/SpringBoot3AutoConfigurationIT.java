// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;

/** The auto-configuration scenarios on Spring Boot 3.5 with Lettuce 6.6. */
class SpringBoot3AutoConfigurationIT extends AutoConfigurationScenarios {

    @Override
    protected Class<?> springBootRedisAutoConfiguration() {
        return RedisAutoConfiguration.class;
    }
}
