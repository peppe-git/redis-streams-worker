// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import io.github.peppe_git.redisstreamsworker.lock.LockListener;
import io.github.peppe_git.redisstreamsworker.lock.RedisLock;
import io.github.peppe_git.redisstreamsworker.lock.RedisSemaphore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * {@link RedisLock} and {@link RedisSemaphore} beans when the optional {@code redis-streams-worker-lock}
 * module is on the classpath. Reuses the Redis client resolved by {@link RedisStreamsWorkerAutoConfiguration}.
 */
@AutoConfiguration(after = RedisStreamsWorkerAutoConfiguration.class)
@ConditionalOnClass(RedisLock.class)
@ConditionalOnBean(RedisStreamsRedisClient.class)
@ConditionalOnProperty(prefix = "redis-streams-worker.lock", name = "enabled", havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(RedisLockAutoConfiguration.LockProperties.class)
public class RedisLockAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public RedisLock redisStreamsLock(RedisStreamsRedisClient client, LockProperties properties,
            ObjectProvider<LockListener> listener) {
        return new RedisLock(client.client(), properties.getKeyPrefix(), listener.getIfUnique(() -> LockListener.NONE));
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisSemaphore redisStreamsSemaphore(RedisStreamsRedisClient client, LockProperties properties) {
        return new RedisSemaphore(client.client(), properties.getSemaphoreKeyPrefix());
    }

    /** Settings of the lock module. */
    @ConfigurationProperties(prefix = "redis-streams-worker.lock")
    public static class LockProperties {

        /** Whether the lock beans are created when the module is on the classpath. */
        private boolean enabled = true;

        /** Prefix of lock keys. */
        private String keyPrefix = "rsw:lock";

        /** Prefix of semaphore slot keys. */
        private String semaphoreKeyPrefix = "rsw:sem";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }

        public String getSemaphoreKeyPrefix() {
            return semaphoreKeyPrefix;
        }

        public void setSemaphoreKeyPrefix(String semaphoreKeyPrefix) {
            this.semaphoreKeyPrefix = semaphoreKeyPrefix;
        }
    }
}
