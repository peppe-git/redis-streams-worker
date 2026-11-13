// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import io.github.peppe_git.redisstreamsworker.CapacityProvider;
import io.github.peppe_git.redisstreamsworker.MessageHandler;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.QueueInspector;
import io.github.peppe_git.redisstreamsworker.ReclaimGuard;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.StreamProducer;
import io.github.peppe_git.redisstreamsworker.WorkKeyExtractor;
import io.github.peppe_git.redisstreamsworker.WorkerListener;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

/**
 * Auto-configuration of redis-streams-worker.
 *
 * <ul>
 *   <li>Always (when enabled): {@link QueueDefinition}, {@link StreamProducer}, {@link QueueInspector}.</li>
 *   <li>When a {@link MessageHandler} bean exists and {@code redis-streams-worker.worker.enabled} is not
 *       {@code false}: a {@link RedisStreamsWorker} started and stopped by a {@code SmartLifecycle}.
 *       Optional beans of type {@link ReclaimGuard}, {@link CapacityProvider}, {@link WorkKeyExtractor} and
 *       {@link WorkerListener} are wired in.</li>
 * </ul>
 * Runs after the Redis auto-configuration of Spring Boot 3 ({@code RedisAutoConfiguration}) and 4
 * ({@code DataRedisAutoConfiguration}), named by string so that one jar serves both.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration",
        "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration"})
@ConditionalOnClass(RedisClient.class)
@ConditionalOnProperty(prefix = "redis-streams-worker", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RedisStreamsWorkerProperties.class)
public class RedisStreamsWorkerAutoConfiguration {

    /** An application RedisClient bean wins over the one of Spring Data Redis. */
    @Bean
    @ConditionalOnMissingBean(RedisStreamsRedisClient.class)
    @ConditionalOnBean(RedisClient.class)
    public RedisStreamsRedisClient redisStreamsRedisClient(RedisClient client) {
        return new RedisStreamsRedisClient(client);
    }

    /** Last resort: fail with a message that says what to add. */
    @Bean
    @ConditionalOnMissingBean(RedisStreamsRedisClient.class)
    public RedisStreamsRedisClient redisStreamsMissingRedisClient() {
        throw new IllegalStateException("redis-streams-worker needs a Lettuce RedisClient: add "
                + "spring-boot-starter-data-redis (Lettuce is its default client) or declare a RedisClient bean");
    }

    /**
     * Reuses the native client of Spring Data Redis' LettuceConnectionFactory, so that host, password,
     * database, SSL and Sentinel settings come from {@code spring.data.redis.*}. Injecting the factory makes
     * it a dependency: it is destroyed after the beans that use its client.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(LettuceConnectionFactory.class)
    @ConditionalOnBean(LettuceConnectionFactory.class)
    @ConditionalOnMissingBean({RedisClient.class, RedisStreamsRedisClient.class})
    static class SpringDataRedisClientConfiguration {

        @Bean
        RedisStreamsRedisClient redisStreamsSpringDataRedisClient(LettuceConnectionFactory factory) {
            AbstractRedisClient nativeClient = factory.getRequiredNativeClient();
            if (nativeClient instanceof RedisClient client) {
                return new RedisStreamsRedisClient(client);
            }
            throw new IllegalStateException("redis-streams-worker supports standalone and Sentinel Redis only; "
                    + "the LettuceConnectionFactory uses " + nativeClient.getClass().getSimpleName()
                    + " (Redis Cluster is not supported in this version)");
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueDefinition redisStreamsQueueDefinition(RedisStreamsWorkerProperties properties) {
        QueueDefinition.Builder b = QueueDefinition.builder(properties.getKeyPrefix())
                .group(properties.getGroup())
                .deadLetterMaxLen(properties.getDeadLetterMaxLen());
        for (String lane : properties.getLanes()) {
            String explicit = properties.getLaneStreamKeys().get(lane);
            if (explicit != null) {
                b.lane(lane, explicit);
            } else {
                b.lanes(lane);
            }
        }
        for (String configured : properties.getLaneStreamKeys().keySet()) {
            if (!properties.getLanes().contains(configured)) {
                throw new IllegalStateException("redis-streams-worker.lane-stream-keys names lane '" + configured
                        + "', which is not in redis-streams-worker.lanes " + properties.getLanes());
            }
        }
        return b.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public StreamProducer redisStreamsProducer(RedisStreamsRedisClient client, QueueDefinition queue,
            RedisStreamsWorkerProperties properties) {
        return new StreamProducer(client.client(), queue, properties.getProducer().getMaxLen());
    }

    @Bean
    @ConditionalOnMissingBean
    public QueueInspector redisStreamsQueueInspector(RedisStreamsRedisClient client, QueueDefinition queue) {
        return new QueueInspector(client.client(), queue);
    }

    /** The consumer side, only where a handler exists and the role allows it. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnBean(MessageHandler.class)
    @ConditionalOnProperty(prefix = "redis-streams-worker.worker", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    static class WorkerConfiguration {

        @Bean
        @ConditionalOnMissingBean
        RedisStreamsWorker redisStreamsWorker(RedisStreamsRedisClient client, QueueDefinition queue,
                MessageHandler handler, RedisStreamsWorkerProperties properties,
                ObjectProvider<ReclaimGuard> reclaimGuard, ObjectProvider<CapacityProvider> capacityProvider,
                ObjectProvider<WorkKeyExtractor> workKey, ObjectProvider<WorkerListener> listeners) {
            return RedisStreamsWorker.builder(client.client(), queue, handler)
                    .options(properties.getWorker().toOptions())
                    .reclaimGuard(reclaimGuard.getIfUnique())
                    .capacityProvider(capacityProvider.getIfUnique())
                    .workKey(workKey.getIfUnique())
                    .listener(WorkerListener.composite(listeners.orderedStream().toList()))
                    .build();
        }

        @Bean
        @ConditionalOnMissingBean
        RedisStreamsWorkerLifecycle redisStreamsWorkerLifecycle(RedisStreamsWorker worker,
                RedisStreamsWorkerProperties properties) {
            return new RedisStreamsWorkerLifecycle(worker, properties.getWorker().getPhase());
        }
    }
}
