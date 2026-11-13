// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.MessageHandler;
import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.QueueInspector;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.StreamProducer;
import io.github.peppe_git.redisstreamsworker.WorkerListener;
import io.github.peppe_git.redisstreamsworker.lock.RedisLock;
import io.github.peppe_git.redisstreamsworker.lock.RedisSemaphore;
import io.github.peppe_git.redisstreamsworker.testing.RedisContainers;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;

/**
 * Auto-configuration scenarios against a real Redis, shared by the Spring Boot 3 build and the Spring
 * Boot 4 compatibility module: subclasses only say which Spring Boot class configures Redis.
 */
public abstract class AutoConfigurationScenarios {

    private RedisClient db3;
    private StatefulRedisConnection<String, String> db3Connection;

    /** {@code RedisAutoConfiguration} (Boot 3) or {@code DataRedisAutoConfiguration} (Boot 4). */
    protected abstract Class<?> springBootRedisAutoConfiguration();

    protected ApplicationContextRunner runner() {
        GenericContainer<?> redis = RedisContainers.shared();
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(springBootRedisAutoConfiguration(),
                        RedisStreamsWorkerAutoConfiguration.class, RedisLockAutoConfiguration.class))
                .withPropertyValues(
                        "spring.data.redis.host=" + redis.getHost(),
                        "spring.data.redis.port=" + redis.getMappedPort(6379),
                        "spring.data.redis.database=3",
                        "redis-streams-worker.key-prefix=app",
                        "redis-streams-worker.worker.poll-timeout=100ms",
                        "redis-streams-worker.worker.reclaim-interval=100ms",
                        "redis-streams-worker.worker.drain-timeout=5s");
    }

    @BeforeEach
    void cleanRedis() {
        GenericContainer<?> redis = RedisContainers.shared();
        db3 = RedisClient.create(RedisURI.builder().withHost(redis.getHost()).withPort(redis.getMappedPort(6379))
                .withDatabase(3).build());
        db3Connection = db3.connect();
        db3Connection.sync().flushall();
    }

    @AfterEach
    void closeRedis() {
        db3Connection.close();
        db3.shutdown();
    }

    @Test
    void producerSideOnlyWithoutAHandler() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(QueueDefinition.class);
            assertThat(context).hasSingleBean(StreamProducer.class);
            assertThat(context).hasSingleBean(QueueInspector.class);
            assertThat(context).hasSingleBean(RedisLock.class);
            assertThat(context).hasSingleBean(RedisSemaphore.class);
            assertThat(context).doesNotHaveBean(RedisStreamsWorker.class);
            assertThat(context.getBean(QueueDefinition.class).lane("default").streamKey()).isEqualTo("app:default");
        });
    }

    @Test
    void aHandlerBeanStartsAWorkerThatUsesTheSpringDataRedisSettings() {
        runner().withUserConfiguration(RecordingHandler.class).run(context -> {
            assertThat(context).hasNotFailed();
            RedisStreamsWorker worker = context.getBean(RedisStreamsWorker.class);
            assertThat(worker.isRunning()).isTrue();
            context.getBean(StreamProducer.class).send(Map.of("hello", "world"));
            RecordingHandler handler = context.getBean(RecordingHandler.class);
            await().atMost(Duration.ofSeconds(10)).until(() -> handler.seen.size() == 1);
            assertThat(handler.seen).containsExactly("world");
            // database 3 from spring.data.redis.database, not the default 0
            assertThat(db3Connection.sync().exists("app:default")).isEqualTo(1);
        });
    }

    @Test
    void closingTheContextDrainsTheRunningMessage() {
        SlowHandler.reset();
        runner().withUserConfiguration(SlowHandler.class).run(context -> {
            context.getBean(StreamProducer.class).send(Map.of("k", "v"));
            assertThat(SlowHandler.started.await(10, TimeUnit.SECONDS)).isTrue();
        });
        // run() closes the context: the lifecycle stopped the worker after the handler finished.
        assertThat(SlowHandler.finished.getCount()).isZero();
        assertThat(db3Connection.sync().xpending("app:default", "workers").getCount()).isZero();
    }

    @Test
    void theWorkerCanBeTurnedOffForProducerOnlyProcesses() {
        runner().withUserConfiguration(RecordingHandler.class)
                .withPropertyValues("redis-streams-worker.worker.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(RedisStreamsWorker.class);
                    assertThat(context).hasSingleBean(StreamProducer.class);
                });
    }

    @Test
    void propertiesShapeTheQueueAndTheWorker() {
        runner().withUserConfiguration(RecordingHandler.class)
                .withPropertyValues(
                        "redis-streams-worker.lanes=urgent,default",
                        "redis-streams-worker.lane-stream-keys.default=legacy:jobs",
                        "redis-streams-worker.group=g1",
                        "redis-streams-worker.worker.consumer-name=node-1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    QueueDefinition q = context.getBean(QueueDefinition.class);
                    assertThat(q.laneNames()).containsExactly("urgent", "default");
                    assertThat(q.lane("default").streamKey()).isEqualTo("legacy:jobs");
                    assertThat(q.group()).isEqualTo("g1");
                    assertThat(context.getBean(RedisStreamsWorker.class).consumerName()).isEqualTo("node-1");
                });
    }

    @Test
    void inconsistentTimingsFailTheStartupWithAnExplanation() {
        runner().withUserConfiguration(RecordingHandler.class)
                .withPropertyValues("redis-streams-worker.worker.lease-ttl=30s",
                        "redis-streams-worker.worker.heartbeat-interval=20s")
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("leaseTtl").hasMessageContaining("heartbeatInterval"));
    }

    @Test
    void everythingCanBeDisabled() {
        runner().withUserConfiguration(RecordingHandler.class)
                .withPropertyValues("redis-streams-worker.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(StreamProducer.class);
                    assertThat(context).doesNotHaveBean(RedisLock.class);
                });
        runner().withPropertyValues("redis-streams-worker.lock.enabled=false").run(context -> {
            assertThat(context).doesNotHaveBean(RedisLock.class);
            assertThat(context).hasSingleBean(StreamProducer.class);
        });
    }

    @Test
    void anApplicationRedisClientBeanWinsOverSpringDataRedis() {
        runner().withUserConfiguration(OwnClient.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(RedisStreamsRedisClient.class).client())
                    .isSameAs(context.getBean(OwnClient.class).client);
            context.getBean(StreamProducer.class).send(Map.of("a", "b"));
            assertThat(db3Connection.sync().exists("app:default")).isZero(); // OwnClient uses database 5
        });
    }

    @Test
    void withoutAnyRedisClientTheFailureSaysWhatToAdd() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(RedisStreamsWorkerAutoConfiguration.class))
                .run(context -> assertThat(context).getFailure().rootCause()
                        .hasMessageContaining("needs a Lettuce RedisClient"));
    }

    @Test
    void listenerBeansAreWiredIn() {
        runner().withUserConfiguration(RecordingHandler.class, ListenerConfig.class).run(context -> {
            assertThat(context.getBean(ListenerConfig.class).started).hasSize(1);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class RecordingHandler implements MessageHandler {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @Override
        public Outcome handle(io.github.peppe_git.redisstreamsworker.QueueMessage message,
                io.github.peppe_git.redisstreamsworker.MessageContext context) {
            seen.add(message.field("hello"));
            return Outcome.ACK;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class SlowHandler {
        static CountDownLatch started;
        static CountDownLatch finished;

        static void reset() {
            started = new CountDownLatch(1);
            finished = new CountDownLatch(1);
        }

        @Bean
        MessageHandler slowHandler() {
            return (m, ctx) -> {
                started.countDown();
                Thread.sleep(500);
                finished.countDown();
                return Outcome.ACK;
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OwnClient {
        final RedisClient client;

        OwnClient() {
            GenericContainer<?> redis = RedisContainers.shared();
            client = RedisClient.create(RedisURI.builder().withHost(redis.getHost())
                    .withPort(redis.getMappedPort(6379)).withDatabase(5).build());
        }

        @Bean(destroyMethod = "shutdown")
        RedisClient ownRedisClient() {
            return client;
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ListenerConfig {
        final List<String> started = new CopyOnWriteArrayList<>();

        @Bean
        WorkerListener recordingListener() {
            return new WorkerListener() {
                @Override
                public void onStarted(String consumerName) {
                    started.add(consumerName);
                }
            };
        }
    }
}
