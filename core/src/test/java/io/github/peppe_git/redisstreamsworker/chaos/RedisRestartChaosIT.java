// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.QueueMessage;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.StreamProducer;
import io.github.peppe_git.redisstreamsworker.WorkerListener;
import io.github.peppe_git.redisstreamsworker.testing.RedisContainers;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * Redis goes down (and comes back with its data) while a handler runs: the handler finishes during the
 * outage, the acknowledgement is retried after the restart, and the message is not executed twice.
 */
class RedisRestartChaosIT {

    @Test
    void anAckThatFailsDuringARedisRestartIsDeliveredAfterwardsWithoutRedelivery() throws Exception {
        GenericContainer<?> container = RedisContainers.dedicated(true);
        RedisClient client = RedisClient.create(RedisContainers.uri(container));
        try {
            QueueDefinition q = QueueDefinition.builder("restart").build();
            StatefulRedisConnection<String, String> conn = client.connect();
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch started = new CountDownLatch(1);
            List<QueueMessage> handled = new CopyOnWriteArrayList<>();
            AtomicInteger ackFailures = new AtomicInteger();
            AtomicInteger recoveries = new AtomicInteger();
            RedisStreamsWorker worker = RedisStreamsWorker.builder(client, q, (m, ctx) -> {
                handled.add(m);
                if ("first".equals(m.field("n"))) {
                    started.countDown();
                    release.await(60, TimeUnit.SECONDS);
                }
                return Outcome.ACK;
            }).options(RedisTestSupport.fastOptions().commandTimeout(Duration.ofSeconds(1)).build())
                    .listener(new WorkerListener() {
                        @Override
                        public void onAckFailed(QueueMessage message, Throwable error) {
                            ackFailures.incrementAndGet();
                        }

                        @Override
                        public void onRedisRecovered() {
                            recoveries.incrementAndGet();
                        }
                    }).build();
            worker.start();
            try (StreamProducer producer = new StreamProducer(client, q)) {
                producer.send(Map.of("n", "first"));
                assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

                var docker = DockerClientFactory.instance().client();
                docker.stopContainerCmd(container.getContainerId()).withTimeout(10).exec();
                release.countDown(); // the handler returns ACK while Redis is down
                await().atMost(Duration.ofSeconds(15)).until(() -> ackFailures.get() >= 1);
                Thread.sleep(2_000); // longer than the lease TTL: the lease expires during the outage
                docker.startContainerCmd(container.getContainerId()).exec();

                await().atMost(Duration.ofSeconds(60)).ignoreExceptions()
                        .until(() -> conn.sync().xpending(q.lane("default").streamKey(), q.group()).getCount() == 0);
                producer.send(Map.of("n", "second"));
                await().atMost(Duration.ofSeconds(30)).until(() -> handled.size() == 2);
                Thread.sleep(2_000); // well past reclaimAfter + leaseTtl: nothing may come back
                assertThat(handled).extracting(m -> m.field("n")).containsExactly("first", "second");
                assertThat(recoveries.get()).isGreaterThanOrEqualTo(1);
                assertThat(worker.isRunning()).isTrue();
            } finally {
                worker.stop();
                conn.close();
            }
        } finally {
            client.shutdown();
            container.stop();
        }
    }
}
