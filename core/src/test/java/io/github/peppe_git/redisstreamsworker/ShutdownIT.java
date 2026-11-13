// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ShutdownIT extends RedisTestSupport {

    @Test
    void stopWaitsForRunningHandlersAndAcknowledgesThem() throws Exception {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch started = new CountDownLatch(1);
        List<Boolean> sawShutdown = new CopyOnWriteArrayList<>();
        RedisStreamsWorker worker = started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            started.countDown();
            Thread.sleep(800);
            sawShutdown.add(ctx.shutdownRequested());
            return Outcome.ACK;
        }).options(fastOptions().build()).build());
        producer.send(Map.of("k", "v"));
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        worker.stop();

        assertThat(sawShutdown).containsExactly(true);
        assertThat(pendingCount(q.lane("default").streamKey(), q.group())).isZero();
        assertThat(worker.isRunning()).isFalse();
    }

    @Test
    void aHandlerInterruptedByShutdownLeavesItsMessageForAnotherWorker() throws Exception {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch started = new CountDownLatch(1);
        List<Outcome> outcomes = new CopyOnWriteArrayList<>();
        RedisStreamsWorker first = started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            started.countDown();
            Thread.sleep(60_000); // ignores shutdownRequested(); only the interrupt stops it
            return Outcome.ACK;
        }).options(fastOptions().drainTimeout(Duration.ofMillis(300)).build()).listener(new WorkerListener() {
            @Override
            public void onMessageCompleted(QueueMessage message, Outcome outcome, Duration d, Throwable e) {
                outcomes.add(outcome);
            }
        }).build());
        producer.send(Map.of("k", "v"));
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        first.stop();
        assertThat(outcomes).containsExactly(Outcome.SHUTDOWN);
        assertThat(pendingCount(q.lane("default").streamKey(), q.group())).isEqualTo(1);
        assertThat(keys(prefix + ":lease:*")).isEmpty(); // released: no need to wait for the TTL

        List<Long> deliveries = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            deliveries.add(m.deliveryCount());
            return Outcome.ACK;
        }).options(fastOptions().build()).build());
        await().atMost(Duration.ofSeconds(10)).until(() -> deliveries.size() == 1);
        assertThat(deliveries).containsExactly(2L);
    }
}
