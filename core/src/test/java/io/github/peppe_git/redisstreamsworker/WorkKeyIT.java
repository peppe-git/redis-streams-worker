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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WorkKeyIT extends RedisTestSupport {

    @Test
    void twoMessagesForTheSameWorkNeverRunTogetherAndTheSecondKeepsItsLease() throws Exception {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxRunning = new AtomicInteger();
        List<String> order = new CopyOnWriteArrayList<>();
        List<String> deferred = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
            order.add(m.field("attempt"));
            if ("1".equals(m.field("attempt"))) {
                releaseFirst.await(20, TimeUnit.SECONDS);
            }
            running.decrementAndGet();
            return Outcome.ACK;
        }).options(fastOptions().build()).workKey(WorkKeyExtractor.field("job")).listener(new WorkerListener() {
            @Override
            public void onDeferred(QueueMessage message, String workKey) {
                deferred.add(message.id());
            }
        }).build());

        producer.send(Map.of("job", "J", "attempt", "1"));
        await().atMost(Duration.ofSeconds(5)).until(() -> order.size() == 1);
        String secondId = producer.send(Map.of("job", "J", "attempt", "2"));
        await().atMost(Duration.ofSeconds(5)).until(() -> deferred.size() == 1);
        Thread.sleep(2_000); // longer than reclaimAfter and leaseTtl: the deferred message must stay ours
        assertThat(redis.exists(q.leaseKey(q.lane("default"), secondId))).isEqualTo(1);
        assertThat(order).containsExactly("1");

        releaseFirst.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> order.size() == 2);
        assertThat(order).containsExactly("1", "2");
        assertThat(maxRunning.get()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
    }
}
