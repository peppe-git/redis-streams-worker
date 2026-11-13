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
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** FLUSHALL and a destroyed consumer group while the worker runs. */
class DataLossChaosIT extends RedisTestSupport {

    @Test
    void afterFlushallTheGroupIsRecreatedAndMessagesProducedMeanwhileAreNotLost() throws Exception {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        List<String> handled = new CopyOnWriteArrayList<>();
        List<String> rejected = new CopyOnWriteArrayList<>();
        List<Boolean> recreated = new CopyOnWriteArrayList<>();
        RedisStreamsWorker worker = started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            if ("inflight".equals(m.field("n"))) {
                started.countDown();
                release.await(30, TimeUnit.SECONDS);
            }
            handled.add(m.field("n"));
            return Outcome.ACK;
        }).options(fastOptions().build()).listener(new WorkerListener() {
            @Override
            public void onAckRejected(QueueMessage message, String reason) {
                rejected.add(message.field("n") + ":" + reason);
            }

            @Override
            public void onGroupCreated(String lane, boolean again) {
                recreated.add(again);
            }
        }).build());
        producer.send(Map.of("n", "inflight"));
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        redis.flushall();
        producer.send(Map.of("n", "right-after-flush")); // recreates the stream, without the group

        await().atMost(Duration.ofSeconds(10)).until(() -> handled.contains("right-after-flush"));
        assertThat(recreated).contains(true);
        release.countDown();
        await().atMost(Duration.ofSeconds(10)).until(() -> rejected.contains("inflight:not-pending"));
        producer.send(Map.of("n", "later"));
        await().atMost(Duration.ofSeconds(10)).until(() -> handled.contains("later"));
        assertThat(worker.isRunning()).isTrue();
        assertThat(pendingCount(q.lane("default").streamKey(), q.group())).isZero();
    }

    @Test
    void afterTheGroupIsDestroyedEveryMessageIsStillHandledAtLeastOnce() {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        Map<String, Integer> handled = new ConcurrentHashMap<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            handled.merge(m.field("n"), 1, Integer::sum);
            return Outcome.ACK;
        }).options(fastOptions().trimInterval(Duration.ZERO).build()).build());
        for (int i = 0; i < 20; i++) {
            producer.send(Map.of("n", "a" + i));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> handled.size() == 20);

        redis.xgroupDestroy(stream, q.group());
        for (int i = 0; i < 5; i++) {
            producer.send(Map.of("n", "b" + i));
        }

        await().atMost(Duration.ofSeconds(15)).until(() -> handled.size() == 25);
        Set<String> all = handled.keySet();
        assertThat(all).hasSize(25);
        // The group restarts from the beginning of what is left in the stream: the first 20 come back.
        // That is the documented price of GroupStart.EARLIEST (at-least-once, never skipped).
        assertThat(handled.values()).allMatch(n -> n >= 1);
        await().atMost(Duration.ofSeconds(10)).until(() -> pendingCount(stream, q.group()) == 0);
    }
}
