// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WorkerBasicsIT extends RedisTestSupport {

    @Test
    void handlesEveryMessageOnceAndAcknowledgesIt() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        Map<String, AtomicInteger> seen = new ConcurrentHashMap<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            seen.computeIfAbsent(m.field("n"), k -> new AtomicInteger()).incrementAndGet();
            assertThat(m.deliveryCount()).isEqualTo(1);
            return Outcome.ACK;
        }).options(fastOptions().build()).build());

        for (int i = 0; i < 50; i++) {
            producer.send("default", Map.of("n", Integer.toString(i)));
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> seen.size() == 50);
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
        assertThat(seen.values()).allMatch(c -> c.get() == 1);
        assertThat(keys(prefix + ":lease:*")).isEmpty();
    }

    @Test
    void drainsHigherLanesFirst() throws Exception {
        QueueDefinition q = queue("high", "low");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        for (int i = 0; i < 5; i++) {
            producer.send("low", Map.of("n", "low" + i));
        }
        for (int i = 0; i < 5; i++) {
            producer.send("high", Map.of("n", "high" + i));
        }
        List<String> order = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            order.add(m.lane());
            return Outcome.ACK;
        }).options(fastOptions().concurrency(1).batchSize(1).fairnessRatio(0).build()).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> order.size() == 10);
        assertThat(order.subList(0, 5)).containsOnly("high");
        assertThat(order.subList(5, 10)).containsOnly("low");
    }

    @Test
    void fairnessRatioServesTheLowestLaneUnderAContinuousHighFlow() {
        QueueDefinition q = queue("high", "low");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        for (int i = 0; i < 30; i++) {
            producer.send("high", Map.of("n", "h" + i));
        }
        producer.send("low", Map.of("n", "l"));
        List<String> order = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            order.add(m.field("n"));
            return Outcome.ACK;
        }).options(fastOptions().concurrency(1).batchSize(1).fairnessRatio(3).build()).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> order.size() == 31);
        // Three polls served by "high", then the fairness poll reads "low" first.
        assertThat(order.indexOf("l")).isEqualTo(3);
    }

    @Test
    void neverRunsMoreHandlersThanTheCapacity() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxSeen = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            int now = running.incrementAndGet();
            maxSeen.accumulateAndGet(now, Math::max);
            Thread.sleep(30);
            running.decrementAndGet();
            done.incrementAndGet();
            return Outcome.ACK;
        }).options(fastOptions().batchSize(10).build()).capacityProvider(inFlight -> 3).build());

        for (int i = 0; i < 40; i++) {
            producer.send(Map.of("n", Integer.toString(i)));
        }
        await().atMost(Duration.ofSeconds(15)).until(() -> done.get() == 40);
        assertThat(maxSeen.get()).isEqualTo(3);
    }

    @Test
    void aFailingHandlerLeavesTheMessagePendingAndItIsDeliveredAgain() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        List<Long> deliveries = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            deliveries.add(m.deliveryCount());
            if (deliveries.size() == 1) {
                throw new IllegalStateException("boom");
            }
            return Outcome.ACK;
        }).options(fastOptions().build()).build());

        producer.send(Map.of("k", "v"));

        await().atMost(Duration.ofSeconds(10)).until(() -> deliveries.size() == 2);
        assertThat(deliveries).containsExactly(1L, 2L);
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
    }

    @Test
    void producerRejectsReservedFieldsAndUnknownLanes() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> producer.send(Map.of("x-dlq-reason", "x")))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> producer.send("nope", Map.of("a", "b")))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> producer.send(Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void groupStartEarliestDeliversExistingEntriesAndLatestSkipsThem() {
        QueueDefinition early = QueueDefinition.builder(prefix + "e").build();
        QueueDefinition late = QueueDefinition.builder(prefix + "l").build();
        new StreamProducer(connection, early, 0).send(Map.of("n", "before"));
        new StreamProducer(connection, late, 0).send(Map.of("n", "before"));
        List<String> earlySeen = new CopyOnWriteArrayList<>();
        List<String> lateSeen = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, early, (m, c) -> {
            earlySeen.add(m.field("n"));
            return Outcome.ACK;
        }).options(fastOptions().groupStart(GroupStart.EARLIEST).build()).build());
        started(RedisStreamsWorker.builder(client, late, (m, c) -> {
            lateSeen.add(m.field("n"));
            return Outcome.ACK;
        }).options(fastOptions().groupStart(GroupStart.LATEST).build()).build());
        new StreamProducer(connection, late, 0).send(Map.of("n", "after"));

        await().atMost(Duration.ofSeconds(10)).until(() -> earlySeen.size() == 1 && lateSeen.size() == 1);
        assertThat(earlySeen).containsExactly("before");
        assertThat(lateSeen).containsExactly("after");
    }
}
