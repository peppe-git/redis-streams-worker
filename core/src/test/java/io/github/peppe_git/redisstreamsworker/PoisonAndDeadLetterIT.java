// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.internal.StreamIds;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PoisonAndDeadLetterIT extends RedisTestSupport {

    @Test
    void aMessageThatAlwaysFailsRunsExactlyMaxDeliveriesTimesThenIsDeadLettered() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        List<Long> runs = new CopyOnWriteArrayList<>();
        List<String> deadLettered = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.add(m.deliveryCount());
            return Outcome.LEAVE_PENDING;
        }).options(fastOptions().maxDeliveries(3).build()).listener(new WorkerListener() {
            @Override
            public void onDeadLettered(QueueMessage message, String reason) {
                deadLettered.add(reason);
            }
        }).build());

        String id = producer.send(Map.of("order", "42"));

        await().atMost(Duration.ofSeconds(15)).until(() -> deadLettered.size() == 1);
        assertThat(runs).containsExactly(1L, 2L, 3L);
        assertThat(deadLettered).containsExactly(DeadLetter.REASON_MAX_DELIVERIES);
        assertThat(pendingCount(q.lane("default").streamKey(), q.group())).isZero();

        QueueInspector inspector = closeLater(new QueueInspector(client, q));
        List<DeadLetter> dls = inspector.deadLetters(10);
        assertThat(dls).hasSize(1);
        DeadLetter dl = dls.get(0);
        assertThat(dl.payload()).containsExactly(Map.entry("order", "42"));
        assertThat(dl.lane()).isEqualTo("default");
        assertThat(dl.sourceId()).isEqualTo(id);
        assertThat(dl.deliveries()).isEqualTo(4);
        assertThat(dl.reason()).isEqualTo(DeadLetter.REASON_MAX_DELIVERIES);
        assertThat(dl.deadLetteredAt()).isNotNull();
    }

    @Test
    void deadLetterOutcomeMovesTheMessageImmediatelyWithTheHandlerReason() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        AtomicInteger runs = new AtomicInteger();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.incrementAndGet();
            ctx.deadLetterReason("malformed payload");
            return Outcome.DEAD_LETTER;
        }).options(fastOptions().build()).build());

        producer.send(Map.of("garbage", "!"));
        QueueInspector inspector = closeLater(new QueueInspector(client, q));

        await().atMost(Duration.ofSeconds(10)).until(() -> inspector.deadLetterCount() == 1);
        assertThat(runs.get()).isEqualTo(1);
        assertThat(inspector.deadLetters(1).get(0).reason()).isEqualTo("malformed payload");
        assertThat(pendingCount(q.lane("default").streamKey(), q.group())).isZero();
    }

    @Test
    void replayPutsTheMessageBackOnceAndDeleteRemovesIt() {
        QueueDefinition q = queue("high", "low");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        List<String> handled = new CopyOnWriteArrayList<>();
        AtomicInteger attempt = new AtomicInteger();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            if (attempt.incrementAndGet() <= 2) {
                return Outcome.DEAD_LETTER;
            }
            handled.add(m.lane() + ":" + m.field("v") + ":" + m.deliveryCount());
            return Outcome.ACK;
        }).options(fastOptions().build()).build());
        producer.send("low", Map.of("v", "a"));
        producer.send("low", Map.of("v", "b"));
        QueueInspector inspector = closeLater(new QueueInspector(client, q));
        await().atMost(Duration.ofSeconds(10)).until(() -> inspector.deadLetterCount() == 2);

        List<DeadLetter> dls = inspector.deadLetters(10);
        assertThat(StreamIds.compare(dls.get(0).id(), dls.get(1).id())).isPositive(); // newest first
        // a and b ran concurrently: their dead-letter order is not defined, pick them by payload.
        DeadLetter oldest = dls.stream().filter(d -> "a".equals(d.payload().get("v"))).findFirst().orElseThrow();
        DeadLetter newest = dls.stream().filter(d -> "b".equals(d.payload().get("v"))).findFirst().orElseThrow();

        String newId = inspector.replay(oldest.id());
        assertThat(newId).isNotBlank();
        assertThatThrownBy(() -> inspector.replay(oldest.id())).isInstanceOf(NoSuchElementException.class);
        await().atMost(Duration.ofSeconds(10)).until(() -> handled.size() == 1);
        assertThat(handled).containsExactly("low:a:1");

        assertThat(inspector.delete(newest.id())).isTrue();
        assertThat(inspector.delete(newest.id())).isFalse();
        assertThat(inspector.deadLetterCount()).isZero();
    }
}
