// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.Orphans;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.models.stream.PendingMessage;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReclaimIT extends RedisTestSupport {

    @Test
    void reclaimsMessagesLeftPendingByADeadConsumer() {
        QueueDefinition q = queue("default");
        List<String> ids = Orphans.create(redis, q, "default", "ghost", 3);
        List<QueueMessage> handled = new CopyOnWriteArrayList<>();
        List<String> previous = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            handled.add(m);
            return Outcome.ACK;
        }).options(fastOptions().build()).listener(new WorkerListener() {
            @Override
            public void onReclaimed(QueueMessage message, String previousConsumer) {
                previous.add(previousConsumer);
            }
        }).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> handled.size() == 3);
        assertThat(handled).extracting(QueueMessage::id).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(handled).allMatch(m -> m.deliveryCount() == 2);
        assertThat(previous).containsOnly("ghost");
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
    }

    @Test
    void aLiveLeaseProtectsAPendingMessageFromReclaim() throws Exception {
        QueueDefinition q = queue("default");
        List<String> ids = Orphans.create(redis, q, "default", "slow-but-alive", 1);
        String leaseKey = q.leaseKey(q.lane("default"), ids.get(0));
        redis.psetex(leaseKey, 2_000, "slow-but-alive");
        AtomicInteger runs = new AtomicInteger();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.incrementAndGet();
            return Outcome.ACK;
        }).options(fastOptions().build()).build());

        Thread.sleep(1_200); // well past reclaimAfter (500 ms), lease still alive
        assertThat(runs.get()).isZero();
        await().atMost(Duration.ofSeconds(10)).until(() -> runs.get() == 1); // lease expired
    }

    @Test
    void aReclaimGuardThatSaysNoKeepsTheMessagePendingWithoutSpendingDeliveries() throws Exception {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        Orphans.create(redis, q, "default", "ghost", 1);
        AtomicBoolean workClaimedElsewhere = new AtomicBoolean(true);
        AtomicInteger guardCalls = new AtomicInteger();
        List<Long> deliveries = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            deliveries.add(m.deliveryCount());
            return Outcome.ACK;
        }).options(fastOptions().maxDeliveries(2).build()).reclaimGuard(c -> {
            guardCalls.incrementAndGet();
            assertThat(c.previousConsumer()).isEqualTo("ghost");
            return !workClaimedElsewhere.get();
        }).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> guardCalls.get() >= 10);
        assertThat(deliveries).isEmpty();
        List<PendingMessage> pending = redis.xpending(stream, q.group(), Range.unbounded(), Limit.from(10));
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getRedeliveryCount()).isEqualTo(1); // untouched by the refusals
        assertThat(pending.get(0).getConsumer()).isEqualTo("ghost");

        workClaimedElsewhere.set(false);
        await().atMost(Duration.ofSeconds(10)).until(() -> deliveries.size() == 1);
        assertThat(deliveries).containsExactly(2L); // not dead-lettered despite maxDeliveries = 2
    }

    @Test
    void aGuardThatThrowsFailsOpen() {
        QueueDefinition q = queue("default");
        Orphans.create(redis, q, "default", "ghost", 1);
        AtomicInteger runs = new AtomicInteger();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.incrementAndGet();
            return Outcome.ACK;
        }).options(fastOptions().build()).reclaimGuard(c -> {
            throw new IllegalStateException("database down");
        }).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> runs.get() == 1);
    }

    @Test
    void aPendingEntryWhoseRecordWasDeletedIsAcknowledgedAndReported() {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        List<String> ids = Orphans.create(redis, q, "default", "ghost", 1);
        redis.xdel(stream, ids.get(0));
        List<String> vanished = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> Outcome.ACK).options(fastOptions().build())
                .listener(new WorkerListener() {
                    @Override
                    public void onMessageVanished(String lane, String id) {
                        vanished.add(id);
                    }
                }).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> vanished.size() == 1);
        assertThat(vanished).containsExactly(ids.get(0));
        assertThat(pendingCount(stream, q.group())).isZero();
    }

    @Test
    void aLongRunningHandlerKeepsItsLeaseAndIsNotStolenByAnotherWorker() {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        AtomicInteger runs = new AtomicInteger();
        List<Boolean> leaseHeldAtEnd = new CopyOnWriteArrayList<>();
        MessageHandler slow = (m, ctx) -> {
            runs.incrementAndGet();
            Thread.sleep(4_000); // > leaseTtl (1.5 s) and > reclaimAfter (0.5 s): only the heartbeat saves it
            leaseHeldAtEnd.add(ctx.leaseHeld());
            return Outcome.ACK;
        };
        started(RedisStreamsWorker.builder(client, q, slow).options(fastOptions().consumerName("a").build()).build());
        producer.send(Map.of("job", "long"));
        await().atMost(Duration.ofSeconds(5)).until(() -> runs.get() == 1);
        started(RedisStreamsWorker.builder(client, q, slow).options(fastOptions().consumerName("b").build()).build());

        await().atMost(Duration.ofSeconds(10)).until(() -> leaseHeldAtEnd.size() == 1);
        assertThat(runs.get()).isEqualTo(1);
        assertThat(leaseHeldAtEnd).containsExactly(true);
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
    }
}
