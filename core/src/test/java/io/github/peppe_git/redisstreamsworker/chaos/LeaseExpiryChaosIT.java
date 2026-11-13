// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.MessageHandler;
import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.QueueMessage;
import io.github.peppe_git.redisstreamsworker.ReclaimGuard;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.StreamProducer;
import io.github.peppe_git.redisstreamsworker.WorkerListener;
import io.github.peppe_git.redisstreamsworker.WorkerOptions;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The lease of a running message expires while its handler is still alive (a long pause, Redis slower
 * than the TTL). Simulated by deleting the lease right after a heartbeat.
 */
class LeaseExpiryChaosIT extends RedisTestSupport {

    /** Slow heartbeat on the owner, so that the other worker reclaims before the owner notices. */
    private static WorkerOptions owner() {
        return fastOptions().consumerName("a").heartbeatInterval(Duration.ofMillis(1000))
                .leaseTtl(Duration.ofMillis(2000)).build();
    }

    private static WorkerOptions thief() {
        return fastOptions().consumerName("b").reclaimInterval(Duration.ofMillis(50)).build();
    }

    @Test
    void withoutAGuardTheWorkRunsTwiceButOnlyTheCurrentOwnerAcknowledges() throws Exception {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch releaseA = new CountDownLatch(1);
        CountDownLatch releaseB = new CountDownLatch(1);
        CountDownLatch bStarted = new CountDownLatch(1);
        CountDownLatch aLostLease = new CountDownLatch(1);
        List<String> runs = new CopyOnWriteArrayList<>();
        List<Boolean> aLeaseHeldAtEnd = new CopyOnWriteArrayList<>();
        List<String> aRejections = new CopyOnWriteArrayList<>();

        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.add("a");
            releaseA.await(30, TimeUnit.SECONDS);
            aLeaseHeldAtEnd.add(ctx.leaseHeld());
            return Outcome.ACK;
        }).options(owner()).listener(new WorkerListener() {
            @Override
            public void onLeaseLost(QueueMessage message) {
                aLostLease.countDown();
            }

            @Override
            public void onAckRejected(QueueMessage message, String reason) {
                aRejections.add(reason);
            }
        }).build());
        String id = producer.send(Map.of("job", "J"));
        await().atMost(Duration.ofSeconds(5)).until(() -> runs.contains("a"));
        String leaseKey = q.leaseKey(q.lane("default"), id);

        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            runs.add("b");
            bStarted.countDown();
            releaseB.await(30, TimeUnit.SECONDS);
            return Outcome.ACK;
        }).options(thief()).build());
        deleteRightAfterARenewal(leaseKey);

        assertThat(bStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(aLostLease.await(5, TimeUnit.SECONDS)).isTrue();
        releaseA.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> aRejections.size() == 1);
        assertThat(aLeaseHeldAtEnd).containsExactly(false);
        assertThat(aRejections).containsExactly("owned-by-another-consumer");
        // A did not remove the entry from the PEL: if B died now, the message could still be reclaimed.
        assertThat(pendingCount(stream, q.group())).isEqualTo(1);

        releaseB.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(stream, q.group()) == 0);
        assertThat(runs).containsExactly("a", "b"); // at-least-once: this is why ReclaimGuard exists
    }

    @Test
    void aReclaimGuardBackedByAClaimOnTheWorkPreventsTheSecondExecution() throws Exception {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        // Stands for a claim row in the system of record (owner + expiry), shared by every instance.
        Map<String, String> claims = new ConcurrentHashMap<>();
        ReclaimGuard guard = c -> !claims.containsKey(c.message().field("job"));
        CountDownLatch releaseA = new CountDownLatch(1);
        List<String> runs = new CopyOnWriteArrayList<>();
        List<Integer> guardRefusals = new CopyOnWriteArrayList<>();
        MessageHandler claimingHandler = (m, ctx) -> {
            if (claims.putIfAbsent(m.field("job"), ctx.consumerName()) != null) {
                return Outcome.LEAVE_PENDING;
            }
            try {
                runs.add(ctx.consumerName());
                if ("a".equals(ctx.consumerName())) {
                    releaseA.await(30, TimeUnit.SECONDS);
                }
                return Outcome.ACK;
            } finally {
                claims.remove(m.field("job"));
            }
        };
        started(RedisStreamsWorker.builder(client, q, claimingHandler).options(owner()).reclaimGuard(guard).build());
        String id = producer.send(Map.of("job", "J"));
        await().atMost(Duration.ofSeconds(5)).until(() -> runs.contains("a"));
        started(RedisStreamsWorker.builder(client, q, claimingHandler).options(thief())
                .reclaimGuard(c -> {
                    boolean allowed = guard.allowReclaim(c);
                    if (!allowed) {
                        guardRefusals.add(1);
                    }
                    return allowed;
                }).build());

        deleteRightAfterARenewal(q.leaseKey(q.lane("default"), id));
        await().atMost(Duration.ofSeconds(5)).until(() -> guardRefusals.size() >= 3);
        assertThat(runs).containsExactly("a");

        releaseA.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(stream, q.group()) == 0);
        Thread.sleep(1_000);
        assertThat(runs).containsExactly("a");
    }

    /** Waits for a fresh heartbeat (TTL just reset), then deletes the lease: the owner notices ~1 s later. */
    private void deleteRightAfterARenewal(String leaseKey) {
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(5))
                .until(() -> redis.pttl(leaseKey) > 1900);
        redis.del(leaseKey);
    }
}
