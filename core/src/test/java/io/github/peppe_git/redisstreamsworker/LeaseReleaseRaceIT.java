// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The heartbeat takes a snapshot of the running messages, then renews their leases one by one. A message
 * can finish in between: its acknowledgement (or release) deletes the lease, and the renewal that follows
 * must not write it back. If it does, the lease outlives the message: an acknowledged message leaves a
 * key behind until the TTL, and a message left pending is protected from reclaim for a whole TTL instead
 * of {@code reclaimAfter}.
 *
 * <p>The order is forced with the worker's test hooks, not with timing: the heartbeat is stopped right
 * before it renews the lease, the handler finishes, then the heartbeat goes on.
 */
class LeaseReleaseRaceIT extends RedisTestSupport {

    @ParameterizedTest
    @EnumSource(value = Outcome.class, names = {"ACK", "LEAVE_PENDING", "DEAD_LETTER"})
    void aRenewalThatLosesTheRaceWithTheReleaseDoesNotResurrectTheLease(Outcome outcome) throws Exception {
        QueueDefinition q = queue("default");
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        CountDownLatch handlerRelease = new CountDownLatch(1);
        CountDownLatch handlerDone = new CountDownLatch(1);
        CountDownLatch renewalReached = new CountDownLatch(1);
        CountDownLatch renewalRelease = new CountDownLatch(1);
        CountDownLatch renewalDone = new CountDownLatch(1);
        AtomicBoolean firstRenewal = new AtomicBoolean(true);

        RedisStreamsWorker worker = closeLater(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            handlerRelease.await(30, TimeUnit.SECONDS);
            handlerDone.countDown();
            return outcome;
        }).options(fastOptions().reclaimAfter(Duration.ofMinutes(5)).reclaimInterval(Duration.ofMinutes(1))
                .build()).build());
        worker.hooks(new RedisStreamsWorker.Hooks() {
            @Override
            public void beforeLeaseRenewal(String lane, String id) {
                if (firstRenewal.compareAndSet(true, false)) {
                    renewalReached.countDown();
                    try {
                        renewalRelease.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public void afterLeaseRenewal(String lane, String id) {
                renewalDone.countDown();
            }
        });
        worker.start();
        String id = producer.send(Map.of("k", "v"));
        String leaseKey = q.leaseKey(q.lane("default"), id);

        // 1. The heartbeat has taken its snapshot and stops right before renewing the lease.
        assertThat(renewalReached.await(10, TimeUnit.SECONDS)).isTrue();
        // 2. The handler finishes and the worker deletes the lease (ack, release or dead-letter).
        handlerRelease.countDown();
        assertThat(handlerDone.await(10, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> worker.inFlightCount() == 0);
        assertThat(redis.exists(leaseKey)).isZero();
        // 3. Only now the stale renewal runs.
        renewalRelease.countDown();
        assertThat(renewalDone.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(redis.exists(leaseKey))
                .as("lease of a message that is no longer running must stay deleted (%s)", outcome)
                .isZero();
    }
}
