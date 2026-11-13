// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.XAddArgs;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Two lanes can hold records with the same id (Redis derives ids from the clock, per stream). Leases
 * and in-flight bookkeeping must be per (lane, id), or finishing one message drops the lease of the
 * other while it still runs.
 */
class CrossLaneIdCollisionIT extends RedisTestSupport {

    @Test
    void finishingOneMessageDoesNotTouchTheLeaseOfAnotherWithTheSameIdOnAnotherLane() throws Exception {
        QueueDefinition q = queue("high", "low");
        // Same explicit id on both streams: what two XADD in the same millisecond produce.
        redis.xadd(q.lane("high").streamKey(), new XAddArgs().id("1000-0"), Map.of("n", "high"));
        redis.xadd(q.lane("low").streamKey(), new XAddArgs().id("1000-0"), Map.of("n", "low"));
        Map<String, CountDownLatch> release = new ConcurrentHashMap<>(Map.of(
                "high", new CountDownLatch(1), "low", new CountDownLatch(1)));
        Map<String, CountDownLatch> started = new ConcurrentHashMap<>(Map.of(
                "high", new CountDownLatch(1), "low", new CountDownLatch(1)));
        RedisStreamsWorker worker = started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            started.get(m.lane()).countDown();
            release.get(m.lane()).await(20, TimeUnit.SECONDS);
            return Outcome.ACK;
        }).options(fastOptions().fairnessRatio(0).build()).build());

        assertThat(started.get("high").await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(started.get("low").await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(worker.inFlightCount()).isEqualTo(2);
        String lowLease = q.leaseKey(q.lane("low"), "1000-0");
        String highLease = q.leaseKey(q.lane("high"), "1000-0");
        assertThat(redis.exists(lowLease, highLease)).isEqualTo(2);

        release.get("high").countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> worker.inFlightCount() == 1);
        assertThat(redis.exists(highLease)).isZero();
        assertThat(redis.get(lowLease)).isEqualTo(worker.consumerName());
        Thread.sleep(2_000); // longer than leaseTtl: the heartbeat must still be renewing the low lease
        assertThat(redis.get(lowLease)).isEqualTo(worker.consumerName());

        release.get("low").countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> worker.inFlightCount() == 0);
        assertThat(pendingCount(q.lane("low").streamKey(), q.group())).isZero();
        assertThat(pendingCount(q.lane("high").streamKey(), q.group())).isZero();
    }
}
