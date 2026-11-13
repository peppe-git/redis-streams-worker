// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.Range;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SafeTrimIT extends RedisTestSupport {

    @Test
    void trimsAcknowledgedEntriesButNeverPendingOrUndeliveredOnes() {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        AtomicInteger acked = new AtomicInteger();
        // The first 300 messages are acknowledged; "stuck" stays pending (LEAVE_PENDING) for a while.
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            if ("stuck".equals(m.field("kind"))) {
                return Outcome.LEAVE_PENDING;
            }
            acked.incrementAndGet();
            return Outcome.ACK;
        }).options(fastOptions().reclaimAfter(Duration.ofSeconds(60)).reclaimInterval(Duration.ofSeconds(1))
                .trimInterval(Duration.ofMillis(200)).build()).build());

        for (int i = 0; i < 300; i++) {
            producer.send(Map.of("kind", "ok", "n", Integer.toString(i)));
        }
        String stuck = producer.send(Map.of("kind", "stuck"));
        List<String> after = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            after.add(producer.send(Map.of("kind", "ok", "n", "after" + i)));
        }

        await().atMost(Duration.ofSeconds(20)).until(() -> acked.get() == 600);
        // The stuck entry and everything after it must survive; most of the first 300 must be gone.
        await().atMost(Duration.ofSeconds(10)).until(() -> redis.xlen(stream) < 450);
        assertThat(redis.xrange(stream, Range.create(stuck, stuck))).hasSize(1);
        assertThat(pendingCount(stream, q.group())).isEqualTo(1);
    }

    @Test
    void doesNotTrimAStreamWhoseEntriesWereNeverDelivered() throws Exception {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        AtomicInteger capacity = new AtomicInteger(0);
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> Outcome.ACK)
                .options(fastOptions().trimInterval(Duration.ofMillis(100)).build())
                .capacityProvider(inFlight -> capacity.get()).build());
        for (int i = 0; i < 250; i++) {
            producer.send(Map.of("n", Integer.toString(i)));
        }
        Thread.sleep(1_000);
        assertThat(redis.xlen(stream)).isEqualTo(250);
    }
}
