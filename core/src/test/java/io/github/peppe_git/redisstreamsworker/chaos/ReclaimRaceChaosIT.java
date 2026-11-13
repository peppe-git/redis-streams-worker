// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.testing.Orphans;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.RepeatedTest;

/** Four workers start together on the same orphaned pending entries: each message runs exactly once. */
class ReclaimRaceChaosIT extends RedisTestSupport {

    @RepeatedTest(3)
    void concurrentReclaimersNeverRunTheSameMessageTwice() throws Exception {
        QueueDefinition q = queue("default");
        List<String> ids = Orphans.create(redis, q, "default", "ghost", 200);
        Thread.sleep(300); // all entries idle beyond reclaimAfter
        Map<String, List<String>> runsById = new ConcurrentHashMap<>();
        List<RedisStreamsWorker> workers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            workers.add(closeLater(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
                runsById.computeIfAbsent(m.id(), k -> new CopyOnWriteArrayList<>()).add(ctx.consumerName());
                return Outcome.ACK;
            }).options(fastOptions().consumerName("w" + i).concurrency(16).reclaimAfter(Duration.ofMillis(200))
                    .reclaimInterval(Duration.ofMillis(20)).build()).build()));
        }
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService starter = Executors.newFixedThreadPool(4);
        for (RedisStreamsWorker w : workers) {
            starter.submit(() -> {
                go.await();
                w.start();
                return null;
            });
        }
        go.countDown();
        starter.shutdown();

        await().atMost(Duration.ofSeconds(30)).until(() -> runsById.size() == 200
                && pendingCount(q.lane("default").streamKey(), q.group()) == 0);
        Thread.sleep(500);
        assertThat(runsById.keySet()).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(runsById.values()).allMatch(consumers -> consumers.size() == 1);
    }
}
