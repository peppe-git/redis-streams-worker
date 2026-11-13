// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.peppe_git.redisstreamsworker.internal.XInfo;
import io.github.peppe_git.redisstreamsworker.testing.Orphans;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.Consumer;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaintenanceIT extends RedisTestSupport {

    @Test
    void prunesIdleConsumersWithoutPendingMessagesAndKeepsTheOthers() throws Exception {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        Orphans.create(redis, q, "default", "dead-with-pending", 1);
        redis.xgroupCreateconsumer(stream, Consumer.from(q.group(), "dead-and-empty"));
        Thread.sleep(1_200);

        started(RedisStreamsWorker.builder(client, q, (m, c) -> Outcome.ACK)
                .options(fastOptions().deadConsumerIdle(Duration.ofSeconds(1))
                        .reclaimAfter(Duration.ofMinutes(5)).reclaimInterval(Duration.ofMinutes(1)).build()).build());

        List<String> names = XInfo.consumers(redis.xinfoConsumers(stream, q.group())).stream()
                .map(XInfo.Consumer::name).toList();
        assertThat(names).contains("dead-with-pending").doesNotContain("dead-and-empty");
    }

    @Test
    void requireNoEvictionRefusesToStartOnAnEvictingRedis() {
        String previous = redis.configGet("maxmemory-policy").get("maxmemory-policy");
        redis.configSet("maxmemory-policy", "allkeys-lru");
        try {
            QueueDefinition q = queue("default");
            RedisStreamsWorker strict = closeLater(RedisStreamsWorker.builder(client, q, (m, c) -> Outcome.ACK)
                    .options(fastOptions().requireNoEviction(true).build()).build());
            assertThatThrownBy(strict::start).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("noeviction");
            RedisStreamsWorker lenient = closeLater(RedisStreamsWorker.builder(client, q, (m, c) -> Outcome.ACK)
                    .options(fastOptions().build()).build());
            lenient.start();
            assertThat(lenient.isRunning()).isTrue();
        } finally {
            redis.configSet("maxmemory-policy", previous);
        }
    }

    @Test
    void aWorkerCanBeStartedOnlyOnce() {
        RedisStreamsWorker worker = started(RedisStreamsWorker.builder(client, queue("default"),
                (m, c) -> Outcome.ACK).options(fastOptions().build()).build());
        assertThatThrownBy(worker::start).isInstanceOf(IllegalStateException.class);
    }
}
