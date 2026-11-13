// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.peppe_git.redisstreamsworker.QueueInspector.LaneStats;
import io.github.peppe_git.redisstreamsworker.QueueInspector.LiveMessage;
import io.github.peppe_git.redisstreamsworker.QueueInspector.LiveState;
import io.github.peppe_git.redisstreamsworker.testing.Orphans;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InspectorIT extends RedisTestSupport {

    @Test
    void statsAndLiveMessagesDistinguishPendingFromUndelivered() {
        QueueDefinition q = queue("high", "low");
        Orphans.create(redis, q, "low", "ghost", 2);
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        producer.send("low", Map.of("n", "fresh"));
        QueueInspector inspector = closeLater(new QueueInspector(client, q));

        List<LaneStats> stats = inspector.stats();
        assertThat(stats).hasSize(2);
        LaneStats high = stats.get(0);
        LaneStats low = stats.get(1);
        assertThat(high.measured()).isTrue();
        assertThat(high.length()).isZero();
        assertThat(low.measured()).isTrue();
        assertThat(low.length()).isEqualTo(3);
        assertThat(low.pending()).isEqualTo(2);
        assertThat(low.consumers()).isEqualTo(1);

        List<LiveMessage> live = inspector.liveMessages(100);
        assertThat(live).hasSize(3);
        assertThat(live).filteredOn(l -> l.state() == LiveState.PENDING).hasSize(2)
                .allMatch(l -> "ghost".equals(l.consumer()));
        assertThat(live).filteredOn(l -> l.state() == LiveState.UNDELIVERED).singleElement()
                .satisfies(l -> assertThat(l.message().field("n")).isEqualTo("fresh"));
    }

    @Test
    void statsReportUnmeasuredInsteadOfZeroWhenRedisCannotBeRead() {
        QueueInspector closed = new QueueInspector(closedConnection(), queue("default"));
        List<LaneStats> stats = closed.stats();
        assertThat(stats).singleElement().satisfies(s -> {
            assertThat(s.measured()).isFalse();
            assertThat(s.pending()).isZero();
        });
    }

    private io.lettuce.core.api.StatefulRedisConnection<String, String> closedConnection() {
        io.lettuce.core.api.StatefulRedisConnection<String, String> c = client.connect();
        c.close();
        return c;
    }
}
