// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.peppe_git.redisstreamsworker.testing.Orphans;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.XClaimArgs;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins down the Redis semantics the poison policy relies on: {@code XPENDING} reports the delivery count
 * <em>before</em> an {@code XCLAIM}, and {@code XCLAIM} increments it.
 *
 * <p>Consequence: a policy that dead-letters when the count read from {@code XPENDING} before claiming
 * is greater than {@code max} lets the handler run {@code max + 1} times. The worker reads the count
 * after its claim instead, so the handler runs at most {@code max} times
 * (see {@link PoisonAndDeadLetterIT}).
 */
class DeliveryCountSemanticsIT extends RedisTestSupport {

    @Test
    void xpendingReportsTheCountBeforeTheClaimAndXclaimIncrementsIt() {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        String id = Orphans.create(redis, q, "default", "ghost", 1).get(0);
        assertThat(count(stream, q.group())).isEqualTo(1);

        redis.xclaim(stream, Consumer.from(q.group(), "other"), XClaimArgs.Builder.minIdleTime(0), id);
        assertThat(count(stream, q.group())).isEqualTo(2);
    }

    @Test
    void aPreClaimGreaterThanPolicyRunsTheHandlerOneTimeMoreThanItsBudget() {
        QueueDefinition q = queue("default");
        String stream = q.lane("default").streamKey();
        String id = Orphans.create(redis, q, "default", "worker-0", 1).get(0);
        int max = 5;
        int executions = 1; // the first delivery, by XREADGROUP
        for (int round = 1; round < 20; round++) {
            long preClaim = count(stream, q.group());
            redis.xclaim(stream, Consumer.from(q.group(), "worker-" + round), XClaimArgs.Builder.minIdleTime(0), id);
            if (preClaim > max) {
                break; // dead-lettered
            }
            executions++;
        }
        assertThat(executions).isEqualTo(max + 1);
    }

    private long count(String stream, String group) {
        List<io.lettuce.core.models.stream.PendingMessage> p = redis.xpending(stream, group, Range.unbounded(),
                Limit.from(1));
        return p.get(0).getRedeliveryCount();
    }
}
