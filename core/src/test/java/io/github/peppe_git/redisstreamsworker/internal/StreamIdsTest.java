// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StreamIdsTest {

    @Test
    void comparesNumericallyNotLexically() {
        assertThat(StreamIds.compare("9-0", "10-0")).isNegative();
        assertThat(StreamIds.compare("10-2", "10-10")).isNegative();
        assertThat(StreamIds.compare("10-1", "10-1")).isZero();
        assertThat(StreamIds.min("10-0", "9-5")).isEqualTo("9-5");
        assertThat(StreamIds.min(null, "9-5")).isEqualTo("9-5");
    }

    @Test
    void nextIsTheSmallestGreaterId() {
        assertThat(StreamIds.next("0-0")).isEqualTo("0-1");
        assertThat(StreamIds.next("1700000000000-41")).isEqualTo("1700000000000-42");
        assertThat(StreamIds.next("5-18446744073709551615")).isEqualTo("6-0");
    }

    @Test
    void backoffGrowsWithJitterAndIsCapped() {
        Backoff b = new Backoff(Duration.ofMillis(100), Duration.ofMillis(400));
        long first = b.next().toMillis();
        assertThat(first).isBetween(50L, 150L);
        b.next();
        b.next();
        long capped = b.next().toMillis();
        assertThat(capped).isBetween(200L, 600L);
        b.reset();
        assertThat(b.next().toMillis()).isBetween(50L, 150L);
    }

    @Test
    void recognisesRedisErrorsAcrossCauses() {
        RuntimeException e = new RuntimeException("wrapper", new IllegalStateException(
                "NOGROUP No such key 'q' or consumer group 'workers'"));
        assertThat(RedisErrors.isNoGroup(e)).isTrue();
        assertThat(RedisErrors.isBusyGroup(e)).isFalse();
        assertThat(RedisErrors.isBusyGroup(new RuntimeException("BUSYGROUP Consumer Group name already exists")))
                .isTrue();
    }
}
