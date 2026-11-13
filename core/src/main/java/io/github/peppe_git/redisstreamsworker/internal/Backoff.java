// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with a multiplicative jitter in {@code [0.5, 1.5)}, used by the poll loop while
 * Redis does not answer. The jitter grows with the delay, so that many workers failing together do not
 * retry together. Not thread-safe.
 */
public final class Backoff {

    private final long initialMillis;
    private final long maxMillis;
    private long current;

    public Backoff(Duration initial, Duration max) {
        this.initialMillis = Math.max(1, initial.toMillis());
        this.maxMillis = Math.max(initialMillis, max.toMillis());
        this.current = initialMillis;
    }

    /** The next delay, then doubles the base up to the maximum. */
    public Duration next() {
        long base = current;
        current = Math.min(maxMillis, current * 2);
        return Duration.ofMillis(jitter(base));
    }

    public void reset() {
        current = initialMillis;
    }

    static long jitter(long millis) {
        if (millis <= 0) {
            return 0;
        }
        double factor = 0.5 + ThreadLocalRandom.current().nextDouble();
        return Math.max(1, Math.round(millis * factor));
    }
}
