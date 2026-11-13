// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/**
 * How many messages this instance may process at the same time, asked before every read.
 *
 * <p>The worker never reads more messages than {@code capacity(inFlight) - inFlight.total()}. Because
 * the answer is recomputed on every poll, it can follow the state of a dependency (a remote service
 * degraded to a local fallback that needs more CPU), and because it sees what is running per lane it can
 * exclude a lane from the count (return {@code base + inFlight.lane("interactive")}). When the capacity
 * drops below what is running, nothing is interrupted: the worker simply stops reading until enough
 * messages finish.
 *
 * <p>An exception keeps the last known capacity.
 */
@FunctionalInterface
public interface CapacityProvider {

    int capacity(InFlight inFlight);

    /** A constant capacity. */
    static CapacityProvider fixed(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        return inFlight -> capacity;
    }
}
