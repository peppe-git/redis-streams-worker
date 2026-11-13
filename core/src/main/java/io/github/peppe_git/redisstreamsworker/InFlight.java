// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.util.Map;

/**
 * Messages running on this worker instance, as seen by a {@link CapacityProvider}.
 *
 * @param total number of messages whose handler is running
 * @param byLane the same, per lane name (lanes with nothing running are absent)
 */
public record InFlight(int total, Map<String, Integer> byLane) {

    public InFlight {
        byLane = Map.copyOf(byLane);
    }

    /** Running messages of one lane. */
    public int lane(String laneName) {
        return byLane.getOrDefault(laneName, 0);
    }
}
