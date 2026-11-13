// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import io.github.peppe_git.redisstreamsworker.Lane;
import java.util.ArrayList;
import java.util.List;

/**
 * Decides in which order the lanes are read on each poll. Pure logic, used by the poll thread only.
 *
 * <p>Normally lanes are read in priority order and the first lane that returns messages serves the
 * poll. After {@code fairnessRatio} consecutive polls served by a lane other than the lowest, one
 * "fairness" poll reads the lowest lane first, so that a continuous flow on the high lanes cannot
 * starve it: a message on the lowest lane waits at most {@code fairnessRatio + 1} served polls. With a
 * ratio of {@code 0} the order is always strict priority.
 */
public final class LaneScheduler {

    private final List<Lane> lanes;
    private final Lane lowest;
    private final int fairnessRatio;
    private int streak;
    private boolean fairnessPoll;

    public LaneScheduler(List<Lane> lanes, int fairnessRatio) {
        if (lanes.isEmpty()) {
            throw new IllegalArgumentException("At least one lane is required");
        }
        this.lanes = List.copyOf(lanes);
        this.lowest = this.lanes.get(this.lanes.size() - 1);
        this.fairnessRatio = fairnessRatio;
    }

    /** The lane the poll may block on when nothing else has messages. */
    public Lane lowest() {
        return lowest;
    }

    /** Read order for the next poll. */
    public List<Lane> order() {
        fairnessPoll = fairnessRatio > 0 && lanes.size() > 1 && streak >= fairnessRatio;
        if (!fairnessPoll) {
            return lanes;
        }
        List<Lane> order = new ArrayList<>(lanes.size());
        order.add(lowest);
        for (Lane lane : lanes) {
            if (lane != lowest) {
                order.add(lane);
            }
        }
        return order;
    }

    /** The poll found messages on {@code lane}. */
    public void served(Lane lane) {
        if (fairnessPoll) {
            streak = 0;
        }
        if (lane.equals(lowest)) {
            streak = 0;
        } else {
            streak++;
        }
    }

    /** Every lane was empty: there is no starvation to correct. */
    public void nothingServed() {
        streak = 0;
    }

    int streak() {
        return streak;
    }
}
