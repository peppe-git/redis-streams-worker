// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.peppe_git.redisstreamsworker.Lane;
import java.util.List;
import org.junit.jupiter.api.Test;

class LaneSchedulerTest {

    private final Lane high = new Lane("high", "q:high");
    private final Lane mid = new Lane("mid", "q:mid");
    private final Lane low = new Lane("low", "q:low");

    @Test
    void strictPriorityWhenTheRatioIsZero() {
        LaneScheduler s = new LaneScheduler(List.of(high, low), 0);
        for (int i = 0; i < 20; i++) {
            assertThat(s.order()).containsExactly(high, low);
            s.served(high);
        }
    }

    @Test
    void afterRatioPollsServedByHigherLanesTheLowestIsReadFirstOnce() {
        LaneScheduler s = new LaneScheduler(List.of(high, mid, low), 2);
        assertThat(s.order()).containsExactly(high, mid, low);
        s.served(high);
        assertThat(s.order()).containsExactly(high, mid, low);
        s.served(mid);
        assertThat(s.order()).containsExactly(low, high, mid); // fairness poll
        s.served(low);
        assertThat(s.order()).containsExactly(high, mid, low);
    }

    @Test
    void anEmptyLowestLaneOnAFairnessPollDoesNotStallTheOthers() {
        LaneScheduler s = new LaneScheduler(List.of(high, low), 1);
        s.order();
        s.served(high);
        assertThat(s.order()).containsExactly(low, high);
        s.served(high); // low was empty, high served the fairness poll
        assertThat(s.streak()).isEqualTo(1);
        assertThat(s.order()).containsExactly(low, high);
    }

    @Test
    void anIdlePollResetsTheStreak() {
        LaneScheduler s = new LaneScheduler(List.of(high, low), 2);
        s.order();
        s.served(high);
        s.order();
        s.nothingServed();
        assertThat(s.streak()).isZero();
        assertThat(s.order()).containsExactly(high, low);
    }

    @Test
    void aSingleLaneIsAlwaysReadAlone() {
        LaneScheduler s = new LaneScheduler(List.of(low), 1);
        s.served(low);
        assertThat(s.order()).containsExactly(low);
    }
}
