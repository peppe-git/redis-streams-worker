// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WorkerOptionsTest {

    @Test
    void defaultsRespectTheTimingRules() {
        WorkerOptions o = WorkerOptions.defaults();
        assertThat(o.leaseTtl()).isGreaterThanOrEqualTo(o.heartbeatInterval().multipliedBy(2));
        assertThat(o.reclaimInterval()).isLessThanOrEqualTo(o.reclaimAfter());
        assertThat(o.maxDeliveries()).isEqualTo(5);
        assertThat(o.groupStart()).isEqualTo(GroupStart.EARLIEST);
        assertThat(o.consumerName()).startsWith("rsw-");
    }

    @Test
    void rejectsALeaseThatOneLateHeartbeatWouldLose() {
        assertThatThrownBy(() -> WorkerOptions.builder().leaseTtl(Duration.ofSeconds(30))
                .heartbeatInterval(Duration.ofSeconds(20)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("leaseTtl");
        assertThat(WorkerOptions.builder().leaseTtl(Duration.ofSeconds(40))
                .heartbeatInterval(Duration.ofSeconds(20)).build().leaseTtl()).isEqualTo(Duration.ofSeconds(40));
    }

    @Test
    void rejectsAReclaimIntervalLongerThanReclaimAfter() {
        assertThatThrownBy(() -> WorkerOptions.builder().reclaimAfter(Duration.ofSeconds(5))
                .reclaimInterval(Duration.ofSeconds(10)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reclaimInterval");
    }

    @Test
    void rejectsNonsenseNumbers() {
        assertThatThrownBy(() -> WorkerOptions.builder().concurrency(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerOptions.builder().maxDeliveries(0).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerOptions.builder().fairnessRatio(-1).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerOptions.builder().trimInterval(Duration.ofSeconds(-1)).build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkerOptions.builder().consumerName("has space"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void trimCanBeDisabled() {
        assertThat(WorkerOptions.builder().trimInterval(Duration.ZERO).build().trimInterval()).isZero();
    }
}
