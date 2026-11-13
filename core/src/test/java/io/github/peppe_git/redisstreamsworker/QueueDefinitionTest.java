// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class QueueDefinitionTest {

    @Test
    void derivesKeysFromThePrefixAndScopesLeasesByLane() {
        QueueDefinition q = QueueDefinition.builder("orders").lanes("urgent", "default").build();
        assertThat(q.lane("urgent").streamKey()).isEqualTo("orders:urgent");
        assertThat(q.deadLetterKey()).isEqualTo("orders:dlq");
        assertThat(q.leaseKey(q.lane("urgent"), "1-0")).isEqualTo("orders:lease:urgent:1-0");
        assertThat(q.leaseKey(q.lane("default"), "1-0")).isNotEqualTo(q.leaseKey(q.lane("urgent"), "1-0"));
        assertThat(q.lowestPriorityLane().name()).isEqualTo("default");
        assertThat(q.group()).isEqualTo("workers");
    }

    @Test
    void acceptsAnExplicitStreamKeyToAdoptAnExistingStream() {
        QueueDefinition q = QueueDefinition.builder("orders").lane("normal", "legacy:stream").build();
        assertThat(q.lane("normal").streamKey()).isEqualTo("legacy:stream");
    }

    @Test
    void defaultsToOneLane() {
        assertThat(QueueDefinition.builder("x").build().laneNames()).containsExactly("default");
    }

    @Test
    void rejectsAmbiguousDefinitions() {
        assertThatThrownBy(() -> QueueDefinition.builder("x").lanes("a", "a").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDefinition.builder("x").lanes("dlq").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDefinition.builder("x").lanes("with space").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDefinition.builder("x").lane("a", "k").lane("b", "k").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDefinition.builder("has space"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueDefinition.builder("x").build().lane("missing"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
