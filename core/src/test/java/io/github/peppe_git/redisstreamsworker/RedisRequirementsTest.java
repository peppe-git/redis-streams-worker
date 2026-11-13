// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RedisRequirementsTest {

    @Test
    void parsesTheServerVersionAndComparesIt() {
        String info = "# Server\r\nredis_version:7.4.1\r\nredis_mode:standalone\r\n";
        assertThat(RedisRequirements.serverVersion(info)).isEqualTo("7.4.1");
        assertThat(RedisRequirements.atLeast("7.4.1", 6, 2)).isTrue();
        assertThat(RedisRequirements.atLeast("6.2.0", 6, 2)).isTrue();
        assertThat(RedisRequirements.atLeast("6.0.9", 6, 2)).isFalse();
        assertThat(RedisRequirements.atLeast("5.0.14", 6, 2)).isFalse();
    }
}
