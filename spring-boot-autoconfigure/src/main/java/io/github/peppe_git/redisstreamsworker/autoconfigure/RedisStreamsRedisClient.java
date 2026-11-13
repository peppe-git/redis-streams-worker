// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import io.lettuce.core.RedisClient;
import java.util.Objects;

/**
 * The Lettuce client used by the auto-configured producer, inspector, worker and lock. Resolved from a
 * {@link RedisClient} bean of the application, else from the native client of Spring Data Redis'
 * {@code LettuceConnectionFactory}. Declare a bean of this type to choose explicitly.
 *
 * @param client a client for a standalone or Sentinel-managed Redis
 */
public record RedisStreamsRedisClient(RedisClient client) {

    public RedisStreamsRedisClient {
        Objects.requireNonNull(client, "client");
    }
}
