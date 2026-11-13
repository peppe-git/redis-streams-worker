// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import io.lettuce.core.api.sync.RedisCommands;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Startup checks on the Redis server: version, and eviction policy. */
final class RedisRequirements {

    private static final Logger log = LoggerFactory.getLogger(RedisRequirements.class);

    private RedisRequirements() {
    }

    static void check(RedisCommands<String, String> redis, boolean requireNoEviction) {
        String version = null;
        try {
            version = serverVersion(redis.info("server"));
        } catch (RuntimeException e) {
            // Some managed offerings restrict INFO: the version cannot be checked, the commands will tell.
            log.debug("INFO server not allowed ({}); Redis version not verified", e.toString());
        }
        if (version != null && !atLeast(version, 6, 2)) {
            throw new IllegalStateException("Redis " + version + " is not supported: 6.2 or newer is required "
                    + "(XPENDING with IDLE, XTRIM MINID)");
        }
        String policy = null;
        try {
            Map<String, String> config = redis.configGet("maxmemory-policy");
            policy = config == null ? null : config.get("maxmemory-policy");
        } catch (RuntimeException e) {
            log.debug("CONFIG GET maxmemory-policy not allowed ({}); eviction policy not verified", e.toString());
        }
        if (policy != null && !"noeviction".equals(policy)) {
            String msg = "Redis maxmemory-policy is '" + policy + "': under memory pressure Redis may evict "
                    + "stream entries and leases without any error. Use 'noeviction'.";
            if (requireNoEviction) {
                throw new IllegalStateException(msg);
            }
            log.warn(msg);
        }
    }

    static String serverVersion(String info) {
        if (info == null) {
            return null;
        }
        for (String line : info.split("\r?\n")) {
            if (line.startsWith("redis_version:")) {
                return line.substring("redis_version:".length()).trim();
            }
        }
        return null;
    }

    static boolean atLeast(String version, int major, int minor) {
        String[] parts = version.split("\\.");
        try {
            int ma = Integer.parseInt(parts[0]);
            int mi = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return ma > major || (ma == major && mi >= minor);
        } catch (NumberFormatException e) {
            return true;
        }
    }
}
