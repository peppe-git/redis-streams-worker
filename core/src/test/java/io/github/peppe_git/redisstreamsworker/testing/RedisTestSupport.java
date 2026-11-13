// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.testing;

import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.WorkerOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Base class of the integration tests: a clean Redis, a unique key prefix, and fast timings. */
public abstract class RedisTestSupport {

    protected RedisClient client;
    protected StatefulRedisConnection<String, String> connection;
    protected RedisCommands<String, String> redis;
    protected String prefix;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeEach
    void connectToRedis() {
        client = RedisContainers.sharedClient();
        connection = client.connect();
        redis = connection.sync();
        redis.flushall();
        prefix = "t" + UUID.randomUUID().toString().substring(0, 6);
    }

    @AfterEach
    void closeResources() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            try {
                closeables.get(i).close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        closeables.clear();
        connection.close();
    }

    protected <T extends AutoCloseable> T closeLater(T closeable) {
        closeables.add(closeable);
        return closeable;
    }

    protected QueueDefinition queue(String... lanes) {
        QueueDefinition.Builder b = QueueDefinition.builder(prefix);
        if (lanes.length > 0) {
            b.lanes(lanes);
        }
        return b.build();
    }

    /** Timings scaled down so that reclaim and lease expiry happen within a second. */
    public static WorkerOptions.Builder fastOptions() {
        return WorkerOptions.builder()
                .pollTimeout(Duration.ofMillis(100))
                .commandTimeout(Duration.ofSeconds(2))
                .leaseTtl(Duration.ofMillis(1500))
                .heartbeatInterval(Duration.ofMillis(200))
                .reclaimAfter(Duration.ofMillis(500))
                .reclaimInterval(Duration.ofMillis(100))
                .drainTimeout(Duration.ofSeconds(5))
                .interruptGrace(Duration.ofSeconds(2));
    }

    protected RedisStreamsWorker started(RedisStreamsWorker worker) {
        closeLater(worker);
        worker.start();
        return worker;
    }

    /**
     * Keys matching a pattern. Uses a script on purpose: {@code RedisCommands.keys} changed its binary
     * signature between Lettuce 6 and 7, and these tests also run on Lettuce 7.
     */
    protected java.util.List<Object> keys(String pattern) {
        return redis.eval("return redis.call('KEYS', ARGV[1])", io.lettuce.core.ScriptOutputType.MULTI,
                new String[0], pattern);
    }

    /** Pending entries of the group on a stream. */
    protected long pendingCount(String streamKey, String group) {
        return redis.xpending(streamKey, group).getCount();
    }
}
