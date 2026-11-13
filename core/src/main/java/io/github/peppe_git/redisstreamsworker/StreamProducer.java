// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Appends messages to the lanes of a queue ({@code XADD}). Thread-safe; holds one connection.
 *
 * <p>By default the producer does not trim: workers trim acknowledged entries safely (see ADR 0003).
 * A {@code maxLen} can be set as a hard cap on memory, at the price of possibly dropping messages that
 * were never delivered when the backlog exceeds it.
 */
public final class StreamProducer implements AutoCloseable {

    private final QueueDefinition queue;
    private final StatefulRedisConnection<String, String> connection;
    private final boolean ownsConnection;
    private final long maxLen;

    /** A producer with its own connection and no trimming. */
    public StreamProducer(RedisClient client, QueueDefinition queue) {
        this(client, queue, 0);
    }

    /**
     * A producer with its own connection.
     *
     * @param maxLen approximate cap per lane ({@code MAXLEN ~}); {@code 0} = no cap. <b>Lossy</b>: it can
     *     remove undelivered or pending messages.
     */
    public StreamProducer(RedisClient client, QueueDefinition queue, long maxLen) {
        this(client.connect(), true, queue, maxLen);
        connection.setTimeout(Duration.ofSeconds(5));
    }

    /** A producer on a connection owned by the caller (not closed by {@link #close()}). */
    public StreamProducer(StatefulRedisConnection<String, String> connection, QueueDefinition queue, long maxLen) {
        this(connection, false, queue, maxLen);
    }

    private StreamProducer(StatefulRedisConnection<String, String> connection, boolean owns, QueueDefinition queue,
            long maxLen) {
        if (maxLen < 0) {
            throw new IllegalArgumentException("maxLen must be >= 0");
        }
        this.connection = Objects.requireNonNull(connection, "connection");
        this.ownsConnection = owns;
        this.queue = Objects.requireNonNull(queue, "queue");
        this.maxLen = maxLen;
    }

    /**
     * Appends a message to a lane.
     *
     * @return the record id assigned by Redis
     * @throws IllegalArgumentException if the lane is unknown, the payload is empty, or a field name uses
     *     the reserved prefix {@value DeadLetter#FIELD_PREFIX}
     */
    public String send(String lane, Map<String, String> fields) {
        Lane target = queue.lane(lane);
        validate(fields);
        XAddArgs args = new XAddArgs();
        if (maxLen > 0) {
            args.maxlen(maxLen).approximateTrimming();
        }
        RedisCommands<String, String> redis = connection.sync();
        return redis.xadd(target.streamKey(), args, fields);
    }

    /** Appends a message to the lowest-priority lane. */
    public String send(Map<String, String> fields) {
        return send(queue.lowestPriorityLane().name(), fields);
    }

    public QueueDefinition queue() {
        return queue;
    }

    private static void validate(Map<String, String> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("A message needs at least one field");
        }
        fields.forEach((k, v) -> {
            if (k == null || v == null) {
                throw new IllegalArgumentException("Null field names or values are not allowed");
            }
            if (k.startsWith(DeadLetter.FIELD_PREFIX)) {
                throw new IllegalArgumentException("Field '" + k + "' uses the reserved prefix "
                        + DeadLetter.FIELD_PREFIX);
            }
        });
    }

    @Override
    public void close() {
        if (ownsConnection) {
            connection.close();
        }
    }
}
