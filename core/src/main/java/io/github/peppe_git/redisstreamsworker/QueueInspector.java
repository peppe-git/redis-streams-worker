// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import io.github.peppe_git.redisstreamsworker.internal.Scripts;
import io.github.peppe_git.redisstreamsworker.internal.StreamIds;
import io.github.peppe_git.redisstreamsworker.internal.XInfo;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessage;
import io.lettuce.core.models.stream.PendingMessages;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-mostly operations for dashboards, alerts and operators: lane statistics, the dead-letter stream
 * (read, replay, delete) and the list of messages that are still "alive" in the queue. Thread-safe;
 * holds one connection.
 */
public final class QueueInspector implements AutoCloseable {

    /**
     * Statistics of one lane.
     *
     * @param length entries in the stream (acknowledged ones included, until trimmed)
     * @param pending entries delivered and not acknowledged (running or waiting for a reclaim)
     * @param consumers consumers of the group on this lane
     * @param lastDeliveredId last id delivered to the group
     * @param lag entries not yet delivered to the group, when Redis (7.0+) can compute it, else {@code null}
     * @param measured {@code false} if Redis could not be read: the numbers are then zeros, not
     *     measurements, and must not be used to decide anything (for example "the queue is empty, it is safe
     *     to restart")
     */
    public record LaneStats(String lane, String streamKey, long length, long pending, long consumers,
            String lastDeliveredId, Long lag, boolean measured) {
    }

    /** Where a live message is. */
    public enum LiveState { UNDELIVERED, PENDING }

    /**
     * A message that will still be processed: not yet delivered to the group, or delivered and not
     * acknowledged.
     *
     * @param consumer for a pending message, the consumer that owns it; otherwise {@code null}
     */
    public record LiveMessage(QueueMessage message, LiveState state, String consumer) {
    }

    private final QueueDefinition queue;
    private final StatefulRedisConnection<String, String> connection;
    private final boolean ownsConnection;

    public QueueInspector(RedisClient client, QueueDefinition queue) {
        this(client.connect(), true, queue);
        connection.setTimeout(Duration.ofSeconds(5));
    }

    /** An inspector on a connection owned by the caller. */
    public QueueInspector(StatefulRedisConnection<String, String> connection, QueueDefinition queue) {
        this(connection, false, queue);
    }

    private QueueInspector(StatefulRedisConnection<String, String> connection, boolean owns, QueueDefinition queue) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.ownsConnection = owns;
        this.queue = Objects.requireNonNull(queue, "queue");
    }

    /** Statistics of every lane. Never throws: a lane that cannot be read has {@code measured = false}. */
    public List<LaneStats> stats() {
        RedisCommands<String, String> redis = connection.sync();
        List<LaneStats> out = new ArrayList<>();
        for (Lane lane : queue.lanes()) {
            try {
                if (redis.exists(lane.streamKey()) == 0) {
                    // No stream: zero is a true measurement, nothing can be pending.
                    out.add(new LaneStats(lane.name(), lane.streamKey(), 0, 0, 0, null, 0L, true));
                    continue;
                }
                long length = redis.xlen(lane.streamKey());
                XInfo.Group group = group(redis, lane);
                if (group == null) {
                    out.add(new LaneStats(lane.name(), lane.streamKey(), length, 0, 0, null, null, true));
                    continue;
                }
                out.add(new LaneStats(lane.name(), lane.streamKey(), length, group.pending(), group.consumers(),
                        group.lastDeliveredId(), group.lag(), true));
            } catch (RuntimeException e) {
                out.add(new LaneStats(lane.name(), lane.streamKey(), 0, 0, 0, null, null, false));
            }
        }
        return out;
    }

    /** Number of entries in the dead-letter stream. Throws if Redis cannot be read. */
    public long deadLetterCount() {
        Long n = connection.sync().xlen(queue.deadLetterKey());
        return n == null ? 0 : n;
    }

    /** The most recent dead letters, newest first. */
    public List<DeadLetter> deadLetters(int max) {
        if (max < 1) {
            throw new IllegalArgumentException("max must be >= 1");
        }
        List<StreamMessage<String, String>> records = connection.sync().xrevrange(queue.deadLetterKey(),
                Range.unbounded(), Limit.from(max));
        List<DeadLetter> out = new ArrayList<>();
        if (records != null) {
            records.forEach(r -> out.add(DeadLetter.fromFields(r.getId(), r.getBody())));
        }
        return out;
    }

    /** One dead letter by id. */
    public Optional<DeadLetter> deadLetter(String id) {
        List<StreamMessage<String, String>> records = connection.sync().xrange(queue.deadLetterKey(),
                Range.create(id, id));
        return records == null || records.isEmpty() ? Optional.empty()
                : Optional.of(DeadLetter.fromFields(records.get(0).getId(), records.get(0).getBody()));
    }

    /**
     * Puts a dead letter back on its lane as a <em>new</em> message (delivery count 1) and removes it
     * from the dead-letter stream, atomically: a second call for the same id fails instead of enqueuing
     * twice. Fix the cause first, or the message will come back.
     *
     * @return the id of the new message on its lane
     * @throws NoSuchElementException if the dead letter does not exist (or was already replayed)
     * @throws IllegalStateException if its lane is not declared any more
     */
    public String replay(String deadLetterId) {
        DeadLetter dl = deadLetter(deadLetterId)
                .orElseThrow(() -> new NoSuchElementException("No dead letter " + deadLetterId));
        if (dl.lane() == null) {
            throw new IllegalStateException("Dead letter " + deadLetterId + " has no lane metadata");
        }
        Lane lane;
        try {
            lane = queue.lane(dl.lane());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Dead letter " + deadLetterId + " comes from lane '" + dl.lane()
                    + "', which is not declared any more", e);
        }
        if (dl.payload().isEmpty()) {
            throw new IllegalStateException("Dead letter " + deadLetterId + " has an empty payload");
        }
        String id = Scripts.replay(connection.sync(), queue.deadLetterKey(), lane.streamKey(), deadLetterId,
                dl.payload());
        if (id == null) {
            throw new NoSuchElementException("Dead letter " + deadLetterId + " was replayed or deleted meanwhile");
        }
        return id;
    }

    /** Deletes a dead letter. Returns {@code false} if it did not exist. */
    public boolean delete(String deadLetterId) {
        Long n = connection.sync().xdel(queue.deadLetterKey(), deadLetterId);
        return n != null && n > 0;
    }

    /**
     * Messages that are still alive in the queue, per lane: undelivered (after the group's last-delivered
     * id) and pending. Meant for an application reconciler that asks "is there still a message for this
     * piece of work?". Throws if Redis cannot be read: without a trustworthy view, reconcile nothing.
     *
     * @param maxPerLane bound on undelivered and on pending entries read per lane
     */
    public List<LiveMessage> liveMessages(int maxPerLane) {
        if (maxPerLane < 1) {
            throw new IllegalArgumentException("maxPerLane must be >= 1");
        }
        RedisCommands<String, String> redis = connection.sync();
        List<LiveMessage> out = new ArrayList<>();
        for (Lane lane : queue.lanes()) {
            if (redis.exists(lane.streamKey()) == 0) {
                continue;
            }
            XInfo.Group group = group(redis, lane);
            if (group == null) {
                continue; // no group: nothing will ever be delivered from here until it is created
            }
            List<StreamMessage<String, String>> undelivered = redis.xrange(lane.streamKey(),
                    Range.create(nextOf(group.lastDeliveredId()), "+"), Limit.from(maxPerLane));
            if (undelivered != null) {
                undelivered.forEach(r -> out.add(new LiveMessage(
                        new QueueMessage(lane.name(), r.getId(), r.getBody(), 0), LiveState.UNDELIVERED, null)));
            }
            List<PendingMessage> pending = redis.xpending(lane.streamKey(), queue.group(), Range.unbounded(),
                    Limit.from(maxPerLane));
            if (pending == null || pending.isEmpty()) {
                continue;
            }
            Map<String, PendingMessage> byId = new HashMap<>();
            String min = null;
            String max = null;
            for (PendingMessage pm : pending) {
                byId.put(pm.getId(), pm);
                min = min == null ? pm.getId() : StreamIds.min(min, pm.getId());
                max = max == null || StreamIds.compare(pm.getId(), max) > 0
                        ? pm.getId() : max;
            }
            // One range read between the oldest and newest pending ids instead of one read per entry.
            List<StreamMessage<String, String>> records = redis.xrange(lane.streamKey(), Range.create(min, max));
            if (records != null) {
                for (StreamMessage<String, String> r : records) {
                    PendingMessage pm = byId.get(r.getId());
                    if (pm != null) {
                        out.add(new LiveMessage(new QueueMessage(lane.name(), r.getId(), r.getBody(),
                                pm.getRedeliveryCount()), LiveState.PENDING, pm.getConsumer()));
                    }
                }
            }
        }
        return out;
    }

    /** Pending summary of one lane (count and id range), or {@code null} if the group does not exist. */
    PendingMessages pendingSummary(String lane) {
        return connection.sync().xpending(queue.lane(lane).streamKey(), queue.group());
    }

    private XInfo.Group group(RedisCommands<String, String> redis, Lane lane) {
        for (XInfo.Group g : XInfo.groups(redis.xinfoGroups(lane.streamKey()))) {
            if (queue.group().equals(g.name())) {
                return g;
            }
        }
        return null;
    }

    private static String nextOf(String id) {
        return StreamIds.next(id == null ? "0-0" : id);
    }

    @Override
    public void close() {
        if (ownsConnection) {
            connection.close();
        }
    }
}
