// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The shape of a queue, shared by producers, workers and the inspector: key prefix, consumer group,
 * lanes in priority order and the dead-letter stream.
 *
 * <p>Redis keys, with prefix {@code P}:
 * <ul>
 *   <li>lane stream: {@code P:<lane>} (unless the lane declares its own key);</li>
 *   <li>dead-letter stream: {@code P:dlq};</li>
 *   <li>lease of a message being processed: {@code P:lease:<lane>:<record id>}.</li>
 * </ul>
 *
 * <p>Lease keys include the lane because two streams can hold records with the same id: Redis derives
 * ids from the clock, so two {@code XADD} in the same millisecond on two streams both get
 * {@code <ms>-0}.
 */
public final class QueueDefinition {

    /** Lane names that would collide with the keys the library derives from the prefix. */
    private static final Set<String> RESERVED_LANES = Set.of("dlq", "lease");

    private final String keyPrefix;
    private final String group;
    private final List<Lane> lanes;
    private final long deadLetterMaxLen;

    private QueueDefinition(Builder builder) {
        this.keyPrefix = builder.keyPrefix;
        this.group = builder.group;
        this.lanes = Collections.unmodifiableList(new ArrayList<>(builder.lanes));
        this.deadLetterMaxLen = builder.deadLetterMaxLen;
    }

    /**
     * Starts a definition.
     *
     * @param keyPrefix prefix of every key, for example {@code "orders"}; no whitespace
     */
    public static Builder builder(String keyPrefix) {
        return new Builder(keyPrefix);
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    /** Name of the consumer group read by the workers. */
    public String group() {
        return group;
    }

    /** Lanes in priority order: the first is read first. */
    public List<Lane> lanes() {
        return lanes;
    }

    /** The last lane: the one the worker blocks on when every lane is empty. */
    public Lane lowestPriorityLane() {
        return lanes.get(lanes.size() - 1);
    }

    /** The lane with this name. */
    public Lane lane(String name) {
        for (Lane lane : lanes) {
            if (lane.name().equals(name)) {
                return lane;
            }
        }
        throw new IllegalArgumentException("Unknown lane '" + name + "'; declared lanes: " + laneNames());
    }

    public List<String> laneNames() {
        List<String> names = new ArrayList<>(lanes.size());
        lanes.forEach(l -> names.add(l.name()));
        return names;
    }

    public String deadLetterKey() {
        return keyPrefix + ":dlq";
    }

    /** Approximate cap of the dead-letter stream ({@code XADD MAXLEN ~}); {@code 0} means no cap. */
    public long deadLetterMaxLen() {
        return deadLetterMaxLen;
    }

    public String leaseKey(Lane lane, String recordId) {
        return keyPrefix + ":lease:" + lane.name() + ":" + recordId;
    }

    @Override
    public String toString() {
        return "QueueDefinition{prefix=" + keyPrefix + ", group=" + group + ", lanes=" + laneNames() + "}";
    }

    /** Builder of {@link QueueDefinition}. */
    public static final class Builder {

        private final String keyPrefix;
        private String group = "workers";
        private final List<Lane> lanes = new ArrayList<>();
        private long deadLetterMaxLen = 10_000;

        private Builder(String keyPrefix) {
            Objects.requireNonNull(keyPrefix, "keyPrefix");
            if (keyPrefix.isBlank() || keyPrefix.chars().anyMatch(Character::isWhitespace)) {
                throw new IllegalArgumentException("Key prefix must be non-blank and without whitespace");
            }
            this.keyPrefix = keyPrefix;
        }

        /** Consumer group name; default {@code workers}. */
        public Builder group(String group) {
            Objects.requireNonNull(group, "group");
            if (group.isBlank()) {
                throw new IllegalArgumentException("Group must not be blank");
            }
            this.group = group;
            return this;
        }

        /** Adds lanes, highest priority first, with derived stream keys. */
        public Builder lanes(String... names) {
            for (String name : names) {
                lane(name, keyPrefix + ":" + name);
            }
            return this;
        }

        /** Adds one lane with an explicit stream key (to adopt an existing stream). */
        public Builder lane(String name, String streamKey) {
            Lane lane = new Lane(name, streamKey);
            if (RESERVED_LANES.contains(name)) {
                throw new IllegalArgumentException("Lane name '" + name + "' is reserved");
            }
            for (Lane existing : lanes) {
                if (existing.name().equals(name)) {
                    throw new IllegalArgumentException("Duplicate lane '" + name + "'");
                }
                if (existing.streamKey().equals(streamKey)) {
                    throw new IllegalArgumentException("Lanes '" + existing.name() + "' and '" + name
                            + "' share the stream " + streamKey);
                }
            }
            lanes.add(lane);
            return this;
        }

        /** Approximate cap of the dead-letter stream; default 10 000, {@code 0} disables the cap. */
        public Builder deadLetterMaxLen(long maxLen) {
            if (maxLen < 0) {
                throw new IllegalArgumentException("deadLetterMaxLen must be >= 0");
            }
            this.deadLetterMaxLen = maxLen;
            return this;
        }

        public QueueDefinition build() {
            if (lanes.isEmpty()) {
                lanes("default");
            }
            Set<String> keys = new HashSet<>();
            for (Lane lane : lanes) {
                keys.add(lane.streamKey());
            }
            String dlq = keyPrefix + ":dlq";
            if (keys.contains(dlq)) {
                throw new IllegalArgumentException("A lane uses the dead-letter key " + dlq);
            }
            return new QueueDefinition(this);
        }
    }
}
