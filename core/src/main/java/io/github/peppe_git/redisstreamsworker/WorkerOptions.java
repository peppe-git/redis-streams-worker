// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Tuning of a {@link RedisStreamsWorker}. Every value has a default; {@link Builder#build()} rejects
 * inconsistent combinations instead of silently correcting them.
 *
 * <p>Timing rules (see {@code docs/tuning.md}):
 * <ul>
 *   <li>{@code leaseTtl >= 2 × heartbeatInterval}: one missed heartbeat must not lose the lease;</li>
 *   <li>{@code reclaimInterval <= reclaimAfter};</li>
 *   <li>a pending message is reclaimed only when it has been idle for {@code reclaimAfter} <b>and</b>
 *       its lease has expired, so after a crash recovery takes about
 *       {@code max(reclaimAfter, leaseTtl) + reclaimInterval}.</li>
 * </ul>
 */
public final class WorkerOptions {

    private final String consumerName;
    private final int concurrency;
    private final int batchSize;
    private final Duration pollTimeout;
    private final Duration commandTimeout;
    private final Duration leaseTtl;
    private final Duration heartbeatInterval;
    private final Duration reclaimAfter;
    private final Duration reclaimInterval;
    private final int reclaimScanLimit;
    private final int maxDeliveries;
    private final int fairnessRatio;
    private final Duration drainTimeout;
    private final Duration interruptGrace;
    private final Duration trimInterval;
    private final Duration deadConsumerIdle;
    private final Duration pruneInterval;
    private final GroupStart groupStart;
    private final boolean requireNoEviction;
    private final boolean virtualThreads;

    private WorkerOptions(Builder b) {
        this.consumerName = b.consumerName != null ? b.consumerName
                : b.consumerNamePrefix + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.concurrency = b.concurrency;
        this.batchSize = b.batchSize;
        this.pollTimeout = b.pollTimeout;
        this.commandTimeout = b.commandTimeout;
        this.leaseTtl = b.leaseTtl;
        this.heartbeatInterval = b.heartbeatInterval;
        this.reclaimAfter = b.reclaimAfter;
        this.reclaimInterval = b.reclaimInterval;
        this.reclaimScanLimit = b.reclaimScanLimit;
        this.maxDeliveries = b.maxDeliveries;
        this.fairnessRatio = b.fairnessRatio;
        this.drainTimeout = b.drainTimeout;
        this.interruptGrace = b.interruptGrace;
        this.trimInterval = b.trimInterval;
        this.deadConsumerIdle = b.deadConsumerIdle;
        this.pruneInterval = b.pruneInterval;
        this.groupStart = b.groupStart;
        this.requireNoEviction = b.requireNoEviction;
        this.virtualThreads = b.virtualThreads;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Defaults only. */
    public static WorkerOptions defaults() {
        return builder().build();
    }

    /** Consumer name in the group; generated as {@code <prefix>-<8 random hex>} unless set. */
    public String consumerName() {
        return consumerName;
    }

    /** Capacity used when no {@link CapacityProvider} is given. Default 4. */
    public int concurrency() {
        return concurrency;
    }

    /** Maximum records per {@code XREADGROUP}. Default 10. */
    public int batchSize() {
        return batchSize;
    }

    /** Blocking time of the read on the lowest-priority lane when everything is empty. Default 1 s. */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /** Timeout of every Redis command (the blocking read gets {@code pollTimeout} on top). Default 5 s. */
    public Duration commandTimeout() {
        return commandTimeout;
    }

    /** Time-to-live of a message lease; renewed every {@link #heartbeatInterval()}. Default 90 s. */
    public Duration leaseTtl() {
        return leaseTtl;
    }

    /** Lease renewal period. Default 20 s. */
    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    /** Minimum idle time of a pending message before it can be reclaimed. Default 60 s. */
    public Duration reclaimAfter() {
        return reclaimAfter;
    }

    /** How often the worker scans the pending entries for messages to reclaim. Default 5 s. */
    public Duration reclaimInterval() {
        return reclaimInterval;
    }

    /** Pending entries inspected per lane and per reclaim pass. Default 1000. */
    public int reclaimScanLimit() {
        return reclaimScanLimit;
    }

    /**
     * Maximum number of times the handler runs for one message. The delivery after the last one moves
     * the message to the dead-letter stream without running it. Default 5.
     */
    public int maxDeliveries() {
        return maxDeliveries;
    }

    /**
     * Anti-starvation: after this many consecutive polls served by a lane other than the lowest, the next
     * poll reads the lowest lane first. {@code 0} disables the correction (strict priority). Default 4.
     */
    public int fairnessRatio() {
        return fairnessRatio;
    }

    /** How long {@code stop()} waits for running handlers before interrupting them. Default 30 s. */
    public Duration drainTimeout() {
        return drainTimeout;
    }

    /** How long {@code stop()} waits after interrupting the handlers. Default 5 s. */
    public Duration interruptGrace() {
        return interruptGrace;
    }

    /** Period of the safe trim ({@code XTRIM MINID}); {@link Duration#ZERO} disables it. Default 60 s. */
    public Duration trimInterval() {
        return trimInterval;
    }

    /** Consumers idle for longer than this and with nothing pending are removed. Default 24 h. */
    public Duration deadConsumerIdle() {
        return deadConsumerIdle;
    }

    /** Period of the dead-consumer pruning. Default 1 h. */
    public Duration pruneInterval() {
        return pruneInterval;
    }

    /** Where a group starts when the worker creates it. Default {@link GroupStart#EARLIEST}. */
    public GroupStart groupStart() {
        return groupStart;
    }

    /**
     * If {@code true}, {@code start()} fails when Redis reports a {@code maxmemory-policy} other than
     * {@code noeviction}; otherwise a warning is logged. Default {@code false}.
     */
    public boolean requireNoEviction() {
        return requireNoEviction;
    }

    /** Runs each handler on a virtual thread (Java 21+). Default {@code false}. */
    public boolean virtualThreads() {
        return virtualThreads;
    }

    @Override
    public String toString() {
        return "WorkerOptions{consumer=" + consumerName + ", concurrency=" + concurrency
                + ", leaseTtl=" + leaseTtl + ", heartbeat=" + heartbeatInterval
                + ", reclaimAfter=" + reclaimAfter + ", maxDeliveries=" + maxDeliveries
                + ", fairnessRatio=" + fairnessRatio + ", groupStart=" + groupStart + "}";
    }

    /** Builder of {@link WorkerOptions}. */
    public static final class Builder {

        private String consumerName;
        private String consumerNamePrefix = "rsw";
        private int concurrency = 4;
        private int batchSize = 10;
        private Duration pollTimeout = Duration.ofSeconds(1);
        private Duration commandTimeout = Duration.ofSeconds(5);
        private Duration leaseTtl = Duration.ofSeconds(90);
        private Duration heartbeatInterval = Duration.ofSeconds(20);
        private Duration reclaimAfter = Duration.ofSeconds(60);
        private Duration reclaimInterval = Duration.ofSeconds(5);
        private int reclaimScanLimit = 1000;
        private int maxDeliveries = 5;
        private int fairnessRatio = 4;
        private Duration drainTimeout = Duration.ofSeconds(30);
        private Duration interruptGrace = Duration.ofSeconds(5);
        private Duration trimInterval = Duration.ofSeconds(60);
        private Duration deadConsumerIdle = Duration.ofHours(24);
        private Duration pruneInterval = Duration.ofHours(1);
        private GroupStart groupStart = GroupStart.EARLIEST;
        private boolean requireNoEviction;
        private boolean virtualThreads;

        private Builder() {
        }

        public Builder consumerName(String name) {
            if (name != null && (name.isBlank() || name.chars().anyMatch(Character::isWhitespace))) {
                throw new IllegalArgumentException("consumerName must be non-blank and without whitespace");
            }
            this.consumerName = name;
            return this;
        }

        public Builder consumerNamePrefix(String prefix) {
            this.consumerNamePrefix = Objects.requireNonNull(prefix, "prefix");
            return this;
        }

        public Builder concurrency(int value) {
            this.concurrency = value;
            return this;
        }

        public Builder batchSize(int value) {
            this.batchSize = value;
            return this;
        }

        public Builder pollTimeout(Duration value) {
            this.pollTimeout = value;
            return this;
        }

        public Builder commandTimeout(Duration value) {
            this.commandTimeout = value;
            return this;
        }

        public Builder leaseTtl(Duration value) {
            this.leaseTtl = value;
            return this;
        }

        public Builder heartbeatInterval(Duration value) {
            this.heartbeatInterval = value;
            return this;
        }

        public Builder reclaimAfter(Duration value) {
            this.reclaimAfter = value;
            return this;
        }

        public Builder reclaimInterval(Duration value) {
            this.reclaimInterval = value;
            return this;
        }

        public Builder reclaimScanLimit(int value) {
            this.reclaimScanLimit = value;
            return this;
        }

        public Builder maxDeliveries(int value) {
            this.maxDeliveries = value;
            return this;
        }

        public Builder fairnessRatio(int value) {
            this.fairnessRatio = value;
            return this;
        }

        public Builder drainTimeout(Duration value) {
            this.drainTimeout = value;
            return this;
        }

        public Builder interruptGrace(Duration value) {
            this.interruptGrace = value;
            return this;
        }

        public Builder trimInterval(Duration value) {
            this.trimInterval = value;
            return this;
        }

        public Builder deadConsumerIdle(Duration value) {
            this.deadConsumerIdle = value;
            return this;
        }

        public Builder pruneInterval(Duration value) {
            this.pruneInterval = value;
            return this;
        }

        public Builder groupStart(GroupStart value) {
            this.groupStart = Objects.requireNonNull(value, "groupStart");
            return this;
        }

        public Builder requireNoEviction(boolean value) {
            this.requireNoEviction = value;
            return this;
        }

        public Builder virtualThreads(boolean value) {
            this.virtualThreads = value;
            return this;
        }

        public WorkerOptions build() {
            atLeast("concurrency", concurrency, 1);
            atLeast("batchSize", batchSize, 1);
            atLeast("reclaimScanLimit", reclaimScanLimit, 1);
            atLeast("maxDeliveries", maxDeliveries, 1);
            atLeast("fairnessRatio", fairnessRatio, 0);
            positive("pollTimeout", pollTimeout, Duration.ofMillis(10));
            positive("commandTimeout", commandTimeout, Duration.ofMillis(100));
            positive("leaseTtl", leaseTtl, Duration.ofMillis(100));
            positive("heartbeatInterval", heartbeatInterval, Duration.ofMillis(10));
            positive("reclaimAfter", reclaimAfter, Duration.ofMillis(10));
            positive("reclaimInterval", reclaimInterval, Duration.ofMillis(10));
            positive("drainTimeout", drainTimeout, Duration.ZERO);
            positive("interruptGrace", interruptGrace, Duration.ZERO);
            positive("deadConsumerIdle", deadConsumerIdle, Duration.ofSeconds(1));
            positive("pruneInterval", pruneInterval, Duration.ofSeconds(1));
            Objects.requireNonNull(trimInterval, "trimInterval");
            if (trimInterval.isNegative()) {
                throw new IllegalArgumentException("trimInterval must be >= 0 (0 disables trimming)");
            }
            if (leaseTtl.compareTo(heartbeatInterval.multipliedBy(2)) < 0) {
                throw new IllegalArgumentException("leaseTtl (" + leaseTtl + ") must be at least twice the "
                        + "heartbeatInterval (" + heartbeatInterval + "): a single late heartbeat would "
                        + "otherwise let another worker reclaim a message that is still running");
            }
            if (reclaimInterval.compareTo(reclaimAfter) > 0) {
                throw new IllegalArgumentException("reclaimInterval (" + reclaimInterval
                        + ") must not exceed reclaimAfter (" + reclaimAfter + ")");
            }
            if (virtualThreads && Runtime.version().feature() < 21) {
                throw new IllegalStateException("virtualThreads requires Java 21+, running on "
                        + Runtime.version());
            }
            return new WorkerOptions(this);
        }

        private static void atLeast(String name, int value, int min) {
            if (value < min) {
                throw new IllegalArgumentException(name + " must be >= " + min + ", was " + value);
            }
        }

        private static void positive(String name, Duration value, Duration min) {
            Objects.requireNonNull(value, name);
            if (value.compareTo(min) < 0 || (min.isZero() && value.isNegative())) {
                throw new IllegalArgumentException(name + " must be >= " + min + ", was " + value);
            }
        }
    }
}
