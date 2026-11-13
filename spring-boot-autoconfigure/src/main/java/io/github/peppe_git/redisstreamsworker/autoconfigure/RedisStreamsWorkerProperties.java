// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import io.github.peppe_git.redisstreamsworker.GroupStart;
import io.github.peppe_git.redisstreamsworker.WorkerOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration of redis-streams-worker under {@code redis-streams-worker.*}. Worker defaults are read
 * from {@link WorkerOptions#defaults()}, so the two never drift apart.
 */
@ConfigurationProperties(prefix = "redis-streams-worker")
public class RedisStreamsWorkerProperties {

    private static final WorkerOptions DEFAULTS = WorkerOptions.defaults();

    /** Whether the auto-configuration is active at all. */
    private boolean enabled = true;

    /** Prefix of every Redis key of the queue. */
    private String keyPrefix = "rsw";

    /** Consumer group name. */
    private String group = "workers";

    /** Lanes in priority order, highest first. */
    private List<String> lanes = new ArrayList<>(List.of("default"));

    /** Optional explicit stream key per lane name (to adopt existing streams). */
    private Map<String, String> laneStreamKeys = new LinkedHashMap<>();

    /** Approximate cap of the dead-letter stream; 0 disables it. */
    private long deadLetterMaxLen = 10_000;

    private final Producer producer = new Producer();

    private final Worker worker = new Worker();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public List<String> getLanes() {
        return lanes;
    }

    public void setLanes(List<String> lanes) {
        this.lanes = lanes;
    }

    public Map<String, String> getLaneStreamKeys() {
        return laneStreamKeys;
    }

    public void setLaneStreamKeys(Map<String, String> laneStreamKeys) {
        this.laneStreamKeys = laneStreamKeys;
    }

    public long getDeadLetterMaxLen() {
        return deadLetterMaxLen;
    }

    public void setDeadLetterMaxLen(long deadLetterMaxLen) {
        this.deadLetterMaxLen = deadLetterMaxLen;
    }

    public Producer getProducer() {
        return producer;
    }

    public Worker getWorker() {
        return worker;
    }

    /** Producer settings. */
    public static class Producer {

        /** Approximate cap per lane on XADD (MAXLEN ~). Lossy: can drop undelivered messages. 0 = none. */
        private long maxLen;

        public long getMaxLen() {
            return maxLen;
        }

        public void setMaxLen(long maxLen) {
            this.maxLen = maxLen;
        }
    }

    /** Worker settings; see {@link WorkerOptions} for the meaning of each value. */
    public static class Worker {

        /** Whether this process consumes. Set to false on processes that only produce. */
        private boolean enabled = true;

        /** Fixed consumer name; generated from the prefix when empty. */
        private String consumerName;

        private String consumerNamePrefix = "rsw";
        private int concurrency = DEFAULTS.concurrency();
        private int batchSize = DEFAULTS.batchSize();
        private Duration pollTimeout = DEFAULTS.pollTimeout();
        private Duration commandTimeout = DEFAULTS.commandTimeout();
        private Duration leaseTtl = DEFAULTS.leaseTtl();
        private Duration heartbeatInterval = DEFAULTS.heartbeatInterval();
        private Duration reclaimAfter = DEFAULTS.reclaimAfter();
        private Duration reclaimInterval = DEFAULTS.reclaimInterval();
        private int reclaimScanLimit = DEFAULTS.reclaimScanLimit();
        private int maxDeliveries = DEFAULTS.maxDeliveries();
        private int fairnessRatio = DEFAULTS.fairnessRatio();
        private Duration drainTimeout = DEFAULTS.drainTimeout();
        private Duration interruptGrace = DEFAULTS.interruptGrace();
        private Duration trimInterval = DEFAULTS.trimInterval();
        private Duration deadConsumerIdle = DEFAULTS.deadConsumerIdle();
        private Duration pruneInterval = DEFAULTS.pruneInterval();
        private GroupStart groupStart = DEFAULTS.groupStart();
        private boolean requireNoEviction = DEFAULTS.requireNoEviction();
        private boolean virtualThreads = DEFAULTS.virtualThreads();

        /**
         * SmartLifecycle phase of the worker. It must stop before the Redis connection factory (phase 0 in
         * Spring Data Redis), so the default is high.
         */
        private int phase = Integer.MAX_VALUE - 4096;

        WorkerOptions toOptions() {
            WorkerOptions.Builder b = WorkerOptions.builder()
                    .consumerNamePrefix(consumerNamePrefix)
                    .concurrency(concurrency)
                    .batchSize(batchSize)
                    .pollTimeout(pollTimeout)
                    .commandTimeout(commandTimeout)
                    .leaseTtl(leaseTtl)
                    .heartbeatInterval(heartbeatInterval)
                    .reclaimAfter(reclaimAfter)
                    .reclaimInterval(reclaimInterval)
                    .reclaimScanLimit(reclaimScanLimit)
                    .maxDeliveries(maxDeliveries)
                    .fairnessRatio(fairnessRatio)
                    .drainTimeout(drainTimeout)
                    .interruptGrace(interruptGrace)
                    .trimInterval(trimInterval)
                    .deadConsumerIdle(deadConsumerIdle)
                    .pruneInterval(pruneInterval)
                    .groupStart(groupStart)
                    .requireNoEviction(requireNoEviction)
                    .virtualThreads(virtualThreads);
            if (consumerName != null && !consumerName.isBlank()) {
                b.consumerName(consumerName);
            }
            return b.build();
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getConsumerName() {
            return consumerName;
        }

        public void setConsumerName(String consumerName) {
            this.consumerName = consumerName;
        }

        public String getConsumerNamePrefix() {
            return consumerNamePrefix;
        }

        public void setConsumerNamePrefix(String consumerNamePrefix) {
            this.consumerNamePrefix = consumerNamePrefix;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }

        public Duration getCommandTimeout() {
            return commandTimeout;
        }

        public void setCommandTimeout(Duration commandTimeout) {
            this.commandTimeout = commandTimeout;
        }

        public Duration getLeaseTtl() {
            return leaseTtl;
        }

        public void setLeaseTtl(Duration leaseTtl) {
            this.leaseTtl = leaseTtl;
        }

        public Duration getHeartbeatInterval() {
            return heartbeatInterval;
        }

        public void setHeartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
        }

        public Duration getReclaimAfter() {
            return reclaimAfter;
        }

        public void setReclaimAfter(Duration reclaimAfter) {
            this.reclaimAfter = reclaimAfter;
        }

        public Duration getReclaimInterval() {
            return reclaimInterval;
        }

        public void setReclaimInterval(Duration reclaimInterval) {
            this.reclaimInterval = reclaimInterval;
        }

        public int getReclaimScanLimit() {
            return reclaimScanLimit;
        }

        public void setReclaimScanLimit(int reclaimScanLimit) {
            this.reclaimScanLimit = reclaimScanLimit;
        }

        public int getMaxDeliveries() {
            return maxDeliveries;
        }

        public void setMaxDeliveries(int maxDeliveries) {
            this.maxDeliveries = maxDeliveries;
        }

        public int getFairnessRatio() {
            return fairnessRatio;
        }

        public void setFairnessRatio(int fairnessRatio) {
            this.fairnessRatio = fairnessRatio;
        }

        public Duration getDrainTimeout() {
            return drainTimeout;
        }

        public void setDrainTimeout(Duration drainTimeout) {
            this.drainTimeout = drainTimeout;
        }

        public Duration getInterruptGrace() {
            return interruptGrace;
        }

        public void setInterruptGrace(Duration interruptGrace) {
            this.interruptGrace = interruptGrace;
        }

        public Duration getTrimInterval() {
            return trimInterval;
        }

        public void setTrimInterval(Duration trimInterval) {
            this.trimInterval = trimInterval;
        }

        public Duration getDeadConsumerIdle() {
            return deadConsumerIdle;
        }

        public void setDeadConsumerIdle(Duration deadConsumerIdle) {
            this.deadConsumerIdle = deadConsumerIdle;
        }

        public Duration getPruneInterval() {
            return pruneInterval;
        }

        public void setPruneInterval(Duration pruneInterval) {
            this.pruneInterval = pruneInterval;
        }

        public GroupStart getGroupStart() {
            return groupStart;
        }

        public void setGroupStart(GroupStart groupStart) {
            this.groupStart = groupStart;
        }

        public boolean isRequireNoEviction() {
            return requireNoEviction;
        }

        public void setRequireNoEviction(boolean requireNoEviction) {
            this.requireNoEviction = requireNoEviction;
        }

        public boolean isVirtualThreads() {
            return virtualThreads;
        }

        public void setVirtualThreads(boolean virtualThreads) {
            this.virtualThreads = virtualThreads;
        }

        public int getPhase() {
            return phase;
        }

        public void setPhase(int phase) {
            this.phase = phase;
        }
    }
}
