// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import io.github.peppe_git.redisstreamsworker.internal.Backoff;
import io.github.peppe_git.redisstreamsworker.internal.LaneScheduler;
import io.github.peppe_git.redisstreamsworker.internal.RedisErrors;
import io.github.peppe_git.redisstreamsworker.internal.Scripts;
import io.github.peppe_git.redisstreamsworker.internal.Scripts.ClaimResult;
import io.github.peppe_git.redisstreamsworker.internal.StreamIds;
import io.github.peppe_git.redisstreamsworker.internal.Threads;
import io.github.peppe_git.redisstreamsworker.internal.XInfo;
import io.lettuce.core.Consumer;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XPendingArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessage;
import io.lettuce.core.models.stream.PendingMessages;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A consumer of one consumer group over the lanes of a {@link QueueDefinition}.
 *
 * <p>What it does, in one poll (on a dedicated thread):
 * <ol>
 *   <li>asks the {@link CapacityProvider} how many messages may run, and reads no more than the free
 *       slots;</li>
 *   <li>every {@code reclaimInterval}, takes over pending messages idle for {@code reclaimAfter} whose
 *       lease has expired, unless they run here, or the {@link ReclaimGuard} says the work is owned
 *       elsewhere; messages delivered more than {@code maxDeliveries} times go to the dead-letter
 *       stream instead;</li>
 *   <li>reads new messages lane by lane in priority order (with the anti-starvation rule of
 *       {@link WorkerOptions#fairnessRatio()}), blocking for {@code pollTimeout} on the lowest lane when
 *       everything is empty.</li>
 * </ol>
 * Each message gets a lease ({@code SET … PX}) renewed by a heartbeat thread, and runs on the handler
 * executor. When the handler returns, the message is acknowledged only if this consumer still owns it.
 *
 * <p>The worker survives Redis outages: the poll loop backs off and retries, a {@code NOGROUP} recreates
 * the groups, and an acknowledgement that cannot reach Redis is retried while the worker runs.
 *
 * <p>Delivery is <b>at-least-once</b>. See {@link MessageHandler} and {@link ReclaimGuard}.
 */
public final class RedisStreamsWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamsWorker.class);

    private static final String DLQ_PREFIX = DeadLetter.FIELD_PREFIX;

    private enum State { NEW, RUNNING, STOPPING, STOPPED }

    private enum FinishAction { ACK, DEAD_LETTER }

    private final RedisClient client;
    private final QueueDefinition queue;
    private final MessageHandler handler;
    private final WorkerOptions options;
    private final ReclaimGuard reclaimGuard;
    private final CapacityProvider capacityProvider;
    private final WorkKeyExtractor workKeyExtractor;
    private final WorkerListener listener;
    private final ExecutorService providedExecutor;
    private final String consumer;
    private final LaneScheduler scheduler;
    private final Map<String, Lane> lanesByName = new HashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
    private final Map<MessageRef, Running> inFlight = new ConcurrentHashMap<>();
    private final Map<String, Deferred> deferredByKey = new ConcurrentHashMap<>();
    private final Set<String> runningKeys = ConcurrentHashMap.newKeySet();
    private final Map<MessageRef, PendingFinish> pendingFinishes = new ConcurrentHashMap<>();
    private final Object slotMonitor = new Object();

    private StatefulRedisConnection<String, String> loopConnection;
    private StatefulRedisConnection<String, String> sharedConnection;
    private ExecutorService executor;
    private ScheduledExecutorService heartbeat;
    private Thread pollThread;

    private volatile long lastPollAtMillis;
    private volatile boolean redisUnavailable;
    private volatile int lastCapacity;
    private long nextReclaimAt;
    private long nextTrimAt;
    private long nextPruneAt;

    private RedisStreamsWorker(Builder b) {
        this.client = b.client;
        this.queue = b.queue;
        this.handler = b.handler;
        this.options = b.options;
        this.reclaimGuard = b.reclaimGuard;
        this.capacityProvider = b.capacityProvider != null ? b.capacityProvider
                : CapacityProvider.fixed(b.options.concurrency());
        this.workKeyExtractor = b.workKeyExtractor;
        this.listener = WorkerListener.safe(b.listener);
        this.providedExecutor = b.executor;
        this.consumer = b.options.consumerName();
        this.scheduler = new LaneScheduler(queue.lanes(), options.fairnessRatio());
        this.lastCapacity = options.concurrency();
        queue.lanes().forEach(l -> lanesByName.put(l.name(), l));
    }

    /**
     * Starts building a worker.
     *
     * @param client a Lettuce client for a standalone or Sentinel-managed Redis (the worker opens two
     *     connections and closes them on {@link #stop()})
     */
    public static Builder builder(RedisClient client, QueueDefinition queue, MessageHandler handler) {
        return new Builder(client, queue, handler);
    }

    // ------------------------------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------------------------------

    /**
     * Connects, creates the consumer groups, and starts the poll and heartbeat threads.
     *
     * @throws io.lettuce.core.RedisConnectionException if Redis cannot be reached
     * @throws IllegalStateException if Redis is older than 6.2, or {@code requireNoEviction} is set and
     *     the eviction policy is not {@code noeviction}
     */
    public synchronized void start() {
        if (!state.compareAndSet(State.NEW, State.RUNNING)) {
            throw new IllegalStateException("Worker already started (state " + state.get() + ")");
        }
        try {
            loopConnection = client.connect();
            loopConnection.setTimeout(options.commandTimeout().plus(options.pollTimeout()));
            sharedConnection = client.connect();
            sharedConnection.setTimeout(options.commandTimeout());
            RedisRequirements.check(sharedConnection.sync(), options.requireNoEviction());
            executor = providedExecutor != null ? providedExecutor
                    : Threads.handlerExecutor("rsw-handler", options.virtualThreads());
            try {
                ensureGroups(loopConnection.sync(), false);
                pruneDeadConsumers(loopConnection.sync());
            } catch (RuntimeException e) {
                log.warn("Consumer groups not verified at startup ({}); the poll loop will retry", e.toString());
            }
            long now = System.currentTimeMillis();
            lastPollAtMillis = now;
            nextTrimAt = now + options.trimInterval().toMillis();
            nextPruneAt = now + options.pruneInterval().toMillis();
            heartbeat = Executors.newSingleThreadScheduledExecutor(Threads.daemon("rsw-heartbeat-" + consumer));
            long hb = options.heartbeatInterval().toMillis();
            heartbeat.scheduleWithFixedDelay(this::heartbeatTick, hb, hb, TimeUnit.MILLISECONDS);
            pollThread = new Thread(this::pollLoop, "rsw-poll-" + consumer);
            pollThread.setDaemon(true);
            pollThread.start();
            log.info("Worker '{}' started on {} ({})", consumer, queue, options);
            listener.onStarted(consumer);
        } catch (RuntimeException e) {
            state.set(State.STOPPED);
            closeQuietly();
            throw e;
        }
    }

    /**
     * Stops reading, waits up to {@code drainTimeout} for running handlers, interrupts the ones still
     * running, retries pending acknowledgements once, then closes the connections. Messages that did not
     * finish stay pending and are reclaimed by another worker. Idempotent.
     */
    public void stop() {
        if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
            return;
        }
        log.info("Worker '{}' stopping: {} message(s) running", consumer, inFlight.size());
        signalSlot();
        joinPollThread();
        boolean drained = awaitInFlight(options.drainTimeout());
        if (!drained) {
            log.warn("Worker '{}': {} handler(s) still running after the drain timeout, interrupting",
                    consumer, inFlight.size());
            inFlight.values().forEach(r -> {
                Future<?> f = r.future;
                if (f != null) {
                    f.cancel(true);
                }
            });
            if (!awaitInFlight(options.interruptGrace())) {
                // Their leases are no longer renewed once the heartbeat stops: another worker will
                // reclaim them while they may still be running in this JVM. Say it loudly.
                log.error("Worker '{}': handler(s) ignored the interrupt and are still running: {}. Their "
                        + "messages will be reclaimed elsewhere once the leases expire.", consumer, inFlight.keySet());
            }
        }
        releaseDeferred();
        if (!pendingFinishes.isEmpty()) {
            try {
                retryPendingFinishes(sharedConnection.sync());
            } catch (RuntimeException e) {
                log.warn("Worker '{}': {} acknowledgement(s) not delivered at shutdown; the messages will be "
                        + "delivered again", consumer, pendingFinishes.size());
            }
        }
        if (providedExecutor == null && executor != null) {
            executor.shutdownNow();
        }
        if (heartbeat != null) {
            heartbeat.shutdownNow();
        }
        closeQuietly();
        state.set(State.STOPPED);
        log.info("Worker '{}' stopped", consumer);
        listener.onStopped(consumer);
    }

    @Override
    public void close() {
        stop();
        state.compareAndSet(State.NEW, State.STOPPED);
    }

    public boolean isRunning() {
        return state.get() == State.RUNNING;
    }

    public String consumerName() {
        return consumer;
    }

    /**
     * When the poll loop last ran. It advances on every poll, also while the worker is saturated; a
     * value that stops advancing means the loop is stuck. Useful for an external dead man's switch.
     */
    public Instant lastPollAt() {
        return Instant.ofEpochMilli(lastPollAtMillis);
    }

    /** Messages whose handler is running on this instance. */
    public int inFlightCount() {
        return inFlight.size();
    }

    /** {@code true} while the poll loop cannot reach Redis. */
    public boolean redisUnavailable() {
        return redisUnavailable;
    }

    // ------------------------------------------------------------------------------------------------
    // Poll loop
    // ------------------------------------------------------------------------------------------------

    private void pollLoop() {
        RedisCommands<String, String> redis = loopConnection.sync();
        Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(5));
        while (state.get() == State.RUNNING) {
            lastPollAtMillis = System.currentTimeMillis();
            try {
                periodicTasks(redis);
                int free = freeSlots();
                if (free <= 0) {
                    awaitSlot(options.pollTimeout().toMillis());
                    continue;
                }
                long now = System.currentTimeMillis();
                if (now >= nextReclaimAt) {
                    nextReclaimAt = now + options.reclaimInterval().toMillis();
                    free -= reclaim(redis, free);
                }
                if (free > 0) {
                    consume(redis, free);
                }
                markHealthy();
                backoff.reset();
            } catch (RuntimeException e) {
                if (state.get() != State.RUNNING) {
                    break;
                }
                if (RedisErrors.isNoGroup(e)) {
                    log.warn("Worker '{}': consumer group '{}' missing (Redis flushed or restarted without "
                            + "persistence?), recreating it", consumer, queue.group());
                    try {
                        ensureGroups(redis, true);
                        continue;
                    } catch (RuntimeException again) {
                        markUnavailable(again);
                    }
                } else {
                    markUnavailable(e);
                }
                sleep(backoff.next().toMillis());
            } catch (Throwable t) {
                log.error("Worker '{}': unexpected error in the poll loop", consumer, t);
                sleep(backoff.next().toMillis());
            }
        }
    }

    private void periodicTasks(RedisCommands<String, String> redis) {
        if (!pendingFinishes.isEmpty()) {
            retryPendingFinishes(redis);
        }
        long now = System.currentTimeMillis();
        if (!options.trimInterval().isZero() && now >= nextTrimAt) {
            nextTrimAt = now + options.trimInterval().toMillis();
            trim(redis);
        }
        if (now >= nextPruneAt) {
            nextPruneAt = now + options.pruneInterval().toMillis();
            pruneDeadConsumers(redis);
        }
    }

    private int freeSlots() {
        Map<String, Integer> byLane = new HashMap<>();
        for (MessageRef ref : inFlight.keySet()) {
            byLane.merge(ref.lane().name(), 1, Integer::sum);
        }
        InFlight snapshot = new InFlight(inFlight.size(), byLane);
        int capacity;
        try {
            capacity = capacityProvider.capacity(snapshot);
            lastCapacity = capacity;
        } catch (RuntimeException e) {
            capacity = lastCapacity;
            log.warn("CapacityProvider failed ({}), keeping capacity {}", e.toString(), capacity);
        }
        return capacity - snapshot.total();
    }

    private void consume(RedisCommands<String, String> redis, int free) {
        List<Lane> order = scheduler.order();
        Lane lowest = scheduler.lowest();
        int count = Math.min(options.batchSize(), free);
        for (int i = 0; i < order.size(); i++) {
            Lane lane = order.get(i);
            boolean block = lane.equals(lowest) && i == order.size() - 1;
            if (read(redis, lane, count, block)) {
                scheduler.served(lane);
                return;
            }
        }
        if (!order.get(order.size() - 1).equals(lowest)) {
            // Fairness poll: the lowest lane was read first and empty; block on it now.
            if (read(redis, lowest, count, true)) {
                scheduler.served(lowest);
                return;
            }
        }
        scheduler.nothingServed();
    }

    private boolean read(RedisCommands<String, String> redis, Lane lane, int count, boolean block) {
        XReadArgs args = XReadArgs.Builder.count(count);
        if (block) {
            args.block(options.pollTimeout());
        }
        List<StreamMessage<String, String>> records = redis.xreadgroup(Consumer.from(queue.group(), consumer),
                args, XReadArgs.StreamOffset.lastConsumed(lane.streamKey()));
        if (records == null || records.isEmpty()) {
            return false;
        }
        for (StreamMessage<String, String> r : records) {
            submit(new QueueMessage(lane.name(), r.getId(), r.getBody(), 1), false);
        }
        return true;
    }

    // ------------------------------------------------------------------------------------------------
    // Reclaim
    // ------------------------------------------------------------------------------------------------

    private int reclaim(RedisCommands<String, String> redis, int free) {
        int started = 0;
        Set<MessageRef> local = new HashSet<>(inFlight.keySet());
        deferredByKey.values().forEach(d -> local.add(d.ref));
        local.addAll(pendingFinishes.keySet());
        long minIdle = options.reclaimAfter().toMillis();
        for (Lane lane : queue.lanes()) {
            if (started >= free) {
                break;
            }
            List<PendingMessage> pending = redis.xpending(lane.streamKey(),
                    XPendingArgs.Builder.xpending(queue.group(), Range.unbounded(),
                            Limit.from(options.reclaimScanLimit())).idle(minIdle));
            if (pending == null) {
                continue;
            }
            for (PendingMessage pm : pending) {
                if (started >= free) {
                    break;
                }
                String id = pm.getId();
                MessageRef ref = new MessageRef(lane, id);
                if (local.contains(ref)) {
                    continue;
                }
                String leaseKey = queue.leaseKey(lane, id);
                if (redis.exists(leaseKey) > 0) {
                    continue;
                }
                // Read the record BEFORE claiming it: XRANGE does not touch the pending entry, while
                // XCLAIM increments its delivery count. The guard decides on the content, and must be
                // able to say "not yet" without spending a delivery.
                List<StreamMessage<String, String>> records = redis.xrange(lane.streamKey(), Range.create(id, id));
                if (records == null || records.isEmpty()) {
                    redis.xack(lane.streamKey(), queue.group(), id);
                    log.warn("Pending message {} on lane '{}' no longer exists in the stream (trimmed or "
                            + "deleted); acknowledged", id, lane.name());
                    listener.onMessageVanished(lane.name(), id);
                    continue;
                }
                QueueMessage candidate = new QueueMessage(lane.name(), id, records.get(0).getBody(),
                        pm.getRedeliveryCount());
                if (!guardAllows(new ReclaimCandidate(candidate, pm.getConsumer(), pm.getSinceLastDelivery()))) {
                    continue;
                }
                ClaimResult claim = Scripts.claim(redis, lane.streamKey(), leaseKey, queue.group(), consumer,
                        minIdle, id, options.leaseTtl().toMillis());
                if (claim.code() == ClaimResult.VANISHED) {
                    listener.onMessageVanished(lane.name(), id);
                    continue;
                }
                if (claim.code() != ClaimResult.CLAIMED) {
                    continue; // another worker got there first, or the lease came back
                }
                QueueMessage message = candidate.withDeliveryCount(claim.deliveries());
                listener.onReclaimed(message, pm.getConsumer());
                if (message.deliveryCount() > options.maxDeliveries()) {
                    log.warn("Message {} on lane '{}' delivered {} times (max {}): dead-lettered", id,
                            lane.name(), message.deliveryCount(), options.maxDeliveries());
                    deadLetter(redis, message, DeadLetter.REASON_MAX_DELIVERIES);
                    continue;
                }
                log.info("Reclaimed message {} on lane '{}' from '{}' (delivery {})", id, lane.name(),
                        pm.getConsumer(), message.deliveryCount());
                if (submit(message, true)) {
                    started++;
                }
            }
        }
        return started;
    }

    private boolean guardAllows(ReclaimCandidate candidate) {
        if (reclaimGuard == null) {
            return true;
        }
        try {
            return reclaimGuard.allowReclaim(candidate);
        } catch (Exception e) {
            log.warn("ReclaimGuard failed for message {} on lane '{}' ({}): reclaiming anyway (fail-open)",
                    candidate.message().id(), candidate.message().lane(), e.toString());
            return true;
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Submission and completion
    // ------------------------------------------------------------------------------------------------

    /** Starts the handler, or defers the message behind one with the same work key. */
    private boolean submit(QueueMessage message, boolean leaseHeld) {
        Lane lane = lanesByName.get(message.lane());
        MessageRef ref = new MessageRef(lane, message.id());
        String key = workKeyOf(message);
        if (key != null && !runningKeys.add(key)) {
            Deferred deferred = new Deferred(ref, message, key);
            Deferred existing = deferredByKey.putIfAbsent(key, deferred);
            if (existing == null) {
                if (!leaseHeld) {
                    acquireLease(ref);
                }
                log.info("Message {} on lane '{}' deferred: work key '{}' is running on this worker",
                        message.id(), message.lane(), key);
                listener.onDeferred(message, key);
                if (!runningKeys.contains(key)) {
                    resumeDeferred(key); // the running one finished in the meantime
                }
            } else if (!existing.ref.equals(ref)) {
                // Third message for the same work: leave it pending (no lease) for a later reclaim.
                releaseLease(ref);
                log.warn("Message {} on lane '{}' left pending: work key '{}' is running and already has "
                        + "a deferred message ({})", message.id(), message.lane(), key, existing.ref.id());
            }
            return false;
        }
        Running running = new Running(ref, message, key);
        inFlight.put(ref, running);
        if (!leaseHeld) {
            acquireLease(ref);
        }
        try {
            running.future = executor.submit(() -> runHandler(running));
        } catch (RejectedExecutionException e) {
            inFlight.remove(ref);
            if (key != null) {
                runningKeys.remove(key);
            }
            releaseLease(ref);
            log.warn("Message {} not started (executor shut down); it stays pending", message.id());
            return false;
        }
        return true;
    }

    private String workKeyOf(QueueMessage message) {
        if (workKeyExtractor == null) {
            return null;
        }
        try {
            return workKeyExtractor.workKey(message);
        } catch (RuntimeException e) {
            log.warn("WorkKeyExtractor failed for message {} ({}): no exclusivity", message.id(), e.toString());
            return null;
        }
    }

    private void runHandler(Running r) {
        long started = System.nanoTime();
        Outcome outcome;
        Throwable error = null;
        listener.onMessageStarted(r.message);
        try {
            outcome = handler.handle(r.message, r);
            if (outcome == null) {
                throw new IllegalStateException("MessageHandler returned null");
            }
        } catch (Throwable t) {
            error = t;
            boolean stopping = state.get() != State.RUNNING;
            outcome = stopping && (t instanceof InterruptedException || Thread.currentThread().isInterrupted())
                    ? Outcome.SHUTDOWN : Outcome.LEAVE_PENDING;
            if (outcome == Outcome.SHUTDOWN) {
                log.info("Message {} on lane '{}' interrupted by shutdown; it stays pending", r.message.id(),
                        r.message.lane());
            } else {
                log.error("Handler failed on message {} on lane '{}' (delivery {}); it stays pending and will be "
                        + "delivered again", r.message.id(), r.message.lane(), r.message.deliveryCount(), t);
            }
        }
        finish(r, outcome, error, Duration.ofNanos(System.nanoTime() - started));
    }

    private void finish(Running r, Outcome outcome, Throwable error, Duration duration) {
        try {
            switch (outcome) {
                case ACK -> complete(r.ref, r.message, FinishAction.ACK, null);
                case DEAD_LETTER -> complete(r.ref, r.message, FinishAction.DEAD_LETTER,
                        r.deadLetterReason != null ? r.deadLetterReason : DeadLetter.REASON_HANDLER);
                case LEAVE_PENDING, SHUTDOWN -> releaseLease(r.ref);
                default -> throw new IllegalStateException(outcome.toString());
            }
            listener.onMessageCompleted(r.message, outcome, duration, error);
        } finally {
            // The slot is freed whatever happened above: inFlight is the local concurrency guard, not a
            // delivery record. Leaving it dirty would cost a slot forever.
            inFlight.remove(r.ref);
            if (r.workKey != null) {
                runningKeys.remove(r.workKey);
                if (outcome != Outcome.SHUTDOWN && state.get() == State.RUNNING) {
                    resumeDeferred(r.workKey);
                }
            }
            signalSlot();
        }
    }

    /** Acknowledges or dead-letters from a handler thread; on a Redis error, retried by the poll loop. */
    private void complete(MessageRef ref, QueueMessage message, FinishAction action, String reason) {
        try {
            applyFinish(sharedConnection.sync(), ref, message, action, reason);
        } catch (RuntimeException e) {
            if (RedisErrors.isNoGroup(e)) {
                log.warn("Message {} on lane '{}' cannot be acknowledged: its group no longer exists (Redis data "
                        + "lost?)", message.id(), message.lane());
                listener.onAckRejected(message, "not-pending");
                return;
            }
            log.warn("Acknowledgement of message {} on lane '{}' failed ({}); retrying while the worker runs",
                    message.id(), message.lane(), e.toString());
            pendingFinishes.put(ref, new PendingFinish(ref, message, action, reason));
            listener.onAckFailed(message, e);
        }
    }

    private void applyFinish(RedisCommands<String, String> redis, MessageRef ref, QueueMessage message,
            FinishAction action, String reason) {
        String leaseKey = queue.leaseKey(ref.lane(), ref.id());
        if (action == FinishAction.ACK) {
            long r = Scripts.ackIfOwner(redis, ref.lane().streamKey(), leaseKey, queue.group(), ref.id(), consumer);
            if (r == 0) {
                log.warn("Message {} on lane '{}' not acknowledged: it now belongs to another consumer, which "
                        + "will acknowledge it", message.id(), message.lane());
                listener.onAckRejected(message, "owned-by-another-consumer");
            } else if (r < 0) {
                log.warn("Message {} on lane '{}' was no longer pending when acknowledged", message.id(),
                        message.lane());
                listener.onAckRejected(message, "not-pending");
            }
            return;
        }
        String result = deadLetterScript(redis, ref, message, reason);
        if ("0".equals(result)) {
            listener.onAckRejected(message, "owned-by-another-consumer");
        } else if ("-1".equals(result)) {
            listener.onAckRejected(message, "not-pending");
        } else {
            listener.onDeadLettered(message, reason);
        }
    }

    private void deadLetter(RedisCommands<String, String> redis, QueueMessage message, String reason) {
        MessageRef ref = new MessageRef(lanesByName.get(message.lane()), message.id());
        try {
            applyFinish(redis, ref, message, FinishAction.DEAD_LETTER, reason);
        } catch (RuntimeException e) {
            pendingFinishes.put(ref, new PendingFinish(ref, message, FinishAction.DEAD_LETTER, reason));
            throw e;
        }
    }

    private String deadLetterScript(RedisCommands<String, String> redis, MessageRef ref, QueueMessage message,
            String reason) {
        Map<String, String> fields = new LinkedHashMap<>(message.fields());
        fields.put(DLQ_PREFIX + "reason", reason);
        fields.put(DLQ_PREFIX + "lane", message.lane());
        fields.put(DLQ_PREFIX + "source-id", message.id());
        fields.put(DLQ_PREFIX + "deliveries", Long.toString(message.deliveryCount()));
        fields.put(DLQ_PREFIX + "consumer", consumer);
        fields.put(DLQ_PREFIX + "at", Long.toString(System.currentTimeMillis()));
        return Scripts.deadLetter(redis, ref.lane().streamKey(), queue.leaseKey(ref.lane(), ref.id()),
                queue.deadLetterKey(), queue.group(), ref.id(), consumer, queue.deadLetterMaxLen(), fields);
    }

    private void retryPendingFinishes(RedisCommands<String, String> redis) {
        for (PendingFinish p : List.copyOf(pendingFinishes.values())) {
            try {
                applyFinish(redis, p.ref, p.message, p.action, p.reason);
                pendingFinishes.remove(p.ref, p);
                log.info("Delayed {} of message {} on lane '{}' delivered", p.action, p.message.id(),
                        p.message.lane());
            } catch (RuntimeException e) {
                if (RedisErrors.isNoGroup(e)) {
                    pendingFinishes.remove(p.ref, p);
                    listener.onAckRejected(p.message, "not-pending");
                } else {
                    throw e;
                }
            }
        }
    }

    private void resumeDeferred(String key) {
        Deferred d = deferredByKey.remove(key);
        if (d != null) {
            log.info("Deferred message {} on lane '{}' starts now", d.message.id(), d.message.lane());
            submit(d.message, true);
        }
    }

    private void releaseDeferred() {
        for (Deferred d : List.copyOf(deferredByKey.values())) {
            deferredByKey.remove(d.workKey, d);
            releaseLease(d.ref);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Leases
    // ------------------------------------------------------------------------------------------------

    private void acquireLease(MessageRef ref) {
        try {
            sharedConnection.sync().psetex(queue.leaseKey(ref.lane(), ref.id()), options.leaseTtl().toMillis(),
                    consumer);
        } catch (RuntimeException e) {
            // Best effort: the heartbeat re-acquires a missing lease. Until then only the ReclaimGuard
            // protects this message from a reclaim, which is worth a warning.
            log.warn("Lease not acquired for message {} on lane '{}' ({}); the heartbeat will retry", ref.id(),
                    ref.lane().name(), e.toString());
        }
    }

    private void releaseLease(MessageRef ref) {
        try {
            Scripts.releaseLease(sharedConnection.sync(), queue.leaseKey(ref.lane(), ref.id()), consumer);
        } catch (RuntimeException e) {
            log.debug("Lease release failed for {} ({}); it will expire", ref, e.toString());
        }
    }

    private void heartbeatTick() {
        try {
            RedisCommands<String, String> redis = sharedConnection.sync();
            long ttl = options.leaseTtl().toMillis();
            for (Running r : List.copyOf(inFlight.values())) {
                long result = renew(redis, r.ref, ttl);
                if (result < 0) {
                    continue;
                }
                boolean held = result != 0;
                if (!held && r.leaseHeld.compareAndSet(true, false)) {
                    log.warn("Lease of running message {} on lane '{}' is now held by another consumer: the "
                            + "message has been reclaimed elsewhere", r.message.id(), r.message.lane());
                    listener.onLeaseLost(r.message);
                }
                if (held && result == 2) {
                    log.warn("Lease of running message {} on lane '{}' had expired; re-acquired", r.message.id(),
                            r.message.lane());
                }
                listener.onLeaseRenewed(r.message, held && r.leaseHeld.get());
            }
            for (Deferred d : List.copyOf(deferredByKey.values())) {
                if (renew(redis, d.ref, ttl) == 0) {
                    deferredByKey.remove(d.workKey, d);
                    log.warn("Deferred message {} was reclaimed by another consumer", d.message.id());
                }
            }
            for (PendingFinish p : List.copyOf(pendingFinishes.values())) {
                if (renew(redis, p.ref, ttl) == 0) {
                    pendingFinishes.remove(p.ref, p);
                }
            }
        } catch (Throwable t) {
            log.warn("Heartbeat failed: {}", t.toString());
        }
    }

    /** 1 renewed, 2 re-acquired, 0 held by another consumer, -1 Redis error. */
    private long renew(RedisCommands<String, String> redis, MessageRef ref, long ttl) {
        try {
            return Scripts.renewLease(redis, queue.leaseKey(ref.lane(), ref.id()), consumer, ttl);
        } catch (RuntimeException e) {
            log.debug("Lease renewal failed for {} ({})", ref, e.toString());
            return -1;
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Group maintenance
    // ------------------------------------------------------------------------------------------------

    private void ensureGroups(RedisCommands<String, String> redis, boolean recreated) {
        String offset = options.groupStart() == GroupStart.EARLIEST ? "0" : "$";
        for (Lane lane : queue.lanes()) {
            try {
                redis.xgroupCreate(XReadArgs.StreamOffset.from(lane.streamKey(), offset), queue.group(),
                        XGroupCreateArgs.Builder.mkstream());
                log.info("Consumer group '{}' created on lane '{}' ({}) from {}", queue.group(), lane.name(),
                        lane.streamKey(), options.groupStart());
                listener.onGroupCreated(lane.name(), recreated);
            } catch (RuntimeException e) {
                if (!RedisErrors.isBusyGroup(e)) {
                    throw e;
                }
            }
        }
    }

    /** Removes consumers idle for long with nothing pending (names are regenerated on each start). */
    private void pruneDeadConsumers(RedisCommands<String, String> redis) {
        long threshold = options.deadConsumerIdle().toMillis();
        for (Lane lane : queue.lanes()) {
            try {
                int removed = 0;
                for (XInfo.Consumer c : XInfo.consumers(redis.xinfoConsumers(lane.streamKey(), queue.group()))) {
                    // A dead consumer WITH pending messages is left alone: deleting it would delete its
                    // pending entries, which the reclaim still has to take over.
                    if (c.pending() == 0 && c.idleMillis() >= threshold && !c.name().equals(consumer)) {
                        redis.xgroupDelconsumer(lane.streamKey(), Consumer.from(queue.group(), c.name()));
                        removed++;
                    }
                }
                if (removed > 0) {
                    log.info("Removed {} idle consumer(s) without pending messages from lane '{}'", removed,
                            lane.name());
                }
            } catch (RuntimeException e) {
                log.debug("Consumer pruning skipped on lane '{}': {}", lane.name(), e.toString());
            }
        }
    }

    /**
     * Trims every lane below the oldest entry still needed by any group: the smallest pending id, or the
     * entry after the group's last-delivered id. Only acknowledged entries are removed.
     */
    private void trim(RedisCommands<String, String> redis) {
        for (Lane lane : queue.lanes()) {
            try {
                List<XInfo.Group> groups = XInfo.groups(redis.xinfoGroups(lane.streamKey()));
                if (groups.isEmpty()) {
                    continue;
                }
                String threshold = null;
                for (XInfo.Group g : groups) {
                    String needed = StreamIds.next(g.lastDeliveredId());
                    if (g.pending() > 0) {
                        PendingMessages summary = redis.xpending(lane.streamKey(), g.name());
                        if (summary != null && summary.getMessageIds() != null
                                && summary.getMessageIds().getLower() != null
                                && summary.getMessageIds().getLower().getValue() != null) {
                            needed = StreamIds.min(needed, summary.getMessageIds().getLower().getValue());
                        }
                    }
                    threshold = StreamIds.min(threshold, needed);
                }
                Long removed = redis.xtrim(lane.streamKey(), XTrimArgs.Builder.minId(threshold).approximateTrimming());
                if (removed != null && removed > 0) {
                    log.debug("Trimmed {} acknowledged entries from lane '{}' (below {})", removed, lane.name(),
                            threshold);
                }
            } catch (RuntimeException e) {
                log.debug("Trim skipped on lane '{}': {}", lane.name(), e.toString());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

    private void markUnavailable(RuntimeException e) {
        if (!redisUnavailable) {
            redisUnavailable = true;
            log.warn("Worker '{}': Redis unavailable, retrying with backoff", consumer, e);
            listener.onRedisUnavailable(e);
        }
    }

    private void markHealthy() {
        if (redisUnavailable) {
            redisUnavailable = false;
            log.info("Worker '{}': Redis reachable again", consumer);
            listener.onRedisRecovered();
        }
    }

    private void signalSlot() {
        synchronized (slotMonitor) {
            slotMonitor.notifyAll();
        }
    }

    private void awaitSlot(long millis) {
        synchronized (slotMonitor) {
            try {
                slotMonitor.wait(Math.max(1, millis));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean awaitInFlight(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (slotMonitor) {
            while (!inFlight.isEmpty()) {
                long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remaining <= 0) {
                    return false;
                }
                try {
                    slotMonitor.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return inFlight.isEmpty();
                }
            }
        }
        return true;
    }

    private void joinPollThread() {
        if (pollThread == null) {
            return;
        }
        try {
            pollThread.join(options.pollTimeout().plus(options.commandTimeout()).toMillis() + 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (pollThread.isAlive()) {
            pollThread.interrupt();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeQuietly() {
        for (StatefulRedisConnection<String, String> c : new ArrayList<>(
                java.util.Arrays.asList(loopConnection, sharedConnection))) {
            if (c != null) {
                try {
                    c.close();
                } catch (RuntimeException e) {
                    log.debug("Error closing connection: {}", e.toString());
                }
            }
        }
    }

    /** A message identity: record ids are unique within a stream only. */
    private record MessageRef(Lane lane, String id) {
    }

    private final class Running implements MessageContext {

        final MessageRef ref;
        final QueueMessage message;
        final String workKey;
        final AtomicBoolean leaseHeld = new AtomicBoolean(true);
        volatile Future<?> future;
        volatile String deadLetterReason;

        Running(MessageRef ref, QueueMessage message, String workKey) {
            this.ref = ref;
            this.message = message;
            this.workKey = workKey;
        }

        @Override
        public boolean leaseHeld() {
            return leaseHeld.get();
        }

        @Override
        public boolean shutdownRequested() {
            return state.get() != State.RUNNING;
        }

        @Override
        public String consumerName() {
            return consumer;
        }

        @Override
        public void deadLetterReason(String reason) {
            this.deadLetterReason = reason;
        }
    }

    private record Deferred(MessageRef ref, QueueMessage message, String workKey) {
    }

    private record PendingFinish(MessageRef ref, QueueMessage message, FinishAction action, String reason) {
    }

    /** Builder of {@link RedisStreamsWorker}. */
    public static final class Builder {

        private final RedisClient client;
        private final QueueDefinition queue;
        private final MessageHandler handler;
        private WorkerOptions options = WorkerOptions.defaults();
        private ReclaimGuard reclaimGuard;
        private CapacityProvider capacityProvider;
        private WorkKeyExtractor workKeyExtractor;
        private WorkerListener listener = WorkerListener.NONE;
        private ExecutorService executor;

        private Builder(RedisClient client, QueueDefinition queue, MessageHandler handler) {
            this.client = Objects.requireNonNull(client, "client");
            this.queue = Objects.requireNonNull(queue, "queue");
            this.handler = Objects.requireNonNull(handler, "handler");
        }

        public Builder options(WorkerOptions options) {
            this.options = Objects.requireNonNull(options, "options");
            return this;
        }

        public Builder reclaimGuard(ReclaimGuard guard) {
            this.reclaimGuard = guard;
            return this;
        }

        public Builder capacityProvider(CapacityProvider provider) {
            this.capacityProvider = provider;
            return this;
        }

        public Builder workKey(WorkKeyExtractor extractor) {
            this.workKeyExtractor = extractor;
            return this;
        }

        public Builder listener(WorkerListener listener) {
            this.listener = listener == null ? WorkerListener.NONE : listener;
            return this;
        }

        /**
         * Runs handlers on this executor instead of the default one. The worker does not shut it down;
         * it must accept at least as many concurrent tasks as the capacity.
         */
        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public RedisStreamsWorker build() {
            return new RedisStreamsWorker(this);
        }
    }
}
