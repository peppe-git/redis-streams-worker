// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task on at most one instance at a time, most of the time: for periodic jobs that every replica
 * schedules but only one should run.
 *
 * <ul>
 *   <li>Acquisition is {@code SET key token NX PX ttl}; the token is unique per acquisition.</li>
 *   <li>While the task runs, the TTL is extended every {@code ttl / 3} with a compare-and-extend script,
 *       on a dedicated thread (not on the scheduler pool the long tasks occupy).</li>
 *   <li>Release is a compare-and-delete script: a lock that expired and was taken by another instance is
 *       never deleted.</li>
 *   <li>The task receives a {@link LockOwnership} to check before each conclusive write.</li>
 *   <li>What happens when Redis cannot be reached is a mandatory {@link LockOutagePolicy}.</li>
 * </ul>
 *
 * <p><b>No fencing token.</b> If the holder pauses for longer than the TTL (garbage collection, a VM
 * freeze), another instance acquires the lock while the first is still running. Use it to avoid duplicate
 * work, not to protect a resource that must never be written by two holders; see ADR 0007.
 *
 * <p>Not for stream consumers: those must run on every replica.
 */
public final class RedisLock implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisLock.class);

    private final StatefulRedisConnection<String, String> connection;
    private final boolean ownsConnection;
    private final String keyPrefix;
    private final LockListener listener;
    private final String instanceId = UUID.randomUUID().toString();
    private final ScheduledExecutorService renewer;

    /** A lock service with its own connection, key prefix {@code rsw:lock}. */
    public RedisLock(RedisClient client) {
        this(client.connect(), true, "rsw:lock", LockListener.NONE);
    }

    public RedisLock(RedisClient client, String keyPrefix, LockListener listener) {
        this(client.connect(), true, keyPrefix, listener);
    }

    /** A lock service on a connection owned by the caller. */
    public RedisLock(StatefulRedisConnection<String, String> connection, String keyPrefix, LockListener listener) {
        this(connection, false, keyPrefix, listener);
    }

    private RedisLock(StatefulRedisConnection<String, String> connection, boolean owns, String keyPrefix,
            LockListener listener) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.ownsConnection = owns;
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        this.listener = listener == null ? LockListener.NONE : listener;
        this.renewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rsw-lock-renewer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Runs {@code task} if the lock {@code name} can be acquired; otherwise skips it.
     *
     * @return {@code true} if the task ran to completion
     */
    public boolean runIfAcquired(String name, Duration ttl, LockOutagePolicy policy, Runnable task) {
        Objects.requireNonNull(task, "task");
        return runIfAcquired(name, ttl, policy, ownership -> task.run());
    }

    /**
     * Like {@link #runIfAcquired(String, Duration, LockOutagePolicy, Runnable)}, but hands the task its
     * {@link LockOwnership}: call {@link LockOwnership#requireHeld(String)} before each conclusive write.
     *
     * @param ttl safety net for a crashed holder, not the expected duration: the lock is renewed while the
     *     task runs
     * @param policy what to do if Redis does not answer; mandatory
     * @return {@code true} if the task ran to completion; {@code false} if the lock was held elsewhere, the
     *     round was skipped by {@link LockOutagePolicy#FAIL_CLOSED}, or the task lost the lock
     * @throws RuntimeException whatever the task throws (the lock is released first)
     */
    public boolean runIfAcquired(String name, Duration ttl, LockOutagePolicy policy, Consumer<LockOwnership> task) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(policy, "policy is mandatory: decide what happens when Redis is down");
        Objects.requireNonNull(task, "task");
        if (ttl == null || ttl.toMillis() < 30) {
            throw new IllegalArgumentException("ttl must be at least 30 ms");
        }
        String key = keyPrefix + ":" + name;
        String token = instanceId + ":" + UUID.randomUUID();
        String acquired;
        try {
            acquired = redis().set(key, token, SetArgs.Builder.nx().px(ttl.toMillis()));
        } catch (RuntimeException e) {
            listener.onOutage(name, policy, e);
            if (policy == LockOutagePolicy.FAIL_CLOSED) {
                log.warn("Lock '{}' cannot be queried ({}): round SKIPPED (fail-closed), retry at next tick",
                        name, e.toString());
                return false;
            }
            log.warn("Lock '{}' cannot be queried ({}): running anyway (fail-open); with several instances this "
                    + "round may run twice until Redis is back", name, e.toString());
            return runUnlocked(name, task);
        }
        if (!"OK".equals(acquired)) {
            listener.onSkipped(name);
            return false;
        }
        Held held = new Held(key, token, ttl, policy);
        long period = Math.max(10, ttl.toMillis() / 3);
        ScheduledFuture<?> renewal = renewer.scheduleWithFixedDelay(held::renew, period, period, TimeUnit.MILLISECONDS);
        try {
            task.accept(held);
            listener.onCompleted(name, true);
            return true;
        } catch (LockLostException e) {
            log.warn("Lock '{}' lost during the round: stopped before '{}'. The remaining work is still due and "
                    + "will be picked up by the next round.", name, e.getMessage());
            listener.onLost(name, e.getMessage());
            return false;
        } catch (RuntimeException e) {
            listener.onFailed(name, e);
            throw e;
        } finally {
            renewal.cancel(false);
            try {
                LuaScript.RELEASE_IF_MINE.run(redis(), key, token);
            } catch (RuntimeException e) {
                log.debug("Release of lock '{}' failed ({}); it will expire", name, e.toString());
            }
        }
    }

    private boolean runUnlocked(String name, Consumer<LockOwnership> task) {
        try {
            task.accept(LockOwnership.assumed());
        } catch (RuntimeException e) {
            listener.onFailed(name, e);
            throw e;
        }
        listener.onCompleted(name, false);
        return true;
    }

    private RedisCommands<String, String> redis() {
        return connection.sync();
    }

    @Override
    public void close() {
        renewer.shutdownNow();
        if (ownsConnection) {
            connection.close();
        }
    }

    /** A lock held by this instance, and the ownership handed to its task. */
    private final class Held implements LockOwnership {

        private final String key;
        private final String token;
        private final Duration ttl;
        private final LockOutagePolicy policy;
        private volatile boolean lost;

        Held(String key, String token, Duration ttl, LockOutagePolicy policy) {
            this.key = key;
            this.token = token;
            this.ttl = ttl;
            this.policy = policy;
        }

        @Override
        public boolean stillHeld() {
            return renew();
        }

        /** Compare-and-extend: proves ownership and extends it in one atomic step. */
        synchronized boolean renew() {
            if (lost) {
                return false;
            }
            long extended;
            try {
                extended = LuaScript.RENEW_IF_MINE.run(redis(), key, token, Long.toString(ttl.toMillis()));
            } catch (RuntimeException e) {
                // Neither ownership nor its absence can be proven: the caller's policy decides.
                log.warn("Renewal of lock {} failed ({}): applying {}", key, e.toString(), policy);
                return policy != LockOutagePolicy.FAIL_CLOSED;
            }
            if (extended != 1L) {
                lost = true;
                log.warn("Lock {} no longer belongs to this holder (expired and taken, or deleted)", key);
                return false;
            }
            return true;
        }
    }
}
