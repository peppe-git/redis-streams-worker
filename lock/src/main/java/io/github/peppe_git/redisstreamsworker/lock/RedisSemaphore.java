// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

import io.lettuce.core.RedisClient;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A concurrency limit valid across instances ("at most N renders at the same time in the cluster").
 *
 * <p>Not a counter but N slot keys: acquiring means winning {@code SET NX PX} on a free slot, releasing
 * means a compare-and-delete. A counter would stay high forever when a holder dies; a slot has a TTL and
 * frees itself. Held slots are renewed every {@code ttl / 3} while held. No fencing token, as for
 * {@link RedisLock}.
 */
public final class RedisSemaphore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RedisSemaphore.class);

    /**
     * A permit. {@link #distributed()} is {@code false} for a permit granted by
     * {@link LockOutagePolicy#FAIL_OPEN} while Redis was down: only distributed permits are renewed and
     * released.
     */
    public record Permit(String key, String token, boolean distributed) {
    }

    private final StatefulRedisConnection<String, String> connection;
    private final boolean ownsConnection;
    private final String keyPrefix;
    private final String instanceId = UUID.randomUUID().toString();
    private final Map<Permit, ScheduledFuture<?>> held = new ConcurrentHashMap<>();
    private final ScheduledExecutorService renewer;

    public RedisSemaphore(RedisClient client) {
        this(client.connect(), true, "rsw:sem");
    }

    public RedisSemaphore(RedisClient client, String keyPrefix) {
        this(client.connect(), true, keyPrefix);
    }

    public RedisSemaphore(StatefulRedisConnection<String, String> connection, String keyPrefix) {
        this(connection, false, keyPrefix);
    }

    private RedisSemaphore(StatefulRedisConnection<String, String> connection, boolean owns, String keyPrefix) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.ownsConnection = owns;
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        this.renewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rsw-semaphore-renewer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Tries to take one of the {@code permits} slots of {@code name}, without waiting.
     *
     * @return the permit, or empty if every slot is taken (or Redis is down and the policy is
     *     {@link LockOutagePolicy#FAIL_CLOSED})
     */
    public Optional<Permit> tryAcquire(String name, int permits, Duration ttl, LockOutagePolicy policy) {
        Objects.requireNonNull(policy, "policy");
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
        if (ttl == null || ttl.toMillis() < 30) {
            throw new IllegalArgumentException("ttl must be at least 30 ms");
        }
        String token = instanceId + ":" + UUID.randomUUID();
        for (int slot = 0; slot < permits; slot++) {
            String key = slotKey(name, slot);
            String taken;
            try {
                taken = connection.sync().set(key, token, SetArgs.Builder.nx().px(ttl.toMillis()));
            } catch (RuntimeException e) {
                if (policy == LockOutagePolicy.FAIL_CLOSED) {
                    log.warn("Semaphore '{}' cannot be queried ({}): permit refused (fail-closed)", name, e.toString());
                    return Optional.empty();
                }
                log.warn("Semaphore '{}' cannot be queried ({}): permit granted (fail-open)", name, e.toString());
                return Optional.of(new Permit(null, null, false));
            }
            if ("OK".equals(taken)) {
                Permit permit = new Permit(key, token, true);
                long period = Math.max(10, ttl.toMillis() / 3);
                held.put(permit, renewer.scheduleWithFixedDelay(() -> renew(permit, ttl), period, period,
                        TimeUnit.MILLISECONDS));
                return Optional.of(permit);
            }
        }
        return Optional.empty();
    }

    /** Releases a permit. No-op for a fail-open or already released permit. */
    public void release(Permit permit) {
        if (permit == null || !permit.distributed()) {
            return;
        }
        ScheduledFuture<?> renewal = held.remove(permit);
        if (renewal != null) {
            renewal.cancel(false);
        }
        try {
            LuaScript.RELEASE_IF_MINE.run(connection.sync(), permit.key(), permit.token());
        } catch (RuntimeException e) {
            log.debug("Release of {} failed ({}); it will expire", permit.key(), e.toString());
        }
    }

    /** Slots currently taken in the cluster (diagnostics). */
    public long usedPermits(String name, int permits) {
        long used = 0;
        for (int slot = 0; slot < permits; slot++) {
            used += connection.sync().exists(slotKey(name, slot));
        }
        return used;
    }

    private void renew(Permit permit, Duration ttl) {
        try {
            if (LuaScript.RENEW_IF_MINE.run(connection.sync(), permit.key(), permit.token(),
                    Long.toString(ttl.toMillis())) != 1L) {
                log.warn("Semaphore slot {} expired while held: the cluster-wide limit may be exceeded", permit.key());
                ScheduledFuture<?> renewal = held.remove(permit);
                if (renewal != null) {
                    renewal.cancel(false);
                }
            }
        } catch (RuntimeException e) {
            log.debug("Renewal of {} failed: {}", permit.key(), e.toString());
        }
    }

    private String slotKey(String name, int slot) {
        return keyPrefix + ":" + name + ":" + slot;
    }

    @Override
    public void close() {
        renewer.shutdownNow();
        if (ownsConnection) {
            connection.close();
        }
    }
}
