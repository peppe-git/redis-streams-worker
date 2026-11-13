// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * The Lua scripts that make multi-step operations atomic (see ADR 0005). Each one receives every key it
 * touches in {@code KEYS}. Not part of the public API.
 */
public final class Scripts {

    private Scripts() {
    }

    /**
     * Claims a pending entry only if nobody holds its lease, then takes the lease.
     * KEYS: stream, lease. ARGV: group, consumer, min idle ms, id, lease ttl ms.
     * Returns {code, deliveries}: 0 = lease held by someone, 1 = not claimed (idle reset by another
     * claimer, or entry no longer pending), 2 = claimed, 3 = the record was deleted (pending entry
     * acknowledged).
     */
    static final Script CLAIM = new Script("""
            if redis.call('EXISTS', KEYS[2]) == 1 then return {0, 0} end
            local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4])
            if #claimed == 0 then return {1, 0} end
            if not claimed[1] then
              redis.call('XACK', KEYS[1], ARGV[1], ARGV[4])
              return {3, 0}
            end
            redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[5])
            local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[4], ARGV[4], 1)
            local deliveries = 0
            if #p > 0 then deliveries = p[1][4] end
            return {2, deliveries}
            """);

    /**
     * Compare-and-extend of a lease. KEYS: lease. ARGV: owner, ttl ms.
     * Returns 1 = renewed, 2 = was missing and has been re-acquired, 0 = held by another owner.
     */
    static final Script RENEW_LEASE = new Script("""
            local v = redis.call('GET', KEYS[1])
            if v == ARGV[1] then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 1
            end
            if not v then
              redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
              return 2
            end
            return 0
            """);

    /** Compare-and-delete of a lease. KEYS: lease. ARGV: owner. Returns 1 if deleted. */
    static final Script RELEASE_LEASE = new Script("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
            return 0
            """);

    /**
     * Acknowledges only if the pending entry still belongs to the consumer, and releases its lease.
     * KEYS: stream, lease. ARGV: group, id, consumer.
     * Returns 1 = acknowledged, 0 = owned by another consumer (nothing done), -1 = not pending.
     */
    static final Script ACK_IF_OWNER = new Script("""
            local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
            local result = -1
            if #p > 0 then
              if p[1][2] ~= ARGV[3] then return 0 end
              redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
              result = 1
            end
            if redis.call('GET', KEYS[2]) == ARGV[3] then redis.call('DEL', KEYS[2]) end
            return result
            """);

    /**
     * Moves an entry owned by the consumer to the dead-letter stream, acknowledges it and releases its
     * lease, atomically. KEYS: stream, lease, dead-letter stream. ARGV: group, id, consumer, max len,
     * then field/value pairs. Returns the new dead-letter id, "0" = owned by another consumer, "-1" = not
     * pending.
     */
    static final Script DEAD_LETTER = new Script("""
            local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
            if #p == 0 then
              if redis.call('GET', KEYS[2]) == ARGV[3] then redis.call('DEL', KEYS[2]) end
              return '-1'
            end
            if p[1][2] ~= ARGV[3] then return '0' end
            local args = {'XADD', KEYS[3]}
            if tonumber(ARGV[4]) > 0 then
              table.insert(args, 'MAXLEN'); table.insert(args, '~'); table.insert(args, ARGV[4])
            end
            table.insert(args, '*')
            for i = 5, #ARGV do table.insert(args, ARGV[i]) end
            local id = redis.call(unpack(args))
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            if redis.call('GET', KEYS[2]) == ARGV[3] then redis.call('DEL', KEYS[2]) end
            return id
            """);

    /**
     * Re-enqueues a dead letter on its lane and deletes it from the dead-letter stream, once.
     * KEYS: dead-letter stream, target stream. ARGV: dead-letter id, then field/value pairs.
     * Returns the new id, or nil if the dead letter no longer exists.
     */
    static final Script REPLAY = new Script("""
            local e = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1])
            if #e == 0 then return false end
            local args = {'XADD', KEYS[2], '*'}
            for i = 2, #ARGV do table.insert(args, ARGV[i]) end
            local id = redis.call(unpack(args))
            redis.call('XDEL', KEYS[1], ARGV[1])
            return id
            """);

    /** Result of {@link #claim}. */
    public record ClaimResult(int code, long deliveries) {

        public static final int LEASE_HELD = 0;
        public static final int NOT_CLAIMED = 1;
        public static final int CLAIMED = 2;
        public static final int VANISHED = 3;
    }

    public static ClaimResult claim(RedisCommands<String, String> redis, String stream, String lease,
            String group, String consumer, long minIdleMs, String id, long leaseTtlMs) {
        List<Object> reply = CLAIM.run(redis, ScriptOutputType.MULTI, keys(stream, lease),
                group, consumer, Long.toString(minIdleMs), id, Long.toString(leaseTtlMs));
        return new ClaimResult(((Number) reply.get(0)).intValue(), ((Number) reply.get(1)).longValue());
    }

    public static long renewLease(RedisCommands<String, String> redis, String lease, String owner, long ttlMs) {
        Long r = RENEW_LEASE.run(redis, ScriptOutputType.INTEGER, keys(lease), owner, Long.toString(ttlMs));
        return r == null ? 0 : r;
    }

    public static boolean releaseLease(RedisCommands<String, String> redis, String lease, String owner) {
        Long r = RELEASE_LEASE.run(redis, ScriptOutputType.INTEGER, keys(lease), owner);
        return r != null && r == 1L;
    }

    public static long ackIfOwner(RedisCommands<String, String> redis, String stream, String lease, String group,
            String id, String consumer) {
        Long r = ACK_IF_OWNER.run(redis, ScriptOutputType.INTEGER, keys(stream, lease), group, id, consumer);
        return r == null ? -1 : r;
    }

    /** Returns the dead-letter id, {@code "0"} (owned by another consumer) or {@code "-1"} (not pending). */
    public static String deadLetter(RedisCommands<String, String> redis, String stream, String lease, String dlq,
            String group, String id, String consumer, long maxLen, Map<String, String> fields) {
        List<String> args = new ArrayList<>(4 + fields.size() * 2);
        args.add(group);
        args.add(id);
        args.add(consumer);
        args.add(Long.toString(maxLen));
        fields.forEach((k, v) -> {
            args.add(k);
            args.add(v);
        });
        return DEAD_LETTER.run(redis, ScriptOutputType.VALUE, keys(stream, lease, dlq), args.toArray(String[]::new));
    }

    /** Returns the new id, or {@code null} if the dead letter does not exist (any more). */
    public static String replay(RedisCommands<String, String> redis, String dlq, String target, String dlqId,
            Map<String, String> fields) {
        List<String> args = new ArrayList<>(1 + fields.size() * 2);
        args.add(dlqId);
        fields.forEach((k, v) -> {
            args.add(k);
            args.add(v);
        });
        return REPLAY.run(redis, ScriptOutputType.VALUE, keys(dlq, target), args.toArray(String[]::new));
    }

    private static String[] keys(String... keys) {
        return keys;
    }

    /** A script with its SHA-1, run with {@code EVALSHA} and re-sent with {@code EVAL} on {@code NOSCRIPT}. */
    static final class Script {

        private final String source;
        private final String sha;

        Script(String source) {
            this.source = source;
            this.sha = sha1(source);
        }

        <T> T run(RedisCommands<String, String> redis, ScriptOutputType type, String[] keys, String... args) {
            try {
                return redis.evalsha(sha, type, keys, args);
            } catch (RedisNoScriptException e) {
                // Script cache empty (Redis restarted, SCRIPT FLUSH): EVAL loads it again.
                return redis.eval(source, type, keys, args);
            }
        }

        private static String sha1(String text) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                return HexFormat.of().formatHex(md.digest(text.getBytes(StandardCharsets.UTF_8)));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
