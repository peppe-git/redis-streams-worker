// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** A Lua script run with {@code EVALSHA}, re-sent with {@code EVAL} after {@code NOSCRIPT}. */
final class LuaScript {

    /** {@code DEL key} only if it still holds the expected token. Returns 1 if deleted. */
    static final LuaScript RELEASE_IF_MINE = new LuaScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end");

    /** {@code PEXPIRE key} only if it still holds the expected token. Returns 1 if extended. */
    static final LuaScript RENEW_IF_MINE = new LuaScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) "
                    + "else return 0 end");

    private final String source;
    private final String sha;

    private LuaScript(String source) {
        this.source = source;
        try {
            this.sha = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    long run(RedisCommands<String, String> redis, String key, String... args) {
        Long r;
        try {
            r = redis.evalsha(sha, ScriptOutputType.INTEGER, new String[] {key}, args);
        } catch (RedisNoScriptException e) {
            r = redis.eval(source, ScriptOutputType.INTEGER, new String[] {key}, args);
        }
        return r == null ? 0 : r;
    }
}
