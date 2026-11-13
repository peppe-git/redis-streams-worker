// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

/** Recognises Redis error replies by their prefix, anywhere in a cause chain. */
public final class RedisErrors {

    private RedisErrors() {
    }

    /** {@code NOGROUP}: the consumer group (or the stream) does not exist. */
    public static boolean isNoGroup(Throwable t) {
        return messageContains(t, "NOGROUP");
    }

    /** {@code BUSYGROUP}: the consumer group already exists. */
    public static boolean isBusyGroup(Throwable t) {
        return messageContains(t, "BUSYGROUP");
    }

    private static boolean messageContains(Throwable t, String token) {
        for (Throwable cur = t; cur != null; cur = cur.getCause() == cur ? null : cur.getCause()) {
            String msg = cur.getMessage();
            if (msg != null && msg.contains(token)) {
                return true;
            }
        }
        return false;
    }
}
