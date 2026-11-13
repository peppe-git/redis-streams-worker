// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

/**
 * Observes {@link RedisLock} rounds, for example to feed a "periodic task is alive" health check.
 * Every method has an empty default; exceptions are logged and ignored.
 */
public interface LockListener {

    LockListener NONE = new LockListener() {
    };

    /** The round ran and completed (with or without holding the lock, see {@code distributed}). */
    default void onCompleted(String name, boolean distributed) {
    }

    /** The lock was held by another instance: the round was served elsewhere. */
    default void onSkipped(String name) {
    }

    /** Redis could not be asked; {@code policy} decided whether the round ran. */
    default void onOutage(String name, LockOutagePolicy policy, Throwable error) {
    }

    /** The task found out it had lost the lock and stopped before a conclusive write. */
    default void onLost(String name, String interruptedWrite) {
    }

    /** The task threw. */
    default void onFailed(String name, Throwable error) {
    }
}
