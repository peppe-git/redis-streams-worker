// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

/**
 * Thrown by {@link LockOwnership#requireHeld(String)} when the task finds out that it no longer holds
 * its lock: the key expired (or was deleted) while the task was still running, and another instance may
 * have acquired it. {@link RedisLock} catches it and reports an interrupted round.
 */
public class LockLostException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public LockLostException(String message) {
        super(message);
    }
}
