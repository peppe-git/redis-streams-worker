// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

/**
 * Proof of ownership handed to a task that acquired a lock, so that it can check, right before each
 * conclusive write, that it still holds the lock.
 *
 * <p>Renewal makes losing the lock rare; this check makes it harmless, and it is the only one of the
 * two that still works when renewal could not reach Redis. It is <b>not</b> a fencing token: between the
 * check and the write the lock can still expire. Writes that must never happen twice need an idempotency
 * key or a fencing check in their own storage.
 */
@FunctionalInterface
public interface LockOwnership {

    /**
     * {@code true} if the lock still belongs to this holder. The Redis implementation extends the TTL in
     * the same atomic step ("am I still the holder?" and "stay the holder a while longer").
     */
    boolean stillHeld();

    /** Like {@link #stillHeld()}, but throws {@link LockLostException} naming the write that did not start. */
    default void requireHeld(String what) {
        if (!stillHeld()) {
            throw new LockLostException(what);
        }
    }

    /**
     * Ownership taken for granted: for direct invocations of a task (tests, an admin endpoint) and for the
     * fail-open path where no lock was ever taken.
     */
    static LockOwnership assumed() {
        return () -> true;
    }
}
