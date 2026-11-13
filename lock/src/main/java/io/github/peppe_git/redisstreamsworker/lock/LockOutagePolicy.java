// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

/**
 * What a task does when Redis cannot be asked whether the lock is free (Redis down, or at
 * {@code maxmemory} with {@code noeviction}, where writes fail while reads still work).
 *
 * <p>There is deliberately no default: whoever writes a periodic task must answer "if Redis is down,
 * does this round run or not?". An inherited default would let silence answer, and silence tends to
 * answer "run it anyway" even for tasks that move money.
 */
public enum LockOutagePolicy {

    /**
     * Run the task anyway. For tasks that are harmless if repeated (cleanups, idempotent reconcilers):
     * the accepted risk is that two instances run the same round together; the avoided risk is that an
     * outage of Redis stops the task forever.
     */
    FAIL_OPEN,

    /**
     * Skip the round and try again at the next tick. For tasks where running twice costs more than
     * running late (payments, invoices, notifications that are not deduplicated). Not a replacement for
     * idempotency: a second line of defence.
     */
    FAIL_CLOSED
}
