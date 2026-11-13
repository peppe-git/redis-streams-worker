// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/** What the worker does with a message after its handler returns. */
public enum Outcome {

    /**
     * The work is done (successfully, or its failure is recorded elsewhere): acknowledge the message,
     * if it still belongs to this consumer, and release its lease.
     */
    ACK,

    /**
     * Do not acknowledge; release the lease. The message stays in the pending entries list and is
     * delivered again by a reclaim once it has been idle for {@code reclaimAfter}. Each redelivery
     * counts towards {@code maxDeliveries}. Typical use: the work is owned by someone else right now, or
     * a transient failure.
     */
    LEAVE_PENDING,

    /**
     * The handler stopped because the worker is shutting down (it noticed
     * {@link MessageContext#shutdownRequested()} or was interrupted). Same effect as
     * {@link #LEAVE_PENDING}, but reported as an interruption rather than a failure, and other
     * messages waiting for the same work key are left for another consumer.
     */
    SHUTDOWN,

    /**
     * The message can never succeed (for example, a malformed payload): move it to the dead-letter
     * stream now instead of spending the remaining deliveries.
     */
    DEAD_LETTER
}
