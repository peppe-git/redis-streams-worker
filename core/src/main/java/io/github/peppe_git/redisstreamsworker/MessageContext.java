// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/** Per-delivery information available to a {@link MessageHandler}. */
public interface MessageContext {

    /**
     * {@code false} once the heartbeat has found the lease of this message held by another consumer:
     * the message has been reclaimed elsewhere and is probably running there too. A handler with a
     * conclusive write (a payment, an e-mail) should check this right before it, and give up if the
     * lease is lost. The worker will not acknowledge a message that another consumer owns.
     */
    boolean leaseHeld();

    /** {@code true} once the worker has started to stop: finish quickly or return {@link Outcome#SHUTDOWN}. */
    boolean shutdownRequested();

    /** Name of the consumer processing this delivery. */
    String consumerName();

    /** Reason recorded in the dead-letter entry if the handler returns {@link Outcome#DEAD_LETTER}. */
    void deadLetterReason(String reason);
}
