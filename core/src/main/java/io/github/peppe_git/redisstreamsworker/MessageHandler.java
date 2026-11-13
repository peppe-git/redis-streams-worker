// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/**
 * Application code that processes one message.
 *
 * <p>The worker guarantees at-least-once delivery: the same message can reach this method more than
 * once (after a crash, a lease lost under a long pause, or a {@link Outcome#LEAVE_PENDING}), possibly
 * on another instance. Make the work idempotent, or protect it with a claim in your system of record
 * and a {@link ReclaimGuard}.
 *
 * <p>An exception thrown by the handler is logged and treated as {@link Outcome#LEAVE_PENDING}: the
 * message will be delivered again, and moved to the dead-letter stream after
 * {@code maxDeliveries} deliveries. Return {@link Outcome#ACK} explicitly if your code has already
 * recorded the failure.
 */
@FunctionalInterface
public interface MessageHandler {

    Outcome handle(QueueMessage message, MessageContext context) throws Exception;
}
