// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.time.Duration;

/**
 * A pending message that a worker is about to reclaim from another consumer.
 *
 * @param message the message (its {@code deliveryCount} is the count <em>before</em> this reclaim)
 * @param previousConsumer the consumer that owns it in the pending entries list
 * @param idle time since its last delivery
 */
public record ReclaimCandidate(QueueMessage message, String previousConsumer, Duration idle) {
}
