// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/**
 * Identifies the unit of work a message refers to, so that two messages about the same work never run
 * at the same time on one worker instance.
 *
 * <p>When a message arrives while another message with the same work key is running on this instance,
 * it is deferred: it keeps its lease (no other instance takes it) and starts as soon as the first one
 * finishes. If a second duplicate arrives meanwhile it is left pending for a later reclaim. Across
 * instances, use a {@link ReclaimGuard}.
 */
@FunctionalInterface
public interface WorkKeyExtractor {

    /** The work key, or {@code null} if the message needs no exclusivity. */
    String workKey(QueueMessage message);

    /** Uses the value of one payload field as the work key. */
    static WorkKeyExtractor field(String fieldName) {
        return message -> message.field(fieldName);
    }
}
