// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/**
 * Last check before a worker reclaims a pending message from another consumer, based on the
 * <em>work</em> rather than on the message.
 *
 * <p>The lease protects a message while its owner heartbeats. It cannot protect the work when the
 * lease is gone but the owner is not (a long garbage-collection pause, Redis unreachable for longer than
 * the lease TTL), nor when the work is owned through another message. If your system of record keeps
 * a claim for the work (for example a row with an owner and an expiry in Postgres), answer
 * {@code false} while that claim is held by someone else and still valid.
 *
 * <p>The guard runs before {@code XCLAIM}: answering {@code false} leaves the message pending with its
 * delivery count unchanged, and the next reclaim pass asks again. A guard that throws is treated as
 * {@code true} (fail-open), so that an outage of your database does not stop recovery for every
 * message.
 */
@FunctionalInterface
public interface ReclaimGuard {

    /** Allows every reclaim. */
    ReclaimGuard ALLOW_ALL = candidate -> true;

    boolean allowReclaim(ReclaimCandidate candidate) throws Exception;
}
