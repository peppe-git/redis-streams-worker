// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

/** Where a consumer group starts when the worker creates it (at startup or after {@code NOGROUP}). */
public enum GroupStart {

    /**
     * From the first entry still in the stream ({@code 0}). Nothing produced after a data loss is
     * skipped; on a stream that already holds history, that history is delivered.
     */
    EARLIEST,

    /** From the next entry ({@code $}). Entries already in the stream are never delivered. */
    LATEST
}
