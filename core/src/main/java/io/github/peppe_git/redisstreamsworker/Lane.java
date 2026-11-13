// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * One priority lane of a queue: a name used by producers and handlers, and the Redis stream that
 * stores its messages.
 *
 * <p>Lanes are declared in priority order on {@link QueueDefinition}. Most applications let the
 * definition derive the stream key ({@code <prefix>:<name>}); an explicit key is useful to adopt a
 * stream that already exists.
 *
 * @param name the lane name, made of letters, digits, {@code _}, {@code -} and {@code .}
 * @param streamKey the Redis key of the stream
 */
public record Lane(String name, String streamKey) {

    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.-]+");

    public Lane {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(streamKey, "streamKey");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Lane name must match " + NAME + ": '" + name + "'");
        }
        if (streamKey.isBlank()) {
            throw new IllegalArgumentException("Stream key of lane '" + name + "' is blank");
        }
    }
}
