// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A message read from a lane.
 *
 * @param lane name of the lane it was read from
 * @param id Redis stream record id ({@code <ms>-<seq>}); unique within the lane only
 * @param fields the payload, as written by the producer (unmodifiable)
 * @param deliveryCount how many times the consumer group has delivered it, this delivery included
 */
public record QueueMessage(String lane, String id, Map<String, String> fields, long deliveryCount) {

    public QueueMessage {
        Objects.requireNonNull(lane, "lane");
        Objects.requireNonNull(id, "id");
        fields = fields == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /** The value of one field, or {@code null}. */
    public String field(String name) {
        return fields.get(name);
    }

    QueueMessage withDeliveryCount(long count) {
        return new QueueMessage(lane, id, fields, count);
    }
}
