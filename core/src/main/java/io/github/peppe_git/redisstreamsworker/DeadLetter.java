// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An entry of the dead-letter stream: the original payload plus metadata stored in fields with the
 * reserved prefix {@value #FIELD_PREFIX}.
 *
 * @param id id of the entry in the dead-letter stream
 * @param lane lane the message came from
 * @param sourceId id the message had on its lane
 * @param reason why it was dead-lettered ({@value #REASON_MAX_DELIVERIES}, {@value #REASON_HANDLER}, or
 *     the reason given by the handler)
 * @param deliveries deliveries counted when it was dead-lettered
 * @param consumer consumer that dead-lettered it
 * @param deadLetteredAt when it was dead-lettered
 * @param payload the original fields, without metadata
 */
public record DeadLetter(String id, String lane, String sourceId, String reason, long deliveries, String consumer,
        Instant deadLetteredAt, Map<String, String> payload) {

    /** Prefix of the metadata fields; producers cannot use it. */
    public static final String FIELD_PREFIX = "x-dlq-";

    /** The message exceeded {@code maxDeliveries}. */
    public static final String REASON_MAX_DELIVERIES = "max-deliveries-exceeded";

    /** The handler returned {@link Outcome#DEAD_LETTER} without a reason. */
    public static final String REASON_HANDLER = "handler-dead-letter";

    public DeadLetter {
        payload = payload == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }

    static DeadLetter fromFields(String id, Map<String, String> fields) {
        Map<String, String> payload = new LinkedHashMap<>();
        fields.forEach((k, v) -> {
            if (!k.startsWith(FIELD_PREFIX)) {
                payload.put(k, v);
            }
        });
        String at = fields.get(FIELD_PREFIX + "at");
        String deliveries = fields.get(FIELD_PREFIX + "deliveries");
        return new DeadLetter(id, fields.get(FIELD_PREFIX + "lane"), fields.get(FIELD_PREFIX + "source-id"),
                fields.get(FIELD_PREFIX + "reason"), deliveries == null ? 0 : Long.parseLong(deliveries),
                fields.get(FIELD_PREFIX + "consumer"), at == null ? null : Instant.ofEpochMilli(Long.parseLong(at)),
                payload);
    }
}
