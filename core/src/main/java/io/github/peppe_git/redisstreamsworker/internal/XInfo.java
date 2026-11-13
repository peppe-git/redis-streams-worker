// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses the raw replies of {@code XINFO GROUPS} and {@code XINFO CONSUMERS}, which Lettuce returns as
 * nested lists of alternating names and values.
 */
public final class XInfo {

    private XInfo() {
    }

    /** One group of {@code XINFO GROUPS}. */
    public record Group(String name, long consumers, long pending, String lastDeliveredId, Long lag) {
    }

    /** One consumer of {@code XINFO CONSUMERS}. */
    public record Consumer(String name, long pending, long idleMillis) {
    }

    public static List<Group> groups(List<Object> raw) {
        List<Group> out = new ArrayList<>();
        for (Map<String, Object> m : entries(raw)) {
            out.add(new Group(str(m.get("name")), num(m.get("consumers")), num(m.get("pending")),
                    str(m.get("last-delivered-id")), m.get("lag") instanceof Number n ? n.longValue() : null));
        }
        return out;
    }

    public static List<Consumer> consumers(List<Object> raw) {
        List<Consumer> out = new ArrayList<>();
        for (Map<String, Object> m : entries(raw)) {
            out.add(new Consumer(str(m.get("name")), num(m.get("pending")), num(m.get("idle"))));
        }
        return out;
    }

    private static List<Map<String, Object>> entries(List<Object> raw) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        for (Object item : raw) {
            if (item instanceof List<?> fields) {
                Map<String, Object> m = new HashMap<>();
                for (int i = 0; i + 1 < fields.size(); i += 2) {
                    m.put(String.valueOf(fields.get(i)), fields.get(i + 1));
                }
                out.add(m);
            } else if (item instanceof Map<?, ?> map) {
                Map<String, Object> m = new HashMap<>();
                map.forEach((k, v) -> m.put(String.valueOf(k), v));
                out.add(m);
            }
        }
        return out;
    }

    private static String str(Object o) {
        return o == null ? null : o.toString();
    }

    private static long num(Object o) {
        if (o instanceof Number n) {
            return n.longValue();
        }
        return o == null ? 0 : Long.parseLong(o.toString());
    }
}
