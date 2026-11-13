// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

/** Arithmetic on Redis stream ids ({@code <ms>-<seq>}). */
public final class StreamIds {

    private StreamIds() {
    }

    public static int compare(String a, String b) {
        long[] x = parse(a);
        long[] y = parse(b);
        int c = Long.compareUnsigned(x[0], y[0]);
        return c != 0 ? c : Long.compareUnsigned(x[1], y[1]);
    }

    /** The smallest id strictly greater than {@code id}. */
    public static String next(String id) {
        long[] p = parse(id);
        if (p[1] == -1L) {
            return Long.toUnsignedString(p[0] + 1) + "-0";
        }
        return Long.toUnsignedString(p[0]) + "-" + Long.toUnsignedString(p[1] + 1);
    }

    public static String min(String a, String b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return compare(a, b) <= 0 ? a : b;
    }

    static long[] parse(String id) {
        int dash = id.indexOf('-');
        if (dash < 0) {
            return new long[] {Long.parseUnsignedLong(id), 0};
        }
        return new long[] {Long.parseUnsignedLong(id.substring(0, dash)),
                Long.parseUnsignedLong(id.substring(dash + 1))};
    }
}
