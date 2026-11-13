// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.testing;

import io.github.peppe_git.redisstreamsworker.Lane;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.lettuce.core.Consumer;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Produces messages left pending by a consumer that "died" right after reading them (no lease). */
public final class Orphans {

    private Orphans() {
    }

    public static List<String> create(RedisCommands<String, String> redis, QueueDefinition q, String lane,
            String deadConsumer, int count) {
        Lane l = q.lane(lane);
        try {
            redis.xgroupCreate(XReadArgs.StreamOffset.from(l.streamKey(), "0"), q.group(),
                    XGroupCreateArgs.Builder.mkstream());
        } catch (RuntimeException busyGroup) {
            // already there
        }
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(redis.xadd(l.streamKey(), Map.of("n", Integer.toString(i))));
        }
        redis.xreadgroup(Consumer.from(q.group(), deadConsumer), XReadArgs.Builder.count(count),
                XReadArgs.StreamOffset.lastConsumed(l.streamKey()));
        return ids;
    }
}
