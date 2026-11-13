// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.chaos;

import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * A worker in its own JVM, to be killed with SIGKILL by {@link WorkerKillChaosIT}. When it starts a
 * message it pushes the record id to {@code <prefix>:started}, then "works" for a minute.
 *
 * <p>Arguments: host, port, key prefix, consumer name.
 */
public final class ChaosWorkerMain {

    private ChaosWorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        RedisClient client = RedisClient.create(RedisURI.create(args[0], Integer.parseInt(args[1])));
        String prefix = args[2];
        QueueDefinition queue = QueueDefinition.builder(prefix).build();
        StatefulRedisConnection<String, String> signals = client.connect();
        RedisStreamsWorker worker = RedisStreamsWorker.builder(client, queue, (message, ctx) -> {
            signals.sync().rpush(prefix + ":started", message.id());
            Thread.sleep(60_000);
            return Outcome.ACK;
        }).options(RedisTestSupport.fastOptions().consumerName(args[3]).build()).build();
        worker.start();
        Thread.sleep(Long.MAX_VALUE);
    }
}
