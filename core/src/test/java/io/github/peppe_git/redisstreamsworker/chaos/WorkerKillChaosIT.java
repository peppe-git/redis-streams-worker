// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.peppe_git.redisstreamsworker.Outcome;
import io.github.peppe_git.redisstreamsworker.QueueDefinition;
import io.github.peppe_git.redisstreamsworker.QueueInspector;
import io.github.peppe_git.redisstreamsworker.QueueMessage;
import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import io.github.peppe_git.redisstreamsworker.StreamProducer;
import io.github.peppe_git.redisstreamsworker.testing.RedisContainers;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

/** SIGKILL of a worker JVM in the middle of a message: another worker finishes it, once. */
class WorkerKillChaosIT extends RedisTestSupport {

    @Test
    void aMessageInterruptedByKill9IsReclaimedAndCompletedByAnotherWorker() throws Exception {
        QueueDefinition q = QueueDefinition.builder(prefix).build();
        StreamProducer producer = closeLater(new StreamProducer(client, q));
        String id = producer.send(Map.of("order", "42"));

        Process child = startChildWorker("doomed");
        try {
            await().atMost(Duration.ofSeconds(60)).until(() -> redis.lrange(prefix + ":started", 0, -1).contains(id));
            assertThat(redis.get(q.leaseKey(q.lane("default"), id))).isEqualTo("doomed");
        } finally {
            child.destroyForcibly(); // SIGKILL: no shutdown hook, no ack, no lease release
            assertThat(child.waitFor(30, TimeUnit.SECONDS)).isTrue();
        }
        long killedAt = System.currentTimeMillis();

        List<QueueMessage> handled = new CopyOnWriteArrayList<>();
        List<Long> handledAfterMs = new CopyOnWriteArrayList<>();
        started(RedisStreamsWorker.builder(client, q, (m, ctx) -> {
            handled.add(m);
            handledAfterMs.add(System.currentTimeMillis() - killedAt);
            return Outcome.ACK;
        }).options(fastOptions().consumerName("survivor").build()).build());

        await().atMost(Duration.ofSeconds(20)).until(() -> handled.size() == 1);
        assertThat(handled.get(0).id()).isEqualTo(id);
        assertThat(handled.get(0).deliveryCount()).isEqualTo(2);
        await().atMost(Duration.ofSeconds(5)).until(() -> pendingCount(q.lane("default").streamKey(), q.group()) == 0);
        Thread.sleep(1_500);
        assertThat(handled).hasSize(1);
        assertThat(closeLater(new QueueInspector(client, q)).deadLetterCount()).isZero();
    }

    private Process startChildWorker(String consumer) throws Exception {
        GenericContainer<?> redisContainer = RedisContainers.shared();
        String java = ProcessHandle.current().info().command()
                .orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Path log = Files.createTempFile("rsw-chaos-child", ".log");
        return new ProcessBuilder(java, "-cp", classpath, ChaosWorkerMain.class.getName(),
                redisContainer.getHost(), Integer.toString(redisContainer.getMappedPort(6379)), prefix, consumer)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }
}
