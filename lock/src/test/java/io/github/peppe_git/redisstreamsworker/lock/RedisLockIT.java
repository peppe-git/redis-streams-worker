// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RedisLockIT extends RedisTestSupport {

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final LockListener recording = new LockListener() {
        @Override
        public void onCompleted(String name, boolean distributed) {
            events.add("completed:" + distributed);
        }

        @Override
        public void onSkipped(String name) {
            events.add("skipped");
        }

        @Override
        public void onOutage(String name, LockOutagePolicy policy, Throwable error) {
            events.add("outage:" + policy);
        }

        @Override
        public void onLost(String name, String interruptedWrite) {
            events.add("lost:" + interruptedWrite);
        }
    };

    @Test
    void onlyOneHolderAtATimeAndTheLockIsReleasedAfterwards() throws Exception {
        RedisLock a = closeLater(new RedisLock(client, prefix, recording));
        RedisLock b = closeLater(new RedisLock(client, prefix, recording));
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<Boolean> first = pool.submit(() -> a.runIfAcquired("job", Duration.ofSeconds(10),
                LockOutagePolicy.FAIL_CLOSED, () -> {
                    inside.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
        AtomicBoolean secondRan = new AtomicBoolean();
        assertThat(b.runIfAcquired("job", Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED,
                () -> secondRan.set(true))).isFalse();
        assertThat(secondRan).isFalse();
        release.countDown();
        assertThat(first.get(5, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(redis.exists(prefix + ":job")).isZero();
        assertThat(events).containsExactly("skipped", "completed:true");
    }

    @Test
    void theLockIsRenewedWhileTheTaskOutlivesItsTtl() {
        RedisLock a = closeLater(new RedisLock(client, prefix, recording));
        RedisLock b = closeLater(new RedisLock(client, prefix, recording));
        List<Boolean> othersGotIn = new CopyOnWriteArrayList<>();
        boolean done = a.runIfAcquired("long", Duration.ofMillis(300), LockOutagePolicy.FAIL_CLOSED, owner -> {
            for (int i = 0; i < 10; i++) {
                sleep(100); // 1 s in total, more than three TTLs
                othersGotIn.add(b.runIfAcquired("long", Duration.ofMillis(300), LockOutagePolicy.FAIL_CLOSED, () -> {
                }));
            }
            owner.requireHeld("final write");
        });
        assertThat(done).isTrue();
        assertThat(othersGotIn).containsOnly(false);
    }

    @Test
    void aHolderThatLostItsLockStopsBeforeTheConclusiveWriteAndDoesNotDeleteTheNewLock() {
        RedisLock a = closeLater(new RedisLock(client, prefix, recording));
        List<String> writes = new CopyOnWriteArrayList<>();
        boolean done = a.runIfAcquired("stolen", Duration.ofSeconds(10), LockOutagePolicy.FAIL_OPEN, owner -> {
            writes.add("first");
            redis.set(prefix + ":stolen", "someone-else"); // expired and re-acquired elsewhere
            owner.requireHeld("second write");
            writes.add("second");
        });
        assertThat(done).isFalse();
        assertThat(writes).containsExactly("first");
        assertThat(events).containsExactly("lost:second write");
        assertThat(redis.get(prefix + ":stolen")).isEqualTo("someone-else"); // compare-and-delete spared it
    }

    @Test
    void theOutagePolicyDecidesWhenRedisCannotBeAsked() {
        StatefulRedisConnection<String, String> dead = client.connect();
        dead.close();
        RedisLock lock = closeLater(new RedisLock(dead, prefix, recording));
        AtomicBoolean ran = new AtomicBoolean();
        assertThat(lock.runIfAcquired("money", Duration.ofSeconds(5), LockOutagePolicy.FAIL_CLOSED,
                () -> ran.set(true))).isFalse();
        assertThat(ran).isFalse();
        assertThat(lock.runIfAcquired("cleanup", Duration.ofSeconds(5), LockOutagePolicy.FAIL_OPEN,
                () -> ran.set(true))).isTrue();
        assertThat(ran).isTrue();
        assertThat(events).containsExactly("outage:FAIL_CLOSED", "outage:FAIL_OPEN", "completed:false");
    }

    @Test
    void thePolicyIsMandatoryAndTheTaskExceptionPropagatesAfterRelease() {
        RedisLock lock = closeLater(new RedisLock(client, prefix, recording));
        assertThatThrownBy(() -> lock.runIfAcquired("x", Duration.ofSeconds(5), null, () -> {
        })).isInstanceOf(NullPointerException.class).hasMessageContaining("policy");
        assertThatThrownBy(() -> lock.runIfAcquired("x", Duration.ofSeconds(5), LockOutagePolicy.FAIL_OPEN, () -> {
            throw new IllegalStateException("task failed");
        })).hasMessage("task failed");
        assertThat(redis.exists(prefix + ":x")).isZero();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
