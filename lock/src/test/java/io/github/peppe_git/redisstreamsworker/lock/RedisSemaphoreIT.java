// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.lock;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.peppe_git.redisstreamsworker.lock.RedisSemaphore.Permit;
import io.github.peppe_git.redisstreamsworker.testing.RedisTestSupport;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RedisSemaphoreIT extends RedisTestSupport {

    @Test
    void grantsAtMostNPermitsAcrossInstances() {
        RedisSemaphore one = closeLater(new RedisSemaphore(client, prefix));
        RedisSemaphore two = closeLater(new RedisSemaphore(client, prefix));
        Optional<Permit> a = one.tryAcquire("render", 2, Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED);
        Optional<Permit> b = two.tryAcquire("render", 2, Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED);
        Optional<Permit> c = two.tryAcquire("render", 2, Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED);
        assertThat(a).isPresent();
        assertThat(b).isPresent();
        assertThat(c).isEmpty();
        assertThat(one.usedPermits("render", 2)).isEqualTo(2);
        one.release(a.get());
        assertThat(two.tryAcquire("render", 2, Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED)).isPresent();
    }

    @Test
    void heldPermitsAreRenewedBeyondTheirTtl() throws Exception {
        RedisSemaphore sem = closeLater(new RedisSemaphore(client, prefix));
        Permit p = sem.tryAcquire("slow", 1, Duration.ofMillis(300), LockOutagePolicy.FAIL_CLOSED).orElseThrow();
        Thread.sleep(1_000);
        assertThat(sem.usedPermits("slow", 1)).isEqualTo(1);
        assertThat(sem.tryAcquire("slow", 1, Duration.ofMillis(300), LockOutagePolicy.FAIL_CLOSED)).isEmpty();
        sem.release(p);
        assertThat(sem.usedPermits("slow", 1)).isZero();
    }

    @Test
    void releasingAnExpiredPermitDoesNotFreeTheSlotOfItsNewHolder() {
        RedisSemaphore sem = closeLater(new RedisSemaphore(client, prefix));
        Permit p = sem.tryAcquire("x", 1, Duration.ofSeconds(10), LockOutagePolicy.FAIL_CLOSED).orElseThrow();
        redis.set(p.key(), "new-holder");
        sem.release(p);
        assertThat(redis.get(p.key())).isEqualTo("new-holder");
    }

    @Test
    void outagePolicyDecidesWhetherAPermitIsGranted() {
        StatefulRedisConnection<String, String> dead = client.connect();
        dead.close();
        RedisSemaphore sem = closeLater(new RedisSemaphore(dead, prefix));
        assertThat(sem.tryAcquire("x", 1, Duration.ofSeconds(1), LockOutagePolicy.FAIL_CLOSED)).isEmpty();
        Optional<Permit> open = sem.tryAcquire("x", 1, Duration.ofSeconds(1), LockOutagePolicy.FAIL_OPEN);
        assertThat(open).isPresent();
        assertThat(open.get().distributed()).isFalse();
        sem.release(open.get()); // no-op
    }
}
