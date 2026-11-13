// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.autoconfigure;

import io.github.peppe_git.redisstreamsworker.RedisStreamsWorker;
import org.springframework.context.SmartLifecycle;

/**
 * Starts the worker with the application context and stops it (draining running handlers) before the
 * Redis connection factory stops. Keep {@code redis-streams-worker.worker.drain-timeout} below
 * {@code spring.lifecycle.timeout-per-shutdown-phase}.
 */
public class RedisStreamsWorkerLifecycle implements SmartLifecycle {

    private final RedisStreamsWorker worker;
    private final int phase;

    public RedisStreamsWorkerLifecycle(RedisStreamsWorker worker, int phase) {
        this.worker = worker;
        this.phase = phase;
    }

    @Override
    public void start() {
        worker.start();
    }

    @Override
    public void stop() {
        worker.stop();
    }

    @Override
    public boolean isRunning() {
        return worker.isRunning();
    }

    @Override
    public int getPhase() {
        return phase;
    }

    public RedisStreamsWorker worker() {
        return worker;
    }
}
