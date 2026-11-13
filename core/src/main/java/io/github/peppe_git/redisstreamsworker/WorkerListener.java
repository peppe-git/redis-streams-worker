// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker;

import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes a worker: metrics, alerts, and side effects that must follow the lifecycle of a message
 * (for example, renewing a claim in your database together with the lease).
 *
 * <p>Every method has an empty default. Exceptions thrown by a listener are logged and never affect the
 * worker. Methods are called from the poll thread, the heartbeat thread or handler threads: keep them
 * fast and thread-safe.
 */
public interface WorkerListener {

    /** No-op listener. */
    WorkerListener NONE = new WorkerListener() {
    };

    default void onStarted(String consumerName) {
    }

    default void onStopped(String consumerName) {
    }

    /** The handler is about to run. */
    default void onMessageStarted(QueueMessage message) {
    }

    /**
     * The handler returned (or threw: {@code error} is then not {@code null} and the outcome is
     * {@link Outcome#LEAVE_PENDING}, or {@link Outcome#SHUTDOWN} if the worker was stopping).
     */
    default void onMessageCompleted(QueueMessage message, Outcome outcome, Duration duration, Throwable error) {
    }

    /** A pending message was taken over from {@code previousConsumer}. */
    default void onReclaimed(QueueMessage message, String previousConsumer) {
    }

    /** The message was moved to the dead-letter stream. */
    default void onDeadLettered(QueueMessage message, String reason) {
    }

    /** The message waits because another message with the same work key is running here. */
    default void onDeferred(QueueMessage message, String workKey) {
    }

    /**
     * Heartbeat of a running message. {@code held} is {@code false} when the lease now belongs to
     * another consumer.
     */
    default void onLeaseRenewed(QueueMessage message, boolean held) {
    }

    /** The heartbeat found the lease of a running message owned by another consumer. */
    default void onLeaseLost(QueueMessage message) {
    }

    /**
     * The acknowledgement was refused: the message belongs to another consumer
     * ({@code reason = "owned-by-another-consumer"}) or is no longer pending
     * ({@code reason = "not-pending"}, for example after a data loss in Redis).
     */
    default void onAckRejected(QueueMessage message, String reason) {
    }

    /** The acknowledgement could not reach Redis; it will be retried while this worker runs. */
    default void onAckFailed(QueueMessage message, Throwable error) {
    }

    /** A pending entry pointed to a record that no longer exists in the stream; it was acknowledged. */
    default void onMessageVanished(String lane, String id) {
    }

    /** The consumer group was created on a lane ({@code recreated} after a {@code NOGROUP}). */
    default void onGroupCreated(String lane, boolean recreated) {
    }

    /** Redis stopped answering the poll loop (reported once per outage). */
    default void onRedisUnavailable(Throwable error) {
    }

    /** Redis answers again after {@link #onRedisUnavailable(Throwable)}. */
    default void onRedisRecovered() {
    }

    /** Calls every listener in order, isolating their exceptions. */
    static WorkerListener composite(List<? extends WorkerListener> listeners) {
        return new CompositeWorkerListener(List.copyOf(listeners));
    }

    /** Wraps a listener so that its exceptions are logged instead of propagated. */
    static WorkerListener safe(WorkerListener listener) {
        return listener instanceof CompositeWorkerListener ? listener
                : new CompositeWorkerListener(List.of(listener));
    }
}

/** Fan-out listener that logs and swallows exceptions of its delegates. */
final class CompositeWorkerListener implements WorkerListener {

    private static final Logger log = LoggerFactory.getLogger(WorkerListener.class);

    private final List<WorkerListener> delegates;

    CompositeWorkerListener(List<WorkerListener> delegates) {
        this.delegates = delegates;
    }

    private void each(String event, java.util.function.Consumer<WorkerListener> call) {
        for (WorkerListener l : delegates) {
            try {
                call.accept(l);
            } catch (RuntimeException e) {
                log.warn("WorkerListener {} failed on {}: {}", l.getClass().getName(), event, e.toString());
            }
        }
    }

    @Override
    public void onStarted(String consumerName) {
        each("onStarted", l -> l.onStarted(consumerName));
    }

    @Override
    public void onStopped(String consumerName) {
        each("onStopped", l -> l.onStopped(consumerName));
    }

    @Override
    public void onMessageStarted(QueueMessage message) {
        each("onMessageStarted", l -> l.onMessageStarted(message));
    }

    @Override
    public void onMessageCompleted(QueueMessage message, Outcome outcome, Duration duration, Throwable error) {
        each("onMessageCompleted", l -> l.onMessageCompleted(message, outcome, duration, error));
    }

    @Override
    public void onReclaimed(QueueMessage message, String previousConsumer) {
        each("onReclaimed", l -> l.onReclaimed(message, previousConsumer));
    }

    @Override
    public void onDeadLettered(QueueMessage message, String reason) {
        each("onDeadLettered", l -> l.onDeadLettered(message, reason));
    }

    @Override
    public void onDeferred(QueueMessage message, String workKey) {
        each("onDeferred", l -> l.onDeferred(message, workKey));
    }

    @Override
    public void onLeaseRenewed(QueueMessage message, boolean held) {
        each("onLeaseRenewed", l -> l.onLeaseRenewed(message, held));
    }

    @Override
    public void onLeaseLost(QueueMessage message) {
        each("onLeaseLost", l -> l.onLeaseLost(message));
    }

    @Override
    public void onAckRejected(QueueMessage message, String reason) {
        each("onAckRejected", l -> l.onAckRejected(message, reason));
    }

    @Override
    public void onAckFailed(QueueMessage message, Throwable error) {
        each("onAckFailed", l -> l.onAckFailed(message, error));
    }

    @Override
    public void onMessageVanished(String lane, String id) {
        each("onMessageVanished", l -> l.onMessageVanished(lane, id));
    }

    @Override
    public void onGroupCreated(String lane, boolean recreated) {
        each("onGroupCreated", l -> l.onGroupCreated(lane, recreated));
    }

    @Override
    public void onRedisUnavailable(Throwable error) {
        each("onRedisUnavailable", l -> l.onRedisUnavailable(error));
    }

    @Override
    public void onRedisRecovered() {
        each("onRedisRecovered", WorkerListener::onRedisRecovered);
    }
}
