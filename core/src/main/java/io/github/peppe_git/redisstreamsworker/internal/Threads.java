// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.internal;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Thread factories and executors of the worker. */
public final class Threads {

    private Threads() {
    }

    /** Daemon platform threads named {@code prefix-N}. */
    public static ThreadFactory daemon(String prefix) {
        AtomicInteger seq = new AtomicInteger(1);
        return r -> {
            Thread t = new Thread(r, prefix + "-" + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }

    /** Executor for handlers: platform threads, or one virtual thread per task on Java 21+. */
    public static ExecutorService handlerExecutor(String prefix, boolean virtual) {
        if (!virtual) {
            return Executors.newCachedThreadPool(daemon(prefix));
        }
        try {
            // Reflection keeps the bytecode at Java 17 while allowing virtual threads at runtime.
            Method m = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) m.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Virtual threads are not available on " + Runtime.version(), e);
        }
    }
}
