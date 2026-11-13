// SPDX-License-Identifier: Apache-2.0
package io.github.peppe_git.redisstreamsworker.testing;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Real Redis servers for tests, started with Testcontainers. The image comes from the system property
 * {@code rsw.redis.image} (default {@code redis:7.4-alpine}).
 */
public final class RedisContainers {

    private static GenericContainer<?> shared;
    private static RedisClient sharedClient;

    private RedisContainers() {
    }

    public static DockerImageName image() {
        return DockerImageName.parse(System.getProperty("rsw.redis.image", "redis:7.4-alpine"))
                .asCompatibleSubstituteFor("redis");
    }

    /** One Redis server shared by the tests of this JVM. */
    public static synchronized GenericContainer<?> shared() {
        if (shared == null) {
            shared = new GenericContainer<>(image())
                    .withExposedPorts(6379)
                    .withCommand("redis-server", "--save", "", "--appendonly", "no",
                            "--maxmemory-policy", "noeviction")
                    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
                    .withStartupTimeout(Duration.ofSeconds(60));
            shared.start();
        }
        return shared;
    }

    /** A client of the shared server. Do not shut it down. */
    public static synchronized RedisClient sharedClient() {
        if (sharedClient == null) {
            GenericContainer<?> c = shared();
            sharedClient = RedisClient.create(RedisURI.create(c.getHost(), c.getMappedPort(6379)));
            Runtime.getRuntime().addShutdownHook(new Thread(sharedClient::shutdown));
        }
        return sharedClient;
    }

    /**
     * A server of its own, bound to a fixed host port so that it keeps the same address across a
     * {@code docker restart}. With {@code persistent}, every write is appended to the AOF.
     */
    public static GenericContainer<?> dedicated(boolean persistent) {
        int port = freePort();
        List<String> cmd = persistent
                ? List.of("redis-server", "--appendonly", "yes", "--appendfsync", "always",
                        "--maxmemory-policy", "noeviction")
                : List.of("redis-server", "--save", "", "--appendonly", "no", "--maxmemory-policy", "noeviction");
        GenericContainer<?> c = new GenericContainer<>(image())
                .withExposedPorts(6379)
                .withCommand(cmd.toArray(String[]::new))
                .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
                .withStartupTimeout(Duration.ofSeconds(60));
        c.setPortBindings(List.of(port + ":6379"));
        c.start();
        return c;
    }

    public static RedisURI uri(GenericContainer<?> c) {
        return RedisURI.create(c.getHost(), c.getMappedPort(6379));
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
