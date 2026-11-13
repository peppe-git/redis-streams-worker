# Spring Boot

The starter works with Spring Boot 3.5 and 4.x. It brings `spring-boot-starter-data-redis`; the
worker reuses the Lettuce client that Spring Boot configures from `spring.data.redis.*` (host, password,
database, SSL, Sentinel).

```xml
<dependency>
  <groupId>io.github.peppe-git</groupId>
  <artifactId>redis-streams-worker-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<!-- optional -->
<dependency>
  <groupId>io.github.peppe-git</groupId>
  <artifactId>redis-streams-worker-lock</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Beans

| Bean | When |
|---|---|
| `QueueDefinition`, `StreamProducer`, `QueueInspector` | always (unless `redis-streams-worker.enabled=false`) |
| `RedisStreamsWorker` + `RedisStreamsWorkerLifecycle` | a `MessageHandler` bean exists and `redis-streams-worker.worker.enabled` is not `false` |
| `RedisLock`, `RedisSemaphore` | `redis-streams-worker-lock` is on the classpath and `redis-streams-worker.lock.enabled` is not `false` |

Each one is `@ConditionalOnMissingBean`: declare your own to replace it.

## A producer process and a worker process

The same application often runs as an API (produces) and a worker (consumes):

```yaml
# api profile
redis-streams-worker.worker.enabled: false
```

```yaml
# worker profile
redis-streams-worker.worker:
  concurrency: 8
  drain-timeout: 25s
spring.lifecycle.timeout-per-shutdown-phase: 30s
```

## Optional beans

```java
@Bean
ReclaimGuard reclaimGuard(JobRepository jobs) { ... }        // see reclaim-guard.md

@Bean
CapacityProvider capacity(Dependencies deps) {
    return inFlight -> deps.remoteAvailable() ? 8 : 2;
}

@Bean
WorkKeyExtractor workKey() {
    return WorkKeyExtractor.field("jobId");
}

@Bean
WorkerListener metrics(MeterRegistry registry) {
    return new WorkerListener() {
        @Override
        public void onDeadLettered(QueueMessage message, String reason) {
            registry.counter("queue.dead_lettered", "lane", message.lane(), "reason", reason).increment();
        }
    };
}
```

## Choosing the Redis client

1. A `RedisClient` bean of your application, if there is one.
2. Otherwise the native client of Spring Data Redis' `LettuceConnectionFactory`.
3. Otherwise the startup fails with a message. Applications on Jedis declare a `RedisClient` bean.

Redis Cluster is not supported: a cluster connection factory is rejected at startup.

## Lifecycle

The worker is a `SmartLifecycle` in a phase above Spring Data Redis' connection factory, so it starts
after it and stops before it. On shutdown it drains running handlers for `drain-timeout`; keep that below
`spring.lifecycle.timeout-per-shutdown-phase`.
