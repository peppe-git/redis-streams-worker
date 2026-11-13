# redis-streams-worker

A consumer-group worker for **Redis Streams** on the JVM that does the parts the clients leave to you:
recovering messages left pending by a dead consumer, keeping a long job from being stolen while it
runs, moving poison messages to a dead-letter stream, and reading priority lanes without starving the
low one. Plain Java on Lettuce, with an optional Spring Boot starter and an optional Redis lock.

> **Status: 0.1.0-SNAPSHOT.** Not published to Maven Central yet. APIs may change before 0.1.0.

## Guarantees, and what is *not* guaranteed

Read this before using it.

- **Delivery is at-least-once.** A message can be handled more than once: after a crash between the
  work and the acknowledgement, after a lease lost under a long pause, or after a handler returns
  `LEAVE_PENDING`. It is never exactly-once. Make handlers idempotent, or protect the work with a claim
  in your system of record.
- **`ReclaimGuard` is the real defence against double execution.** The lease (a Redis key renewed by
  a heartbeat) stops other workers from reclaiming a message whose owner is alive, but a lease can
  expire under a live handler (a long garbage-collection pause, Redis unreachable for longer than the
  TTL). If running the same work twice is expensive, keep a claim row for the work in your database
  (owner + expiry) and give the worker a `ReclaimGuard` that refuses to reclaim while that claim is
  held. The guard runs *before* `XCLAIM`, so refusing does not spend a delivery. See
  [docs/guides/reclaim-guard.md](docs/guides/reclaim-guard.md).
- **A worker only acknowledges messages it still owns.** If a message was reclaimed by another
  consumer, the late finisher's acknowledgement is refused, so the message stays recoverable until its
  current owner finishes.
- **Requirements on Redis:**
  - Redis **6.2 or newer** (Valkey should work, untested), standalone or Sentinel. **Redis Cluster is
    not supported** in 0.1.
  - `maxmemory-policy noeviction`. Any eviction policy can silently drop stream entries and leases.
    The worker warns at startup (or refuses to start with `requireNoEviction`).
  - Persistence (AOF) if you cannot afford to lose queued messages when Redis restarts.
- **Order of the timings** (checked at startup, the worker refuses inconsistent values):
  `heartbeatInterval × 2 ≤ leaseTtl`, and a pending message is reclaimed only when it has been idle
  for `reclaimAfter` **and** its lease has expired. After a crash, recovery takes about
  `max(reclaimAfter, leaseTtl) + reclaimInterval` (≈ 95 s with the defaults).
- **The lock module has no fencing token.** `RedisLock` avoids duplicate runs of periodic tasks most
  of the time; it cannot guarantee mutual exclusion against a holder that pauses longer than the TTL.
  See [ADR 0007](docs/adr/0007-lock-without-fencing-token.md).
- **Messages are not ordered** across consumers or after a redelivery.

## Quickstart

### Plain Java

```xml
<dependency>
  <groupId>io.github.peppe-git</groupId>
  <artifactId>redis-streams-worker-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```java
RedisClient redis = RedisClient.create("redis://localhost:6379");
QueueDefinition queue = QueueDefinition.builder("orders")   // key prefix
        .lanes("urgent", "default")                         // priority order
        .build();

try (StreamProducer producer = new StreamProducer(redis, queue)) {
    producer.send("default", Map.of("orderId", "42"));
}

RedisStreamsWorker worker = RedisStreamsWorker.builder(redis, queue, (message, ctx) -> {
            ship(message.field("orderId"));
            return Outcome.ACK;           // or LEAVE_PENDING, SHUTDOWN, DEAD_LETTER
        })
        .options(WorkerOptions.builder().concurrency(4).build())
        .build();
worker.start();
// ...
worker.stop();                            // drains running handlers
```

### Spring Boot (3.5 and 4.x)

```xml
<dependency>
  <groupId>io.github.peppe-git</groupId>
  <artifactId>redis-streams-worker-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
spring.data.redis.url: redis://localhost:6379
redis-streams-worker:
  key-prefix: orders
  lanes: [urgent, default]
  worker:
    concurrency: 4
```

```java
@Bean
MessageHandler orderHandler(OrderService orders) {
    return (message, ctx) -> {
        orders.ship(message.field("orderId"));
        return Outcome.ACK;
    };
}
```

A `StreamProducer` and a `QueueInspector` bean are always available; the worker starts when a
`MessageHandler` bean exists, unless `redis-streams-worker.worker.enabled=false` (processes that only
produce). See [docs/guides/spring-boot.md](docs/guides/spring-boot.md).

## What it does

| | |
|---|---|
| **Lanes** | Several streams read in priority order; after `fairnessRatio` polls served by higher lanes, the lowest lane is read first once. |
| **Capacity-aware reads** | Never reads more messages than free slots; the capacity can change at every poll (`CapacityProvider`). |
| **Lease + heartbeat** | Each running message has a lease key renewed on a dedicated thread; a long job is not reclaimed while its owner is alive. |
| **Reclaim** | Pending messages idle for `reclaimAfter` without a lease are taken over; lease check, claim and new lease are one Lua script. |
| **Dead-letter** | After `maxDeliveries` executions (or `Outcome.DEAD_LETTER`), the message moves atomically to `<prefix>:dlq`, with its reason. Read, replay and delete with `QueueInspector`. |
| **Ack if owner** | The acknowledgement is refused if another consumer owns the message now; failed acknowledgements are retried after a Redis outage. |
| **Work key** | Two messages for the same work never run at the same time on one instance. |
| **Self-healing** | `NOGROUP` (Redis flushed) recreates the groups; Redis outages are retried with backoff; dead consumers are pruned. |
| **Safe trimming** | `XTRIM MINID` below the oldest entry any group still needs, instead of a `MAXLEN` that can drop undelivered messages. |
| **Graceful stop** | Stops reading, waits for running handlers, interrupts the late ones, leaves unfinished messages to other workers. |

Everything above is tested against a real Redis (Testcontainers), including chaos tests: `kill -9` of a
worker JVM in the middle of a message, Redis stopped and restarted, `FLUSHALL`, a destroyed consumer
group, a lease expiring under a running handler, and four workers racing on the same reclaim.

## Compared with the alternatives

Checked against each project's documentation and sources in November 2026; corrections welcome.

| | redis-streams-worker | Spring Data Redis `StreamMessageListenerContainer` | Redisson `RStream` (4.8) | rqueue (3.4 / 4.0) |
|---|---|---|---|---|
| Redis data structure | Streams | Streams | Streams | Lists, sorted sets, sets (not Streams) |
| Recovers messages of a crashed consumer | yes (reclaim) | no: you call `XCLAIM` yourself | primitives (`claim`, `autoClaim`), no policy | yes (visibility timeout) |
| Lease renewed while a job runs | yes | no | no | periodic check-in for long jobs |
| Dead-letter | yes, with replay | no | no (the "Reliable Queue" with DLQ is Redisson PRO only) | yes |
| Priority | lanes with anti-starvation | no | no | weighted / strict priority groups |
| Delayed / scheduled messages | no | no | no | yes |
| Dashboard | no | no | no | yes |
| Spring | optional starter (Boot 3.5, 4.x) | Spring Data | optional | Spring / Spring Boot |

- **Spring Data Redis** gives you a solid reading loop and nothing for what happens after a crash; note
  that `receiveAutoAck` reads with `NOACK`, so a crash loses the message.
- **Redisson** has every Streams command, so you can build all of this on it; this library is the
  policy layer you would otherwise write.
- **rqueue** is the better choice if you want delayed or periodic jobs and a dashboard, and do not
  need Redis Streams specifically.

For the lock module:

| | `RedisLock` (this library) | ShedLock 7.11 (Redis providers) | Redisson `RLock` |
|---|---|---|---|
| Renewal while the task runs | yes, every `ttl / 3` | with `KeepAliveLockProvider` (extends at half of `lockAtMostFor`) | watchdog, 30 s default |
| Release only by the owner | always (compare-and-delete) | only with `safeUpdate=true` (default: plain `DEL`) | yes |
| Explicit behaviour when Redis is down | mandatory `FAIL_OPEN` / `FAIL_CLOSED` | provider-dependent | exceptions |
| Fencing token | **no** | no | **yes**, `RFencedLock` |
| Backends | Redis only | many (JDBC, Mongo, Redis, …) | Redis only |

If you need fencing tokens use Redisson's `RFencedLock`; if you need annotations or non-Redis
backends, use ShedLock.

## Modules

| Artifact | Content | Dependencies |
|---|---|---|
| `redis-streams-worker-core` | queue, worker, producer, inspector | Lettuce, SLF4J |
| `redis-streams-worker-lock` | `RedisLock`, `RedisSemaphore` (optional) | Lettuce, SLF4J |
| `redis-streams-worker-spring-boot-autoconfigure` | auto-configuration | Spring Boot |
| `redis-streams-worker-spring-boot-starter` | the above + Spring Data Redis | |

Java 17+. Built and tested with Spring Boot 3.5 / Lettuce 6.6, and with Spring Boot 4.1 / Lettuce 7.5
(`compat/spring-boot-4`).

## Documentation

- [Concepts](docs/concepts.md): PEL, lease, reclaim, dead-letter, lanes, failure scenarios
- [Configuration reference](docs/configuration.md)
- [Tuning](docs/tuning.md): timings and their order, capacity, Redis settings
- [Operations](docs/operations.md): reading and replaying the dead-letter stream, diagnosing a stuck queue
- [Spring Boot guide](docs/guides/spring-boot.md) · [ReclaimGuard with a database claim](docs/guides/reclaim-guard.md)
- [Architecture decisions](docs/adr/)

## Building

```bash
./mvnw verify
```

Needs Docker for the Testcontainers tests. `-Drsw.redis.image=redis:6.2-alpine` runs the suite on
another Redis image.

## License

[Apache License 2.0](LICENSE).
