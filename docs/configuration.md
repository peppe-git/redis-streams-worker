# Configuration reference

Plain Java configures `QueueDefinition`, `WorkerOptions` and the builder of `RedisStreamsWorker`.
Spring Boot maps the same values from `redis-streams-worker.*`; defaults are identical (the properties
read them from `WorkerOptions.defaults()`).

## Queue (`QueueDefinition`)

| Java | Property | Default | Meaning |
|---|---|---|---|
| `builder(prefix)` | `redis-streams-worker.key-prefix` | `rsw` | Prefix of every key: lanes `P:<lane>`, dead-letter `P:dlq`, leases `P:lease:<lane>:<id>`. |
| `group(...)` | `redis-streams-worker.group` | `workers` | Consumer group. |
| `lanes(...)` | `redis-streams-worker.lanes` | `[default]` | Lanes, highest priority first. Names: letters, digits, `_ - .`; `dlq` and `lease` are reserved. |
| `lane(name, streamKey)` | `redis-streams-worker.lane-stream-keys.<lane>` | derived | Explicit stream key for a lane, to adopt an existing stream. |
| `deadLetterMaxLen(...)` | `redis-streams-worker.dead-letter-max-len` | `10000` | Approximate cap of the dead-letter stream; `0` = none. |

## Producer (`StreamProducer`)

| Java | Property | Default | Meaning |
|---|---|---|---|
| constructor `maxLen` | `redis-streams-worker.producer.max-len` | `0` | `XADD … MAXLEN ~ n`. **Lossy**: can remove undelivered messages. Prefer the worker's safe trimming. |

## Worker (`WorkerOptions`)

Properties are under `redis-streams-worker.worker.`.

| Option | Property | Default | Meaning |
|---|---|---|---|
| — | `enabled` | `true` | Start a worker when a `MessageHandler` bean exists. `false` on producer-only processes. |
| `consumerName` | `consumer-name` | generated | Fixed consumer name. Must be unique among live workers. |
| `consumerNamePrefix` | `consumer-name-prefix` | `rsw` | Prefix of the generated name. |
| `concurrency` | `concurrency` | `4` | Capacity when no `CapacityProvider` is given. |
| `batchSize` | `batch-size` | `10` | Maximum records per read (also bounded by free slots). |
| `pollTimeout` | `poll-timeout` | `1s` | Block time on the lowest lane when everything is empty; worst-case latency for higher lanes. |
| `commandTimeout` | `command-timeout` | `5s` | Timeout of every Redis command of the worker. |
| `leaseTtl` | `lease-ttl` | `90s` | TTL of a message lease. Must be ≥ 2 × `heartbeatInterval`. |
| `heartbeatInterval` | `heartbeat-interval` | `20s` | Lease renewal period. |
| `reclaimAfter` | `reclaim-after` | `60s` | Minimum idle time before a pending message (without lease) can be reclaimed; also the retry delay after `LEAVE_PENDING`. |
| `reclaimInterval` | `reclaim-interval` | `5s` | Period of the reclaim scan. Must be ≤ `reclaimAfter`. |
| `reclaimScanLimit` | `reclaim-scan-limit` | `1000` | Pending entries inspected per lane per scan. |
| `maxDeliveries` | `max-deliveries` | `5` | Maximum handler runs per message; the next delivery dead-letters it. |
| `fairnessRatio` | `fairness-ratio` | `4` | Polls served by higher lanes before the lowest lane is read first once; `0` = strict priority. |
| `drainTimeout` | `drain-timeout` | `30s` | Wait for running handlers on stop. Keep it below `spring.lifecycle.timeout-per-shutdown-phase`. |
| `interruptGrace` | `interrupt-grace` | `5s` | Wait after interrupting late handlers. |
| `trimInterval` | `trim-interval` | `60s` | Period of the safe trim; `0` disables it. |
| `deadConsumerIdle` | `dead-consumer-idle` | `24h` | Idle time after which a consumer with nothing pending is removed. |
| `pruneInterval` | `prune-interval` | `1h` | Period of the dead-consumer pruning. |
| `groupStart` | `group-start` | `EARLIEST` | Where a group starts when created: `EARLIEST` (`0`) or `LATEST` (`$`). |
| `requireNoEviction` | `require-no-eviction` | `false` | Refuse to start if `maxmemory-policy` is not `noeviction` (otherwise warn). |
| `virtualThreads` | `virtual-threads` | `false` | One virtual thread per message (Java 21+). |
| — | `phase` | `Integer.MAX_VALUE - 4096` | `SmartLifecycle` phase; must be above the Redis connection factory's (0). |

## Extension points (Java builder or Spring beans)

| Type | Builder | Spring | Purpose |
|---|---|---|---|
| `MessageHandler` | constructor | bean (required for a worker) | The work. |
| `ReclaimGuard` | `reclaimGuard(...)` | single bean | Refuse reclaim while the work is owned elsewhere. |
| `CapacityProvider` | `capacityProvider(...)` | single bean | Dynamic capacity. |
| `WorkKeyExtractor` | `workKey(...)` | single bean | Per-instance exclusivity per work key. |
| `WorkerListener` | `listener(...)` | all beans, in `@Order` | Metrics, alerts, side effects. |
| `ExecutorService` | `executor(...)` | — | Own handler executor (not shut down by the worker). |
| `RedisClient` | constructor | bean (else Spring Data Redis' client) | Connection settings. |

## Lock module

| Java | Property | Default | Meaning |
|---|---|---|---|
| — | `redis-streams-worker.lock.enabled` | `true` | Create `RedisLock` and `RedisSemaphore` beans when the module is present. |
| `new RedisLock(client, prefix, listener)` | `redis-streams-worker.lock.key-prefix` | `rsw:lock` | Lock keys: `<prefix>:<name>`. |
| `new RedisSemaphore(client, prefix)` | `redis-streams-worker.lock.semaphore-key-prefix` | `rsw:sem` | Slot keys: `<prefix>:<name>:<slot>`. |
| `LockListener` | — | single bean | Observes lock rounds. |

## Everything off

`redis-streams-worker.enabled=false` disables the whole auto-configuration.
