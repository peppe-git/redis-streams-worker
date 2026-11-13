# Operations

Commands below use the default prefix `rsw`, group `workers` and one lane `default`. Replace them with
your values. Prefer `--scan` to `KEYS *` on a production Redis: `KEYS` blocks the server.

## Is the queue moving?

```bash
redis-cli XINFO GROUPS rsw:default          # pending, last-delivered-id, lag
redis-cli XINFO CONSUMERS rsw:default workers
redis-cli XPENDING rsw:default workers      # count, oldest and newest pending id, per consumer
redis-cli XPENDING rsw:default workers IDLE 60000 - + 20   # pending for more than a minute
redis-cli --scan --pattern 'rsw:lease:*' | head
```

From the application: `QueueInspector.stats()` returns length, pending, consumers and lag per lane. Its
`measured` flag is `false` when Redis could not be read: never take a decision ("the queue is empty, it
is safe to restart") on an unmeasured zero.

How to read it:

| Symptom | Likely cause |
|---|---|
| `lag` grows, consumers present | Not enough capacity, or handlers too slow. |
| `lag` grows, no consumer idle for less than a few seconds | No worker is running. |
| `pending` high and stable, leases present | Long jobs in progress: normal. Do not intervene. |
| `pending` entries idle > `reclaimAfter` without a lease | A worker died; another one will reclaim them on its next scan. If none runs, start one. |
| Pending entries that never get reclaimed | A `ReclaimGuard` refuses them (work claimed elsewhere), or every worker is saturated. |

**Dead man's switch.** `RedisStreamsWorker.lastPollAt()` advances on every poll, also when the worker
is saturated. Ping an external monitor only when it is recent:

```java
scheduler.scheduleAtFixedRate(() -> {
    if (worker.lastPollAt().isAfter(Instant.now().minus(Duration.ofMinutes(3)))) {
        http.sendAsync(HttpRequest.newBuilder(monitorUrl).build(), BodyHandlers.discarding());
    }
}, 1, 1, TimeUnit.MINUTES);
```

## The dead-letter stream

### Read it

```bash
redis-cli XLEN rsw:dlq
redis-cli XREVRANGE rsw:dlq + - COUNT 20     # newest first
```

```java
for (DeadLetter dl : inspector.deadLetters(20)) {      // newest first
    log.info("{} from {} ({}): {} after {} deliveries", dl.id(), dl.lane(), dl.sourceId(),
            dl.reason(), dl.deliveries());
}
```

Before replaying, look at what the entries have in common: one payload failing again and again points
at the data; many different payloads failing at the same time point at a dependency.

### Replay it

Fix the cause first, or the message comes back.

```java
String newId = inspector.replay(deadLetterId);
```

`replay` puts the original payload back on its original lane as a **new** message (delivery count 1)
and deletes the dead letter, in one atomic step: calling it twice for the same id fails the second
time instead of enqueuing twice. It refuses dead letters whose lane is not declared any more.

By hand (not atomic: check that the `XDEL` returns `1`, and do not run it twice):

```bash
redis-cli XRANGE rsw:dlq <id> <id>                        # copy the payload fields (not the x-dlq-* ones)
redis-cli XADD rsw:default '*' orderId 42                 # same fields
redis-cli XDEL rsw:dlq <id>
```

### Delete it

```java
inspector.delete(deadLetterId);
```

```bash
redis-cli XDEL rsw:dlq <id>
redis-cli DEL rsw:dlq            # everything: irreversible
```

The dead-letter stream is capped at about `deadLetterMaxLen` entries (10 000 by default); the oldest
entries go first.

## A message is running too long

Do not delete its lease: another worker would reclaim it and run it a second time while the first one
is still working. If the worker process is really dead, its lease expires within `leaseTtl` and the
message is reclaimed. To abandon a message on purpose, stop its worker gracefully (`SIGTERM`): the
handler is interrupted and the message is left for another worker.

## NOGROUP in the logs

`consumer group 'workers' missing (Redis flushed or restarted without persistence?), recreating it`
means the group disappeared. The worker recreates it and continues. Check why: a `FLUSHALL`, a restart
without AOF, a failover to an empty replica. Messages that were pending at that moment are gone with
the data; messages produced afterwards are delivered (with the default `GroupStart.EARLIEST`).

If someone ran `XGROUP DESTROY` on a stream that still holds entries, the recreated group delivers
those entries again (at-least-once).

## Redis close to `maxmemory`

With `noeviction`, Redis keeps answering reads but refuses writes: `XADD` fails on producers, leases
cannot be renewed. Look at `XLEN` of each lane and of the dead-letter stream:

- a lane that grows is a backlog: add capacity or workers;
- a lane full of acknowledged entries means trimming is off (`trimInterval = 0`) or no worker runs;
- a large dead-letter stream: read it, fix, replay or delete.

Never switch to an eviction policy to "make room": it deletes queue entries and leases without an error.

## Deploying

- Stop workers with `SIGTERM` and give them `drainTimeout` plus a few seconds. Spring Boot: keep
  `redis-streams-worker.worker.drain-timeout` below `spring.lifecycle.timeout-per-shutdown-phase`.
- Rolling restarts are safe: messages of a stopping worker are left pending and reclaimed by the others.
- Changing the key prefix, the group or a lane's stream key is a migration: drain the old queue first.

## Leftover consumers

Each start registers a new consumer name. Workers remove consumers idle for longer than
`deadConsumerIdle` (24 h) that own nothing pending. By hand:

```bash
redis-cli XINFO CONSUMERS rsw:default workers
redis-cli XGROUP DELCONSUMER rsw:default workers <name>   # only if its pending count is 0
```

Deleting a consumer that still owns pending entries deletes those entries from the PEL: their messages
will never be delivered again.
