# Concepts

This page explains what the worker does with a message from the moment it is produced to the moment
it is acknowledged or dead-lettered, and what happens when something breaks in between.

## Streams, consumer groups and the PEL

A Redis **stream** is an append-only log of entries (`XADD`). A **consumer group** reads it
cooperatively: each entry is delivered to one consumer of the group (`XREADGROUP … >`). Delivered
entries stay in the group's **Pending Entries List (PEL)** until a consumer acknowledges them
(`XACK`). For each pending entry Redis keeps:

- the **owner** (the consumer it was delivered to),
- the **idle time** since the last delivery,
- the **delivery count**, incremented by every `XREADGROUP` and every `XCLAIM`.

`XACK` removes the entry from the PEL but not from the stream. Trimming the stream is a separate
decision (see [Safe trimming](#safe-trimming)).

The worker uses one group (default `workers`) over all lanes. Each worker instance is one consumer,
named `<prefix>-<8 random hex>` unless you set `consumerName`.

## Lanes

A queue has one or more **lanes**, each a stream, declared in priority order:

```java
QueueDefinition.builder("orders").lanes("urgent", "default").build();
// streams: orders:urgent, orders:default
```

On every poll the worker reads lanes in order with non-blocking reads, and the first lane that returns
messages serves the poll. When all are empty it blocks for `pollTimeout` on the lowest lane. A message
arriving on a higher lane during that block waits at most `pollTimeout`.

**Anti-starvation.** Under a continuous flow on the higher lanes the lowest one would never be read.
After `fairnessRatio` consecutive polls served by a lane other than the lowest, the next poll reads the
lowest lane first. A message on the lowest lane therefore waits at most `fairnessRatio + 1` served
polls. `fairnessRatio = 0` means strict priority.

## Capacity

Before reading, the worker asks the `CapacityProvider` how many messages may run at once and reads at
most `capacity - running`. The provider sees what is running per lane (`InFlight`), so it can:

- follow a dependency (fewer slots when a remote service has degraded to a local, CPU-bound fallback);
- exclude a lane from the count (`base + inFlight.lane("interactive")`), for small interactive work
  that is capped elsewhere.

Lowering the capacity never interrupts anything: the worker just stops reading until enough messages
finish. Without a provider the capacity is `concurrency` (default 4).

## Leases and the heartbeat

When a message starts, the worker writes a **lease**: `SET <prefix>:lease:<lane>:<id> <consumer> PX
leaseTtl`. A dedicated heartbeat thread renews every running message's lease every `heartbeatInterval`
with a compare-and-extend script:

| Lease value | Heartbeat does | Meaning |
|---|---|---|
| this consumer | extend the TTL | normal case |
| missing | take it again | it expired (pause, Redis restart without the key) and nobody took the message |
| another consumer | stop, report `onLeaseLost` | another worker reclaimed the message; `MessageContext.leaseHeld()` turns `false` |

Lease keys include the lane because two streams can hold the same record id: Redis builds ids from the
clock, so two `XADD` in the same millisecond on two streams both get `<ms>-0`.

The heartbeat runs on its own thread so that slow handlers, or a blocking read, never delay it.

## Reclaim

Every `reclaimInterval` the worker scans the PEL of each lane for entries idle for at least
`reclaimAfter` (`XPENDING … IDLE`). For each candidate:

1. skip it if it is running, deferred or waiting for an acknowledgement on this instance;
2. skip it if its lease exists (its owner is alive);
3. read it with `XRANGE` — this does **not** change the delivery count;
4. ask the `ReclaimGuard`, if any; "no" leaves the entry untouched;
5. run one Lua script: if there is still no lease, `XCLAIM` it (with the same minimum idle time, so two
   workers cannot both win) and write our lease;
6. read the delivery count after the claim; if it exceeds `maxDeliveries`, dead-letter the message
   instead of running it.

A message is therefore reclaimed only when it has been idle for `reclaimAfter` **and** has no lease.
After a crash this takes about `max(reclaimAfter, leaseTtl) + reclaimInterval`.

If the entry is pending but its record was deleted from the stream (trimmed by someone else,
`XDEL`), the worker acknowledges it and reports `onMessageVanished`.

### ReclaimGuard

The lease protects the message while its owner heartbeats. It cannot protect the *work* when the lease
is gone but the owner is not (a long pause), nor when the same work is referenced by another message.
A `ReclaimGuard` answers "is this work still owned by someone alive?" from your system of record,
typically a claim row with an owner and an expiry. Because it runs before `XCLAIM`, refusing does not
spend a delivery: a message whose work is legitimately claimed elsewhere is not pushed towards the
dead-letter stream by repeated reclaim attempts. A guard that throws is treated as "allow": an outage
of your database must not stop recovery for every message. See [the guide](guides/reclaim-guard.md).

## Handler outcomes

| Outcome | Acknowledged | Lease | Delivered again |
|---|---|---|---|
| `ACK` | yes, if still owned | released | no |
| `LEAVE_PENDING` | no | released | after `reclaimAfter`, counts as a delivery |
| `SHUTDOWN` | no | released | after `reclaimAfter`, by another worker |
| `DEAD_LETTER` | moved to the dead-letter stream | released | no |
| exception | as `LEAVE_PENDING` (or `SHUTDOWN` if the worker is stopping) | released | yes |

**Ack if owner.** The acknowledgement is one script: it acknowledges only if the PEL entry still belongs
to this consumer. If the message was reclaimed meanwhile, the late finisher's acknowledgement is refused
(`onAckRejected`, reason `owned-by-another-consumer`), so the entry stays in the PEL until its current
owner finishes, and stays recoverable if that owner dies.

**Failed acknowledgements.** If Redis does not answer when a handler finishes, the acknowledgement is
kept, its lease keeps being renewed, and the poll loop retries it until Redis is back. The message is
not delivered again in the meantime.

## Dead-letter

A message goes to the dead-letter stream `<prefix>:dlq` when its delivery count would exceed
`maxDeliveries` (the handler has run `maxDeliveries` times) or when the handler returns `DEAD_LETTER`.
The move is one script: `XADD` to the dead-letter stream, `XACK` on the lane, lease release. The entry
keeps the original fields and adds metadata fields prefixed with `x-dlq-` (a prefix producers cannot
use):

| Field | Content |
|---|---|
| `x-dlq-reason` | `max-deliveries-exceeded`, `handler-dead-letter` or the reason given by the handler |
| `x-dlq-lane` | lane of origin |
| `x-dlq-source-id` | id on the lane |
| `x-dlq-deliveries` | delivery count when dead-lettered |
| `x-dlq-consumer` | consumer that moved it |
| `x-dlq-at` | epoch milliseconds |

The dead-letter stream is capped at about `deadLetterMaxLen` entries (default 10 000). See
[Operations](operations.md) for reading and replaying it.

## Work keys

Two messages can refer to the same piece of work (a retry enqueued while the first attempt is still
finishing). A `WorkKeyExtractor` returns a key per message; on one instance, a message whose key is
already running is **deferred**: it keeps a lease, is not reclaimed by others, and starts as soon as the
first one finishes. A third message for the same key is left pending for a later reclaim. Across
instances, use a `ReclaimGuard`.

## Consumer groups: creation and recovery

At startup the worker creates the group on every lane with `XGROUP CREATE … MKSTREAM`, from `0`
(`GroupStart.EARLIEST`, default) or `$` (`LATEST`). If a read fails with `NOGROUP` — Redis flushed,
restarted without persistence, or the group deleted — the worker recreates the groups and goes on.

With `EARLIEST`, messages produced after a data loss but before the worker noticed are delivered. The
price: if the group is deleted on a stream that still holds entries, those entries are delivered again.
See [ADR 0004](adr/0004-group-start-earliest.md).

Consumer names change on every start, so the group accumulates dead consumers. Every `pruneInterval`
the worker deletes consumers idle for longer than `deadConsumerIdle` **with nothing pending**; a dead
consumer that still owns pending entries is left for the reclaim.

## Safe trimming

Every `trimInterval` the worker computes, per lane, the lowest id still needed by any consumer group of
the stream — the oldest pending id, or the entry after the group's last-delivered id — and runs
`XTRIM <stream> MINID ~ <id>`. Only entries that every group has received and acknowledged are removed.
The producer does not trim by default: `MAXLEN` on `XADD` would also remove messages that were never
delivered when the backlog is larger than the cap. See [ADR 0003](adr/0003-safe-trimming-instead-of-maxlen.md).

## Stopping

`stop()` (or the Spring `SmartLifecycle`):

1. stops reading and reclaiming;
2. waits up to `drainTimeout` for running handlers (`MessageContext.shutdownRequested()` is `true`);
3. interrupts the handlers still running and waits `interruptGrace`;
4. releases the leases of deferred messages, retries pending acknowledgements once;
5. stops the heartbeat and closes its connections.

Messages that did not finish stay pending and are reclaimed by another worker after `reclaimAfter`.
The graceful path is never more destructive than a crash.

## What happens when…

| Event | Effect |
|---|---|
| A worker process dies (`kill -9`, OOM) | Its leases expire; after `max(reclaimAfter, leaseTtl)` another worker reclaims its messages. Each runs again (delivery + 1). |
| A handler pauses longer than `leaseTtl` | Another worker may reclaim and run the message too. The paused one sees `leaseHeld() == false` and its acknowledgement is refused. A `ReclaimGuard` prevents the second run. |
| Redis restarts **with** persistence | Workers retry with backoff; Lettuce reconnects; failed acknowledgements are delivered after the restart. |
| Redis restarts **without** persistence, or `FLUSHALL` | Queued and pending messages are **lost** (they were only in Redis). Workers recreate the groups; new messages flow again. |
| The group is deleted (`XGROUP DESTROY`) | Recreated from `0` by default: entries still in the stream are delivered again. |
| Redis reaches `maxmemory` with `noeviction` | `XADD` fails on the producer side (loud). Without `noeviction`, entries and leases may be evicted silently. |
| A handler always fails | It runs `maxDeliveries` times (one every `reclaimAfter`), then the message is dead-lettered. |
