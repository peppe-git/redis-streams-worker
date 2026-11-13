# Tuning

## The order of the timings

```
heartbeatInterval  <  leaseTtl / 2          (checked at startup)
reclaimInterval    <= reclaimAfter          (checked at startup)
reclaim happens only after: idle >= reclaimAfter  AND  lease expired
```

- **`leaseTtl` vs `heartbeatInterval`.** The lease must survive at least one late or failed heartbeat.
  With the defaults (90 s / 20 s) four consecutive heartbeats can fail before the lease expires. Make
  the ratio larger if your JVM has long pauses or your Redis has slow failovers.
- **`reclaimAfter`** does not need to be longer than your longest job: the lease protects running jobs.
  It is the minimum time before a message abandoned by a dead consumer, or left with `LEAVE_PENDING`,
  is delivered again, i.e. your retry delay.
- **Recovery after a crash** ≈ `max(reclaimAfter, leaseTtl) + reclaimInterval`. Lower both to recover
  faster; the cost is more Redis traffic and less tolerance to pauses.
- **`commandTimeout`** bounds how long a thread waits on an unreachable Redis. The poll loop adds
  `pollTimeout` to it for the blocking read.

A configuration with fast recovery (about 15 s), for short jobs on a stable network:

```yaml
redis-streams-worker.worker:
  lease-ttl: 15s
  heartbeat-interval: 3s
  reclaim-after: 10s
  reclaim-interval: 2s
```

## Retries and dead-letter

The handler of an always-failing message runs `maxDeliveries` times, `reclaimAfter` apart, then the
message is dead-lettered. With the defaults that is 5 runs over about 4 minutes. If your failures are
transient over longer periods (an upstream outage), raise `reclaimAfter` rather than `maxDeliveries`, or
return `ACK` after recording the failure and retry through your own scheduler.

A `ReclaimGuard` that refuses a reclaim does not spend a delivery.

## Capacity and throughput

- `concurrency` (or a `CapacityProvider`) is the number of handlers running at once on one instance.
  Size it on what the handler waits for: network-bound handlers tolerate a high value, CPU-bound ones
  should stay near the number of cores they can use.
- `batchSize` only bounds a single read; the worker never reads more than the free slots.
- Lowering the capacity at runtime is safe: nothing is interrupted.
- With `virtualThreads(true)` (Java 21+) each message runs on a virtual thread; useful for many
  concurrent network-bound handlers.

## Latency

- A message on the lowest lane is picked up immediately if the worker is idle (blocking read).
- A message on a higher lane, while the worker is idle, waits up to `pollTimeout` (default 1 s).
- Under load, the lowest lane waits at most `fairnessRatio + 1` served polls.

## Redis settings

| Setting | Recommended | Why |
|---|---|---|
| `maxmemory-policy` | `noeviction` | Eviction silently deletes stream entries and leases. With `noeviction`, a full Redis makes `XADD` fail loudly. |
| `appendonly` | `yes` (`appendfsync everysec` is a reasonable compromise) | Without persistence a restart loses every queued and pending message. |
| `maxmemory` | headroom for the backlog | Streams are trimmed only below what every group has acknowledged; a stopped worker means a growing stream. |
| Version | 6.2+ | `XPENDING … IDLE`, `XTRIM MINID`. |

## Trimming

`trimInterval` (default 60 s) trims acknowledged entries with `MINID ~`. The `~` removes whole internal
nodes only (`stream-node-max-entries`, 100 by default), so a stream keeps up to about a hundred
acknowledged entries per lane; this is expected. Set `trimInterval` to `0` if another process trims.
