# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Core: `QueueDefinition` (prefix, group, priority lanes, dead-letter stream), `StreamProducer`,
  `RedisStreamsWorker`, `QueueInspector`.
- Worker: capacity-aware reads, lanes with anti-starvation (`fairnessRatio`), per-message leases renewed
  by a dedicated heartbeat thread, reclaim of idle pending messages without a lease, `ReclaimGuard`
  consulted before `XCLAIM`, dead-letter after `maxDeliveries`, acknowledgement only by the current
  owner, retry of acknowledgements that failed during a Redis outage, work-key exclusivity per
  instance, consumer group recreation on `NOGROUP`, pruning of dead consumers, safe trimming with
  `XTRIM MINID`, graceful drain on stop.
- Handler outcomes `ACK`, `LEAVE_PENDING`, `SHUTDOWN`, `DEAD_LETTER`.
- Dead-letter read, replay (atomic, once) and delete.
- Optional lock module: `RedisLock` with renewal, compare-and-delete release, mandatory outage policy,
  `LockOwnership`; `RedisSemaphore`. No fencing token.
- Spring Boot auto-configuration and starter (Spring Boot 3.5 and 4.x).
- Tests against a real Redis with Testcontainers, including chaos tests.
