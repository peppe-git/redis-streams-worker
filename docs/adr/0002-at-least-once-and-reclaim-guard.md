# ADR 0002 — At-least-once, and a `ReclaimGuard` for the work, not the message

- Status: accepted
- Date: 2026-11-12

## Context

A Redis Stream consumer group can only offer at-least-once delivery: a consumer that crashes after
doing the work and before `XACK` leaves the message pending, and someone must deliver it again. A
lease with a heartbeat makes double execution rare, but cannot exclude it: a lease can expire under a
live handler (long GC pause, Redis unreachable for longer than the TTL, clock or scheduling stalls).

## Decision

1. The library promises **at-least-once** and says so on the first page of the README.
2. Reclaim of a pending message requires both `idle ≥ reclaimAfter` **and** no live lease.
3. Before claiming, the worker asks an optional application-provided `ReclaimGuard` whether the
   *work* (not the message) is still owned by someone alive — typically a claim row in the
   application database. A guard that throws is treated as "allow" (fail-open), because a guard that
   fails closed on a database outage would stop recovery for every message.
4. The guard runs **before** `XCLAIM`, after a non-mutating `XRANGE`, so that saying "not yet" does not
   consume one of the message's deliveries.
5. Acknowledgement is conditional: a worker acks only if the pending entry still belongs to it.

## Consequences

- Handlers must be idempotent or protected by a claim in the system of record.
- The guard is the real defence against double execution; the lease is an optimisation that makes the
  guard rarely needed.
