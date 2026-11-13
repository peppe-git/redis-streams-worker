# ADR 0007 — The lock module has no fencing token

- Status: accepted
- Date: 2026-11-12

## Context

A Redis lock based on `SET NX PX` with a TTL cannot guarantee mutual exclusion: if the holder pauses
for longer than the TTL, another process acquires the lock while the first one is still running. The
standard fix is a fencing token checked by the protected resource, which requires that resource's
cooperation.

## Decision

`RedisLock` is meant for "run this periodic job once in the cluster, most of the time". It renews the
TTL while the task runs (every `ttl / 3`), releases with compare-and-delete, and gives the task a
`LockOwnership` to check before each conclusive write. It does **not** produce fencing tokens, and the
README says so. The outage behaviour (`FAIL_OPEN` / `FAIL_CLOSED`) is a mandatory argument of every
call.

## Consequences

Tasks that must never run twice need an idempotency key or a fencing check in their own storage.
