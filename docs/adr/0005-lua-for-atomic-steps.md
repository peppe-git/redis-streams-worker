# ADR 0005 — Lua scripts for every step that must be atomic

- Status: accepted
- Date: 2026-11-12

## Context

Several steps are check-then-act sequences that race with other workers or with the heartbeat:

- "no live lease → claim → take the lease" races with the previous owner re-acquiring its lease;
- "copy to the dead-letter stream → acknowledge" leaves a duplicate dead letter if the process dies in
  between;
- "acknowledge" must not remove a pending entry that now belongs to another consumer;
- "replay a dead letter" must not run twice for the same entry.

`MULTI/EXEC` is not usable on a connection shared between threads (Lettuce multiplexes commands of
all threads on one connection).

## Decision

Each of these steps is one Lua script (`EVALSHA`, falling back to `EVAL` on `NOSCRIPT` after a Redis
restart). Scripts only touch keys derived from the queue prefix and pass every key in `KEYS`.

## Consequences

- Correct under concurrency without client-side transactions.
- Requires `EVAL` to be allowed by the Redis provider (it is on all mainstream managed offerings).
- Scripts touch more than one key (stream, lease, dead-letter stream): they are not Redis Cluster
  compatible without hash tags. Cluster is out of scope for 0.1.
