# ADR 0003 — Safe trimming with `XTRIM MINID` instead of `MAXLEN` on `XADD`

- Status: accepted
- Date: 2026-11-12

## Context

`XACK` does not delete entries, so a stream grows forever unless it is trimmed. The common recipe,
`XADD … MAXLEN ~ N`, deletes the oldest entries whether or not they were delivered or acknowledged:
with a backlog larger than `N`, undelivered and pending messages are silently lost.

## Decision

- The producer does **not** trim by default.
- Each worker periodically computes, for every lane, the lowest id that is still needed by **any**
  consumer group of the stream: the smallest pending id, or the entry after the group's
  `last-delivered-id` when nothing is pending. It then runs `XTRIM <stream> MINID ~ <id>`, which only
  removes entries that every group has received and acknowledged.
- `maxLen` stays available on the producer as an explicit hard cap, documented as lossy.
- The dead-letter stream is capped with `MAXLEN ~` (default 10 000): it is an archive, and losing the
  oldest dead letters is acceptable and documented.

## Consequences

- If no worker runs, streams grow until Redis reaches `maxmemory`; with `noeviction` the producer then
  fails loudly instead of losing data silently.
- Approximate trimming (`~`) removes whole radix-tree nodes only, so a tail of already acknowledged
  entries may stay until the next trim.
