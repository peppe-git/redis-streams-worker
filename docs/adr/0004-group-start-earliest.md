# ADR 0004 — Consumer groups start at `0` (EARLIEST) by default

- Status: accepted (to be confirmed)
- Date: 2026-11-12

## Context

A consumer group is created at startup and recreated when Redis answers `NOGROUP` (data lost after
`FLUSHALL`, a restart without persistence, or an operator deleting the group). Creating it at `$`
(LATEST) skips every entry already in the stream; creating it at `0` (EARLIEST) delivers them.

The realistic causes of `NOGROUP` also delete the stream, so after recreation the stream contains only
messages produced after the data loss. With LATEST, the ones produced before the worker noticed are
never delivered.

## Decision

Default `GroupStart.EARLIEST`, configurable to `LATEST`. Groups are created with `MKSTREAM`.

## Consequences

- No message produced after a data loss is skipped.
- When a group is created for the first time on a stream that already holds history (a renamed group,
  a stream shared with another application, or `XGROUP DESTROY` on a live stream), every entry still in
  the stream is delivered again. This is consistent with at-least-once, and documented in
  `docs/operations.md`; set `LATEST` to skip history.
