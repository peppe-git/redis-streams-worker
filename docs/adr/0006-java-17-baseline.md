# ADR 0006 — Java 17 baseline, virtual threads optional

- Status: accepted
- Date: 2026-11-12

## Context

Spring Boot 3.x and 4.x both run on Java 17. The source code this library was extracted from runs one
virtual thread per message, but the number of concurrent handlers is already bounded by the worker's
capacity, so a platform-thread pool is enough.

## Decision

Compile with `--release 17`. The default executor uses platform threads (daemon, named
`rsw-handler-N`). `WorkerOptions.virtualThreads(true)` switches to one virtual thread per message on
Java 21+ and fails fast on older runtimes. Applications can also pass their own `ExecutorService`.

## Consequences

Usable on Java 17 and 21+; tests run on 21 and CI is configured for both.
