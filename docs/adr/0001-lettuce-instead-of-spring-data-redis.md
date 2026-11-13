# ADR 0001 — Lettuce in the core, not Spring Data Redis

- Status: accepted (to be confirmed by the maintainer before 0.1.0)
- Date: 2026-11-12

## Context

The worker needs blocking `XREADGROUP`, `XPENDING` with `IDLE`, `XCLAIM`, `XTRIM MINID`, `XINFO` and
Lua scripts. It must be usable without Spring, and it must work inside Spring Boot applications that
already configure Redis.

## Decision

The core module depends on **Lettuce** (`io.lettuce:lettuce-core`) and SLF4J only. The Spring Boot
autoconfiguration reuses the native `RedisClient` of Spring Data Redis' `LettuceConnectionFactory`
(`getRequiredNativeClient()`), or a `RedisClient` bean declared by the application.

The worker opens two connections: a dedicated one for its poll loop (the only place where a blocking
read happens) and a shared one for heartbeats and acknowledgements from handler threads.

## Consequences

- No Spring on the classpath of the core; one transitive dependency (Lettuce) that Spring Boot users
  already have.
- Blocking reads never run on a connection shared with the application.
- Applications that use Jedis must declare a `RedisClient` bean.
- The core is compiled against the oldest supported Lettuce line (6.6, managed by Spring Boot 3.5) and
  tested against 7.5 (managed by Spring Boot 4.1) in `compat/spring-boot-4`.
- Redis Cluster is not supported in 0.1 (a `RedisClusterClient` is rejected at startup).

## Alternatives considered

- **Spring Data Redis `StreamOperations`**: would tie the core to Spring and hide Lettuce details
  (connection used for blocking reads, script caching) behind an extra layer.
- **Jedis**: synchronous and simple, but Spring Boot defaults to Lettuce, so reusing the application's
  connection settings would be harder.
