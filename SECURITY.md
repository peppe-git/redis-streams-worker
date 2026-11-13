# Security policy

## Supported versions

Only the latest released version receives fixes. Before 0.1.0 there is no supported release.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's private vulnerability reporting:
<https://github.com/peppe-git/redis-streams-worker/security/advisories/new>. Do not open a public issue,
and do not send reports by e-mail: there is no security mailbox.

Include what you found, how to reproduce it, and the impact you expect. You will get an
acknowledgement within a week; this project is maintained in spare time, so a fix may take longer.

## Scope and known limits

These are documented behaviours, not vulnerabilities:

- delivery is at-least-once: a message can be processed more than once;
- `RedisLock` has no fencing token and cannot guarantee mutual exclusion against a paused holder;
- anyone with write access to the Redis keys of a queue can read, forge, acknowledge or delete
  messages: protect Redis with authentication, TLS and network isolation;
- message payloads are stored in clear text in Redis.
