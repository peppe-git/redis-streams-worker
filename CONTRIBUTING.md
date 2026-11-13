# Contributing

Thanks for considering a contribution. This is a small project maintained in spare time: small,
focused pull requests with tests are the ones that get merged.

## Before you start

- For anything bigger than a bug fix, open an issue first and describe the problem, not only the
  solution. The scope is deliberately narrow (see the README); features outside it are likely to be
  declined.
- Security issues: do not open a public issue, see [SECURITY.md](SECURITY.md).

## Building

```bash
./mvnw verify                 # needs Docker for Testcontainers
scripts/scrub-check.sh        # must pass; CI runs it
```

The scrub check has generic rules (secrets, e-mail addresses, IP addresses, money, dates, id formats,
forbidden paths). Project-specific names live in a private pattern file kept **outside** the
repository, passed through an environment variable:

```bash
SCRUB_PRIVATE_PATTERNS=$HOME/.config/scrub/private-patterns.txt scripts/scrub-check.sh
```

The file holds one extended regular expression per line, matched case-insensitively; blank lines and
lines starting with `#` are ignored. Without the variable the script warns and applies the generic
rules only; if the variable names a missing file it exits with 2. Never commit that file.

## Rules for changes

- Every behaviour change comes with a test against a real Redis (Testcontainers), not only mocks.
- A test that guards against a failure must be seen failing once: break the code, watch the test go
  red, restore it.
- Keep the core free of Spring and of new runtime dependencies.
- Public API changes are documented in `CHANGELOG.md` and in `docs/`.
- Every source file starts with `SPDX-License-Identifier: Apache-2.0`.
- Commit messages: `type(scope): sentence`, for example `fix(core): keep the lease of deferred messages`.

## Releases

See [RELEASING.md](RELEASING.md).

## License

By contributing you agree that your contributions are licensed under the Apache License 2.0.
