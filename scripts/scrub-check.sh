#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Fails when the repository contains data that must not be published: secrets, e-mail addresses, IP
# addresses, money amounts, dates, internal issue or job id formats, forbidden paths. It also checks
# that every source file carries an SPDX license header, and scans commit messages.
#
# Project-specific names (products, vendors, people) are NOT in this file: they are read from a
# private pattern file outside the repository, named by the SCRUB_PRIVATE_PATTERNS environment
# variable. One extended regular expression per line, matched case-insensitively; blank lines and
# lines starting with '#' are ignored. See CONTRIBUTING.md.
#
# Usage:
#   scripts/scrub-check.sh              scan the repository (tracked + untracked, not ignored)
#   scripts/scrub-check.sh --self-test  prove that every rule fails on a dirty sample, then passes
#   scripts/scrub-check.sh --dir PATH   scan an arbitrary directory (no git required)
#
# Exit codes: 0 clean, 1 findings, 2 usage error or unreadable private pattern file.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Validates SCRUB_PRIVATE_PATTERNS; warns when it is not set.
check_private_patterns() {
  if [[ -z "${SCRUB_PRIVATE_PATTERNS:-}" ]]; then
    echo "scrub-check: SCRUB_PRIVATE_PATTERNS not set, only the generic rules apply" >&2
    return 0
  fi
  if [[ ! -f "$SCRUB_PRIVATE_PATTERNS" || ! -r "$SCRUB_PRIVATE_PATTERNS" ]]; then
    echo "scrub-check: SCRUB_PRIVATE_PATTERNS points to a missing or unreadable file" >&2
    exit 2
  fi
}

scan() {
  local dir="$1" with_git="$2"
  ( cd "$dir"
    if [[ "$with_git" == "yes" ]] && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      git ls-files -z -c -o --exclude-standard
    else
      find . -type f -not -path './.git/*' -print0 | perl -0 -pe 's#^\./##'
    fi
  ) | SCRUB_DIR="$dir" perl -0 -ne '
    BEGIN {
      use strict; use warnings;
      our @rules = (
        [ "issue-id",       qr/(?<![A-Za-z0-9_.\/-])(?:PR-\d{1,4}|it-\d{2,4}|[ABDEFLMORSTV]\d{1,4}(?:\.\d+)?|[ABDEFLMORSTV]-\d{2,4})(?![A-Za-z0-9_])/ ],
        [ "email",          qr/[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}/ ],
        [ "host",           qr/\b(?:\d{1,3}\.){3}\d{1,3}\b/ ],
        [ "money",          qr/\xE2\x82\xAC|\bEUR\b|\b\d+(?:[.,]\d+)?\s?euro\b/i ],
        [ "secret",         qr/-----BEGIN [A-Z ]*PRIVATE KEY-----|AKIA[0-9A-Z]{16}|\bsk-[A-Za-z0-9]{20,}|ghp_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{10,}|(?i:(?:password|secret|api[_-]?key|token)\s*[=:]\s*["\x27][^"\x27\s]{8,}["\x27])/ ],
        [ "prompts-path",   qr/resources\/prompts/ ],
        [ "date",           qr/\b20\d\d-[01]\d-[0-3]\d\b|\b[0-3]?\d\/[01]?\d\/20\d\d\b/ ],
        [ "job-id",         qr/\b(?=[0-9a-f]*[a-f])(?=[0-9a-f]*[0-9])[0-9a-f]{8}\b/ ],
      );
      # Private patterns: reported by line number only, so that the pattern text never reaches a log.
      if (my $private = $ENV{SCRUB_PRIVATE_PATTERNS}) {
        open(my $pf, "<", $private) or do { print STDERR "scrub-check: cannot read private patterns\n"; exit 2 };
        local $/ = "\n";
        my $n = 0;
        while (my $p = <$pf>) {
          $n++;
          $p =~ s/\r?\n$//;
          next if $p =~ /^\s*(?:#|$)/;
          my $re = eval { qr/$p/i };
          if (!$re) { print STDERR "scrub-check: invalid private pattern on line $n\n"; exit 2 }
          push @rules, [ "private-pattern:$n", $re ];
        }
        close($pf);
      }
      our %allow_email = map { $_ => 1 } ("noreply\@anthropic.com");
      our $failures = 0;
    }
    chomp(my $file = $_);
    next if $file eq "";
    next if $file =~ m{^(?:mvnw|mvnw\.cmd|\.mvn/|LICENSE$)};   # third-party Apache files, verbatim
    my $path = "$ENV{SCRUB_DIR}/$file";
    next unless -f $path;
    open(my $fh, "<", $path) or next;
    my $head = ""; read($fh, $head, 8000); seek($fh, 0, 0);
    next if $head =~ /\x00/;                                    # binary
    if ($file =~ /(?:\.java|\.sh|\.lua|pom\.xml|\.ya?ml)$/ && $head !~ /SPDX-License-Identifier: Apache-2\.0/) {
      print "$file:1: [spdx-header] missing SPDX-License-Identifier: Apache-2.0\n"; $failures++;
    }
    my $is_changelog = ($file eq "CHANGELOG.md");
    local $/ = "\n";
    while (my $line = <$fh>) {
      for my $r (@rules) {
        my ($name, $re) = @$r;
        pos($line) = undef;
        while ($line =~ /($re)/g) {
          my $hit = $1;
          next if $name eq "email" && ($allow_email{$hit} || $hit =~ /\@(?:users\.noreply\.github\.com|example\.(?:com|org))$/);
          next if $name eq "date" && ($line =~ /^- Date: / || $is_changelog);
          next if $name eq "host" && $hit =~ /^(?:127\.0\.0\.1|0\.0\.0\.0)$/;
          (my $shown = $line) =~ s/\s+$//;
          print "$file:$.: [$name] $shown\n"; $failures++;
          last;
        }
      }
    }
    close($fh);
    END { exit($failures > 0 ? 1 : 0) unless $? == 2; }
  '
}

scan_commits() {
  ( cd "$ROOT"
    git rev-parse --verify -q HEAD >/dev/null || exit 0
    git log --format='commit %h%n%B' ) | perl -ne '
    BEGIN {
      our @private;
      if (my $file = $ENV{SCRUB_PRIVATE_PATTERNS}) {
        open(my $pf, "<", $file) or exit 2;
        while (my $p = <$pf>) { $p =~ s/\r?\n$//; next if $p =~ /^\s*(?:#|$)/; push @private, qr/$p/i; }
      }
      our $f = 0;
    }
    $c = $1 if /^commit (\w+)/;
    if (/(?<![A-Za-z0-9_.\/-])(?:PR-\d{1,4}|it-\d{2,4}|[ABDEFLMORSTV]\d{1,4})(?![A-Za-z0-9_])/) { print "commit $c: [commit-message] $_"; $f++ }
    for my $re (@private) { if ($_ =~ $re) { print "commit $c: [commit-message private-pattern] $_"; $f++; last } }
    for my $e (/([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})/g) { next if $e eq "noreply\@anthropic.com" || $e eq "support\@github.com" || $e =~ /\@users\.noreply\.github\.com$/; print "commit $c: [commit-email] $e\n"; $f++ }
    END { exit($f > 0 ? 1 : 0) }'
}

self_test() {
  local tmp; tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$tmp/repo/src"
  local repo="$tmp/repo"
  # A fictitious private pattern file: the mechanism is tested without any real name.
  printf '# fictitious private patterns\n\nacme[^a-z0-9]{0,3}widgets\n' > "$tmp/private.txt"
  # Synthetic samples, assembled at run time so that this file does not match its own rules.
  printf '// SPDX-License-Identifier: Apache-2.0\nclass A { String k = "%s-%s:jobs"; }\n' Acme Widgets > "$repo/src/private.java"
  printf 'Fixed in %s%s and %s-%s.\n' B 999 it 999 > "$repo/issue.md"
  printf 'Contact: someone@%s.example.it\n' "corp" > "$repo/email.md"
  printf 'connect to %s.%s.%s.%s\n' 10 1 2 3 > "$repo/host.md"
  printf 'Costs 12 \xe2\x82\xac per month\n' > "$repo/money.md"
  printf '%s = "%s"\n' password "$(printf 'x%.0s' {1..12})" > "$repo/secret.md"
  printf 'see src/main/%s/prompts/x.txt\n' resources > "$repo/prompts.md"
  printf 'the outage of %s-%s-%s\n' 2099 01 01 > "$repo/date.md"
  printf 'job %s%s failed\n' 0a1b 2c3d > "$repo/jobid.md"
  printf 'class NoHeader {}\n' > "$repo/src/noheader.java"
  local out status=0
  out="$(export SCRUB_PRIVATE_PATTERNS="$tmp/private.txt"; scan "$repo" no)" || status=$?
  if [[ $status -ne 1 ]]; then echo "self-test FAILED: dirty sample did not fail (exit $status)"; return 1; fi
  local rule missing=0
  for rule in private-pattern issue-id email host money secret prompts-path date job-id spdx-header; do
    if ! grep -qF "[$rule" <<<"$out"; then echo "self-test FAILED: rule '$rule' did not fire"; missing=1; fi
  done
  [[ $missing -eq 0 ]] || return 1

  # Private pattern only: fails with the file, passes (with a warning) without the variable.
  rm -rf "${repo:?}"/*
  printf 'Built for %s %s.\n' Acme Widgets > "$repo/notes.md"
  status=0; SCRUB_PRIVATE_PATTERNS="$tmp/private.txt" "$0" --dir "$repo" >/dev/null 2>&1 || status=$?
  if [[ $status -ne 1 ]]; then echo "self-test FAILED: private pattern did not fire (exit $status)"; return 1; fi
  local err; status=0
  err="$( (unset SCRUB_PRIVATE_PATTERNS; "$0" --dir "$repo") 2>&1 >/dev/null)" || status=$?
  if [[ $status -ne 0 ]]; then echo "self-test FAILED: without private patterns the sample should pass"; return 1; fi
  if ! grep -qF "SCRUB_PRIVATE_PATTERNS not set" <<<"$err"; then echo "self-test FAILED: no warning without the variable"; return 1; fi
  status=0; SCRUB_PRIVATE_PATTERNS="$tmp/does-not-exist.txt" "$0" --dir "$repo" >/dev/null 2>&1 || status=$?
  if [[ $status -ne 2 ]]; then echo "self-test FAILED: a missing private file must exit 2 (got $status)"; return 1; fi
  printf 'unbalanced(\n' > "$tmp/broken.txt"
  status=0; SCRUB_PRIVATE_PATTERNS="$tmp/broken.txt" "$0" --dir "$repo" >/dev/null 2>&1 || status=$?
  if [[ $status -ne 2 ]]; then echo "self-test FAILED: an invalid private pattern must exit 2 (got $status)"; return 1; fi

  rm -rf "${repo:?}"/*
  printf '# SPDX-License-Identifier: Apache-2.0\nlanes: [urgent, default]\n' > "$repo/clean.yml"
  if ! (export SCRUB_PRIVATE_PATTERNS="$tmp/private.txt"; scan "$repo" no >/dev/null); then
    echo "self-test FAILED: clean sample did not pass"; return 1
  fi
  echo "self-test OK: every rule fires on a dirty sample, private patterns work, a clean sample passes"
}

case "${1:-}" in
  --self-test) self_test ;;
  --dir)
    check_private_patterns
    scan "$2" no && echo "scrub-check OK"
    ;;
  "")
    check_private_patterns
    status=0
    scan "$ROOT" yes || status=$?
    if [[ $status -eq 2 ]]; then exit 2; fi
    scan_commits || status=1
    if [[ $status -ne 0 ]]; then echo "scrub-check FAILED"; exit 1; fi
    echo "scrub-check OK"
    ;;
  *) echo "usage: $0 [--self-test | --dir PATH]" >&2; exit 2 ;;
esac
