#!/usr/bin/env python3
"""Scan text, files or a diff's added lines for customer data before it reaches a public place.

Morpheus is a public repository. Nothing read from Logfire, Sentry, a database or a provider may be
written into code, tests, fixtures, commit messages, PR bodies or comments. This scanner is the
deterministic safety net behind that rule; it is run by the PreToolUse guard hook
(pii_guard_hook.py), by post_pr_review.py and by the review skills.

  pii_scan.py --text [FILE ...]        scan whole files (or stdin) line by line
  pii_scan.py --diff [FILE]            scan the ADDED lines of a unified diff (stdin by default)
  pii_scan.py --staged                 scan `git diff --cached`
  pii_scan.py --range <rev-range>      scan `git diff <rev-range>`, e.g. origin/master...HEAD
  pii_scan.py ... --denylist PATH      literal terms to flag (default: .claude/pii-denylist.txt)

Exit 0 when clean, 1 on a hit, 2 when the scan could not run. Hits print as `path:line: rule`
WITHOUT the matched value, so the output itself is safe to paste anywhere.

Rules: `email` (addresses outside the allowlist), `phone` (phone-number shapes outside the fake
numbers the test-suite already uses), `secret` (cloud/API key shapes, DSNs and URLs carrying a
password, non-placeholder secret assignments), `hostname` (hosts under hosting-provider domains) and
`denylist` (case-insensitive literal terms from the gitignored `.claude/pii-denylist.txt`, one per
line, `#` comments allowed; the file is optional).

Known limits: a regex cannot recognise a customer or agency name, a company code or a realistic
fixture. Those are caught only by the denylist (which nobody commits) and by the reviewer reading
the diff. The written rule in CLAUDE.md still applies in full; a clean scan is not a clearance.
Standard library only.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# Domains and literal addresses that are placeholder or code-owned, so they never count as a hit.
# `testing.com` and `recipient.com` are the fake domains the existing test-suite uses; `anthropic.com`
# and the GitHub domains appear in commit trailers and bot messages.
ALLOWED_EMAIL_DOMAINS = frozenset(
    {
        'example.com',
        'example.org',
        'example.net',
        'testing.com',
        'recipient.com',
        'anthropic.com',
        'github.com',
        'users.noreply.github.com',
    }
)
ALLOWED_ADDRESSES = frozenset(
    {
        'tom@tutorcruncher.com',  # packaging/morpheus-mail/setup.py author_email
        'example@sentry.io',  # tests/test_parity.py DSN placeholder
    }
)
# Digits-only forms of the numbers already in the codebase: the Settings send numbers and the
# invented UK/US numbers tests reuse. Add a new synthetic number here when a test needs one.
ALLOWED_NUMBERS = frozenset(
    {
        '15744445663',
        '12048170659',
        '61488826088',
        '07896541236',
        '447896541236',
        '07891123850',
        '447891123850',
        '447891123851',
        '447891123852',
        '07891123856',
        '447891123856',
        '18183373095',
    }
)

# `user:password@host` inside a connection URL is a credential, not an address: the `secret` rule owns it.
URL_CREDENTIAL_RE = re.compile(r'://[^\s/@]+:[^@\s/]+@')
EMAIL_RE = re.compile(r'(?<![\w.+-])([\w.+-]+)@([\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,})(?![\w-])')
# Candidate phone shapes: an optional +, then 10-15 digits with optional spaces, dots, dashes or
# brackets between groups. Pure digit runs without a + or a separator are only treated as a phone
# when they start with a trunk 0 (UK local form), so ids and timestamps do not trip it.
PHONE_RE = re.compile(r'(?<![\w+])(\+?\(?\d[\d\s().-]{8,18}\d)(?![\w])')
ISO_DATETIME_RE = re.compile(r'\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?')
SECRET_RES = (
    re.compile(r'AKIA[0-9A-Z]{16}'),  # AWS access key id
    re.compile(r'\bgh[pousr]_[A-Za-z0-9]{30,}'),  # GitHub token
    re.compile(r'\bpylf_v\d_[A-Za-z0-9_-]{20,}'),  # Logfire token
    re.compile(r'\b[sr]k_(?:live|test)_[A-Za-z0-9]{16,}'),  # Stripe key
    re.compile(r'https?://[0-9a-f]{20,}@[\w.-]*sentry\.io'),  # Sentry DSN with a real key
    # A connection URL carrying a password to anything other than a local host.
    re.compile(
        r'\b(?:postgres(?:ql)?|redis|rediss|amqp|mysql)://[^:\s/@]+:[^@\s/]+@(?!(?:localhost|127\.0\.0\.1|db|redis)(?:[:/\s]|$))[\w.-]+'
    ),
)
ASSIGNMENT_RE = re.compile(
    r'(?i)\b(?:mandrill_key|mandrill_webhook_key|messagebird_key|auth_key|user_auth_key|webhook_auth_key|'
    r'api[_-]?key|secret[_-]?key|client[_-]?secret|access[_-]?token|auth[_-]?token|password|passwd|token)\b'
    r"\s*[:=]\s*(?:b?['\"])([^'\"\s]{16,})['\"]"
)
PLACEHOLDER_RE = re.compile(
    r'(?i)test|insecure|dummy|fake|example|placeholder|changeme|xxx|secret|<[^>]+>|\{[^}]+\}|\$\{?\w'
)
HOSTNAME_RE = re.compile(
    r'(?i)\b[\w-]+(?:\.[\w-]+)*\.(?:herokuapp\.com|herokudns\.com|amazonaws\.com|redis-cloud\.com|redislabs\.com|rlrcp\.com)\b'
)
DIFF_FILE_RE = re.compile(r'^\+\+\+ (?:b/)?(.*)$')
HUNK_RE = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@')
DEFAULT_DENYLIST = Path('.claude') / 'pii-denylist.txt'


class ScanError(Exception):
    """The scan could not run; callers must treat this as a failure, never as a clean result."""


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    rule: str
    detail: str = ''

    def __str__(self) -> str:
        return f'{self.path}:{self.line}: {self.rule}' + (f' ({self.detail})' if self.detail else '')


def load_denylist(path: Path | None = None) -> list[str]:
    """Literal terms to flag. A missing file is not an error: most clones will not have one."""
    path = path or DEFAULT_DENYLIST
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return []
    except OSError as e:
        raise ScanError(f'cannot read denylist {path}: {e}') from e
    return [line.strip().lower() for line in raw.splitlines() if line.strip() and not line.lstrip().startswith('#')]


def _email_hit(line: str) -> bool:
    for m in EMAIL_RE.finditer(URL_CREDENTIAL_RE.sub('://', line)):
        address, domain = m.group(0).lower(), m.group(2).lower()
        if address in ALLOWED_ADDRESSES or domain in ALLOWED_EMAIL_DOMAINS:
            continue
        return True
    return False


def _phone_hit(line: str) -> bool:
    stripped = ISO_DATETIME_RE.sub(' ', line)
    for m in PHONE_RE.finditer(stripped):
        raw = m.group(1)
        digits = re.sub(r'\D', '', raw)
        if not 10 <= len(digits) <= 15:
            continue
        if digits in ALLOWED_NUMBERS or (raw.startswith('+') and digits in ALLOWED_NUMBERS):
            continue
        has_plus = raw.startswith('+')
        has_separator = bool(re.search(r'[\s().-]', raw.strip()))
        trunk_zero = digits.startswith('0') and len(digits) in (10, 11)
        if has_plus or has_separator or trunk_zero:
            return True
    return False


def _secret_hit(line: str) -> bool:
    if any(r.search(line) for r in SECRET_RES):
        return True
    return any(not PLACEHOLDER_RE.search(m.group(1)) for m in ASSIGNMENT_RE.finditer(line))


def _denylist_hit(line: str, denylist: list[str]) -> str | None:
    lowered = line.lower()
    for i, term in enumerate(denylist, start=1):
        if term in lowered:
            return f'entry {i}'
    return None


def scan_line(line: str, denylist: list[str] | None = None) -> list[tuple[str, str]]:
    """(rule, detail) pairs for one line of text. Never includes the matched value."""
    found: list[tuple[str, str]] = []
    if _email_hit(line):
        found.append(('email', ''))
    if _phone_hit(line):
        found.append(('phone', ''))
    if _secret_hit(line):
        found.append(('secret', ''))
    if HOSTNAME_RE.search(line):
        found.append(('hostname', ''))
    if denylist and (detail := _denylist_hit(line, denylist)):
        found.append(('denylist', detail))
    return found


def scan_text(text: str, path: str = '<stdin>', denylist: list[str] | None = None) -> list[Hit]:
    hits = []
    for n, line in enumerate(text.splitlines(), start=1):
        hits.extend(Hit(path, n, rule, detail) for rule, detail in scan_line(line, denylist))
    return hits


def scan_diff(diff: str, denylist: list[str] | None = None) -> list[Hit]:
    """Scan only the added (`+`) lines of a unified diff, reporting new-file line numbers."""
    hits, path, new_no = [], '<diff>', 0
    for raw in diff.splitlines():
        if m := DIFF_FILE_RE.match(raw):
            path = m.group(1)
        elif m := HUNK_RE.match(raw):
            new_no = int(m.group(1))
        elif raw.startswith('+'):
            hits.extend(Hit(path, new_no, rule, detail) for rule, detail in scan_line(raw[1:], denylist))
            new_no += 1
        elif raw.startswith(' '):
            new_no += 1
        # a removed line consumes no new-file line number
    return hits


def git_diff(args: list[str], cwd: Path | None = None) -> str:
    r = subprocess.run(['git', 'diff', *args], capture_output=True, text=True, check=False, cwd=cwd)
    if r.returncode != 0:
        raise ScanError(f'git diff {" ".join(args)} failed: {r.stderr.strip()}')
    return r.stdout


def format_hits(hits: list[Hit]) -> str:
    return '\n'.join(str(h) for h in hits)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--text', action='store_true', help='scan FILE(s) or stdin as plain text')
    mode.add_argument('--diff', action='store_true', help='scan the added lines of a unified diff (FILE or stdin)')
    mode.add_argument('--staged', action='store_true', help='scan git diff --cached')
    mode.add_argument('--range', metavar='REV_RANGE', help='scan git diff REV_RANGE')
    p.add_argument('files', nargs='*')
    p.add_argument('--denylist', type=Path, default=None)
    args = p.parse_args(argv)

    try:
        denylist = load_denylist(args.denylist)
        hits: list[Hit] = []
        if args.text:
            if args.files:
                for f in args.files:
                    try:
                        hits.extend(scan_text(Path(f).read_text(errors='replace'), f, denylist))
                    except OSError as e:
                        raise ScanError(f'cannot read {f}: {e}') from e
            else:
                hits = scan_text(sys.stdin.read(), '<stdin>', denylist)
        elif args.diff:
            if args.files:
                try:
                    diff = Path(args.files[0]).read_text(errors='replace')
                except OSError as e:
                    raise ScanError(f'cannot read {args.files[0]}: {e}') from e
            else:
                diff = sys.stdin.read()
            hits = scan_diff(diff, denylist)
        elif args.staged:
            hits = scan_diff(git_diff(['--cached']), denylist)
        else:
            hits = scan_diff(git_diff([args.range]), denylist)
    except ScanError as e:
        print(f'pii_scan: could not run: {e}', file=sys.stderr)
        return 2

    if hits:
        print(format_hits(hits))
        print(
            f'pii_scan: {len(hits)} hit(s). Replace the value with a placeholder (foobar@example.org, '
            '<company_code>, billing-test) or, for a genuinely code-owned value, extend the allowlist in '
            '.claude/tools/pii_scan.py.',
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
