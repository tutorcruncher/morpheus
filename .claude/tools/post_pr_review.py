#!/usr/bin/env python3
"""Post /full-review findings to a GitHub PR as one review: a summary plus inline comments.

  post_pr_review.py <PR> --findings findings.json --summary summary.md [--event COMMENT|REQUEST_CHANGES] [--dry-run]

findings.json: [{"path": "app/x.py", "line": 12, "tier": "BLOCKER"|"SUGGESTED", "body": "..."}, ...]

GitHub rejects the whole review (422) if any inline comment sits on a line that is not in the diff,
so only findings whose `path:line` is a `+` or context line on the RIGHT side of the diff go inline;
the rest are folded into the summary under a heading that says why. If the inline POST still fails,
a body-only review is posted so the review always lands. `--dry-run` prints INLINE / SUMMARY per
finding and posts nothing; run it first and re-anchor anything that fell to SUMMARY.

Every comment leads with `CLAUDE SAYS:` (the convention for Claude-authored GitHub text). The
summary and every inline body are run through pii_scan.py before anything is sent; a hit aborts
the post, because a review comment on a public repository is as public as the code.

`APPROVE` is deliberately not offered: a bot does not formally approve a PR. Targets
tutorcruncher/morpheus; override with PR_REVIEW_REPO=<owner/repo>. REST API only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pii_scan  # noqa: E402

REPO = os.environ.get('PR_REVIEW_REPO', 'tutorcruncher/morpheus')
CLAUDE_SAYS = '**CLAUDE SAYS:**'
HUNK = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@')
LABEL_RE = re.compile(r'^\*\*\[[A-Z]+\]\*\*\s*')


def gh(args: list[str], stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(['gh', *args], capture_output=True, text=True, input=stdin, check=False)


def commentable_lines(pr: str) -> set[tuple[str, int]]:
    """(path, new-file line) pairs GitHub accepts an inline comment on (RIGHT side of the diff)."""
    r = gh(['pr', 'diff', str(pr), '--repo', REPO])
    if r.returncode != 0:
        sys.exit(f'post_pr_review: `gh pr diff {pr}` failed: {r.stderr.strip()}')
    lines, path, new_no = set(), None, 0
    for raw in r.stdout.splitlines():
        if raw.startswith('+++ b/'):
            path = raw[6:]
        elif m := HUNK.match(raw):
            new_no = int(m.group(1))
        elif path and ((raw.startswith('+') and not raw.startswith('+++')) or raw.startswith(' ')):
            lines.add((path, new_no))
            new_no += 1
        # a removed line consumes no new-file line number
    return lines


def _label(f: dict) -> str:
    return f'**[{(f.get("tier") or "NOTE").upper()}]**'


def _claude_says(body: str) -> str:
    """Idempotent: a body that already opens with the line is returned unchanged."""
    return body if body.lstrip().startswith(CLAUDE_SAYS) else f'{CLAUDE_SAYS}\n\n{body}'


def _without_claude_says(body: str) -> str:
    stripped = body.lstrip()
    if stripped.startswith(CLAUDE_SAYS):
        return stripped[len(CLAUDE_SAYS) :].lstrip('\n').strip()
    return body


def _bullet(label: str, loc: str, body: str) -> str:
    """One summary list item; continuation lines are indented so markdown keeps them in the bullet."""
    head = f'{label} {loc} — {body}'.strip() if loc else f'{label} {body}'.strip()
    first, *rest = head.split('\n')
    return '\n'.join([f'- {first}', *[f'  {line}' if line.strip() else '' for line in rest]])


def _is_pending_review_conflict(r: subprocess.CompletedProcess) -> bool:
    return r.returncode != 0 and 'one pending review per pull request' in (r.stdout or '') + (r.stderr or '')


def _drop_pending_reviews(pr: str) -> None:
    """A previous run that died mid-post leaves a PENDING review; GitHub allows one per user per PR."""
    listed = gh(['api', f'repos/{REPO}/pulls/{pr}/reviews?per_page=100', '--paginate'])
    try:
        reviews = json.loads(listed.stdout)
    except (json.JSONDecodeError, TypeError):
        return
    for review in reviews:
        if review.get('state') == 'PENDING':
            gh(['api', '-X', 'DELETE', f'repos/{REPO}/pulls/{pr}/reviews/{review["id"]}'])
            print(f'post_pr_review: cleared stale pending review {review["id"]}', file=sys.stderr)


def _pii_gate(summary: str, inline: list[dict]) -> None:
    denylist = pii_scan.load_denylist()
    hits = pii_scan.scan_text(summary, 'summary', denylist)
    for c in inline:
        hits += pii_scan.scan_text(c['body'], f'inline {c["path"]}:{c["line"]}', denylist)
    if hits:
        sys.exit(
            'post_pr_review: REFUSED, the review text looks like it contains customer data (value not shown):\n'
            + pii_scan.format_hits(hits)
            + '\nReplace the value with a placeholder or an id and run again.'
        )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('pr')
    p.add_argument('--findings', required=True)
    p.add_argument('--summary', required=True)
    p.add_argument('--event', default='COMMENT', choices=['COMMENT', 'REQUEST_CHANGES'])
    p.add_argument('--dry-run', action='store_true', help='print where each finding would land; post nothing')
    args = p.parse_args()

    findings = json.loads(Path(args.findings).read_text())
    summary = Path(args.summary).read_text().strip()
    ok = commentable_lines(args.pr)
    changed_files = {path for path, _ in ok}

    inline, elsewhere, unchanged_line, no_line = [], [], [], []
    for f in findings:
        path, line = f.get('path'), f.get('line')
        label = _label(f)
        body = LABEL_RE.sub('', (f.get('body') or '').strip(), count=1)
        if path and isinstance(line, int) and (path, line) in ok:
            inline.append({'path': path, 'line': line, 'side': 'RIGHT', 'body': _claude_says(f'{label} {body}')})
            continue
        entry = _bullet(label, f'`{path}:{line}`' if path else '', body)
        if not path or not isinstance(line, int):
            no_line.append(entry)
        elif path not in changed_files:
            elsewhere.append(entry)
        else:
            unchanged_line.append(entry)

    if args.dry_run:
        for c in inline:
            print(f'INLINE   {c["path"]}:{c["line"]}')
        for group, name in (
            (elsewhere, 'file not in diff'),
            (unchanged_line, 'line not in diff'),
            (no_line, 'no path/line'),
        ):
            for entry in group:
                print(f'SUMMARY  ({name}) {entry.splitlines()[0][:100]}')
        print(
            f'dry run: {len(inline)} inline, {len(elsewhere) + len(unchanged_line) + len(no_line)} in summary. Nothing posted.'
        )
        _pii_gate(summary, inline)
        print('pii scan: clean')
        return

    _pii_gate(summary, inline)

    body = f'{CLAUDE_SAYS}\n\n{summary}'
    if elsewhere:
        body += (
            '\n\n### Pre-existing, in files this PR does not touch\n'
            '<sub>Not introduced here and not blocking it. Recorded so they are not lost.</sub>\n\n'
            + '\n'.join(elsewhere)
        )
    if unchanged_line:
        body += (
            '\n\n### In changed files, on lines this PR does not change\n'
            '<sub>GitHub only accepts an inline comment on a line in the diff.</sub>\n\n' + '\n'.join(unchanged_line)
        )
    if no_line:
        body += (
            '\n\n### Not about one line\n<sub>Missing tests, or a point about the change as a whole.</sub>\n\n'
            + '\n'.join(no_line)
        )

    head_r = gh(['pr', 'view', str(args.pr), '--repo', REPO, '--json', 'headRefOid', '--jq', '.headRefOid'])
    if head_r.returncode != 0:
        sys.exit(f'post_pr_review: `gh pr view {args.pr}` failed: {head_r.stderr.strip()}')
    head = head_r.stdout.strip()

    def post(comments: list) -> subprocess.CompletedProcess:
        payload: dict = {'commit_id': head, 'event': args.event, 'body': body}
        if comments:
            payload['comments'] = comments
        return gh(
            ['api', f'repos/{REPO}/pulls/{args.pr}/reviews', '--method', 'POST', '--input', '-'],
            stdin=json.dumps(payload),
        )

    r = post(inline)
    if _is_pending_review_conflict(r):
        _drop_pending_reviews(args.pr)
        r = post(inline)
    if r.returncode != 0 and inline:
        extra = '\n'.join(f'- {_without_claude_says(c["body"])} (`{c["path"]}:{c["line"]}`)' for c in inline)
        body += '\n\n### Inline findings (could not attach to the diff)\n' + extra
        r = post([])
    if r.returncode != 0:
        detail = (r.stdout or '').strip() or (r.stderr or '').strip()
        sys.exit(f'post_pr_review: FAILED to post review:\n{detail}')

    url = json.loads(r.stdout).get('html_url', '') if r.stdout.strip().startswith('{') else ''
    n_body = len(elsewhere) + len(unchanged_line) + len(no_line)
    print(
        f'posted {args.event} review to PR #{args.pr}: {len(inline)} inline, {n_body} in summary '
        f'({len(elsewhere)} pre-existing, {len(unchanged_line)} unchanged-line, {len(no_line)} no-line). {url}'
    )


if __name__ == '__main__':
    main()
