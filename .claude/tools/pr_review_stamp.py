#!/usr/bin/env python3
"""Record and check the last fully reviewed commit on a PR, in the PR description.

A PR is reviewed with /full-review before it is handed to a human, but a wording tweak should not
force a full re-review. The review skills stamp the commit they reviewed into the PR body and
`check` says whether HEAD is still covered.

  pr_review_stamp.py stamp <pr> <sha> --verdict APPROVED|CHANGES_REQUESTED|DISCUSSION_NEEDED   # upsert
  pr_review_stamp.py check <pr>        # is HEAD reviewed? exit 0 yes (APPROVED) / 1 stale or not approved / 2 none
  pr_review_stamp.py show <pr>         # the stamp as JSON ({} when there is none)

The marker is one block at the TOP of the PR body; the rest of the body is preserved:

  <!-- reviewed-up-to: <full-sha> | verdict: <V> | <date> -->
  > Reviewed up to `<short>`: **<V>**, <date> (`/full-review`). Re-run after non-trivial changes.

The body is edited through the REST API (`gh api -X PATCH .../pulls/{n}`), not `gh pr edit`, which
requests a GraphQL field GitHub rejects on repositories with classic Projects and then exits
non-zero without saving. Every `gh` call is checked; a failure aborts loudly.

Targets tutorcruncher/morpheus; override with PR_REVIEW_REPO=<owner/repo>. Requires `gh` logged in.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date

REPO = os.environ.get('PR_REVIEW_REPO', 'tutorcruncher/morpheus')
VERDICTS = ('APPROVED', 'CHANGES_REQUESTED', 'DISCUSSION_NEEDED')
# The comment line plus the visible quote line after it (and a trailing blank line). Not DOTALL:
# each part is line-bounded, otherwise `>.*` would eat the rest of the body.
MARKER_RE = re.compile(r'<!-- reviewed-up-to:.*?-->\n(?:>.*\n)?\n?')
PARSE_RE = re.compile(r'reviewed-up-to:\s*(\S+)\s*\|\s*verdict:\s*(\S+?)(?:\s*\|\s*(\S+))?\s*-->')
# Anyone who can open a PR can edit its body, and callers interpolate the stamped sha into git
# commands. Anything that is not a plain hex object name counts as no stamp at all.
SHA_RE = re.compile(r'^[0-9a-f]{7,40}$')


def gh(args: list[str]) -> str:
    r = subprocess.run(['gh', *args], capture_output=True, text=True, check=False)
    if r.returncode != 0:
        sys.exit(f'`gh {" ".join(args)}` failed (exit {r.returncode}): {r.stderr.strip()}')
    return r.stdout.strip()


def git(args: list[str]) -> str:
    # Unchecked on purpose: check() uses this for a best-effort local diff and expects '' when the
    # PR branch is not fetched locally.
    return subprocess.run(['git', *args], capture_output=True, text=True, check=False).stdout.strip()


def pr_body(pr: str) -> str:
    return gh(['pr', 'view', pr, '--repo', REPO, '--json', 'body', '--jq', '.body'])


def pr_head(pr: str) -> str:
    return gh(['pr', 'view', pr, '--repo', REPO, '--json', 'headRefOid', '--jq', '.headRefOid'])


def stamp(pr: str, sha: str, verdict: str) -> int:
    if not SHA_RE.match(sha):
        sys.exit(f'sha must be a hex object name, got {sha!r}')
    today = date.today().isoformat()
    block = (
        f'<!-- reviewed-up-to: {sha} | verdict: {verdict} | {today} -->\n'
        f'> Reviewed up to `{sha[:9]}`: **{verdict}**, {today} (`/full-review`). Re-run after non-trivial changes.\n\n'
    )
    body = MARKER_RE.sub('', pr_body(pr)).lstrip('\n')
    gh(['api', '-X', 'PATCH', f'repos/{REPO}/pulls/{pr}', '-f', f'body={block + body}'])
    print(f'stamped PR #{pr}: reviewed-up-to {sha[:9]} ({verdict})')
    return 0


def show(pr: str) -> int:
    m = PARSE_RE.search(pr_body(pr))
    if not m or not SHA_RE.match(m.group(1)):
        print('{}')
        return 0
    print(json.dumps({'sha': m.group(1), 'verdict': m.group(2), 'date': m.group(3)}))
    return 0


def check(pr: str) -> int:
    body, head = pr_body(pr), pr_head(pr)
    m = PARSE_RE.search(body)
    if not m or not SHA_RE.match(m.group(1)):
        print(f'PR #{pr}: no review stamp. Run /full-review before assigning it out.')
        return 2
    reviewed, verdict = m.group(1), m.group(2)
    if reviewed == head:
        print(f'PR #{pr}: HEAD ({head[:9]}) is fully reviewed, verdict {verdict}.')
        return 0 if verdict.upper() == 'APPROVED' else 1
    print(f'PR #{pr}: reviewed up to {reviewed[:9]} ({verdict}) but HEAD is {head[:9]}.')
    log = git(['log', '--oneline', f'{reviewed}..{head}'])
    if log:
        print('\nUnreviewed commits since the stamp:\n' + log)
        print('\nDiff stat:\n' + git(['diff', '--stat', f'{reviewed}..{head}']))
        print('\nIf those are trivial (wording, comments), re-stamp; otherwise re-run /full-review.')
    else:
        print('(could not diff locally: fetch the branch, or re-run /full-review)')
    return 1


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    s = sub.add_parser('stamp')
    s.add_argument('pr')
    s.add_argument('sha')
    s.add_argument('--verdict', required=True, choices=VERDICTS)
    c = sub.add_parser('check')
    c.add_argument('pr')
    w = sub.add_parser('show')
    w.add_argument('pr')
    args = p.parse_args()
    if args.cmd == 'stamp':
        sys.exit(stamp(args.pr, args.sha, args.verdict))
    if args.cmd == 'show':
        sys.exit(show(args.pr))
    sys.exit(check(args.pr))


if __name__ == '__main__':
    main()
