#!/usr/bin/env python3
"""Claude Code PreToolUse hook: refuse git/gh commands that would publish customer data.

Wired up in .claude/settings.json for the Bash tool. Reads the hook JSON on stdin and, for the
commands below, runs pii_scan.py over what the command is about to publish:

  git commit            the staged diff (plus the working tree with -a), anything a `git add` earlier
                        in the same command will stage, and the message (-m, -F file or heredoc)
  git push              the diff and commit messages of every commit not yet on the upstream
                        (or origin/master when the branch has no upstream), leaving out what
                        origin/master already holds
  gh pr create|edit|comment|review, gh issue create|edit|comment, gh api writes
                        the command text itself and any --body-file / -F file / --input file

A hit, or a scan that cannot run (unreadable body file, no ref to diff against, git failure),
returns a `deny` decision with the `path:line: rule` list. Text piped in on stdin cannot be seen,
so `-F -` and `--body-file -` are refused unless it comes from a heredoc in the command itself.
Read-only commands are never touched.
Standard library only; the scanner is imported from the same directory.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pii_scan  # noqa: E402

GH_PR_WRITE = {'create', 'edit', 'comment', 'review'}
GH_ISSUE_WRITE = {'create', 'edit', 'comment'}
GH_API_WRITE_FLAGS = {'-f', '-F', '--field', '--raw-field', '--input'}
SEGMENT_SPLIT = re.compile(r'\s*(?:&&|\|\||;|\||\n)\s*')
ENV_PREFIX = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*=')
HEREDOC = re.compile(r"""<<-?\s*(['"]?)(\w+)\1[^\n]*\n.*?^\s*\2\s*$""", re.DOTALL | re.MULTILINE)


def _deny(reason: str) -> None:
    print(
        json.dumps(
            {
                'hookSpecificOutput': {
                    'hookEventName': 'PreToolUse',
                    'permissionDecision': 'deny',
                    'permissionDecisionReason': reason,
                }
            }
        )
    )


def _segments(command: str) -> list[list[str]]:
    """Each simple command in a compound shell line as a token list (env prefixes dropped)."""
    out = []
    # a heredoc body is text, not commands; keep the `<<` marker so stdin can be traced to it
    command = HEREDOC.sub(lambda m: m.group(0).split('\n', 1)[0], command)
    for seg in SEGMENT_SPLIT.split(command):
        seg = seg.strip().lstrip('($').strip()
        if not seg:
            continue
        try:
            tokens = shlex.split(seg)
        except ValueError:
            tokens = seg.split()
        while tokens and ENV_PREFIX.match(tokens[0]):
            tokens.pop(0)
        if tokens:
            tokens[0] = os.path.basename(tokens[0])
            out.append(tokens)
    return out


def _flag_value(tokens: list[str], *flags: str) -> list[str]:
    values = []
    for i, t in enumerate(tokens):
        for flag in flags:
            if t == flag and i + 1 < len(tokens):
                values.append(tokens[i + 1])
            elif t.startswith(flag + '='):
                values.append(t[len(flag) + 1 :])
    return values


def _has_heredoc(tokens: list[str]) -> bool:
    return any(t.startswith('<<') for t in tokens)


def _stdin_error() -> pii_scan.ScanError:
    return pii_scan.ScanError('the text comes from stdin, which the hook cannot read; use a file or a heredoc')


def _run(args: list[str], cwd: Path) -> str:
    r = subprocess.run(args, capture_output=True, text=True, check=False, cwd=cwd)
    if r.returncode != 0:
        raise pii_scan.ScanError(f'`{" ".join(args)}` failed: {r.stderr.strip() or r.stdout.strip()}')
    return r.stdout


def _ref_exists(ref: str, cwd: Path) -> bool:
    r = subprocess.run(['git', 'rev-parse', '--verify', '-q', ref], capture_output=True, text=True, cwd=cwd)
    return r.returncode == 0


def _push_base(cwd: Path) -> str:
    for ref in ('@{push}', '@{upstream}', 'origin/master'):
        if _ref_exists(ref, cwd):
            return ref
    raise pii_scan.ScanError(
        'no upstream and no origin/master to compare the pushed commits against; run `git fetch origin master` first'
    )


def _file_lines(rev: str, path: str, cwd: Path) -> list[str]:
    r = subprocess.run(['git', 'show', f'{rev}:{path}'], capture_output=True, text=True, cwd=cwd)
    return r.stdout.splitlines() if r.returncode == 0 else []


def _not_on_master(hits: list[pii_scan.Hit], cwd: Path) -> list[pii_scan.Hit]:
    """Drop hits on lines origin/master already holds, e.g. those a merge of master brings in."""
    head, master = {}, {}
    kept = []
    for hit in hits:
        if hit.path not in head:
            head[hit.path] = _file_lines('HEAD', hit.path, cwd)
            master[hit.path] = set(_file_lines('origin/master', hit.path, cwd))
        lines = head[hit.path]
        if not (0 < hit.line <= len(lines) and lines[hit.line - 1] in master[hit.path]):
            kept.append(hit)
    return kept


def _scan_git_add(tokens: list[str], cwd: Path, denylist: list[str]) -> list[pii_scan.Hit]:
    """What `git add` will stage: this hook runs before the command, so the index does not show it yet."""
    paths = [t for t in tokens[2:] if not t.startswith('-')]
    hits = pii_scan.scan_diff(_run(['git', 'diff', 'HEAD', '--', *paths], cwd), denylist)
    for f in _run(['git', 'ls-files', '-o', '--exclude-standard', '--', *paths], cwd).splitlines():
        hits += _scan_file(cwd / f, denylist)
    return hits


def _scan_git_commit(tokens: list[str], command: str, cwd: Path, denylist: list[str]) -> list[pii_scan.Hit]:
    hits = pii_scan.scan_diff(_run(['git', 'diff', '--cached'], cwd), denylist)
    if any(t in ('-a', '--all') or (t.startswith('-') and not t.startswith('--') and 'a' in t[1:]) for t in tokens[2:]):
        hits += pii_scan.scan_diff(_run(['git', 'diff', 'HEAD'], cwd), denylist)
    for msg in _flag_value(tokens, '-m', '--message'):
        hits += pii_scan.scan_text(msg, '<commit message>', denylist)
    for f in _flag_value(tokens, '-F', '--file'):
        if f != '-':
            hits += _scan_file(cwd / f, denylist)
        elif _has_heredoc(tokens):
            hits += pii_scan.scan_text(command, '<commit message>', denylist)
        else:
            raise _stdin_error()
    return hits


def _scan_git_push(tokens: list[str], cwd: Path, denylist: list[str]) -> list[pii_scan.Hit]:
    base = _push_base(cwd)
    hits = pii_scan.scan_diff(_run(['git', 'diff', f'{base}...HEAD'], cwd), denylist)
    # A branch that merges master would otherwise push master's commits through the scan again;
    # they are public already, and a hit there cannot be fixed on the branch.
    not_master = []
    if base != 'origin/master' and _ref_exists('origin/master', cwd):
        hits = _not_on_master(hits, cwd)
        not_master = ['--not', 'origin/master']
    messages = _run(['git', 'log', '--format=%H%n%B', f'{base}..HEAD', *not_master], cwd)
    hits += pii_scan.scan_text(messages, '<commit messages>', denylist)
    return hits


def _scan_file(path: Path, denylist: list[str]) -> list[pii_scan.Hit]:
    try:
        return pii_scan.scan_text(path.read_text(errors='replace'), str(path), denylist)
    except OSError as e:
        raise pii_scan.ScanError(f'cannot read {path}: {e}') from e


def _scan_gh(tokens: list[str], command: str, cwd: Path, denylist: list[str]) -> list[pii_scan.Hit]:
    hits = pii_scan.scan_text(command, '<command>', denylist)
    files = _flag_value(tokens, '--body-file', '--input')
    if tokens[1:2] == ['api']:
        files += [v[1:] for v in _flag_value(tokens, '-F', '--field') if v.startswith('@')]
        files += [v.split('=@', 1)[1] for v in _flag_value(tokens, '-F', '--field') if '=@' in v]
    else:
        files += _flag_value(tokens, '-F')
    for f in files:
        if f != '-':
            hits += _scan_file(cwd / f, denylist)
        elif not _has_heredoc(tokens):  # a heredoc is part of the command text scanned above
            raise _stdin_error()
    if tokens[1:3] == ['pr', 'create'] and any(t in tokens for t in ('--fill', '--fill-first', '--fill-verbose')):
        # the body is built from the commit messages, so scan the commits like a push would
        hits += _scan_git_push(tokens, cwd, denylist)
    return hits


def _is_gh_write(tokens: list[str]) -> bool:
    if tokens[1:2] == ['pr']:
        return tokens[2:3] != [] and tokens[2] in GH_PR_WRITE
    if tokens[1:2] == ['issue']:
        return tokens[2:3] != [] and tokens[2] in GH_ISSUE_WRITE
    if tokens[1:2] == ['api']:
        methods = [m.upper() for m in _flag_value(tokens, '-X', '--method')]
        return bool(methods and methods[0] != 'GET') or any(
            t in GH_API_WRITE_FLAGS or t.startswith('--input=') for t in tokens
        )
    return False


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    if payload.get('tool_name') != 'Bash':
        return 0
    command = (payload.get('tool_input') or {}).get('command') or ''
    cwd = Path(payload.get('cwd') or os.environ.get('CLAUDE_PROJECT_DIR') or os.getcwd())

    try:
        denylist = pii_scan.load_denylist(cwd / pii_scan.DEFAULT_DENYLIST)
        hits: list[pii_scan.Hit] = []
        scanned = []
        segments = []
        for tokens in _segments(command):
            git_cwd = cwd
            if tokens[0] == 'git' and tokens[1:2] == ['-C'] and len(tokens) > 2:
                git_cwd = cwd / tokens[2]
                tokens = ['git', *tokens[3:]]
            segments.append((tokens, git_cwd))
        commits = any(t[0] == 'git' and t[1:2] == ['commit'] for t, _ in segments)
        for tokens, git_cwd in segments:
            if tokens[0] == 'git' and tokens[1:2] == ['add'] and commits:
                hits += _scan_git_add(tokens, git_cwd, denylist)
                scanned.append('git add')
            elif tokens[0] == 'git' and tokens[1:2] == ['commit']:
                hits += _scan_git_commit(tokens, command, git_cwd, denylist)
                scanned.append('git commit')
            elif tokens[0] == 'git' and tokens[1:2] == ['push']:
                hits += _scan_git_push(tokens, git_cwd, denylist)
                scanned.append('git push')
            elif tokens[0] == 'gh' and _is_gh_write(tokens):
                hits += _scan_gh(tokens, command, cwd, denylist)
                scanned.append(' '.join(tokens[:3]))
    except pii_scan.ScanError as e:
        _deny(f'PII guard: the scan could not run, so the command is blocked ({e}). Fix that and retry.')
        return 0
    except Exception as e:  # a scanner bug must never silently let the command through
        _deny(f'PII guard: the scan crashed ({type(e).__name__}: {e}), so the command is blocked.')
        return 0

    if hits:
        unique = list(dict.fromkeys(str(h) for h in hits))
        _deny(
            f'PII guard: {", ".join(scanned)} would publish text that looks like customer data '
            f'({len(unique)} hit(s), value not shown):\n'
            + '\n'.join(unique)
            + '\n\nThis repository is public. Replace each value with a placeholder (foobar@example.org, '
            '<company_code>, billing-test), use an id instead of a name or address, and retry. If the value is '
            'genuinely code-owned, extend the allowlist in .claude/tools/pii_scan.py. Reproduce with '
            '`python3 .claude/tools/pii_scan.py --staged` (or `--range origin/master...HEAD`).'
        )
    return 0


if __name__ == '__main__':
    sys.exit(main())
