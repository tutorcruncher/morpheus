"""Tests for pii_scan.py and the PreToolUse guard hook. Synthetic samples only.

Run with `uv run pytest .claude/tools/test_pii_scan.py`. CI does not run this file (it runs
`tests/` only), so run it by hand after changing the scanner.

Every sample that must trip the scanner is assembled at runtime (string concatenation) so that
the scanner's own diff never contains a literal that looks like customer data.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import pii_scan  # noqa: E402

AT = '@'
FAKE_EMAIL = 'person' + AT + 'realdomain.test'
FAKE_PHONE = '+44 ' + '7700 ' + '900' + '123'
FAKE_AWS_KEY = 'AKIA' + 'ABCDEFGHIJKLMNOP'


def _diff(*added: str, path: str = 'app/foo.py', removed: tuple[str, ...] = ()) -> str:
    plus = ''.join(f'+{line}\n' for line in added)
    minus = ''.join(f'-{line}\n' for line in removed)
    return f'diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1,{len(removed)} +1,{len(added)} @@\n{minus}{plus}'


def test_unallowlisted_email_hits():
    hits = pii_scan.scan_text(f'to = "{FAKE_EMAIL}"', 'x.py')
    assert [(h.path, h.line, h.rule) for h in hits] == [('x.py', 1, 'email')]
    assert FAKE_EMAIL not in str(hits)


def test_allowlisted_addresses_pass():
    text = "to = ['foobar@example.org', 'testing@example.com', 'foobar@testing.com', 'tom@tutorcruncher.com']"
    assert pii_scan.scan_text(text, 'x.py') == []


def test_package_version_pins_pass():
    assert pii_scan.scan_text('https://cdn.jsdelivr.net/npm/xterm' + AT + '5.3.0/lib/xterm.js', 'x.sh') == []
    assert [h.rule for h in pii_scan.scan_text('person' + AT + 'mail.co.uk', 'x.py')] == ['email']


def test_phone_number_hits():
    hits = pii_scan.scan_text(f'number = "{FAKE_PHONE}"', 'x.py')
    assert [h.rule for h in hits] == ['phone']


def test_existing_test_numbers_and_dates_pass():
    text = "n = '+44 7891 123856'\nts = '2026-10-30T12:34:56+00:00'\nus = settings.us_send_number  # 15744445663"
    assert pii_scan.scan_text(text, 'x.py') == []


def test_secret_shapes_hit():
    url = 'postgresql://app:' + 'hunter2hunter2' + AT + 'db.internal:5432/morpheus'
    hits = pii_scan.scan_text(f'{FAKE_AWS_KEY}\nDATABASE_URL={url}\n', 'x.py')
    assert [(h.line, h.rule) for h in hits] == [(1, 'secret'), (2, 'secret')]


def test_placeholder_assignments_pass():
    text = "auth_key = 'insecure'\nmandrill_key = 'testing-key'\nDATABASE_URL=postgresql://postgres@localhost:5432/morpheus"
    assert pii_scan.scan_text(text, 'x.py') == []


def test_provider_hostname_hits():
    host = 'morpheus-prod-1234' + '.herokuapp.com'
    assert [h.rule for h in pii_scan.scan_text(f'curl https://{host}/', 'x.py')] == ['hostname']


def test_denylist_hit_does_not_echo_term(tmp_path):
    deny = tmp_path / 'deny.txt'
    deny.write_text('# comment\n\nAcmeTutors\nacme-london\n')
    hits = pii_scan.scan_text('company = "acmetutors"', 'x.py', denylist=pii_scan.load_denylist(deny))
    assert [(h.rule, h.detail) for h in hits] == [('denylist', 'entry 1')]
    assert 'acme' not in pii_scan.format_hits(hits).lower()


def test_missing_denylist_is_not_an_error(tmp_path):
    assert pii_scan.load_denylist(tmp_path / 'absent.txt') == []


def test_diff_scans_added_lines_only():
    diff = _diff('clean = 1', removed=(f'old = "{FAKE_EMAIL}"',))
    assert pii_scan.scan_diff(diff) == []
    diff = _diff('clean = 1', f'to = "{FAKE_EMAIL}"', path='tests/test_x.py')
    hits = pii_scan.scan_diff(diff)
    assert [(h.path, h.line, h.rule) for h in hits] == [('tests/test_x.py', 2, 'email')]


def test_cli_exit_codes(tmp_path):
    tool = Path(__file__).parent / 'pii_scan.py'
    clean = subprocess.run(
        [sys.executable, tool, '--text'], input='nothing here\n', capture_output=True, text=True, cwd=tmp_path
    )
    assert clean.returncode == 0, clean.stderr
    dirty = subprocess.run(
        [sys.executable, tool, '--text'], input=f'{FAKE_EMAIL}\n', capture_output=True, text=True, cwd=tmp_path
    )
    assert dirty.returncode == 1
    assert '<stdin>:1: email' in dirty.stdout
    assert FAKE_EMAIL not in dirty.stdout + dirty.stderr
    missing = subprocess.run(
        [sys.executable, tool, '--text', str(tmp_path / 'nope.md')], capture_output=True, text=True
    )
    assert missing.returncode == 2


# --- guard hook ---------------------------------------------------------------------------------

HOOK = Path(__file__).parent / 'pii_guard_hook.py'


def _run_hook(command: str, cwd: Path) -> dict:
    payload = json.dumps({'tool_name': 'Bash', 'tool_input': {'command': command}, 'cwd': str(cwd)})
    r = subprocess.run([sys.executable, HOOK], input=payload, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout) if r.stdout.strip() else {}


def _decision(out: dict) -> str | None:
    return out.get('hookSpecificOutput', {}).get('permissionDecision')


@pytest.fixture
def repo(tmp_path):
    subprocess.run(['git', 'init', '-q', '-b', 'master', tmp_path], check=True)
    subprocess.run(['git', '-C', tmp_path, 'config', 'user.email', 'ci@example.org'], check=True)
    subprocess.run(['git', '-C', tmp_path, 'config', 'user.name', 'ci'], check=True)
    (tmp_path / 'a.txt').write_text('base\n')
    subprocess.run(['git', '-C', tmp_path, 'add', '.'], check=True)
    subprocess.run(['git', '-C', tmp_path, 'commit', '-qm', 'base'], check=True)
    return tmp_path


def test_hook_ignores_read_only_commands(repo):
    for cmd in ('git diff --cached', 'gh pr view 1 --json body', 'gh api repos/o/r/pulls/1', 'ls -la'):
        assert _run_hook(cmd, repo) == {}


def test_hook_blocks_commit_with_pii_in_staged_diff(repo):
    (repo / 'a.txt').write_text(f'contact {FAKE_EMAIL}\n')
    subprocess.run(['git', '-C', repo, 'add', 'a.txt'], check=True)
    out = _run_hook('git commit -m "add contact"', repo)
    assert _decision(out) == 'deny'
    reason = out['hookSpecificOutput']['permissionDecisionReason']
    assert 'a.txt:1: email' in reason
    assert FAKE_EMAIL not in reason


def test_hook_allows_clean_commit(repo):
    (repo / 'a.txt').write_text('contact foobar@example.org\n')
    subprocess.run(['git', '-C', repo, 'add', 'a.txt'], check=True)
    assert _run_hook('git commit -m "add contact"', repo) == {}


def test_hook_blocks_commit_message_with_pii(repo):
    (repo / 'a.txt').write_text('fine\n')
    subprocess.run(['git', '-C', repo, 'add', 'a.txt'], check=True)
    out = _run_hook(f'git commit -m "reported by {FAKE_EMAIL}"', repo)
    assert _decision(out) == 'deny'


def test_hook_blocks_gh_body_text_and_body_file(repo):
    out = _run_hook(f'gh pr comment 1 --body "CLAUDE SAYS: ping {FAKE_EMAIL}"', repo)
    assert _decision(out) == 'deny'
    body = repo / 'body.md'
    body.write_text(f'row for {FAKE_PHONE}\n')
    out = _run_hook(f'gh pr create --title t --body-file {body}', repo)
    assert _decision(out) == 'deny'
    assert 'body.md' in out['hookSpecificOutput']['permissionDecisionReason']
    body.write_text('all placeholders here\n')
    assert _run_hook(f'gh issue comment 1 --body-file {body}', repo) == {}


def test_hook_blocks_when_body_file_unreadable(repo):
    out = _run_hook(f'gh pr edit 1 --body-file {repo / "missing.md"}', repo)
    assert _decision(out) == 'deny'
    assert 'could not run' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_blocks_gh_api_writes_only(repo):
    assert _run_hook('gh api repos/o/r/issues/1/comments', repo) == {}
    out = _run_hook(f'gh api repos/o/r/issues/1/comments -f body="{FAKE_EMAIL}"', repo)
    assert _decision(out) == 'deny'


def test_hook_scans_commits_being_pushed(repo):
    remote = repo.parent / 'remote.git'
    subprocess.run(['git', 'init', '-q', '--bare', remote], check=True)
    subprocess.run(['git', '-C', repo, 'remote', 'add', 'origin', remote], check=True)
    subprocess.run(['git', '-C', repo, 'push', '-q', '-u', 'origin', 'master'], check=True)
    (repo / 'a.txt').write_text(f'{FAKE_PHONE}\n')
    subprocess.run(['git', '-C', repo, 'commit', '-qam', 'bad'], check=True)
    out = _run_hook('git push', repo)
    assert _decision(out) == 'deny'
    assert 'a.txt:1: phone' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_blocks_push_when_range_cannot_be_resolved(repo):
    # No upstream and no origin/master: the scanner has nothing to compare against, so it must
    # refuse rather than wave the push through.
    out = _run_hook('git push origin HEAD', repo)
    assert _decision(out) == 'deny'
    assert 'could not run' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_scans_commit_message_from_heredoc(repo):
    (repo / 'a.txt').write_text('fine\n')
    subprocess.run(['git', '-C', repo, 'add', 'a.txt'], check=True)
    assert _run_hook("git commit -F - <<'MSG'\nclean message\nMSG", repo) == {}
    assert _decision(_run_hook(f"git commit -F - <<'MSG'\nreported by {FAKE_EMAIL}\nMSG", repo)) == 'deny'


def test_hook_blocks_body_from_stdin_it_cannot_read(repo):
    for cmd in ('cat notes.txt | git commit -F -', 'cat notes.txt | gh pr comment 1 --body-file -'):
        out = _run_hook(cmd, repo)
        assert _decision(out) == 'deny'
        assert 'stdin' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_does_not_treat_heredoc_text_as_commands(repo):
    assert _run_hook("cat > notes.txt <<'EOF'\ngit commit -F missing.txt\nEOF", repo) == {}


def test_hook_scans_new_file_added_in_the_same_command(repo):
    (repo / 'new.py').write_text(f'to = "{FAKE_EMAIL}"\n')
    for cmd in ("git add new.py && git commit -m 'msg'", "git add -A && git commit -m 'msg'"):
        out = _run_hook(cmd, repo)
        assert _decision(out) == 'deny'
        assert 'new.py:1: email' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_scans_tracked_change_added_in_the_same_command(repo):
    (repo / 'a.txt').write_text(f'contact {FAKE_EMAIL}\n')
    out = _run_hook("git add a.txt && git commit -m 'msg'", repo)
    assert _decision(out) == 'deny'
    assert 'a.txt:1: email' in out['hookSpecificOutput']['permissionDecisionReason']


def test_hook_ignores_files_not_being_added(repo):
    (repo / 'other.py').write_text(f'to = "{FAKE_EMAIL}"\n')
    (repo / 'clean.py').write_text('x = 1\n')
    assert _run_hook("git add clean.py && git commit -m 'msg'", repo) == {}


def test_hook_leaves_add_alone_without_a_commit(repo):
    (repo / 'new.py').write_text(f'to = "{FAKE_EMAIL}"\n')
    assert _run_hook('git add new.py', repo) == {}


def test_hook_handles_git_dash_c(repo):
    (repo / 'a.txt').write_text(f'contact {FAKE_EMAIL}\n')
    subprocess.run(['git', '-C', repo, 'add', 'a.txt'], check=True)
    out = _run_hook(f"git -C {repo} commit -m 'msg'", repo.parent)
    assert _decision(out) == 'deny'
