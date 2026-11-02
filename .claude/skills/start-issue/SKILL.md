---
name: start-issue
description: >-
  Morpheus-specific (tutorcruncher/morpheus). Take a GitHub issue (or a pasted spec), classify it
  (bug, small feature, large feature, Dependabot), diagnose a bug with no write-up via
  /bug-investigate, consult the architect for a feature, write a plan that answers the four
  Morpheus deploy questions, STOP for approval, then implement on a branch off fresh origin/master
  (bugs test-first), run scoped tests and make lint, loop /full-review until APPROVED, draft the PR
  description and STOP. Pushes, opens the PR and runs /pr-uat only on an explicit go. Use when the user runs
  /start-issue, says "start this issue", "implement this ticket", or hands over an issue.
---

# Start Issue

Issue in, reviewed branch and draft PR description out. Two stops: after the plan, and after the
review loop. Finalize (push, PR) runs only when the user says go.

## Hard rules

- **Never push or merge to `master`.** All work goes to a feature branch and a PR; merging is the
  user's decision.
- **Customer data never leaves the diagnosis.** This repository is public. Nothing read from Logfire,
  Sentry, a database or a provider appears in code, tests, fixtures, commit messages, branch names,
  PR bodies or comments. Ids and placeholders (`foobar@example.org`, `<company_code>`,
  `billing-test`) only. Every subagent brief restates this. The PII scan runs before finalize.
- **Bugs are fixed test-first.** The failing test is written and seen to fail for the reported
  reason before the fix.
- **Tests are scoped.** `uv run pytest tests/<file>` for the files you change; never `make test`
  while another session may share the test database and Redis (tests truncate and flush).
- **Never run the Heroku CLI.** Ask the user.
- **Customer-facing text** (error messages, template copy, labels) uses plain words, never internal
  identifiers or enum values.

## 1. Get the issue

`gh issue view <n> --repo tutorcruncher/morpheus --comments`. A pasted spec or a Sentry id is
accepted too; say which you were given. Read every comment: decisions often live there.

## 2. Classify

- **Bug** with a confirmed root cause in the issue: test-first plan.
- **Bug** without one: run `/bug-investigate` first (its Step 5 stop replaces this skill's plan
  stop until the diagnosis is confirmed), then plan the fix from the confirmed cause.
- **Small feature**: one plan, one PR.
- **Large feature**: phases with an exit check each; propose a split into PRs before planning in
  detail and let the user choose.
- **Dependabot**: `gh pr view <n>` for the bump, read the dependency's changelog between the two
  versions for breaking changes that touch Morpheus (FastAPI, SQLModel, SQLAlchemy, Celery, httpx,
  pydantic, redis, mistune, chevron, libsass), run the scoped tests that exercise it, and report;
  no plan stop needed unless something breaks.

Restate the issue in a short paragraph: goal, scope, what done looks like. Ask when the spec is
thin; one round of questions, before planning.

## 3. Architect consult (features)

For a feature with a design choice, launch the `architect` agent with the issue, the restatement
and the PII rule, asking for the approach, the files involved and answers to the four questions
below. For a bug or a change with no design choice, draft the plan yourself.

## 4. Plan, then STOP

Write `${WT}/.claude/plans/<n>-<slug>.md` (gitignored) and present it. The plan must answer, each
with yes or no and the detail:

1. **Production SQL?** Any change to `app/messages/models.py`, `bootstrap.sql` or
   `post_bootstrap.sql` needs the exact one-off statements (`ALTER TABLE`, `CREATE INDEX
   CONCURRENTLY`, `ALTER TYPE ... ADD VALUE`, `DROP MATERIALIZED VIEW`) for the PR body.
   `create_all` only creates missing tables.
2. **Celery signature change?** New task, renamed task, new or removed argument: how the previous
   release's worker and the new one coexist during the deploy (defaults, kept names).
3. **TC2 contract change?** Status codes, response keys, validation on an endpoint TC2 calls, and
   the impact statement for the PR body.
4. **Render version bump?** Any change under `app/render/` bumps `VERSION` in
   `packaging/morpheus-mail/setup.py`.

Plus: files to change, tests to add (file and assertion), and for a bug the failing test first.
Wait for approval.

## 5. Branch

```bash
git fetch origin master
git checkout -b <fix|feat|chore>/<n>-<slug> origin/master
```

Never use bare `git stash`; the stash is shared across worktrees.

## 6. Implement

- Bug: write the test, run `uv run pytest tests/<file> -k <test>`, see it fail for the right reason,
  then fix.
- Feature: tests alongside the code; every new branch, `except` and early return has a test.
- Fixtures from `tests/conftest.py`; a new provider call gets a route in `tests/dummy_server.py`;
  assert status code and body; placeholders only.
- Errors through `app/common/api/errors.py`; `Depends(get_db)`; `db.close()` before slow work on
  high-volume endpoints; JSON-serialisable task arguments; `AdminAuth` on TC2-facing routers.
- `uv run pytest tests/<changed files>` and `make lint` green before review. Report counts.
- Commit in logical steps with the issue number (`Fixes #<n>` on the final one).

## 7. Review loop

Run `/full-review` (no PR yet, so it reports locally). Fix every BLOCKER, consider each SUGGESTED,
re-run scoped tests and `make lint`, run `/full-review` again. Loop until the verdict is
`APPROVED`. If a finding is wrong, say why in the terminal rather than silently ignoring it.

## 8. PR description, then STOP

Run `/pr-description`, save the output to `${WT}/.claude/plans/<n>-pr-body.md`, run
`uv run python .claude/tools/pii_scan.py --text` on it and on `--range origin/master...HEAD`.
Present the description and stop. Default state: reviewed local commits, draft body, nothing pushed.

## 9. Finalize (explicit go only)

When the user says push, finalize or hand it off:

1. `git fetch origin master`; confirm the branch is based on current `origin/master` or say how far
   behind it is before doing anything about it.
2. `uv run python .claude/tools/pii_scan.py --range origin/master...HEAD` must exit 0.
3. `git push -u origin HEAD`.
4. `gh pr create --repo tutorcruncher/morpheus --title "<title from the issue>" --body-file <body> --assignee @me`.
5. `uv run python .claude/tools/pr_review_stamp.py stamp <pr> "$(git rev-parse HEAD)" --verdict APPROVED`
   (the verdict from Step 7).
6. `gh pr checks <pr> --watch`; report the result. A red check comes back to Step 6.
7. Once CI is green, run `/pr-uat <pr>`. Skip it only when the diff cannot change how the service behaves
   (docs, tooling, tests only), and say so. A failed case comes back to Step 6, then Step 7 and this list
   again from item 2, re-stamping the new commit.

Then report the PR URL and the UAT result. Merging is the user's call.
