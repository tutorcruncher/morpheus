---
name: bug-investigate
description: >-
  Morpheus-specific (tutorcruncher/morpheus). Investigate a bug from a GitHub issue number, a
  MORPHEUS-* Sentry id or URL, a Logfire link, a pasted issue, a written description or a traceback:
  restate it, find or offer a tracking issue, check prior PRs, gather evidence read-only (whichever
  MCP tools the session has, such as Sentry and Logfire, plus the code and the local morpheus
  database), separate causation from correlation, then present
  root cause, evidence, fix and blast radius and STOP for confirmation before any edit. After
  confirmation, fixes test-first. Use when the user runs /bug-investigate, says "investigate this
  bug", "what is causing this error", or hands over a Sentry or Logfire link.
---

# Bug Investigate

Diagnose before touching code. Steps 1 to 5 are read-only. The user confirms the diagnosis before
any edit, and every finding is written up so it can be reopened later.

## Hard rules

- **Read-only until confirmed.** No code edits, no database writes, no task runs until Step 5 is
  confirmed. A question from the user ("why does this happen?") is answered, not acted on.
- **Customer data never leaves the diagnosis.** This repository is public. Logfire, Sentry, the
  database and provider payloads may be read; nothing from them is written into code, tests,
  fixtures, commit messages, branch names, PR bodies, issue or review comments, screenshots or
  saved reports. Prefer ids (message id, group id, company id, trace id). When a value must be
  quoted, replace it with a placeholder (`foobar@example.org`, `<company_code>`, `billing-test`).
  Quote the minimum in the terminal too. A subagent brief restates this rule.
- **Evidence first, theory second.** Read the trace, the span or the traceback before forming a
  theory, and confirm the evidence is the reported failure (same request, same time, same error)
  rather than a look-alike.
- **Causation, not correlation.** A change deployed near `first_seen`, a company that appears in
  every event, a worker restart at the same minute: each is a lead, not a cause, until the code
  path from input to failure is traced.
- **Local databases.** `morpheus` and `morpheus_test` are the only databases this skill touches,
  read-only (`psql`, `SELECT` only). Neither holds production data, so they show data shape, not what
  happened to a customer.
- **Never run the Heroku CLI.** Draft the command and ask the user to run it and paste the result.
- **Never call Seer** or any automated root-cause feature of Sentry; do the analysis yourself.

## Step 1: Restate

Write back what is broken, for whom, since when, and what correct behaviour is. Name the input
type you were given (issue, Sentry id, Logfire link, description, traceback). Note which checkout
and branch you are in (`git rev-parse --show-toplevel`, `git branch --show-current`).

## Step 2: Tracking issue and prior work

- Issue given: `gh issue view <n> --repo tutorcruncher/morpheus --comments`. Read every comment.
- No issue: `gh issue list --repo tutorcruncher/morpheus --search "<key words>" --state all --limit 50`.
  Offer to create one after Step 5 (with the redacted diagnosis), do not create it now.
- `gh pr list --repo tutorcruncher/morpheus --search "<key words>" --state all --limit 50` and
  `git log --oneline -S '<symbol>' -- <path>` for earlier attempts at the same area. A bug that has
  been "fixed" before usually has a reason it came back.

## Step 3: Evidence

Check which MCP tools this session has, including deferred ones (`ToolSearch` for `sentry` and
`logfire`), and use every one that can see the bug. A missing tool never stops the investigation.

Spawn one investigator agent on Fable (`Agent` with `model: fable`) with a read-only brief: the
restatement, the links, which MCP tools are available, the rules above word for word, and "return evidence with ids and
`path:line` citations, no customer values". Ask it for both the confirming evidence and the
evidence that would rule the theory out.

**Logfire (if the MCP is available).** Project `morpheus`. Run `query_schema_reference` first. Filter on
`deployment_environment`, never on `sentry-environment` (that is the caller's propagated baggage
and attributes TC2's environment, not ours). Discover attribute values with a `SELECT DISTINCT`
query rather than assuming them (environment names, span names, task names). Use
`query_find_exceptions_in_file` when you have a path from a traceback, `query_exception_context` for
a span, `query_run` for frequency, first and last seen, affected company ids and correlated spans
(the Celery task a request enqueued, the webhook that followed a send).

**Sentry (if the MCP is available).** Use it for the issue, tags and breadcrumbs. A `MORPHEUS-*` id
is a Sentry short id; cite it in comments and commit messages.

**No MCP for a source.** Work from what the user gave: a pasted issue, a GitHub issue number or a
written description. Start from the code and the tests, and ask the user to paste only the specific
output that would settle an open question (a Sentry traceback, a Logfire query result).

**Code.** Trace the path: the route in `app/messages/api/`, the schema in `app/messages/schemas.py`,
the task in `app/messages/tasks.py`, the model and trigger (`app/messages/models.py`,
`app/core/bootstrap.sql`, `app/core/post_bootstrap.sql`), the provider call in `app/ext/clients.py`,
the render in `app/render/`. Check the test that should have caught it.

**Local database.** `psql morpheus` for data shape (`\d messages`, enum values, a `SELECT` by id).
Say which database every query ran against.

If the evidence is thin, say so. Do not present a cause with open competing theories; list them and
what would settle each.

## Step 4: Challenge the diagnosis

Before presenting: can the proposed cause produce exactly the observed symptom, every time it is
observed, and nothing else? What else would be true if it were the cause, and is it? Does the fix
change behaviour TC2 relies on (status codes, response keys), a task signature, or the schema?

## Step 5: Present and STOP

Write `${WT}/.claude/investigations/<slug>.md` (gitignored). Run
`uv run python .claude/tools/pii_scan.py --text <file>` on it before you consider it written, and
redact any hit. Then present in the terminal:

1. **Root cause**: why, not where. Input to failure.
2. **Evidence**: `path:line`, trace or span ids, Sentry short id, query results as counts and ids.
3. **Proposed fix**: file, change, and the test that proves it (name the test file and what it asserts).
4. **Blast radius**: local-only, or live behaviour; how many messages, companies or sends, by count;
   whether any production data needs repair (draft the SQL for the user; never run it).
5. **Risks**: production SQL, Celery signature change, TC2 contract change, `app/render/` version bump.

Offer to post the redacted diagnosis on the tracking issue (`CLAUDE SAYS:` first). Wait for the user
to confirm before any edit.

## Step 6: Fix (after confirmation only)

1. Branch from fresh `origin/master` if not already on a feature branch.
2. Write the failing test first in the matching `tests/test_*.py`, using the fixtures in
   `tests/conftest.py` and a route in `tests/dummy_server.py` for any new provider call. Run
   `uv run pytest tests/<file> -k <test>` and confirm it fails for the reported reason, not a setup
   error. Placeholders only in test data.
3. Make the minimal fix. One mechanism; no defensive clamp alongside it.
4. `uv run pytest tests/<file>` then `make lint`. Report pass and fail counts.
5. Commit with the issue or Sentry id in the message. Do not push; hand back to the user, or to
   `/start-issue` if it called this skill.
