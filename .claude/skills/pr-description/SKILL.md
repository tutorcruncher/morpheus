---
name: pr-description
description: >-
  Morpheus-specific (tutorcruncher/morpheus). Write a PR description for the current branch from
  its diff and commits against origin/master, as raw markdown: Summary, Non-dev summary, Production
  SQL (mandatory when the schema changed), TC2 contract, Celery compatibility, morpheus-mail, Manual
  testing. Empty sections are omitted. Output is run through the PII scan. Use when the user runs
  /pr-description, says "write the PR description", or a PR body needs writing or rewriting.
---

# PR Description

A PR body says what changed and what a reviewer or deployer must know to act on it. Not how the
work was arrived at, not alternatives considered, not reassurance. Every sentence carries
information; cut the rest. Plain words a colleague would say aloud.

## Customer data

This repository is public. No names, addresses, phone numbers, company codes, message bodies or
payloads in the body, even when the fix was found from a real event. Cite ids (`MORPHEUS-XXXX`,
issue numbers, message ids) and placeholders (`<company_code>`). The finished text is scanned.

## Gather

```bash
git fetch origin master --quiet
git log --oneline origin/master...HEAD
git diff --stat origin/master...HEAD
git diff origin/master...HEAD
```

Read the changed files for anything the diff alone does not show. Check specifically:

- `app/messages/models.py`, `app/core/bootstrap.sql`, `app/core/post_bootstrap.sql`: schema change,
  so the Production SQL section is mandatory.
- `app/messages/tasks.py`, `app/core/celery.py`: task names, signatures, beat entries.
- `app/messages/api/*.py`, `app/messages/schemas.py`: endpoints TC2 calls, status codes, response
  keys, validation.
- `app/render/`: `VERSION` in `packaging/morpheus-mail/setup.py`.
- `app/core/config.py`: a new or changed `Settings` field needs an env var set in production.

## Format

Output raw markdown inside one fenced block (so the terminal does not render it), omitting any
section with nothing to say. Only Summary and Non-dev summary are always present.

```
## Summary

[1-3 sentences: what the PR does and the non-obvious behaviour. Written for a developer who knows
FastAPI and Celery but not this codebase. `Fixes #<n>` or `Closes #<n>` on its own line.]

## Non-dev summary

[1-3 sentences in plain words for support and product: what an agency or a recipient notices, or
"Nothing visible, internal change". Describe the improvement, never the past failure: "SMS replies
now reach the agency more reliably", not "SMS replies were being lost".]

## Production SQL

[Mandatory when the schema changed. The exact statements to run once before or after deploy, in
order, and when. `CREATE INDEX CONCURRENTLY` for indexes on messages or events; `ALTER TYPE ...
ADD VALUE` for enums; `DROP MATERIALIZED VIEW message_aggregation` plus a note that the aggregation
view is empty until the next hourly refresh. Or: "None needed: new table only, created by
create_db_and_tables."]

## TC2 contract

[Each endpoint TC2 calls whose status codes, response keys or validation changed, and what TC2
sees: a table of endpoint, before, after. State whether TC2 needs a change and whether it must
deploy first.]

## Celery compatibility

[New, renamed or removed tasks; new arguments and their defaults; new beat entries. How a worker on
the previous release handles tasks queued by this one and the reverse.]

## morpheus-mail

[The VERSION bump and what changes for package users.]

## Manual testing

- [ ] [One concrete check a human can do, with the command or URL]
- [ ] [...]

## New settings

[Env var, default, and what must be set in production before deploy.]
```

## Rules

- Tables for anything enumerable (endpoints, statements, env vars).
- Non-dev summary follows the release-notes rule: never imply the product previously failed
  customers or lost data; describe the capability.
- Nothing about how the bug was investigated; the diagnosis lives in the issue.
- No section with "N/A" or "None" except Production SQL, where "None needed" and the reason is
  itself the information.

## Scan before handing over

Save the body to a file under `${WT}/.claude/plans/` or the scratchpad and run
`uv run python .claude/tools/pii_scan.py --text <file>`. Fix any hit before showing the body. The
scan cannot recognise a customer name or company code on its own; read the body once more for them.
