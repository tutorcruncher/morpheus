---
name: full-review
description: >-
  Morpheus-specific (tutorcruncher/morpheus). Review a PR or the current branch against master:
  deterministic gates first (production SQL for schema changes, morpheus-mail VERSION bump, Celery
  task compatibility, PII scan, stale references), then the four specialist agents (architect,
  code-reviewer, security-auditor, performance-reviewer) in parallel, synthesised into BLOCKER /
  SUGGESTED with a computed verdict, posted as one GitHub review and stamped on the PR. Report-only:
  never edits code. Use when the user runs /full-review, or asks for a full review of a branch or PR.
---

# Full Review

Four specialists in parallel, one synthesis, one GitHub review. Two tiers: **BLOCKER** (the changed
code breaks, loses data, is exploitable, cannot deploy safely, or publishes customer data) and
**SUGGESTED** (everything else, including anything outside the diff). Verdict is computed:
**APPROVED iff zero blockers stand**. This skill never edits source files; it reports.

## Usage

```
/full-review [PR_NUMBER] [--scope PATH]
```

No argument reviews the current branch against `origin/master`. A PR number checks that PR out
first. `--scope` restricts all four reviewers to a path and disables posting and stamping.

## Customer data

This repository is public. Customer data read while reviewing (Logfire, Sentry, a database, a
provider payload) is never written into a report, a finding, a PR comment or the terminal beyond
the minimum needed. Use ids and placeholders (`foobar@example.org`, `<company_code>`). Every
subagent brief below restates this.

## Step 0: Preflight

```bash
WT=$(git rev-parse --show-toplevel)
REPO=tutorcruncher/morpheus
TS=$(date +%Y-%m-%d-%H%M)
git fetch origin master --quiet
```

- PR given: `gh pr checkout "$PR"`. Otherwise `PR=$(gh pr view --repo "$REPO" --json number --jq .number)`;
  if that fails there is no PR and Steps 5 and 6 are skipped. Never hide the error with `2>/dev/null`.
- `BODY=$(gh pr view "$PR" --repo "$REPO" --json body --jq .body)` when there is a PR. Read it: the
  synthesis drops findings the body already documents.
- Dirty tree (`git status --porcelain` non-empty): review in degraded mode, say so in Step 7, and do
  not stamp. A review of a half-committed tree is not a review of HEAD.
- `DIFF="git diff origin/master...HEAD"` (append `-- <scope>` when scoped). Merge-base form only.
- `mkdir -p "$WT/.claude/"{architect,reviews,security,performance}`.

## Step 1: Deterministic gates

Run all of them; each failure is a BLOCKER carried into the synthesis regardless of what the agents say.

1. **Schema change without production SQL.** If the diff touches `app/messages/models.py`,
   `app/core/bootstrap.sql` or `app/core/post_bootstrap.sql`, the PR body must contain a
   `Production SQL` section with statements (`ALTER TABLE`, `CREATE INDEX CONCURRENTLY`,
   `ALTER TYPE ... ADD VALUE`, `DROP MATERIALIZED VIEW`, or an explicit "none needed: new table
   only, created by `create_all`"). No PR: the local report records the requirement.
2. **`app/render/` changed without a version bump.** `git diff origin/master...HEAD --stat -- app/render/`
   non-empty requires `packaging/morpheus-mail/setup.py` to change `VERSION`.
3. **Celery task compatibility.** Compare `grep -n "name='app.messages.tasks\." app/messages/tasks.py`
   and each task's `def` line between `origin/master` and HEAD (`git show origin/master:app/messages/tasks.py`).
   A removed or renamed task name, a removed parameter, or a new parameter without a default fails.
4. **PII scan.** `uv run python "$WT/.claude/tools/pii_scan.py" --range origin/master...HEAD`
   (plus `--text` on the PR body saved to a temp file). Exit 1 is a BLOCKER per hit; exit 2 means
   the scan could not run, which is also a BLOCKER until it does. The scan is a safety net with
   known limits: it cannot recognise a customer name or a company code that is not in the local
   denylist, so the code-reviewer's real-looking-data check still matters.
5. **Stale references.** For every symbol, task name, path or route the diff renames or removes,
   `grep -rn` the old name in `CLAUDE.md`, `.claude/`, `README.md` and `app/`. A hit is SUGGESTED
   unless it is a task name (then BLOCKER, see gate 3).

## Step 2: Launch the four agents in one message

Four `Agent` calls in a single message: `subagent_type` `architect`, `code-reviewer`,
`security-auditor`, `performance-reviewer`. Each brief contains:

- `${WT}`, `${TS}`, the exact diff command, the PR number and the PR body (or "no PR").
- "Read `CLAUDE.md` first. Shared checkout: read-only. Never run pytest; tests truncate every table
  and flush Redis."
- "Customer data read while reviewing is never written into the report: no names, addresses, phone
  numbers, company codes, message bodies or payloads; ids and placeholders only."
- "Write `${WT}/.claude/<dir>/${TS}-pr-review.md` and end with the Findings JSON block."
- The gate results from Step 1, so the agents do not re-derive them.

Always fresh: never reuse report files from an earlier run.

## Step 3: Synthesise

Read the four reports. Build `${WT}/.claude/reviews/${TS}-full-review-synthesis.md`:

- Merge the Findings JSON blocks and the Step 1 gate failures. Deduplicate by `path:line` and
  substance; a finding two agents raise is noted as consensus.
- Tier each finding with the rubric above. An agent may call something a blocker that is not under
  the rubric; demote it and say so. Never promote.
- **Drop** findings the PR body already documents as a deliberate choice or a known follow-up, and
  findings that restate a comment in the code explaining why. Record the dropped list.
- Cap at about 15 findings. Past that, keep every BLOCKER and the highest-value SUGGESTED items;
  summarise the rest in one line each.
- Every code finding anchors to a line the diff adds or changes (`path`, `line`); only a finding
  about the change as a whole omits them.
- Verdict: `APPROVED` with zero blockers; `CHANGES_REQUESTED` otherwise; `DISCUSSION_NEEDED` when
  the only open items need a decision from the author (for example a TC2 contract change that may
  be intended).

Write `${WT}/.claude/reviews/${TS}-findings.json` (`[{path, line, tier, body}]`, body = claim,
evidence, fix in two or three sentences) and `${WT}/.claude/reviews/${TS}-summary.md` (three lines:
verdict, counts, path of the synthesis). No `CLAUDE SAYS:` in the summary file; the poster adds it.

## Step 4: No PR

Present the synthesis in the terminal (blockers first, then suggested, then what was dropped and
why) and stop. Reports stay under `.claude/`, which is gitignored.

## Step 5: Post one GitHub review

Dry-run first; it also runs the PII scan on every comment body:

```bash
uv run python "$WT/.claude/tools/post_pr_review.py" "$PR" \
  --findings "$WT/.claude/reviews/${TS}-findings.json" --summary "$WT/.claude/reviews/${TS}-summary.md" --dry-run
```

A code finding printed as `SUMMARY` lost its anchor: open the diff, find the changed line it is
about, fix `line`, dry-run again. Then post with `--event REQUEST_CHANGES` if any blocker stands,
else `--event COMMENT`. Never `APPROVE`: a bot does not formally approve a PR. The tool refuses to
post on a PII hit; fix the text, do not work around it. Skip posting on a `--scope` run.

## Step 6: Stamp

```bash
uv run python "$WT/.claude/tools/pr_review_stamp.py" stamp "$PR" "$(git rev-parse HEAD)" --verdict <VERDICT>
```

Stamp the SHA GitHub has (the PR head). Never stamp from a `--scope` run or a dirty tree.

## Step 7: Report to the user

Verdict, counts (blockers, suggested, dropped, inline versus summary), any gate that failed, the
degraded-mode note if it applies, and the paths of the five report files. No automatic fixes: the
user decides what to act on.
