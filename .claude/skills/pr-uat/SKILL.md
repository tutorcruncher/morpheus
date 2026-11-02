---
name: pr-uat
description: >-
  Morpheus-specific (tutorcruncher/morpheus). Test a PR, or any test the user describes, by hand on a real
  local stack (web server, Celery worker, fake Mandrill and MessageBird, test keys, nothing real reachable).
  Derives cases from the diff and takes any extra cases the user gives, drives each one as a real caller
  would, checks Postgres, Redis, the worker log and what the provider received, and captures terminal
  screenshots. With a PR, publishes the evidence into the PR description; with no PR, stays local. Use when
  the user runs /pr-uat, says "UAT this", "test this PR by hand", or asks to try a scenario on a running stack.
---

# PR UAT

Run real cases on a real stack and hand the reviewer evidence they can read without checking out the branch.

```
/pr-uat                        the open PR for this branch
/pr-uat <PR#> [extra cases]    that PR, plus any extra cases
/pr-uat <any test described>   ad hoc: just these cases, local only, nothing published
```

`<PR#>` that is not this branch's PR means checking that branch out first; the stack always runs the checkout
you are in. With no PR and no cases, ask what to test.

## Hard rules

- **Any case the user gives is run.** Add it to the checklist as written, whichever area of Morpheus it touches
  and whether or not the diff touches it. Never drop, merge or reinterpret it. If it truly cannot run, mark it
  NOT RUN with the reason. Existing test coverage is never a reason to skip a case: the tests run Celery eagerly
  with no worker, and this does not.
- **Synthetic data only.** This repository and `pr-screenshots` are public. Use placeholders
  (`foobar@example.org`, `billing-test`, made-up names and numbers), and never copy a value from Logfire, Sentry or
  any real system into a case. Read every screenshot for real data before it is uploaded.
- **Only `stack.sh`'s stack.** It uses test keys and a fake provider, and refuses a remote database or Redis. Never
  point a case at a real provider, a shared environment or production, and never edit `.env` for a run.
- **Nothing is fixed mid-run.** Log a bug with its repro and severity, then carry on.
- **Report failures as failures.** Publish them with their evidence and never soften them.

## 1. Plan

Set `RUN_DIR=<checkout>/.claude/uat/<PR#>-<yyyy-mm-dd>` (`adhoc-<slug>-<yyyy-mm-dd>` with no PR), always absolute,
and create `$RUN_DIR/screenshots/`. It is gitignored.

With a PR, read `git diff origin/master...HEAD` and the PR body, then derive cases: success, errors, auth, settings
on and off, retries and failures, edge cases. Add the user's cases. With no PR, the user's cases are the plan, plus
any obvious check needed to prove each one.

Write `$RUN_DIR/checklist.md` before testing, with cases on exactly this shape (`gate.sh` parses it):

```
- [ ] 1.1 Email send stores a message -> p1-01-01-send.png, p1-01-02-row.png
- [x] 1.1 Email send stores a message -> p1-01-01-send.png, p1-01-02-row.png | PASS: status send, one row
- [x] 1.2 Bad company code is refused -> p1-02-01-send.png | FAIL: returned 500, expected 422. Severity: high
- [n] 1.3 Case that cannot run -> NOT RUN: reason
```

Every surface a case names gets its own image: "in the API and in the database" is two. Write what you expect to
see in each store before acting, and compare against that afterwards. Log anything you change outside the API
under `## Mutations` so it can be undone.

## 2. Start the stack

```bash
<checkout>/.claude/skills/pr-uat/stack.sh start <checkout> --fresh
```

It prints the URL and an `env.sh` for journeys to `source`, and starts everything on this checkout's code. Use
`--fresh` for the first start of a run: `create_all` never alters a table, so a reused database can lack a branch's
new column. Leave it off when restarting mid-run, to keep the data.

- A setting a case needs goes on the environment of `start`, e.g. `SOME_SETTING=x stack.sh start <checkout>`. Re-run
  `start` to change one, and after any code change: the worker does not reload.
- Provider keys, provider URLs and telemetry settings cannot be overridden.
- `stack.sh status <checkout>` shows what is running and the log paths.
- A migration or a production SQL case: build the old state by hand, log it under `## Mutations`, and run the PR
  body's SQL with `psql` against the stack's database.

## 3. Run each case

For each case, in order:

1. **Act as the caller.** Call the endpoint TC2 or the provider would call. A direct `task.delay()` or SQL insert
   proves nothing about an endpoint. Call a task directly only for a scheduled one (nothing else calls it), with
   `uv run python -c "from app.messages.tasks import <task>; <task>.delay()"`.
2. **Check every store the case names**, before and after. Tasks run on the worker: a send returns 201 before
   anything is sent, so wait for the worker's log line first.
3. **Capture every surface**, then read each image. A traceback, an unexpected 500 or wrong data in the shot is a
   finding, even if it is not what the case was about and even if it was already there.
4. **Update the checklist**, and send the user progress at the end of each phase.

### What to drive

| To test | Do this |
|---------|---------|
| Send with no provider | `method: email-test` / `sms-test`. The rendered message lands in `$TEST_OUTPUT/<msg id>.txt` |
| The real provider path | `method: email-mandrill` / `sms-messagebird` hits the fake. `$STACK_DIR/provider-requests.jsonl` shows what it received |
| Provider failure or retry | Mandrill subject `__502__`, `__500__`, `__500_nginx__`, `__slow__` or `__connect_timeout__`, as in `tests/dummy_server.py`. Add a route there if a case needs a new response |
| Status change on a test send | `POST /webhook/test/` |
| Mandrill webhook | HMAC-SHA1 over `https://<HOST_NAME>/webhook/mandrill/mandrill_events<body>` with `MANDRILL_WEBHOOK_KEY`; see `app/messages/api/webhooks.py`. `_id` is the message's `external_id`: read it from the row |
| Any TC2-facing endpoint | `api <METHOD> <path> -d '<json>'`. Check `app/messages/api/` for the route and `app/messages/schemas.py` for the payload |
| `/messages/*` | `company`, `expires` and `signature` (see `app/common/auth.py`) signed with `USER_AUTH_KEY`; build the URL in a shell |
| Click tracking | Pass a URL in `context` and use it in the template (`apply_short_links` in `app/render/main.py`); the `links` row holds the token for `/l<token>` |
| Rendering | Send a test send and read the `$TEST_OUTPUT` file. Links in `main_template` itself are not tracked |
| Scheduled job | Call the task directly (above). Beat is not running |
| Queue contents | `redis-cli -u "$REDIS_URL" LLEN celery`, with the worker stopped: a running worker drains it first |
| Analytics | `REFRESH MATERIALIZED VIEW message_aggregation` first, and log it as a mutation |
| Anything else | Find the route or task in `app/`, call it the way its real caller does, and read the stores it writes |

A `uid` is single-use for 24 hours (409 on reuse): make one per send with `uuidgen`. For SMS use the test suite's
`07891123856`: reserved fake numbers fail validation and end as `render_failed`. Postgres tables: `companies`,
`message_groups`, `messages`, `events`, `links`. A webhook's effect shows on `messages`, through the `update_message`
trigger on `events`.

### Capturing evidence

Screenshots show only real commands and their real output. All narration lives in the checklist and the PR
description.

```bash
<checkout>/.claude/skills/pr-uat/setup-terminal.sh     # once, prints the harness URL
<checkout>/.claude/skills/pr-uat/write-case.sh <checkout> '<title>' <<'JOURNEY'
source /tmp/uat-stack-.../env.sh
body='{"uid":"'$(uuidgen)'","company_code":"billing-test","method":"sms-test","main_template":"Hi","recipients":[{"number":"07891123856"}]}'
run "api POST /send/sms/ -d '$body'"
sleep 3
run "psql \"\$DATABASE_URL\" -x -c 'select id, status from messages order by id desc limit 1'"
run "grep -c 'send_sms.*succeeded' \$STACK_DIR/worker.log"
JOURNEY
```

`env.sh` gives you `run`, which prints a command before running it, and `api <METHOD> <path> [curl args]`, which makes
a request with the auth and JSON headers and prints the status. Use plain `curl` only when a case is about a missing
or wrong header. Use the real path from `stack.sh`'s output in place of `/tmp/uat-stack-...`.

Then, with whichever browser MCP the session has:

1. Navigate to the harness URL. Each navigation runs the case again in a fresh terminal, so a sending case sends again.
2. Wait on elapsed time, because a canvas terminal cannot be read by a text wait.
3. Take the screenshot in a **later message**, to `$RUN_DIR/screenshots/<planned name>` by absolute path. Calls in
   one message run together and would capture a blank terminal.
4. Crop it: `uv run --with pillow python <checkout>/.claude/skills/pr-uat/crop-terminal.py <image>`.
5. Read the image and confirm the output is complete.

The terminal is 40 rows. For more, `TSH_ROWS=<n> setup-terminal.sh`, write the case again, and take a full-page shot.
Do not `2>/dev/null` a command in a journey, because its output vanishes; use `2>&1 | grep -v claude-code-hint`. Avoid
commands that print home-directory paths.

For a page (`/`, `/l<token>`, `/messages/...`, or `/docs`, which `DEV_MODE` turns on), drive it in the browser and
screenshot it.

With no browser MCP, save each journey's output to `$RUN_DIR/screenshots/<case>.txt`, tell the user there are no
screenshots, and do not publish.

## 4. Gate

```bash
<checkout>/.claude/skills/pr-uat/gate.sh "$RUN_DIR"
```

It must pass before anything is published. Report its counts. If it fails, capture what is missing and run it again.

## 5. Publish (PR runs only)

An ad hoc run stops here: give the user the verdict and `$RUN_DIR`, then go to step 6.

Upload with the script, which uses the REST API only. Never improvise with git objects: an empty SHA in a push can
delete the branch.

```bash
<checkout>/.claude/skills/pr-uat/upload-screenshots.sh tutorcruncher/morpheus pr-screenshots \
  uat-<PR#>-<yyyy-mm-dd> "$RUN_DIR/screenshots"
```

Then fetch the current PR body, append a `---` and the section below, and patch it with
`gh api -X PATCH repos/tutorcruncher/morpheus/pulls/<n> --input payload.json` (build it with `jq -Rs '{body: .}'`).
Never touch other sections. If you patch again, rebuild from the original body so the section is not doubled.

```markdown
### Manual Testing

- [x] **Phase 1: <group name>**
  <details><summary>Test cases</summary>

  - [x] 1.1 <short plain description>. <What the screenshots prove>
    <details><summary>Screenshots</summary>

    <img width="820" src="https://github.com/tutorcruncher/morpheus/raw/pr-screenshots/<dir>/<image>">
    </details>

  </details>
```

Show failed cases as `- [ ]` with **FAIL** and NOT RUN cases with their reason. Use short, plain wording. Re-fetch the
body and confirm the section appears once and every uploaded filename is in it.

## 6. Clean up

Undo everything under `## Mutations`, remove any temporary prints from the code, and run
`stack.sh stop <checkout>`. Leave `$RUN_DIR` in place. Finish with the verdict, the gate's counts and every failure.
