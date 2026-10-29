Run an exhaustive, screenshot-evidenced UAT (User Acceptance Testing) of a Morpheus PR: derive a test plan from the branch diff, drive every test case the way Morpheus's real callers do, verify every backend effect in Postgres and Redis, capture screenshot evidence for EVERY test case, then publish the whole run into the PR description as a collapsible Manual Testing section with the screenshots inline.

Morpheus has almost no pages of its own. Its callers are TutorCruncher (the send, SMS, subaccount and messages APIs) and the providers (the Mandrill and MessageBird webhooks), so most cases are HTTP calls and database reads. This command bundles a PTY harness under `.claude/commands/pr-uat/` that turns those into terminal screenshots, and `stack.sh`, which runs this checkout's server and worker. It drives any page through whichever browser MCP server the session has.

## Usage

```
/pr-uat [<PR#>] [<extra test cases>]
```

- `<PR#>` defaults to the open PR for the current branch (`gh api "repos/tutorcruncher/morpheus/pulls?state=open&head=tutorcruncher:<branch>"`).
- Runs against the checkout you are in. The server, worker, database and Redis must belong to THIS checkout's code: `stack.sh` (step 2) sees to that.
- **User-supplied test cases are first-class input.** Any extra cases the user gives — in the invocation text after the PR number, pasted as a list, or added mid-run in conversation — go into the plan as their own checklist lines (their own phase if they form a group), each with planned screenshot filenames like every derived case (one per surface the case names), and they count in the GATE. Never drop, merge away, or reinterpret a user case; if one cannot be run, record it in the checklist as NOT RUN with the reason instead of silently skipping it.

---

## Screenshot evidence requirement

Every test case ships with at least one screenshot, and ideally several across the case's journey (before state, action moment, result). Reviewers read the PR description without checking out the branch or running the app, so the screenshot is the only evidence they get. A ticked case with no image is a claim, not a result, and it is the one failure mode that makes the whole run worthless to them.

Apply it like this:

1. Assign each case its screenshot filenames while writing the plan, before any testing starts. The 1:1 mapping then exists from minute one.
2. Most Morpheus cases have no page. Their surface is the terminal: the request and its response, the worker log line, the `psql` rows, the `redis-cli` read. Treat "this case is not visual" as a signal to capture the terminal that shows the effect, not as grounds to skip the shot.
3. When a case names more than one surface, screenshot every surface it names, one shot each. A case reading "status on the messages API and in the messages table" needs the API response AND the `psql` read; "the copy on the mirror and the queue length in Redis" needs both reads. One shot leaves the other half of the case unproven, and a user case that says "and also in X" is never covered by the first shot on its own.
4. When the case is a flow (send -> webhook -> status), capture each step and suffix the filenames in order so they sort in journey order.
5. Count the mapping at the GATE (step 5). Checklist lines and files on disk are both countable, so coverage is arithmetic. Report the two numbers rather than judging whether coverage feels adequate.

Screenshot layout and naming. A run writes everything it produces under `.claude/uat/<PR#>-<yyyy-mm-dd>/` in the checkout under test, which `.gitignore` already keeps out of git (`/.claude/*` ignores everything except the committed `commands` dir). Name that directory in the plan step as an **absolute** path (`RUN_DIR=<checkout>/.claude/uat/<PR#>-<yyyy-mm-dd>`) and use it everywhere: the Bash cwd can reset between calls, and the browser MCP resolves relative filenames against its own directory, so a relative path silently lands in the wrong place. Use the same `<PR#>-<yyyy-mm-dd>` stem for the upload directory on `pr-screenshots`.

```
.claude/uat/<PR#>-<yyyy-mm-dd>/checklist.md
.claude/uat/<PR#>-<yyyy-mm-dd>/screenshots/phase<N>-<slug>/p<N>-<case>-<step>-<desc>.png
e.g. .claude/uat/568-2026-10-29/screenshots/phase1-send/p1-02-01-send-email.png
     .claude/uat/568-2026-10-29/screenshots/phase1-send/p1-02-02-mirror-row.png
```

One directory per run, checklist included. That is what lets the GATE (step 5) count this run's images without picking up images left behind by an earlier run, and it keeps the artefacts out of `git status`.

---

## Workflow

### 1. Plan

- Read the PR diff (`git diff master...HEAD`) and the PR body. Derive phases (test case groups) and concrete test cases: success, error, auth (missing or wrong `Authorization`, bad webhook signature, expired user session), setting on/off, retry and failure paths, edge cases.
- Add every user-supplied case (see Usage) to the checklist verbatim alongside the derived ones — same screenshot-filename rules and GATE treatment.
- Write a living checklist at `.claude/uat/<PR#>-<yyyy-mm-dd>/checklist.md`. Every case line carries its planned screenshot filename(s) BEFORE testing starts, so the 1:1 case-to-image mapping exists from minute one.
- Write every case on a line of exactly this shape, and nothing else in the file on that shape, so the GATE can count them with a grep: `- [ ] <phase>.<n> <description> -> <planned filename(s)>`.
- A case that names more than one surface gets one planned filename per surface on that line, not one for the case. Read each user-supplied case for the word "and": "on the messages API and in the database" is two surfaces, so two filenames.
- Create the run directory `.claude/uat/<PR#>-<yyyy-mm-dd>/` and its per-phase screenshot dirs. Every path for the rest of the run hangs off this one directory, and `.gitignore` already keeps it untracked.
- Update the checklist after EVERY case (PASS/FAIL + evidence note). Send it to the user at each phase boundary so they can follow progress.
- Bugs found are LOGGED with repro + severity and the run continues. Nothing is fixed mid-UAT.

### 2. Preflight

- **Stack:** `<checkout>/.claude/commands/pr-uat/stack.sh start <checkout>` starts the web server and a Celery worker on this checkout's code and prints the URL, database and Redis it uses. The worker serves every queue in `task_routes`. It takes the checkout's `.env` `DATABASE_URL`/`REDIS_URL` when set; otherwise it makes its own `morpheus_uat_<n>` database and Redis db number, so it never shares rows or tasks with another checkout. `stack.sh status <checkout>` shows the logs' paths.
- Settings a case needs go on the environment of `stack.sh start`, e.g. `MIRROR_DATABASE_URL=... TEST_OUTPUT=$RUN_DIR/sent stack.sh start <checkout>`. **Never edit `.env` for a run**: the tests read it too, so an override left behind breaks them later. To change a setting mid-run, re-run `start` with the new environment; it stops the old stack first.
- `TEST_OUTPUT=<dir>` makes every `email-test`/`sms-test` send write the rendered message to `<dir>/<msg id>.txt`: that file is the evidence of what a recipient would get.
- Send with `method: email-test` or `sms-test`. They run the whole send path and store the message, but call no provider. `email-mandrill` and `sms-messagebird` call the real provider with an empty key, which fails. A test send's status changes through `POST /webhook/test/`.
- Data map: enumerate the tables, Redis keys and tasks the diff touches and predict expected outcomes before acting.
- Log every DB mutation you make outside the API (a hand `UPDATE`, a dropped row) in a "Demo-DB mutations" section of the checklist so the run is resettable.

### 3. Per-case loop (act -> checkdb -> screenshot -> tick)

For each case, in order:

1. **Act as the caller.** Call the endpoint TutorCruncher or the provider would call, with the payload they send, against this stack's URL. No shortcuts past the API: a direct `task.delay()` or SQL insert in place of an HTTP call proves nothing about the endpoint. Call a task directly only for a scheduled job (beat runs it, nobody calls it) or a manual one the diff documents (e.g. a `check_*` you run from a shell).
2. **checkdb**: snapshot relevant rows BEFORE, act, verify AFTER. Compare against the prediction you wrote in the Preflight data map, not against vibes. Where the case names a store other than Postgres (Redis, a second database, a task's log line, the `TEST_OUTPUT` file), read that store the same way, before and after. Tasks run on the worker: wait for its log line before you check.
3. **Screenshot every surface the case names**, into the case's pre-assigned filenames, via the terminal harness in 3b, or the browser for a page. Verify each file exists and is non-trivial (`file` / size > 10KB-ish).
4. **Inspect every captured image before ticking.** A case is not PASS if its own screenshot shows an anomaly — a traceback in the worker log, a 500 the case did not expect, wrong data in a column the case was not about — even when the assertion you were making holds and even when the anomaly is pre-existing. Log it as a finding.
5. Tick the checklist. A case is not tickable while its image is missing.

### 3b. Evidence style (terminal cases)

The audience is developers reviewing the PR — evidence must look like a dev's real session, not a narrated demo.

- **Terminal screenshots contain ONLY real commands and their genuine output.** Show the `curl` with its status and body, the `psql` query and its rows, the `redis-cli` read, the worker log lines (`grep` the log in `stack.sh status`). NEVER echo narration headers, case banners, or prose into the terminal — an annotated terminal reads as fabricated evidence.
- **Capture terminal evidence with the bundled PTY harness** (`.claude/commands/pr-uat/`). Run the checkout's own copy by absolute path: `<checkout>/.claude/commands/pr-uat/setup-terminal.sh` builds and starts **this checkout's** harness (its own `/tmp/tsh-<checkout>-<n>` dir and a free port, so another session's harness is never touched) and prints its URL. Write each case with `<checkout>/.claude/commands/pr-uat/write-case.sh <checkout> '<title>'`, journey on stdin; it prints the URL again. Then with the browser MCP: navigate to that URL (each navigation spawns a fresh PTY), wait on elapsed TIME (the terminal is an xterm.js canvas — an accessibility snapshot cannot read it, so text waits never fire), then take the screenshot **in a later message** — calls in one message run in parallel, so a screenshot sent with the wait fires at t≈0 and captures a blank PTY. Read the image to confirm the output is complete, not truncated mid-run. The terminal is 40 rows so it fits a 1280x720 viewport; for taller output restart the harness with `TSH_ROWS=<n>` (then re-run `write-case.sh`: the restart clears the case and may change the port) and take a full-page screenshot.
- **Crop terminal captures tight** — the xterm page is mostly empty black below short output, and a screenshot that is 90% void buries the evidence. `uv run --with pillow python <checkout>/.claude/commands/pr-uat/crop-terminal.py <image> [...]` trims everything below the last line of output. Read the image after cropping.
- The journey runs in the checkout with its `.venv` active, but NOT with the stack's environment: export the stack's `DATABASE_URL`/`REDIS_URL` (from `stack.sh status`) at the top of the journey when they did not come from `.env`.
- Keep `curl` output readable: `curl -sS -w '\nHTTP %{http_code}\n'`, and pipe JSON through `python -m json.tool`. Use `psql -x` for wide rows.
- `export PAGER=cat` at the top of every journey. A wide `psql` result otherwise opens `less`, and the screenshot shows one page and `(END)`.
- Show each command before its output with a small helper at the top of the journey, e.g. `run() { printf '$ %s\n' "$*"; eval "$*"; }`, then `run "psql ..."`. Keep the URLs and the helper in a file in `$RUN_DIR` and `source` it.
- In journeys, don't `2>/dev/null` a command — in the PTY its output vanishes with it. Use `2>&1 | grep -v claude-code-hint`.
- Technical content in the terminal is good: real commands, real tracebacks, real return values. Minimal shell-native context (the command line itself, an exit code) is all the framing a terminal needs.
- **Bare temporary print statements inside the actual code under test are allowed** when they surface something real (e.g. which rows a task selects). They are local-only and never committed. Restart the stack after adding one: the worker does not reload.
- **All narration lives in the markdown**: the checklist annotates each screenshot (what it shows, what it proves), and that annotation is carried into the PR description as text inside the collapsible section beside the embedded images. GitHub is where the story is told; the terminal only shows what happened.

### 4. Pages

Morpheus serves a few pages: `/` (the index), `/l<token>` (the click redirect), the `/messages/...` JSON a signed user session reads, and, with `DEV_MODE` (which `stack.sh` sets), the API docs at `/docs`. A case about one of these drives it through the browser and screenshots the page. A user-session URL needs `company`, `expires` and `signature` (HMAC-SHA256 of `<company>:<expires>` with `USER_AUTH_KEY`, see `app/common/auth.py`); build it in a shell and paste the full URL.

### 5. THE GATE (before any upload)

```
RUN_DIR=<checkout>/.claude/uat/<PR#>-<yyyy-mm-dd>  # the absolute directory created in step 1
CHECKLIST=$RUN_DIR/checklist.md
cases  = grep -cE '^- \[[ x]\] [0-9]+\.[0-9]+ ' "$CHECKLIST"
images = find "$RUN_DIR/screenshots" -type f \( -name '*.jpeg' -o -name '*.jpg' -o -name '*.png' \) | wc -l
```

Count inside `$RUN_DIR` only. A glob across `.claude/uat/` would also count screenshots from earlier runs, and the GATE would then pass on another run's evidence.

Every planned filename in the checklist must exist on disk (diff the planned filenames against `ls`), so a case that planned two shots needs both present, not one. Any shortfall means going back and capturing the missing shots, then re-running the count before moving on. Publishing a run with a gap in it is what this step exists to prevent.

### 6. Upload screenshots (pure REST)

Images go to the `pr-screenshots` branch on origin (`tutorcruncher/morpheus`) under `uat-<PR#>-<yyyy-mm-dd>/`, the same stem as the local run directory. The upload is flat: the per-phase dirs are not recreated remotely, and the `p<N>-<case>-<step>-<desc>` filenames keep the images unique and in journey order.

Upload with the bundled script — it does the whole flow server-side (blob per file -> tree with `base_tree` -> commit with the current tip as parent -> PATCH the ref) and then byte-size-verifies every file against the remote tree. NEVER use local git objects instead: the branch tip only exists on the remote, `git commit-tree -p <remote-sha>` fails, and the resulting empty `$COMMIT_SHA` turns a push into a DELETE refspec that deletes the branch — the script exists so that sequence is never improvised.

```bash
<checkout>/.claude/commands/pr-uat/upload-screenshots.sh tutorcruncher/morpheus pr-screenshots \
  uat-<PR#>-<yyyy-mm-dd> "$RUN_DIR/screenshots"
```

Pass the directory, not a glob: the script uploads every `.jpeg`/`.jpg`/`.png` under it (the same set the GATE counts), and the Bash tool's zsh aborts the whole command on a glob with no matches. The script exits non-zero on any byte-size mismatch. Image URL base: `https://github.com/tutorcruncher/morpheus/raw/pr-screenshots/<dir>/<name>`.

### 7. PR description (the deliverable)

Append a `### Manual Testing` section to the PR body. Rules:

- Fetch the CURRENT body first; append after a `---` divider; never touch existing sections. If you re-patch, rebuild from the ORIGINAL fetched body, not the already-patched one (or you double-append).
- Patch via `gh api -X PATCH repos/tutorcruncher/morpheus/pulls/<n> --input payload.json` with `jq -Rs '{body: .}'` to build the JSON.
- Test-case text: terse, short, very simple language, NO em-dashes.
- Structure: each phase is a visible checked line; its cases live in a collapsible block; each case's screenshots live in their own nested collapsible block, all shots stacked in journey order.

```markdown
### Manual Testing

- [x] **Phase 1: <group name>**
  <details><summary>Test cases</summary>

  - [x] 1.1 <terse case description>
    <details><summary>Screenshots</summary>

    <img width="820" src="https://github.com/tutorcruncher/morpheus/raw/pr-screenshots/<dir>/<img1>">
    <img width="820" src="https://github.com/tutorcruncher/morpheus/raw/pr-screenshots/<dir>/<img2>">
    </details>
  - [x] 1.2 <next case>
    ...

  </details>

- [x] **Phase 2: ...**
```

- **Verify by re-fetching the body**: `### Manual Testing` present once, phase count right, image-URL count == screenshot file count, and every uploaded filename appears in the body. Grep each filename; a missing one means a case lost its evidence — fix before finishing.
- **Clean up once that verification passes.** Undo any leftover demo-DB mutations first, using the checklist's "Demo-DB mutations" section, since that is the only record of them. Remove any temporary print statements. Stop the stack (`stack.sh stop <checkout>`). Leave the run directory: it is gitignored and goes with the checkout.

---

## Quirks crib sheet (read before driving anything)

**Calling the API**
- `POST /send/email/` takes the TutorCruncher payload (`EmailSendModel` in `app/messages/schemas.py`): `uid`, `company_code`, `method`, `from_address`, `main_template`, `subject_template`, `recipients`. A `uid` is single-use for 24 hours (Redis key `group:<uid>`, 409 on reuse): make a fresh one per send with `uuidgen`.
- `/send/sms/`, `/billing/...`, `/validate/sms/` and the subaccount endpoints need `Authorization: <AUTH_KEY>` (default `insecure`).
- `/webhook/mandrill/` checks an HMAC-SHA1 signature over `https://<HOST_NAME>/webhook/mandrill/mandrill_events<body>` with `MANDRILL_WEBHOOK_KEY`; build it in Python, see `app/messages/api/webhooks.py`. `/webhook/test/` needs no signature and updates `email-test` sends.
- Mandrill webhook events carry `_id` = the message's `external_id` (`<group uid>-<address with non-alphanumerics stripped>`); read it from the `messages` row rather than building it by hand.
- A send returns 201 before the worker sends anything. The row appears after the worker's `send_email` task logs success.
- Tracked links come from `context` values that look like URLs (`apply_short_links` in `app/render/main.py`), not from links written in `main_template`. A click case sends `"context": {"lesson_link": "https://..."}` and a template using `{{ lesson_link }}`; the `links` row then holds the token for `/l<token>`.
- A restarted worker takes back the tasks the old one held unacked, retries waiting on a countdown included, so a stack restart does not lose them.

**checkdb**
- `psql "$DATABASE_URL"` (from `.env` or `stack.sh status`). Tables: `companies`, `message_groups`, `messages`, `events`, `links`. The `update_message` trigger on `events` sets `messages.status` and `update_ts`, so a webhook's effect shows on `messages`, not only `events`.
- `message_aggregation` is a materialized view refreshed hourly by beat; a case on analytics runs `REFRESH MATERIALIZED VIEW message_aggregation` (log it as a mutation) or calls `update_aggregation_view` by hand.
- `redis-cli -u "$REDIS_URL"`: the Celery queue is a list named after the queue (`LLEN celery`). Read it with the worker stopped if the case is about what is queued: a running worker drains it before you look.
- Python checks: `uv run python -c "..."` with self-contained imports, with the stack's env exported. `app.core.config.settings` reads the same env.

**Environment**
- Server or worker not answering: `stack.sh status`, read the logs, `stack.sh start` again. A stack someone else started on the same port is never yours: `stack.sh` picks a free port every start.
- Several checkouts share this machine. Never curl `:8000` or another well-known port; use the URL `stack.sh` printed.

---

## Style rules for everything user-facing in this flow

- Checklist + PR desc: terse, short sentences, very simple language, no em-dashes, no jargon.
- Report failures as failures with the evidence; never soften. Log bugs with repro + severity and keep going.
- Run every planned case, capture a screenshot for each, and put every screenshot in the PR. Where a case cannot run, record it as NOT RUN with the reason so the reviewer sees the gap instead of a silent omission.
- **Existing test coverage is never a reason to drop a case.** "There is a test for that", "the test asserts the exact value" — a case with a green test still gets run and still gets a screenshot. A test proves the code behaves as its author expected, with Celery eager and no real worker; UAT proves it works on a real stack and hands the reviewer an image they can check without running anything. The two are not substitutes, so coverage never shortens the plan. The only NOT RUN reasons are a surface this diff does not have, or tooling that is genuinely unavailable.
