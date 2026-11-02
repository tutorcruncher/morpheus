#!/bin/bash
# Start, stop or show the /pr-uat stack for a checkout: the web server, one Celery worker and a fake
# Mandrill / MessageBird, all on that checkout's code, with test-only keys.
#
# Usage:
#   stack.sh start <checkout> [--fresh]    (re)start; --fresh rebuilds the stack's own database and Redis db
#   stack.sh stop <checkout>
#   stack.sh status <checkout>
#
# The stack never reaches anything real, whatever .env holds:
#   - Every key is a test value and telemetry is off, so no real secret can appear in a screenshot.
#   - The provider URLs always point at fake_providers.py, so no message can leave the machine.
#   - A database or Redis on a remote host is refused before any DDL runs.
#
# Database and Redis: a local DATABASE_URL / REDIS_URL from the environment or .env wins. Otherwise the
# stack gets its own database (morpheus_uat_<n>) and Redis db number, so it never shares rows or tasks
# with another checkout. create_all never alters a table, so start a run with --fresh or a branch's new
# column will be missing. --fresh only ever touches the stack's own database and Redis db.
#
# Any other setting a case needs goes on the environment of `start`, e.g.
#   TEST_OUTPUT=/some/dir stack.sh start <checkout>
# Journeys `source "$STACK_DIR/env.sh"` (path printed by start and status) for the URL, keys and stores.
set -euo pipefail

ACTION=${1:?usage: stack.sh start|stop|status <checkout> [--fresh]}
CHECKOUT="$(cd "${2:?usage: stack.sh start|stop|status <checkout> [--fresh]}" && pwd)"
FRESH=${3:-}
HERE="$(cd "$(dirname "$0")" && pwd)"
n=$(printf '%s' "$CHECKOUT" | cksum | awk '{print $1 % 100000}')
STACK_DIR="/tmp/uat-stack-$(basename "$CHECKOUT")-$n"
mkdir -p "$STACK_DIR"
cd "$CHECKOUT"

stop() {
  for name in web worker providers; do
    if [ -f "$STACK_DIR/$name.pid" ]; then
      kill "$(cat "$STACK_DIR/$name.pid")" 2>/dev/null || true
      rm -f "$STACK_DIR/$name.pid"
    fi
  done
  # uvicorn and celery fork children; the parent pid alone can leave them on the port.
  local web="uvicorn app.main:app --port $(cat "$STACK_DIR/port" 2>/dev/null || echo none)"
  local worker="celery -A app.worker worker -n $(basename "$STACK_DIR")@"
  local providers="fake_providers.py --dir $STACK_DIR"
  pkill -f "$web" 2>/dev/null || true
  pkill -f "$worker" 2>/dev/null || true
  pkill -f "$providers" 2>/dev/null || true
  # Celery's warm shutdown finishes the running tasks first: wait, so a restart never runs two workers.
  for _ in $(seq 40); do
    pgrep -f "$web" >/dev/null || pgrep -f "$worker" >/dev/null || return 0
    sleep 0.5
  done
  pkill -9 -f "$web" 2>/dev/null || true
  pkill -9 -f "$worker" 2>/dev/null || true
}

status() {
  for name in web worker providers; do
    if [ -f "$STACK_DIR/$name.pid" ] && kill -0 "$(cat "$STACK_DIR/$name.pid")" 2>/dev/null; then
      echo "$name: running (pid $(cat "$STACK_DIR/$name.pid"), log $STACK_DIR/$name.log)"
    else
      echo "$name: stopped"
    fi
  done
  [ -f "$STACK_DIR/summary" ] && cat "$STACK_DIR/summary"
  return 0
}

free_port() {
  for candidate in $(seq "$1" "$2"); do
    if ! lsof -nP -iTCP:"$candidate" -sTCP:LISTEN -t >/dev/null 2>&1; then echo "$candidate"; return 0; fi
  done
  echo "no free port in $1-$2" >&2
  return 1
}

case "$ACTION" in
  stop) stop; echo "stopped"; exit 0 ;;
  status) status; exit 0 ;;
  start) ;;
  *) echo "unknown action: $ACTION" >&2; exit 2 ;;
esac
if [ -n "$FRESH" ] && [ "$FRESH" != "--fresh" ]; then echo "unknown option: $FRESH" >&2; exit 2; fi

stop

PG_ADMIN="postgresql://postgres@localhost:5432/postgres"
OWN_DB=''
if [ -z "${DATABASE_URL:-}" ] && ! grep -qE '^DATABASE_URL=' .env 2>/dev/null; then
  OWN_DB="morpheus_uat_$n"
  export DATABASE_URL="postgresql://postgres@localhost:5432/$OWN_DB"
fi
OWN_REDIS=''
if [ -z "${REDIS_URL:-}${REDISCLOUD_URL:-}" ] && ! grep -qE '^(REDIS_URL|REDISCLOUD_URL)=' .env 2>/dev/null; then
  # db 0 is the dev default every other checkout uses, and the test suite flushes db 4.
  redis_db=$((n % 15 + 1))
  [ "$redis_db" -eq 4 ] && redis_db=5
  OWN_REDIS="redis://localhost:6379/$redis_db"
  export REDIS_URL="$OWN_REDIS"
fi

PROVIDERS_PORT=$(free_port 8200 8299)
export AUTH_KEY=${AUTH_KEY:-uat-auth-key}
export USER_AUTH_KEY=${USER_AUTH_KEY:-uat-user-auth-key}
export WEBHOOK_AUTH_KEY=${WEBHOOK_AUTH_KEY:-uat-webhook-auth-key}
export MANDRILL_WEBHOOK_KEY=${MANDRILL_WEBHOOK_KEY:-uat-mandrill-webhook-key}
export HOST_NAME=${HOST_NAME:-localhost}
export TEST_OUTPUT=${TEST_OUTPUT:-$STACK_DIR/sent}
# Not overridable: these are what guarantee nothing real is called or reported to.
export MANDRILL_KEY=good-mandrill-testing-key
export MESSAGEBIRD_KEY=good-messagebird-testing-key
export MANDRILL_URL="http://127.0.0.1:$PROVIDERS_PORT/mandrill"
export MESSAGEBIRD_URL="http://127.0.0.1:$PROVIDERS_PORT/messagebird"
export SENTRY_DSN=''
export LOGFIRE_TOKEN=''
export DEV_MODE=true
# macOS only, and harmless elsewhere: a forked Celery child segfaults inside CoreFoundation, the known causes being libpq
# looking up Kerberos settings and httpx looking up system proxies. Production runs on Linux and never hits it.
export PGGSSENCMODE=disable
export NO_PROXY='*'

# Before anything writes: create_db_and_tables below runs DDL.
uv run python -c "
import sys
from urllib.parse import urlparse
from app.core.config import settings
local = {'localhost', '127.0.0.1', '::1'}
for name, url in (('database', settings.database_url), ('redis', settings.redis_url)):
    if urlparse(url).hostname not in local:
        sys.exit(f'refusing to start: the {name} is not on this machine; set a local URL for /pr-uat')
"

if [ -n "$OWN_DB" ]; then
  [ -n "$FRESH" ] && psql "$PG_ADMIN" -qc "DROP DATABASE IF EXISTS $OWN_DB WITH (FORCE)"
  psql "$PG_ADMIN" -tAc "SELECT 1 FROM pg_database WHERE datname = '$OWN_DB'" | grep -q 1 \
    || psql "$PG_ADMIN" -qc "CREATE DATABASE $OWN_DB"
elif [ -n "$FRESH" ]; then
  echo "--fresh: keeping the database from the environment or .env, it is not this stack's own" >&2
fi
if [ -n "$OWN_REDIS" ] && [ -n "$FRESH" ]; then redis-cli -u "$OWN_REDIS" FLUSHDB >/dev/null; fi
if [ -n "$FRESH" ]; then rm -rf "$STACK_DIR/sent" "$STACK_DIR/provider-requests.jsonl"; fi

# The app no longer builds its schema on boot (issue #511), so the stack does it. It is idempotent.
uv run python -c 'from app.core.database import create_db_and_tables; create_db_and_tables()'

PORT=$(free_port 8100 8199)
echo "$PORT" > "$STACK_DIR/port"

QUEUES=$(uv run python -c "
from app.core.celery import celery_app
print(','.join(sorted({'celery', *(r['queue'] for r in (celery_app.conf.task_routes or {}).values())})))")

# Output goes to a file: a backgrounded process whose stdout pipe closes dies on its next write.
nohup uv run python "$HERE/fake_providers.py" --dir "$STACK_DIR" --port "$PROVIDERS_PORT" --checkout "$CHECKOUT" \
  >"$STACK_DIR/providers.log" 2>&1 &
echo $! > "$STACK_DIR/providers.pid"
nohup uv run uvicorn app.main:app --port "$PORT" >"$STACK_DIR/web.log" 2>&1 &
echo $! > "$STACK_DIR/web.pid"
nohup uv run celery -A app.worker worker -n "$(basename "$STACK_DIR")@%h" -Q "$QUEUES" --concurrency 2 \
  --loglevel=info >"$STACK_DIR/worker.log" 2>&1 &
echo $! > "$STACK_DIR/worker.pid"
disown -a

DB_URL=$(uv run python -c "from app.core.config import settings; print(settings.database_url)")
RD_URL=$(uv run python -c "from app.core.config import settings; print(settings.redis_url)")
{
  printf 'export UAT_URL=%q\n' "http://127.0.0.1:$PORT"
  printf 'export STACK_DIR=%q\n' "$STACK_DIR"
  printf 'export DATABASE_URL=%q\n' "$DB_URL"
  printf 'export REDIS_URL=%q\n' "$RD_URL"
  printf 'export AUTH_KEY=%q\n' "$AUTH_KEY"
  printf 'export USER_AUTH_KEY=%q\n' "$USER_AUTH_KEY"
  printf 'export MANDRILL_WEBHOOK_KEY=%q\n' "$MANDRILL_WEBHOOK_KEY"
  printf 'export HOST_NAME=%q\n' "$HOST_NAME"
  printf 'export TEST_OUTPUT=%q\n' "$TEST_OUTPUT"
  printf 'export PAGER=cat\n'
  printf 'run() { printf %q "$*"; eval "$*"; }\n' '$ %s\n'
  # api <METHOD> <path> [curl args]: a request as TC2 makes it, with the status line after the body.
  cat <<'HELPER'
api() { local m=$1 p=$2; shift 2; curl -sS -w '\nHTTP %{http_code}\n' -X "$m" -H "Authorization: $AUTH_KEY" -H 'Content-Type: application/json' "$UAT_URL$p" "$@"; }
HELPER
} > "$STACK_DIR/env.sh"
{
  echo "server: http://127.0.0.1:$PORT/"
  echo "env: source $STACK_DIR/env.sh"
  echo "queues: $QUEUES"
  echo "provider requests: $STACK_DIR/provider-requests.jsonl"
  echo "rendered test sends: $TEST_OUTPUT"
} > "$STACK_DIR/summary"

for _ in $(seq 40); do
  if curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
    cat "$STACK_DIR/summary"
    exit 0
  fi
  sleep 0.5
done
echo "server FAILED to start: $STACK_DIR/web.log" >&2
tail -20 "$STACK_DIR/web.log" >&2
exit 1
