#!/bin/bash
# Start, stop or show the /pr-uat stack for a checkout: the web server and one Celery worker, both on
# that checkout's code, database and Redis.
#
# Usage:
#   stack.sh start <checkout>    (re)start; prints the server URL, the database and the Redis
#   stack.sh stop <checkout>
#   stack.sh status <checkout>
#
# Database and Redis: the checkout's .env wins when it sets DATABASE_URL and REDIS_URL (My Little
# Commander writes a private Postgres and Redis there). Otherwise the stack gets its own database
# (morpheus_uat_<n>) and Redis db number on the shared local servers, so it never reads or writes
# another checkout's rows, and no other checkout's worker takes its tasks.
#
# Any other setting a case needs goes on the environment of `start`, e.g.
#   MIRROR_DATABASE_URL=postgresql://postgres@localhost:5432/morpheus_mirror stack.sh start <checkout>
# The worker serves every queue in task_routes, so a task routed to its own queue still runs.
set -euo pipefail

ACTION=${1:?usage: stack.sh start|stop|status <checkout>}
CHECKOUT="$(cd "${2:?usage: stack.sh start|stop|status <checkout>}" && pwd)"
STACK_DIR="/tmp/uat-stack-$(basename "$CHECKOUT")-$(printf '%s' "$CHECKOUT" | cksum | awk '{print $1 % 100000}')"
mkdir -p "$STACK_DIR"
cd "$CHECKOUT"

stop() {
  for name in web worker; do
    if [ -f "$STACK_DIR/$name.pid" ]; then
      kill "$(cat "$STACK_DIR/$name.pid")" 2>/dev/null || true
      rm -f "$STACK_DIR/$name.pid"
    fi
  done
  # uvicorn and celery fork children; the parent pid alone can leave them on the port.
  local web="uvicorn app.main:app --port $(cat "$STACK_DIR/port" 2>/dev/null || echo none)"
  local worker="celery -A app.worker worker -n $(basename "$STACK_DIR")@"
  pkill -f "$web" 2>/dev/null || true
  pkill -f "$worker" 2>/dev/null || true
  # Celery's warm shutdown finishes the running tasks first: wait, so a restart never runs two workers.
  for _ in $(seq 40); do
    pgrep -f "$web" >/dev/null || pgrep -f "$worker" >/dev/null || return 0
    sleep 0.5
  done
  pkill -9 -f "$web" 2>/dev/null || true
  pkill -9 -f "$worker" 2>/dev/null || true
}

status() {
  for name in web worker; do
    if [ -f "$STACK_DIR/$name.pid" ] && kill -0 "$(cat "$STACK_DIR/$name.pid")" 2>/dev/null; then
      echo "$name: running (pid $(cat "$STACK_DIR/$name.pid"), log $STACK_DIR/$name.log)"
    else
      echo "$name: stopped"
    fi
  done
  [ -f "$STACK_DIR/env" ] && cat "$STACK_DIR/env"
  return 0
}

case "$ACTION" in
  stop) stop; echo "stopped"; exit 0 ;;
  status) status; exit 0 ;;
  start) ;;
  *) echo "unknown action: $ACTION" >&2; exit 2 ;;
esac

stop
n=$(printf '%s' "$CHECKOUT" | cksum | awk '{print $1 % 100000}')
if ! grep -qE '^DATABASE_URL=' .env 2>/dev/null; then
  export DATABASE_URL="postgresql://postgres@localhost:5432/morpheus_uat_$n"
  psql "postgresql://postgres@localhost:5432/postgres" -tAc "SELECT 1 FROM pg_database WHERE datname = 'morpheus_uat_$n'" \
    | grep -q 1 || psql "postgresql://postgres@localhost:5432/postgres" -qc "CREATE DATABASE morpheus_uat_$n"
fi
if ! grep -qE '^(REDIS_URL|REDISCLOUD_URL)=' .env 2>/dev/null; then
  # db 0 is the dev default every other checkout uses.
  export REDIS_URL="redis://localhost:6379/$((n % 15 + 1))"
fi

# The app no longer builds its schema on boot (issue #511), so the stack does it. It is idempotent.
uv run python -c 'from app.core.database import create_db_and_tables; create_db_and_tables()'

PORT=''
for candidate in $(seq 8100 8199); do
  if ! lsof -nP -iTCP:"$candidate" -sTCP:LISTEN -t >/dev/null 2>&1; then PORT=$candidate; break; fi
done
[ -n "$PORT" ] || { echo "no free port in 8100-8199" >&2; exit 1; }
echo "$PORT" > "$STACK_DIR/port"

QUEUES=$(uv run python -c "
from app.core.celery import celery_app
print(','.join(sorted({'celery', *(r['queue'] for r in (celery_app.conf.task_routes or {}).values())})))")

# Output goes to a file: a backgrounded process whose stdout pipe closes dies on its next write.
DEV_MODE=true nohup uv run uvicorn app.main:app --port "$PORT" >"$STACK_DIR/web.log" 2>&1 &
echo $! > "$STACK_DIR/web.pid"
nohup uv run celery -A app.worker worker -n "$(basename "$STACK_DIR")@%h" -Q "$QUEUES" --concurrency 2 \
  --loglevel=info >"$STACK_DIR/worker.log" 2>&1 &
echo $! > "$STACK_DIR/worker.pid"
disown -a

uv run python -c "
from app.core.config import settings
print(f'database: {settings.pg_host}:{settings.pg_port}/{settings.pg_name}')
print(f'redis: {settings.redis_url}')" > "$STACK_DIR/env"
echo "queues: $QUEUES" >> "$STACK_DIR/env"

for _ in $(seq 40); do
  if curl -fsS "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
    echo "server: http://127.0.0.1:$PORT/  (logs in $STACK_DIR)"
    cat "$STACK_DIR/env"
    exit 0
  fi
  sleep 0.5
done
echo "server FAILED to start: $STACK_DIR/web.log" >&2
tail -20 "$STACK_DIR/web.log" >&2
exit 1
