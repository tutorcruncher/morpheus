#!/bin/bash
# Build and (re)start this checkout's /pr-uat terminal harness, and print the URL to navigate to.
# Idempotent — re-run to restart. The directory and port are this checkout's own (harness-env.sh),
# so restarting never touches another session's harness.
#
# TSH_ROWS / TSH_COLS size the terminal (default 40x118, which fits the Playwright MCP's 1280x720
# viewport). Output taller than the terminal scrolls its first lines out: raise TSH_ROWS and take the
# screenshot with fullPage.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/harness-env.sh"
mkdir -p "$TSH_DIR"

# 1. venv with terminado + tornado (real PTY over websocket). uv rather than `python3 -m venv`:
#    Homebrew's python3 builds venvs without pip. Rebuilt whenever the imports fail.
if ! "$TSH_DIR/venv/bin/python" -c 'import terminado, tornado' 2>/dev/null; then
  echo "creating $TSH_DIR/venv ..."
  uv venv -q --clear "$TSH_DIR/venv"
  uv pip install -q --python "$TSH_DIR/venv/bin/python" terminado tornado
fi

# 2. vendor xterm.js / xterm.css from the jsDelivr CDN (Morpheus has no node_modules)
fetch() {  # <cdn-subpath> <out>
  curl -fsSL "https://cdn.jsdelivr.net/npm/xterm@5.3.0/$1" -o "$2"
}
[ -s "$TSH_DIR/xterm.js" ]  || fetch lib/xterm.js  "$TSH_DIR/xterm.js"
[ -s "$TSH_DIR/xterm.css" ] || fetch css/xterm.css "$TSH_DIR/xterm.css"

# 3. server + frontend + a ready placeholder case
cp "$HERE/termserver.py" "$TSH_DIR/termserver.py"
sed -e "s/__ROWS__/${TSH_ROWS:-40}/g" -e "s/__COLS__/${TSH_COLS:-118}/g" -e "s#__DIR__#$TSH_DIR#g" \
  "$HERE/index.html" > "$TSH_DIR/index.html"
cat > "$TSH_DIR/run_case.sh" <<'EOF'
#!/bin/bash
echo 'pr-uat terminal harness ready. Write a case with write-case.sh, then navigate here again.'
sleep 3600
EOF

# 4. (re)start this checkout's server on a free port. The pkill matches this checkout's directory only.
pkill -f "$TSH_DIR/termserver.py" 2>/dev/null || true
sleep 0.5
PORT=''
for candidate in $(seq 8765 8799); do
  if ! lsof -nP -iTCP:"$candidate" -sTCP:LISTEN -t >/dev/null 2>&1; then PORT=$candidate; break; fi
done
[ -n "$PORT" ] || { echo "no free port in 8765-8799"; exit 1; }
echo "$PORT" > "$TSH_DIR/port"
TSH_DIR="$TSH_DIR" TSH_PORT="$PORT" nohup "$TSH_DIR/venv/bin/python" "$TSH_DIR/termserver.py" >"$TSH_DIR/server.log" 2>&1 &
disown

# Ready means this checkout's page is served on the port, not just that something is listening there.
for _ in $(seq 20); do
  if curl -fsS "http://127.0.0.1:$PORT/" 2>/dev/null | grep -qF "content=\"$TSH_DIR\""; then
    echo "harness up: http://127.0.0.1:$PORT/  (dir $TSH_DIR)"
    exit 0
  fi
  sleep 0.25
done
echo "FAILED to start — $TSH_DIR/server.log:"; cat "$TSH_DIR/server.log"; exit 1
