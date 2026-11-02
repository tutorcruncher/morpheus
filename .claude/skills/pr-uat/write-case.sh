#!/bin/bash
# Write this checkout's harness run_case.sh from a journey supplied on stdin. The next browser
# navigation to the harness URL (printed below) spawns a fresh PTY that runs it.
#
# Usage:
#   write-case.sh <workdir> <title>  <<'JOURNEY'
#   printf '$ some command            # what it does\n'
#   some command 2>&1 | grep -v claude-code-hint
#   JOURNEY
#
# The script cds into <workdir>, activates its .venv, prints a "=== <title> ===" banner, runs the
# journey, prints "--- done ---", then `sleep 3600` to keep the PTY alive for the screenshot.
# Do not `2>/dev/null` a command: in the PTY its stdout vanishes with it. Filter
# with `2>&1 | grep -v claude-code-hint` (the shell profile prints that line into the PTY).
set -euo pipefail

WORKDIR=${1:?usage: write-case.sh <workdir> <title>  (journey on stdin)}
TITLE=${2:?usage: write-case.sh <workdir> <title>  (journey on stdin)}
source "$(dirname "$0")/harness-env.sh"
[ -f "$TSH_DIR/port" ] || { echo "no harness for this checkout — run setup-terminal.sh first" >&2; exit 1; }
BODY=$(cat)

# %q-quoted so any title or path is safe; syntax-checked before it replaces the live case.
{
  echo '#!/bin/bash'
  printf 'cd %q\n' "$WORKDIR"
  echo 'source .venv/bin/activate 2>/dev/null || true'
  printf 'echo %q\n' "=== $TITLE ==="
  echo 'echo'
  printf '%s\n' "$BODY"
  echo 'echo'
  echo "echo '--- done ---'"
  echo 'sleep 3600'
} > "$TSH_DIR/run_case.sh.new"
bash -n "$TSH_DIR/run_case.sh.new"
mv "$TSH_DIR/run_case.sh.new" "$TSH_DIR/run_case.sh"

echo "wrote case: $TITLE — navigate to http://127.0.0.1:$(cat "$TSH_DIR/port")/"
