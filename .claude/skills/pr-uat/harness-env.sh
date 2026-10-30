# Sourced by the /pr-uat harness scripts: this checkout's own harness directory. Each checkout gets
# its own directory (and setup-terminal.sh its own port, recorded in $TSH_DIR/port), so two sessions
# running /pr-uat at once never kill or overwrite each other's harness.
CHECKOUT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TSH_DIR="/tmp/tsh-$(basename "$CHECKOUT")-$(printf '%s' "$CHECKOUT" | cksum | awk '{print $1 % 100000}')"
