#!/bin/bash
# The /pr-uat gate: every case has been run, and every planned screenshot exists in this run's directory.
#
# Usage: gate.sh <run dir>     (exit 0 only when nothing is missing)
#
# Case lines in checklist.md:
#   - [ ] 1.1 description -> p1-01-01-send.png, p1-01-02-row.png    planned, not run yet
#   - [x] 1.1 description -> p1-01-01-send.png | PASS: note         run, with its result
#   - [n] 1.2 description -> NOT RUN: reason                        could not be run (no image needed)
set -euo pipefail

RUN_DIR=${1:?usage: gate.sh <run dir>}
CHECKLIST="$RUN_DIR/checklist.md"
[ -f "$CHECKLIST" ] || { echo "no checklist at $CHECKLIST" >&2; exit 2; }

CASE_LINE='^- \[[ xn]\] [0-9]+\.[0-9]+ '
cases=$(grep -cE "$CASE_LINE" "$CHECKLIST" || true)
open=$(grep -cE '^- \[ \] [0-9]+\.[0-9]+ ' "$CHECKLIST" || true)
not_run=$(grep -cE '^- \[n\] [0-9]+\.[0-9]+ ' "$CHECKLIST" || true)
failed=$(grep -E '^- \[x\] [0-9]+\.[0-9]+ ' "$CHECKLIST" | grep -c '| FAIL' || true)
unmarked=$(grep -E '^- \[x\] [0-9]+\.[0-9]+ ' "$CHECKLIST" | grep -vcE '\| (PASS|FAIL)' || true)
images=$(find "$RUN_DIR/screenshots" -type f \( -name '*.jpeg' -o -name '*.jpg' -o -name '*.png' \) 2>/dev/null | wc -l | tr -d ' ')

missing=0
planned=0
while IFS= read -r name; do
  planned=$((planned + 1))
  if [ -z "$(find "$RUN_DIR/screenshots" -type f -name "$name" -size +0 2>/dev/null)" ]; then
    echo "MISSING: $name"
    missing=$((missing + 1))
  fi
done < <(grep -E '^- \[[ x]\] [0-9]+\.[0-9]+ ' "$CHECKLIST" | grep -oE '[A-Za-z0-9._-]+\.(png|jpe?g)' || true)

# A case line with no planned image is a claim with no evidence.
no_image=$(grep -E '^- \[[ x]\] [0-9]+\.[0-9]+ ' "$CHECKLIST" | grep -vcE '\.(png|jpe?g)' || true)

echo "cases=$cases not_run=$not_run failed=$failed planned_images=$planned images_on_disk=$images"
if [ "$cases" -eq 0 ] || [ "$open" -ne 0 ] || [ "$unmarked" -ne 0 ] || [ "$missing" -ne 0 ] || [ "$no_image" -ne 0 ]; then
  echo "GATE FAILED: cases=$cases open=$open no_result=$unmarked missing_images=$missing cases_without_image=$no_image"
  exit 1
fi
echo "GATE PASSED"
