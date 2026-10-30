#!/bin/bash
# Upload screenshots to a repo branch (default: pr-screenshots) via the GitHub REST
# base_tree API. This NEVER does a git push — a raw git-object push with an empty
# COMMIT_SHA degrades to `push origin :ref` and DELETES the branch.
#
# Flow: create a blob per file -> create a tree with base_tree=<current tree> ->
# create a commit with the current head as parent -> PATCH the ref. Then byte-size
# verify each file (checked against the tree, which also works on a private repo).
#
# Usage:
#   upload-screenshots.sh <repo> <branch> <dir> <image or directory> [...]
# e.g.
#   upload-screenshots.sh tutorcruncher/morpheus pr-screenshots uat-568-2026-10-29 "$RUN_DIR/screenshots"
# A directory argument uploads every .jpeg/.jpg/.png under it, so no shell glob is needed (zsh aborts on a
# glob with no matches). Files land flat as "<dir>/<basename>" on <branch>.
set -euo pipefail

REPO=${1:?repo}; BRANCH=${2:?branch}; DIR=${3:?dir}; shift 3
FILES=()
for arg in "$@"; do
  if [ -d "$arg" ]; then
    while IFS= read -r found; do FILES+=("$found"); done \
      < <(find "$arg" -type f \( -name '*.jpeg' -o -name '*.jpg' -o -name '*.png' \) | sort)
  else
    FILES+=("$arg")
  fi
done
[ "${#FILES[@]}" -ge 1 ] || { echo "no image files given" >&2; exit 2; }
TMP=$(mktemp -d)   # private to this run, so concurrent uploads can't overwrite each other's payloads
trap 'rm -rf "$TMP"' EXIT

HEAD_SHA=$(gh api "repos/$REPO/git/refs/heads/$BRANCH" --jq '.object.sha')
BASE_TREE=$(gh api "repos/$REPO/git/commits/$HEAD_SHA" --jq '.tree.sha')
echo "head=$HEAD_SHA base_tree=$BASE_TREE"

entries='[]'
for f in "${FILES[@]}"; do
  name="${DIR}/$(basename "$f")"
  # Pipe the payload into jq — passing it as an argv string (--arg c "$b64") dies with
  # "Argument list too long" once a single argument exceeds 128KB (MAX_ARG_STRLEN),
  # which is any screenshot over ~96KB.
  # openssl rather than base64: GNU needs `-w0` to stop wrapping, BSD/macOS has no such flag and
  # rejects a positional input file outright, so the upload died before any blob was created.
  # `openssl base64 -A` is single-line on both.
  openssl base64 -A -in "$f" | jq -Rs '{content: rtrimstr("\n"), encoding:"base64"}' > "$TMP/blob.json"
  sha=$(gh api "repos/$REPO/git/blobs" --input "$TMP/blob.json" --jq '.sha')
  entries=$(echo "$entries" | jq --arg p "$name" --arg s "$sha" \
    '. + [{path:$p, mode:"100644", type:"blob", sha:$s}]')
  echo "  blob $name -> $sha"
done

jq -n --arg b "$BASE_TREE" --argjson t "$entries" '{base_tree:$b, tree:$t}' > "$TMP/tree.json"
NEW_TREE=$(gh api "repos/$REPO/git/trees" --input "$TMP/tree.json" --jq '.sha')
jq -n --arg m "Add UAT screenshots ($DIR)" --arg t "$NEW_TREE" --arg p "$HEAD_SHA" \
  '{message:$m, tree:$t, parents:[$p]}' > "$TMP/commit.json"
NEW_COMMIT=$(gh api "repos/$REPO/git/commits" --input "$TMP/commit.json" --jq '.sha')
jq -n --arg s "$NEW_COMMIT" '{sha:$s, force:false}' > "$TMP/ref.json"
gh api "repos/$REPO/git/refs/heads/$BRANCH" -X PATCH --input "$TMP/ref.json" \
  --jq '"ref now at "+.object.sha'

echo "=== byte-size verify (against the tree) ==="
for f in "${FILES[@]}"; do
  name="${DIR}/$(basename "$f")"
  remote=$(gh api "repos/$REPO/contents/$name?ref=$BRANCH" --jq '.size')
  local=$(stat -c%s "$f" 2>/dev/null || stat -f%z "$f")   # GNU || BSD
  if [ "$remote" = "$local" ]; then
    echo "  OK  $name ($local bytes)"
  else
    echo "  MISMATCH $name remote=$remote local=$local" >&2; exit 1
  fi
done
