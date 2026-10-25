#!/usr/bin/env bash
# Prove every test named in the invariants tables of docs/ARCHITECTURE.md
# (and docs/architecture/*.md when present) exists: `cargo test ... <name>
# -- --list` shows one test whose path ends in the name (a lib test is
# listed with its module path, so `--exact` would miss it). A row whose
# test column is `(no test)` is counted and skipped. Exit 1 when a named
# test is missing or more than one test ends in the name.
#
#   tools/arch-doc-tests.sh            # every row
#   tools/arch-doc-tests.sh --dry-run  # parse only, no cargo
#
# A row's test column is `name` for a lib test, or `name` (`--test file`)
# for an integration test. The invariants tables are the ones whose header
# row is `| invariant | charter | test |`.
set -u
cd "$(dirname "$0")/.."
dry=0; [ "${1:-}" = "--dry-run" ] && dry=1
docs="docs/ARCHITECTURE.md"
[ -d docs/architecture ] && docs="$docs $(ls docs/architecture/*.md 2>/dev/null)"

rows=0; tested=0; untested=0; failed=0
while IFS= read -r line; do
  rows=$((rows + 1))
  cell="${line##*|}"                 # the text after the last pipe is empty
  cell="${line%|}"; cell="${cell##*|}"
  if printf '%s' "$cell" | grep -q '(no test)'; then
    untested=$((untested + 1)); continue
  fi
  name=$(printf '%s' "$cell" | grep -o '`[a-z0-9_]*`' | head -1 | tr -d '`')
  file=$(printf '%s' "$cell" | grep -o -- '--test [a-z0-9_]*' | head -1 | awk '{print $2}')
  if [ -z "$name" ]; then
    echo "NO NAME: $line"; failed=$((failed + 1)); continue
  fi
  tested=$((tested + 1))
  if [ -n "$file" ]; then cmd="cargo test --test $file $name -- --list"
  else cmd="cargo test --lib $name -- --list"; fi
  if [ "$dry" = 1 ]; then echo "$cmd"; continue; fi
  out=$($cmd 2>/dev/null | grep -E "(^|::)$name: test\$")
  n=$(printf '%s\n' "$out" | grep -c ': test$')
  if [ "$n" = 1 ]; then echo "ok   $cmd -> $out"
  else echo "FAIL $cmd -> $n matches"; failed=$((failed + 1)); fi
done < <(awk '
  /^\| invariant \| charter \| test \|/ { table = 1; next }
  table && /^\|---/ { next }
  table && /^\|/ { print; next }
  { table = 0 }
' $docs)

echo "rows: $rows  with a test: $tested  (no test): $untested  failed: $failed"
[ "$failed" = 0 ]
